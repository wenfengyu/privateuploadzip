import os
import subprocess
import zipfile
import tempfile
import shutil
import sys
import struct
import zlib
import hashlib
from devid import getsignsha1

def calc_combined_png_md5(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()

    assert data[:8] == b'\x89PNG\r\n\x1a\n'
    offset = 8
    idat_data_all = b''

    while offset < len(data):
        length = struct.unpack(">I", data[offset:offset+4])[0]
        chunk_type = data[offset+4:offset+8]
        chunk_data = data[offset+8:offset+8+length]
        offset += 12 + length

        if chunk_type == b'IDAT':
            idat_data_all += chunk_data

    if idat_data_all:
        full_data = b'IDAT' + idat_data_all  # 加上块类型名
        md5 = hashlib.md5(full_data).hexdigest()
        print(f"[PNG] Combined IDAT MD5 = {md5}")
    else:
        print("No IDAT chunk found.")
    return md5

def calc_combined_webp_md5(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()

    if data[:4] != b'RIFF' or data[8:12] != b'WEBP':
        raise ValueError("Not a valid WebP file")

    offset = 12
    combined_data = b''
    chunk_type_for_md5 = None

    while offset < len(data):
        chunk_type = data[offset:offset+4]
        chunk_size = struct.unpack('<I', data[offset+4:offset+8])[0]
        chunk_data = data[offset+8:offset+8+chunk_size]

        if chunk_type in (b'VP8 ', b'VP8L', b'VP8X'):
            if chunk_type_for_md5 is None:
                chunk_type_for_md5 = chunk_type  # use the first one
            combined_data += chunk_data

        offset += 8 + chunk_size
        if chunk_size % 2 == 1:
            offset += 1  # padding

    if combined_data and chunk_type_for_md5:
        full_data = chunk_type_for_md5 + combined_data
        md5 = hashlib.md5(full_data).hexdigest()
        print(f"[WebP] Combined {chunk_type_for_md5.decode().strip()} MD5 = {md5}")
    else:
        print("No VP8*/VP8L/VP8X chunks found.")
    return md5


def calc_jpg_crc(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()

    # JPEG 文件必须以 0xFFD8 开始
    assert data[:2] == b'\xFF\xD8', "Not a valid JPEG file."

    offset = 2
    sos_start = None
    eoi_pos = None

    # 查找 SOS (0xFFDA) 与 EOI (0xFFD9)
    while offset < len(data) - 1:
        if data[offset] != 0xFF:
            offset += 1
            continue

        marker = data[offset:offset+2]
        offset += 2

        # EOI 无后续长度字段
        if marker == b'\xFF\xD9':
            eoi_pos = offset - 2
            break

        # SOS 后面数据长度字段长度为2字节
        if marker == b'\xFF\xDA':
            length = struct.unpack(">H", data[offset:offset+2])[0]
            sos_start = offset + 2 + length  # SOS 数据起点
            offset = sos_start
            continue

        # 其他段：跳过数据
        length = struct.unpack(">H", data[offset:offset+2])[0]
        offset += length

    if sos_start and eoi_pos:
        image_data = data[sos_start:eoi_pos]
        crc = zlib.crc32(b'IMG')
        crc = zlib.crc32(image_data, crc) & 0xffffffff
        print(f"[JPG] Image Data CRC = 0x{crc:08X}")
        return crc
    else:
        print("Failed to locate SOS or EOI segment.")
        return None


def calc_png_crc(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()

    assert data[:8] == b'\x89PNG\r\n\x1a\n'
    offset = 8
    idat_data_all = b''

    while offset < len(data):
        length = struct.unpack(">I", data[offset:offset+4])[0]
        chunk_type = data[offset+4:offset+8]
        chunk_data = data[offset+8:offset+8+length]
        offset += 12 + length

        if chunk_type == b'IDAT':
            idat_data_all += chunk_data

    if idat_data_all:
        crc = zlib.crc32(b'IDAT')
        crc = zlib.crc32(idat_data_all, crc) & 0xffffffff
        print(f"[PNG] Combined IDAT CRC = 0x{crc:08X}")
        return crc
    else:
        print("No IDAT chunk found.")
        return None

def calc_webp_crc(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()

    if data[:4] != b'RIFF' or data[8:12] != b'WEBP':
        raise ValueError("Not a valid WebP file")

    offset = 12
    combined_data = b''
    chunk_type_for_crc = None

    while offset < len(data):
        chunk_type = data[offset:offset+4]
        chunk_size = struct.unpack('<I', data[offset+4:offset+8])[0]
        chunk_data = data[offset+8:offset+8+chunk_size]

        if chunk_type in (b'VP8 ', b'VP8L', b'VP8X'):
            if chunk_type_for_crc is None:
                chunk_type_for_crc = chunk_type  # use the first one
            combined_data += chunk_data

        offset += 8 + chunk_size
        if chunk_size % 2 == 1:
            offset += 1  # padding

    if combined_data and chunk_type_for_crc:
        crc = zlib.crc32(chunk_type_for_crc)
        crc = zlib.crc32(combined_data, crc) & 0xffffffff
        print(f"[WebP] Combined {chunk_type_for_crc.decode().strip()} CRC = 0x{crc:08X}")
        return crc
    else:
        print("No VP8*/VP8L/VP8X chunks found.")
        return None

def run_cmd(cmd):
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",     # 强制 utf-8 解码
        errors="replace",     # 遇到非法字符用 � 替换，防止崩溃
        text=True
    )
    return result.stdout

def parse_aapt(apk_path):
    output = run_cmd(['./aapt2', 'dump', 'badging', apk_path])
    info = {
        'package': '',
        'label': '',
        'icon': '',
        'permissions': []
    }
    for line in output.splitlines():
        if line.startswith("package:"):
            parts = line.split()
            for part in parts:
                if part.startswith("name="):
                    info['package'] = part.split('=')[1].strip("'")
        elif line.startswith("application-label:"):
            info['label'] = line.split(':', 1)[1].strip().strip("'")
        elif line.startswith("application-icon-"):
            dpi_path = line.split(':', 1)[1].strip().strip("'")
            info['icon'] = dpi_path  # 只保留最后一个（最大 dpi）
        elif line.startswith("icon="):
            dpi_path = line.split("icon='")[1].split("'")[0]
            if dpi_path.find(".")>1:
                info['icon'] = dpi_path
        elif line.startswith("uses-permission:"):
            perm = line.split(":", 1)[1].strip().strip("'")
            info['permissions'].append(perm)
    return info

def parse_iconfile_in_resource(entry_name, resources_output):
    resolved_paths=[]
    capture = False
    for line in resources_output.splitlines():
        line = line.strip()
        if line.startswith("resource") and entry_name in line:
            capture = True
            continue
        if capture:
            if line.startswith("("):
                if "(file)" in line:
                    try:
                        path = line.split("(file)")[1].strip().split()[0]
                        if path.find(".png")>0 or path.find(".webp")>0 or path.find(".jpg")>0:
                            item=[]
                            item.append(line)
                            item.append(path)
                            resolved_paths.append(item)
                    except:
                        continue
            else:
                break
    return resolved_paths

# 🔽 新增：aapt2 解析 XML 寻找真正的 drawable 路径
def resolve_icon_from_xml_with_aapt2(apk_path, xml_path_in_apk):
    resources_output = run_cmd(['./aapt2', 'dump', 'resources', apk_path])
    
    entry_name = None
    current_entry = []
    inside_block = False

    # Step 1: 找到 xml_path 对应的 entry 名字（如 mipmap/ic_launcher）
    for line in resources_output.splitlines():
        line = line.strip()

        if line.startswith("resource"):
            if inside_block and entry_name and current_entry:
                # 分析当前 block 是否包含目标 xml_path
                if any(xml_path_in_apk in l for l in current_entry):
                    break
                current_entry = []
                entry_name = None

            entry_name = line.split()[-1]
            inside_block = True
            continue

        if inside_block:
            current_entry.append(line)

    # Step 2: 再次扫描，收集该 entry_name 对应的所有分辨率图标
    if not entry_name:
        print("[-] Failed to locate resource entry for:", xml_path_in_apk)
        return []

    print(f"[+] Matched resource entry: {entry_name}")

    resolved_paths=parse_iconfile_in_resource(entry_name, resources_output)
    if len(resolved_paths)<1:
        resolved_paths=parse_iconfile_in_resource("mipmap/ic_launcher", resources_output)

    print(f"[+] Resolved icon paths: {resolved_paths}")
    return resolved_paths

def extract_icon(apk_path, icon_path_list, output_basename="resolved_icon", onlyextractdefault=False):
    outlist=[]
    found = False
    with zipfile.ZipFile(apk_path, 'r') as zf:
        count = 1
        default=""
        path_list=[]
        i=0
        for item in icon_path_list:
            path=item[1]
            line=item[0]
            if i==0:
                default=path
            i=i+1
            if line.find("mdpi")>-1:
                default=path
                break
        if onlyextractdefault==True:
            path_list.append(default)
        else:
            for item in icon_path_list:
                path=item[1]
                path_list.append(path)
        for path in path_list:
            if path in zf.namelist():
                ext = os.path.splitext(path)[1] or ".png"
                if path.find(".webp")>0:
                    ext = ext.replace(".png",".webp")
                if path.find(".jpg")>0:
                    ext = ext.replace(".png",".jpg")
                output_name = f"{output_basename}_{count}{ext}"
                with open(output_name, 'wb') as f:
                    f.write(zf.read(path))
                f.close()
                crc=""
                if output_name.find(".png")>0:
                    crc=calc_png_crc(output_name)
                elif output_name.find(".jpg")>0:
                    crc=calc_jpg_crc(output_name)
                else:
                    crc=calc_webp_crc(output_name)
                print(f"CRC:{crc:08X}")
                print(f"[+] Icon extracted to: {output_name}")
                tmplist=[]
                tmplist.append(output_name.replace("./images/",""))
                tmplist.append(crc)
                outlist.append(tmplist)
                count += 1
                found = True
    if not found:
        print("[-] No icons found in APK."+apk_path)
    return outlist

def extract_cert_thumbprint(apk_path):
    with zipfile.ZipFile(apk_path, 'r') as zf:
        cert_files = [f for f in zf.namelist() if f.startswith('META-INF/') and (f.endswith('.RSA') or f.endswith('.DSA'))]
        if not cert_files:
            print("[-] No certificate file found.")
            return None

        cert_file = cert_files[0]
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, os.path.basename(cert_file))
            with open(cert_path, 'wb') as f:
                f.write(zf.read(cert_file))

            pem = run_cmd(['openssl', 'pkcs7', '-inform', 'DER', '-in', cert_path, '-print_certs'])
            pem_path = os.path.join(tmpdir, 'cert.pem')
            with open(pem_path, 'w') as pf:
                pf.write(pem)

            thumb = run_cmd(['openssl', 'x509', '-in', pem_path, '-noout', '-fingerprint', '-sha1'])
            return thumb.strip()

def extract_apk_info(apk_path):
    print(f"[+] Analyzing APK: {apk_path}")
    info = parse_aapt(apk_path)

    print("[+] Package:", info['package'])
    print("[+] Label:", info['label'])
    print("[+] Icon Path:", info['icon'])
    print("[+] Permissions:")
    for p in info['permissions']:
        print("   -", p)

    icon_output = "resolved_icon.png"
    icon_path_list = []

    if info['icon'].endswith(".xml"):
        print("[*] Icon is an XML file, resolving real drawable...")
        icon_path_list = resolve_icon_from_xml_with_aapt2(apk_path, info['icon'])
        if not icon_path_list:
            print("[-] Failed to resolve XML drawable.")
    else:
        item=[]
        item.append(" (mdpi)")
        item.append(info['icon'])
        icon_path_list.append(item)

    crclist=extract_icon(apk_path, icon_path_list, "./images/"+info['package'], True)
    for item in crclist:
        print(item[0])
        print(f"{item[1]:08X}")
    devid, certsha1=getsignsha1(apk_path)
    print(devid)

    thumbprint=None
    try:
        thumbprint = extract_cert_thumbprint(apk_path)
    except:
        pass
    if thumbprint:
        print("[+] Certificate Thumbprint:", thumbprint)
    
    fw=open("./info/"+info['package']+".txt","w")
    fw.write(f"[+] Analyzing APK: {apk_path}\n")
    fw.write("package: "+info['package']+"\n")
    fw.write("label: "+info['label']+"\n\n")
    fw.write("Permissions: "+"\n")
    for p in info['permissions']:
        fw.write("    "+p+"\n")
    fw.write("\n")
    if thumbprint:
        fw.write("[+] Certificate Thumbprint:"+thumbprint+"\n")
    fw.write("devid: "+devid+"\n")
    fw.write("\nIcon hash:\n")
    for item in crclist:
        fw.write("    "+item[0]+"="+f"{item[1]:08X}"+"\n")
    fw.close()


def main(apk_path):
    extract_apk_info(apk_path)

# 调用主函数
if __name__ == "__main__":
    apk_path = sys.argv[1]  # 替换为你的 APK 路径
    main(apk_path)
