import os
import sys
import time
import shutil
import hashlib
import zipfile
import struct
import subprocess
from pathlib import Path
from typing import List, Set, Optional

# ==== 依赖：aapt2 + devid ====
# ./aapt2 需在同目录下或按需修改 AAPT2_PATH
AAPT2_PATH = "./aapt2"

# devid.py 需提供 getsignsha1(apk_path) -> (devid, certsha1)
from devid import getsignsha1

# ==== 下载客户端 ====
CLIENT_PATH = Path("./tool/linux_client")

# ==== 目录配置 ====
DIR_REQUEST_BY_HASH     = Path("./requestbyhash")
DIR_REQUEST_BY_HASH_DONE= Path("./requestbyhashdone")
DIR_REQUEST_OUT         = Path("./request")
DIR_UPLOADIMAGES        = Path("./uploadimages")
DIR_DOWNLOAD            = Path("./download")

POLL_INTERVAL = 3.0  # 轮询 requestbyhash/ 的间隔（秒）
STABLE_WAIT   = 0.2  # 本地文件大小稳定检查

# ----------------- 实用函数 -----------------
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd: List[str]) -> str:
    """运行命令，返回 stdout（utf-8, replace 错误字符）"""
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, encoding="utf-8", errors="replace")
    return res.stdout

def ensure_client() -> Path:
    client = CLIENT_PATH.resolve()
    if not client.exists():
        print(f"[ERR] download client not found: {client}")
        sys.exit(3)
    if not os.access(client, os.X_OK):
        print(f"[ERR] client is not executable: {client}\n  try: chmod +x {client}")
        sys.exit(3)
    return client

def run_client_download(client: Path, h: str, outdir: Path) -> Path:
    """
    调用下载器：保存至 outdir/{hash}.bin，若不存在则找 {hash}.zip 或 {hash}.*
    """
    ensure_dir(outdir)
    cmd = [str(client), "--hash", h, "--outdir", str(outdir)]
    print("[DL ]", " ".join(cmd))
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        print(f"[ERR] download failed: hash={h}, rc={res.returncode}")
        if res.stdout: print("stdout:\n", res.stdout)
        if res.stderr: print("stderr:\n", res.stderr)
        raise RuntimeError(f"download failed: {h}")

    p_bin = outdir / f"{h}.bin"
    if p_bin.exists():
        return p_bin.resolve()

    p_zip = outdir / f"{h}.zip"
    if p_zip.exists():
        print(f"[DL ] {h}.bin not found, using: {p_zip.name}")
        return p_zip.resolve()

    candidates = list(outdir.glob(f"{h}.*"))
    if candidates:
        print(f"[DL ] use candidate: {candidates[0].name}")
        return candidates[0].resolve()

    raise FileNotFoundError(f"downloaded file not found for hash={h}")

def read_hashes_from_txt(txt: Path) -> List[str]:
    hs = []
    with txt.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            hs.append(s)
    return hs

def stable_new_txts(dirpath: Path, known: Set[Path]) -> List[Path]:
    out = []
    for f in dirpath.glob("*.txt"):
        if f in known:
            continue
        try:
            s1 = f.stat().st_size
            time.sleep(STABLE_WAIT)
            s2 = f.stat().st_size
            if s1 == s2 and s1 > 0:
                out.append(f)
        except FileNotFoundError:
            pass
    return out

def move_to_done(src: Path, dst_dir: Path):
    ensure_dir(dst_dir)
    dst = dst_dir / src.name
    if dst.exists():
        base, ext = os.path.splitext(src.name)
        dst = dst_dir / f"{base}_{int(time.time())}{ext}"
    shutil.move(str(src), str(dst))
    print(f"[MV ] {src.name} -> {dst}")

# ----------------- aapt2 解析 -----------------
def parse_aapt(apk_path: str) -> dict:
    """
    aapt2 dump badging，抓取 package / label / icon / permissions
    """
    out = run_cmd([AAPT2_PATH, "dump", "badging", apk_path])
    info = {"package": "", "label": "", "icon": "", "permissions": []}
    for line in out.splitlines():
        if line.startswith("package:"):
            parts = line.split()
            for part in parts:
                if part.startswith("name="):
                    info["package"] = part.split("=", 1)[1].strip("'")
        elif line.startswith("application-label:"):
            info["label"] = line.split(":", 1)[1].strip().strip("'")
        elif line.startswith("application-icon-"):
            dpi_path = line.split(":", 1)[1].strip().strip("'")
            info["icon"] = dpi_path  # 最后一个通常是更高 dpi
        elif line.startswith("icon="):
            try:
                dpi_path = line.split("icon='")[1].split("'")[0]
                if "." in dpi_path:
                    info["icon"] = dpi_path
            except Exception:
                pass
        elif line.startswith("uses-permission:"):
            perm = line.split(":", 1)[1].strip().strip("'")
            info["permissions"].append(perm)
    return info

def parse_iconfile_in_resource(entry_name: str, resources_output: str) -> List[List[str]]:
    """
    在 aapt2 dump resources 输出中，找到 entry 的 (file) 行，收集具体资源文件路径。
    返回 [[原行, 路径], ...]，并只收 .png/.webp/.jpg
    """
    resolved = []
    capture = False
    for raw in resources_output.splitlines():
        line = raw.strip()
        if line.startswith("resource") and entry_name in line:
            capture = True
            continue
        if capture:
            if line.startswith("("):
                if "(file)" in line:
                    try:
                        path = line.split("(file)")[1].strip().split()[0]
                        if (".png" in path) or (".webp" in path) or (".jpg" in path) or (".jpeg" in path):
                            resolved.append([line, path])
                    except Exception:
                        continue
            else:
                break
    return resolved

def resolve_icon_from_xml_with_aapt2(apk_path: str, xml_path_in_apk: str) -> List[List[str]]:
    """
    通过 aapt2 dump resources，将 XML icon 解析到实际的文件资源路径列表。
    优先返回包含 mdpi 的项作为“默认”。
    """
    res_out = run_cmd([AAPT2_PATH, "dump", "resources", apk_path])
    entry_name = None
    current_block = []
    inside = False

    # 第一次扫描：找到包含该 xml 的 resource block 名称
    for raw in res_out.splitlines():
        line = raw.strip()
        if line.startswith("resource"):
            # 进入新 block，先判断旧 block 是否包含目标
            if inside and entry_name and current_block:
                if any(xml_path_in_apk in r for r in current_block):
                    break
                # 重置
                entry_name = None
                current_block = []
            entry_name = line.split()[-1]
            inside = True
            continue
        if inside:
            current_block.append(line)

    if not entry_name:
        print("[-] resolve: failed to locate resource entry for", xml_path_in_apk)
        return []

    paths = parse_iconfile_in_resource(entry_name, res_out)
    if not paths:
        # 兜底：常见的 mipmap/ic_launcher
        paths = parse_iconfile_in_resource("mipmap/ic_launcher", res_out)
    return paths

# ----------------- 图标提取 + 命名 -----------------
def pick_default_icon_path(items: List[List[str]]) -> Optional[str]:
    """
    传入 [[line, path], ...]，优先返回包含 'mdpi' 的 path；若无，返回第一项的 path。
    """
    if not items:
        return None
    # 优先 mdpi
    for line, p in items:
        if "mdpi" in line:
            return p
    return items[0][1]

def extract_file_from_apk(apk_path: str, inner_path: str, out_file: Path) -> bool:
    """
    从 APK (zip) 中提取 inner_path 到 out_file。
    """
    try:
        with zipfile.ZipFile(apk_path, "r") as zf:
            if inner_path not in zf.namelist():
                return False
            data = zf.read(inner_path)
        ensure_dir(out_file.parent)
        with open(out_file, "wb") as f:
            f.write(data)
        return True
    except Exception as e:
        print(f"[ERR] extract {inner_path} from {apk_path}: {e}")
        return False

def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()  # 小写 hex

# ----------------- 单个 hash 的处理 -----------------
def process_one_hash(client: Path, h: str) -> Optional[dict]:
    """
    下载 -> 解析 aapt2 -> 拿 devid -> 解析图标 -> 提取默认图标 -> 以 sha256 命名放入 uploadimages -> 返回 request JSON dict
    返回的 dict 形如：
    {
      "hash": "...",
      "package": "...",
      "label": "...",
      "icon_filename": "<sha256>.<ext>",
      "devid": "...",
      "permissions": "perm1;perm2;perm3"
    }
    若失败，返回 None（继续下一个 hash）
    """
    try:
        pkg_file = run_client_download(client, h, DIR_DOWNLOAD)
    except Exception as e:
        print(f"[ERR] hash={h} download: {e}")
        return None

    apk_path = str(pkg_file)
    try:
        info = parse_aapt(apk_path)
    except Exception as e:
        print(f"[ERR] aapt2 badging failed: {apk_path}: {e}")
        # 清理下载文件
        safe_remove(pkg_file)
        return None

    package = info.get("package", "") or ""
    label   = info.get("label", "") or ""
    iconref = info.get("icon", "") or ""
    perms   = info.get("permissions", []) or []
    perms_str = ";".join([p for p in perms if p])

    # devid
    devid = ""
    try:
        devid, _ = getsignsha1(apk_path)
    except Exception as e:
        print(f"[WARN] getsignsha1 failed: {e}")

    # 解析图标真实路径
    icon_path_in_apk = None
    if iconref.endswith(".xml"):
        items = []
        try:
            items = resolve_icon_from_xml_with_aapt2(apk_path, iconref)
        except Exception as e:
            print(f"[WARN] resolve icon xml failed: {e}")
        picked = pick_default_icon_path(items)
        icon_path_in_apk = picked
    else:
        icon_path_in_apk = iconref

    icon_filename = ""  # 最终写入 request JSON 的名字
    if icon_path_in_apk:
        # 提取到临时文件，计算 sha256，移动并命名
        ext = os.path.splitext(icon_path_in_apk)[1].lower()
        if ext not in [".png", ".webp", ".jpg", ".jpeg"]:
            # 不识别的扩展名默认 .png
            ext = ".png"

        tmp_out = DIR_DOWNLOAD / f"__icon_tmp_{h}{ext}"
        ok = extract_file_from_apk(apk_path, icon_path_in_apk, tmp_out)
        if not ok:
            print(f"[WARN] icon inner path not found: {icon_path_in_apk}")
        else:
            digest = sha256_of_file(tmp_out)  # 小写 hex
            final_name = f"{digest}{ext}"
            final_path = DIR_UPLOADIMAGES / final_name
            ensure_dir(DIR_UPLOADIMAGES)
            # 覆盖或跳过：这里选择幂等覆盖（若同内容相同哈希则一样）
            shutil.move(str(tmp_out), str(final_path))
            icon_filename = final_name
            print(f"[ICON] saved -> {final_path}")

    # 生成 request JSON 结构
    req_obj = {
        "hash": h,
        "package": package,
        "label": label,
        "icon_filename": icon_filename,
        "devid": devid,
        "permissions": perms_str
    }

    # 写到 ./request/<hash>.json
    ensure_dir(DIR_REQUEST_OUT)
    req_path = DIR_REQUEST_OUT / f"{h}.json"
    write_json(req_path, req_obj)
    print(f"[OUT] request json -> {req_path}")

    # 清理下载文件
    safe_remove(pkg_file)

    return req_obj

def write_json(p: Path, obj: dict):
    import json
    ensure_dir(p.parent)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def safe_remove(p: Path):
    try:
        os.remove(str(p))
    except Exception:
        pass

# ----------------- 处理一个 txt -----------------
def process_txt(txt: Path, client: Path):
    print(f"[PROC] {txt.name}")
    hashes = read_hashes_from_txt(txt)
    if not hashes:
        print(f"[WARN] empty txt: {txt}")
        return

    for h in hashes:
        try:
            process_one_hash(client, h)
        except Exception as e:
            # 默认 continue-on-error
            print(f"[ERR] process hash={h}: {e}")

# ----------------- 主循环 -----------------
def main():
    print("=== Request-By-Hash Watcher ===")
    for d in [DIR_REQUEST_BY_HASH, DIR_REQUEST_BY_HASH_DONE, DIR_REQUEST_OUT, DIR_UPLOADIMAGES, DIR_DOWNLOAD]:
        ensure_dir(d)

    client = ensure_client()
    print(f"[INIT] using client: {client}")

    seen = set()  # type: Set[Path]
    print(f"[WATCH] {DIR_REQUEST_BY_HASH.resolve()}")

    while True:
        try:
            new_txts = stable_new_txts(DIR_REQUEST_BY_HASH, seen)
            if new_txts:
                print(f"[DETECT] {len(new_txts)} new txt: {[p.name for p in new_txts]}")

            for txt in new_txts:
                seen.add(txt)
                try:
                    process_txt(txt, client)
                finally:
                    # 不论成功失败，移动到 done 目录（避免卡死/阻塞）
                    move_to_done(txt, DIR_REQUEST_BY_HASH_DONE)

            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\n[EXIT] bye")
            break
        except Exception as e:
            print(f"[LOOP ERR] {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
