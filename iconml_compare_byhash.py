#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import shutil
import hashlib
import zipfile
import subprocess
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Set

# ================== 外部依赖 ==================
# 1) aapt2 可执行文件路径（与脚本同目录或自行修改）
AAPT2_PATH = "./aapt2"
# 2) 下载客户端：./tool/linux_client --hash <h> --outdir ./download
CLIENT_PATH = Path("./tool/linux_client")
# 3) devid.py：必须提供 getsignsha1(apk_path) -> (devid, certsha1)
from devid import getsignsha1

# ================== 本地目录配置 ==================
DIR_REQUEST_BY_HASH       = Path("./requestbyhash")
DIR_REQUEST_BY_HASH_DONE  = Path("./requestbyhashdone")
DIR_REQUEST_OUT           = Path("./request")          # 生成的 request/<hash>.json
DIR_UPLOADIMAGES          = Path("./uploadimages")     # 提取出的 icon 命名为 sha256.ext
DIR_DOWNLOAD              = Path("./download")         # 临时下载包
# 结果观察目录（由你的其他服务生成）
DIR_INFORESULTS           = Path("./inforesults")
DIR_BAKINFORESULTS        = Path("./bakinforesults")
DIR_IMAGERESULTS          = Path("./imageresults")
DIR_BAKIMAGERESULTS       = Path("./bakimageresults")

# ================== 轮询与超时 ==================
POLL_INTERVAL      = 3.0    # 主循环轮询间隔
STABLE_WAIT        = 0.2    # txt 文件稳定判定
MAX_WAIT_SECONDS   = 1800   # 每批最多等待 30 分钟（根据需要调整）

# ================== 工具函数 ==================
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd: List[str]) -> str:
    """运行外部命令，返回 stdout（utf-8, errors=replace）"""
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       text=True, encoding="utf-8", errors="replace")
    return r.stdout

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
    调用下载器：保存至 outdir/{hash}.bin；若不存在则找 {hash}.zip 或 {hash}.*
    """
    ensure_dir(outdir)
    cmd = [str(client), "--hash", h, "--outdir", str(outdir)]
    print("[DL ]", " ".join(cmd))
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if r.returncode != 0:
        print(f"[ERR] download failed: hash={h}, rc={r.returncode}")
        if r.stdout: print("stdout:\n", r.stdout)
        if r.stderr: print("stderr:\n", r.stderr)
        raise RuntimeError(f"download failed: {h}")

    p_bin = outdir / f"{h}.bin"
    if p_bin.exists():
        return p_bin.resolve()

    p_zip = outdir / f"{h}.zip"
    if p_zip.exists():
        print(f"[DL ] {h}.bin not found, using: {p_zip.name}")
        return p_zip.resolve()

    cands = list(outdir.glob(f"{h}.*"))
    if cands:
        print(f"[DL ] using candidate: {cands[0].name}")
        return cands[0].resolve()

    raise FileNotFoundError(f"downloaded file not found for hash={h}")

def read_hashes_from_txt(txt: Path) -> List[str]:
    out: List[str] = []
    with txt.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out

def stable_new_txts(dirpath: Path, seen_mtime: Dict[Path, float]) -> List[Path]:
    """
    返回需要新处理的 txt 列表。安全修复点：
    - 对同名文件的“再次出现”与“覆盖写入”都能识别为新任务。
    判定规则：
      1) 先做 size 稳定性判定（等待 STABLE_WAIT），确保文件写入完成；
      2) 如果文件不在 seen_mtime 中 => 新；
      3) 如果文件在 seen_mtime 中，但 stat().st_mtime > seen_mtime[f] => 新。
    """
    ret: List[Path] = []
    for f in dirpath.glob("*.txt"):
        try:
            s1 = f.stat().st_size
            t1 = f.stat().st_mtime
            time.sleep(STABLE_WAIT)
            s2 = f.stat().st_size
            t2 = f.stat().st_mtime
        except FileNotFoundError:
            # 写入过程中被移动/删除，下一轮再说
            continue

        # 写入稳定且非空
        if s1 == s2 and s2 > 0:
            last = seen_mtime.get(f)
            if last is None or t2 > last:
                ret.append(f)
    return ret

def move_to_done(src: Path, dst_dir: Path):
    ensure_dir(dst_dir)
    dst = dst_dir / src.name
    if dst.exists():
        base, ext = os.path.splitext(src.name)
        dst = dst_dir / f"{base}_{int(time.time())}{ext}"
    shutil.move(str(src), str(dst))
    print(f"[MOVE] {src.name} -> {dst.name}")

def write_json(p: Path, obj: dict):
    ensure_dir(p.parent)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_text(p: Path, s: str):
    ensure_dir(p.parent)
    with open(p, "w", encoding="utf-8") as f:
        f.write(s)

def safe_remove(p: Path):
    try:
        os.remove(str(p))
    except Exception:
        pass

# ================== aapt2 解析与图标抽取 ==================
def parse_aapt(apk_path: str) -> dict:
    """aapt2 dump badging，抓取 package / label / icon / permissions"""
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
            info["icon"] = dpi_path  # 通常后出现的为更高 dpi
        elif line.startswith("icon="):
            try:
                dpi_path = line.split("icon='")[1].split("'")[0]
                if "." in dpi_path:
                    info["icon"] = dpi_path
            except Exception:
                pass
        elif line.startswith("uses-permission:"):
            perm = line.split(":", 1)[1].strip().strip("'")
            if perm:
                info["permissions"].append(perm)
    return info

def parse_iconfile_in_resource(entry_name: str, resources_output: str) -> List[List[str]]:
    """
    在 aapt2 dump resources 输出中，找到 entry 的 (file) 行，收集资源文件路径。
    返回 [[原行, 路径], ...]；仅收 .png/.webp/.jpg/.jpeg
    """
    resolved: List[List[str]] = []
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
                        lo = path.lower()
                        if any(ext in lo for ext in [".png", ".webp", ".jpg", ".jpeg"]):
                            resolved.append([line, path])
                    except Exception:
                        continue
            else:
                break
    return resolved

def resolve_icon_from_xml_with_aapt2(apk_path: str, xml_path_in_apk: str) -> List[List[str]]:
    """通过 aapt2 dump resources，将 XML icon 解析到实际文件资源路径列表。"""
    res_out = run_cmd([AAPT2_PATH, "dump", "resources", apk_path])

    entry_name = None
    current_block: List[str] = []
    inside = False

    for raw in res_out.splitlines():
        line = raw.strip()
        if line.startswith("resource"):
            if inside and entry_name and current_block:
                if any(xml_path_in_apk in r for r in current_block):
                    break  # 命中
                entry_name = None
                current_block = []
            entry_name = line.split()[-1]
            inside = True
            continue
        if inside:
            current_block.append(line)

    if not entry_name:
        return []

    paths = parse_iconfile_in_resource(entry_name, res_out)
    if not paths:
        # 兜底：常见的 mipmap/ic_launcher
        paths = parse_iconfile_in_resource("mipmap/ic_launcher", res_out)
    return paths

def pick_default_icon_path(items: List[List[str]]) -> Optional[str]:
    """优先返回包含 'mdpi' 的 path；否则第一项"""
    if not items:
        return None
    for line, p in items:
        if "mdpi" in line:
            return p
    return items[0][1]

def extract_file_from_apk(apk_path: str, inner_path: str, out_file: Path) -> bool:
    """从 APK(Zip) 里提取 inner_path 到 out_file。"""
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
    return h.hexdigest()

# ================== 单个 hash 处理 ==================
def process_one_hash(client: Path, h: str) -> Tuple[str, str, str, str]:
    """
    下载 -> aapt2 -> devid -> 解析并提取 icon(默认 mdpi 或首个) -> 生成 request/<hash>.json
    返回四元组：(hash, icon_filename, status, reason)
      - status ∈ {"success_ready", "info_only", "failed"}
      - reason：失败/降级原因描述（成功时空字符串）
    """
    pkg_file: Optional[Path] = None
    try:
        pkg_file = run_client_download(client, h, DIR_DOWNLOAD)
    except Exception as e:
        return (h, "", "failed", f"download_failed:{e}")

    apk_path = str(pkg_file)
    try:
        info = parse_aapt(apk_path)
    except Exception as e:
        safe_remove(pkg_file)
        return (h, "", "failed", f"aapt2_badging_failed:{e}")

    package = info.get("package", "") or ""
    label   = info.get("label", "") or ""
    iconref = info.get("icon", "") or ""
    perms   = info.get("permissions", []) or []
    perms_str = ";".join([p for p in perms if p])

    devid = ""
    try:
        devid, _ = getsignsha1(apk_path)
    except Exception as e:
        print(f"[WARN] getsignsha1 failed: {e}")

    # 解析图标真实路径
    icon_path_in_apk: Optional[str] = None
    if iconref.endswith(".xml"):
        items: List[List[str]] = []
        try:
            items = resolve_icon_from_xml_with_aapt2(apk_path, iconref)
        except Exception as e:
            print(f"[WARN] resolve icon xml failed: {e}")
        icon_path_in_apk = pick_default_icon_path(items)
    else:
        icon_path_in_apk = iconref if iconref else None

    icon_filename = ""
    if icon_path_in_apk:
        ext = os.path.splitext(icon_path_in_apk)[1].lower()
        if ext not in [".png", ".webp", ".jpg", ".jpeg"]:
            ext = ".png"
        tmp_out = DIR_DOWNLOAD / f"__icon_tmp_{h}{ext}"
        ok = extract_file_from_apk(apk_path, icon_path_in_apk, tmp_out)
        if ok:
            digest = sha256_of_file(tmp_out)
            # 统一扩展名（把 .jpeg 也归并成 .jpg）
            if ext == ".jpeg":
                ext = ".jpg"
            final_name = f"{digest}{ext}"
            final_path = DIR_UPLOADIMAGES / final_name
            ensure_dir(DIR_UPLOADIMAGES)
            if final_path.exists():
                os.remove(str(final_path))
            shutil.move(str(tmp_out), str(final_path))
            icon_filename = final_name
            print(f"[ICON] {h} -> {final_name}")
        else:
            print(f"[WARN] icon inner path not found: {icon_path_in_apk}")

    # 生成 request JSON（icon_filename 可能为空）
    req_obj = {
        "hash": h,
        "package": package,
        "label": label,
        "icon_filename": icon_filename,
        "devid": devid,
        "permissions": perms_str
    }
    req_path = DIR_REQUEST_OUT / f"{h}.json"
    write_json(req_path, req_obj)
    print(f"[REQ ] write -> {req_path.name}")

    # 清理下载件
    safe_remove(pkg_file)

    if icon_filename:
        return (h, icon_filename, "success_ready", "")
    else:
        return (h, "", "info_only", "no_icon_extracted")

# ================== 结果探测 ==================
def result_exists_for_hash(h: str) -> bool:
    """inforesults/<hash>.json 或 bakinforesults/<hash>.json 是否存在"""
    return (DIR_INFORESULTS / f"{h}.json").exists() or (DIR_BAKINFORESULTS / f"{h}.json").exists()

def result_exists_for_icon(icon_filename: str) -> bool:
    """
    imageresults 下的 JSON 命名为 <icon_hash>.json，
    其中 icon_filename = <icon_hash>.<ext>（如 .png/.webp/...）。
    """
    if not icon_filename:
        return False
    base, _ = os.path.splitext(icon_filename)  # base = icon_hash
    cand = DIR_IMAGERESULTS / f"{base}.json"
    bak  = DIR_BAKIMAGERESULTS / f"{base}.json"
    return cand.exists() or bak.exists()

# ================== 批次状态与监控 ==================
@dataclass
class BatchState:
    name: str                              # 原始 txt 文件名（含扩展名）
    path: Path                             # 原始 txt 路径（requestbyhash/ 下）
    start_ts: float
    all_hashes: List[str]
    # 分类
    success_ready: List[Tuple[str, str]] = field(default_factory=list)  # [(hash, icon_filename)]
    info_only: List[str] = field(default_factory=list)                  # [hash]
    failed: List[Tuple[str, str]] = field(default_factory=list)         # [(hash, reason)]
    # 监控表：仅可监控项
    remaining: Dict[str, str] = field(default_factory=dict)             # {hash: icon_filename}

active_batches: Dict[str, BatchState] = {}  # key = txt.stem

def build_summary_text(original_txt: str,
                       success_hashes: List[str],
                       pending_hashes: List[str],
                       info_only_hashes: List[str],
                       failed_hashes: List[Tuple[str, str]],
                       all_hashes: List[str],
                       success_ready_pairs: List[Tuple[str, str]]) -> str:
    lines: List[str] = []
    lines.append(f"SUMMARY FOR: {original_txt}")
    lines.append(f"TOTAL HASHES: {len(all_hashes)}")
    lines.append("")
    lines.append(f"[SUCCESS COMPLETED] ({len(success_hashes)})")
    for h in success_hashes:
        # 找到对应 icon
        icon = ""
        for hh, ic in success_ready_pairs:
            if hh == h:
                icon = ic
                break
        lines.append(f"  - {h}  icon={icon}")
    lines.append("")
    lines.append(f"[PENDING_OR_TIMEOUT] ({len(pending_hashes)})")
    for h in pending_hashes:
        # 也补 icon 以便排查
        icon = ""
        for hh, ic in success_ready_pairs:
            if hh == h:
                icon = ic
                break
        lines.append(f"  - {h}  icon={icon}")
    lines.append("")
    lines.append(f"[INFO_ONLY_NO_ICON] ({len(info_only_hashes)})")
    for h in info_only_hashes:
        lines.append(f"  - {h}")
    lines.append("")
    lines.append(f"[FAILED_EARLY] ({len(failed_hashes)})")
    for h, why in failed_hashes:
        lines.append(f"  - {h}  reason={why}")
    lines.append("")
    lines.append("NOTE:")
    lines.append("  SUCCESS COMPLETED: inforesults/<hash>.json & imageresults/<icon>.json are both present (or in bak*).")
    lines.append("  PENDING_OR_TIMEOUT: missing either inforesult or imageresult when batch finalized.")
    lines.append("  INFO_ONLY_NO_ICON: request json generated but icon could not be extracted, so no image result expected.")
    lines.append("  FAILED_EARLY: download or parsing failed; no request was monitored.")
    return "\n".join(lines)

def process_txt_start(txt: Path, client: Path):
    name = txt.stem
    hashes = read_hashes_from_txt(txt)
    bs = BatchState(
        name=txt.name,
        path=txt,
        start_ts=time.time(),
        all_hashes=hashes
    )

    for h in hashes:
        try:
            hash_, icon_filename, status, reason = process_one_hash(client, h)
            if status == "success_ready" and icon_filename:
                bs.success_ready.append((hash_, icon_filename))
            elif status == "info_only":
                bs.info_only.append(hash_)
            else:
                bs.failed.append((hash_, reason))
        except Exception as e:
            bs.failed.append((h, f"exception:{e}"))

    bs.remaining = {h: icon for (h, icon) in bs.success_ready}
    active_batches[name] = bs
    print(f"[ENQUEUE] batch={name}: watch={len(bs.remaining)} info_only={len(bs.info_only)} failed={len(bs.failed)}")

def tick_monitor():
    if not active_batches:
        return
    now = time.time()
    done_names: List[str] = []

    for name, bs in list(active_batches.items()):
        # 更新可监控剩余项
        finished_now: List[str] = []
        for h, icon in list(bs.remaining.items()):
            has_info  = result_exists_for_hash(h)
            has_image = result_exists_for_icon(icon)
            if not (has_info and has_image):
                print(f"[WAIT] {bs.name} hash={h} info={has_info} image={has_image} icon={icon}")
            if has_info and has_image:
                finished_now.append(h)
        for h in finished_now:
            bs.remaining.pop(h, None)

        # 完成或超时则收尾
        timeout = (now - bs.start_ts) > MAX_WAIT_SECONDS
        if not bs.remaining or timeout:
            completed = [h for (h, _) in bs.success_ready if h not in bs.remaining]
            pending   = list(bs.remaining.keys())

            summary = build_summary_text(
                original_txt=bs.name,
                success_hashes=completed,
                pending_hashes=pending,
                info_only_hashes=bs.info_only,
                failed_hashes=bs.failed,
                all_hashes=bs.all_hashes,
                success_ready_pairs=bs.success_ready
            )
            out_txt = DIR_REQUEST_BY_HASH_DONE / bs.name
            write_text(out_txt, summary)
            print(f"[OUT ] done -> {out_txt.name}")

            # 2) 避免目录内重名：既然已在 done 目录生成了同名汇总，就删除源 txt
            safe_remove(bs.path)

            done_names.append(name)

    for nm in done_names:
        active_batches.pop(nm, None)

# ================== 主循环 ==================
def main():
    print("=== Enhanced Non-blocking Request-By-Hash Watcher (bug-fixed) ===")
    for d in [
        DIR_REQUEST_BY_HASH, DIR_REQUEST_BY_HASH_DONE,
        DIR_REQUEST_OUT, DIR_UPLOADIMAGES, DIR_DOWNLOAD,
        DIR_INFORESULTS, DIR_BAKINFORESULTS, DIR_IMAGERESULTS, DIR_BAKIMAGERESULTS
    ]:
        ensure_dir(d)

    client = ensure_client()
    print(f"[INIT] client: {client}")

    # 用 mtime 追踪：修复“同名文件再次出现不再处理”的问题
    seen_mtime: Dict[Path, float] = {}
    print(f"[WATCH] {DIR_REQUEST_BY_HASH.resolve()} (poll={POLL_INTERVAL}s)")

    while True:
        try:
            # —— 清理 seen_mtime 中已不存在的文件（被移动到 done 后）——
            seen_mtime = {p: t for p, t in seen_mtime.items() if p.exists()}

            # 发现新 txt：启动批次（不阻塞）
            new_txts = stable_new_txts(DIR_REQUEST_BY_HASH, seen_mtime)
            if new_txts:
                print(f"[DETECT] new/updated txt: {[p.name for p in new_txts]}")

            for txt in new_txts:
                # 记录当前 mtime（避免重复触发）
                try:
                    seen_mtime[txt] = txt.stat().st_mtime
                except FileNotFoundError:
                    # 被瞬时移动，下一轮再处理
                    continue

                try:
                    process_txt_start(txt, client)
                except Exception as e:
                    # 出错也给出最小化 summary，并把原始 txt 移到 done
                    print(f"[FATAL] start batch failed: {txt.name}: {e}")
                    summary = build_summary_text(
                        original_txt=txt.name,
                        success_hashes=[],
                        pending_hashes=[],
                        info_only_hashes=[],
                        failed_hashes=[("BATCH_EXCEPTION", str(e))],
                        all_hashes=read_hashes_from_txt(txt) if txt.exists() else [],
                        success_ready_pairs=[]
                    )
                    write_text(DIR_REQUEST_BY_HASH_DONE / f"{txt.stem}_summary.txt", summary)
                    if txt.exists():
                        move_to_done(txt, DIR_REQUEST_BY_HASH_DONE)

            # 统一监控所有活跃批次
            tick_monitor()

            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\n[EXIT] bye")
            break
        except Exception as e:
            print(f"[LOOP ERR] {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
