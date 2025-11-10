import argparse
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Set
import shutil

# your interface
from extracttool import extract_apk_info

DEFAULT_OUTDIR = Path("./download")
CLIENT_PATH = Path("./tool/linux_client")


def ensure_client() -> Path:
    client = CLIENT_PATH.resolve()
    if not client.exists():
        print(f"Error: client not found: {client}")
        sys.exit(3)
    if not os.access(client, os.X_OK):
        print(f"Error: client is not executable: {client}")
        print(f"Hint: run: chmod +x {client}")
        sys.exit(3)
    return client


def run_client(client: Path, h: str, outdir: Path) -> Path:
    """
    Run ./tool/linux_client to download into ./download/{hash}.bin.
    If {hash}.bin does not exist, try {hash}.zip or {hash}.*
    (We still pass the resulting file path to extract_apk_info without unzip.)
    """
    outdir.mkdir(parents=True, exist_ok=True)
    cmd = [str(client), "--hash", h, "--outdir", str(outdir)]
    print("Running:", " ".join(cmd), flush=True)
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        print(f"Download failed: hash={h}, rc={res.returncode}")
        if res.stdout:
            print("stdout:\n", res.stdout)
        if res.stderr:
            print("stderr:\n", res.stderr)
        raise RuntimeError(f"download failed: {h}")

    # prefer {hash}.bin
    bin_path = outdir / f"{h}.bin"
    if bin_path.exists():
        return bin_path.resolve()

    # fallback candidates (no unzip; just pass through)
    alt_zip = outdir / f"{h}.zip"
    if alt_zip.exists():
        print(f"Note: expected {h}.bin not found, using: {alt_zip}")
        return alt_zip.resolve()

    candidates = list(outdir.glob(f"{h}.*"))
    if candidates:
        print(f"Note: using candidate: {candidates[0]}")
        return candidates[0].resolve()

    raise FileNotFoundError(f"downloaded file not found: {bin_path}")


def read_hashes(txt: Path):
    with txt.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            yield s


def process_one(client: Path, h: str, outdir: Path):
    print(f"Processing hash={h}")
    pkg_path = run_client(client, h, outdir)
    print(f"Downloaded file: {pkg_path}")

    # directly pass the file path to extract_apk_info
    info=""
    #try:
    info = extract_apk_info(str(pkg_path))
    #except:
       # print("fail extract "+str(pkg_path))
    print(f"[{h}] extract_apk_info result:\n{info}\n", flush=True)
    os.system("rm "+str(pkg_path))
    try:
        os.remove(str(pkg_path))
    except:
        print("fail rm "+str(pkg_path))


# ========== 配置部分 ==========
ADDSAMPLES_DIR = Path("./addsamples")
PROCESSED_DIR = Path("./addsampleprocessed")
DEFAULT_OUTDIR = Path(DEFAULT_OUTDIR)
POLL_INTERVAL = 3.0  # 每 3 秒扫描一次新 txt 文件


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def stable_new_files(dirpath: Path, known: Set[Path]) -> list[Path]:
    """
    返回新增且“稳定”的 txt 文件（大小不再变化）。
    """
    result = []
    for f in dirpath.glob("*.txt"):
        if f in known:
            continue
        try:
            s1 = f.stat().st_size
            time.sleep(0.2)
            s2 = f.stat().st_size
            if s1 == s2 and s1 > 0:
                result.append(f)
        except FileNotFoundError:
            pass
    return result


def move_to_processed(src: Path, dst_dir: Path = PROCESSED_DIR):
    ensure_dir(dst_dir)
    dst = dst_dir / src.name
    if dst.exists():
        base, ext = os.path.splitext(src.name)
        dst = dst_dir / f"{base}_{int(time.time())}{ext}"
    shutil.move(str(src), str(dst))
    print(f"[MOVE] {src.name} -> {dst}")


def process_txt_file(client, txt_path: Path, outdir: Path):
    """
    读取 txt 中每个 hash，调用 process_one。
    """
    print(f"[PROCESS] Start: {txt_path.name}")
    hashes = read_hashes(txt_path)
    if not hashes:
        print(f"[WARN] Empty or invalid file: {txt_path}")
        return

    for h in hashes:
        try:
            process_one(client, h, outdir)
        except Exception as e:
            print(f"[ERROR] Failed hash={h}: {e}")
            print("Continuing...", flush=True)

    print(f"[DONE] Finished processing {len(hashes)} hashes from {txt_path.name}")


def main():
    print("=== IconML AddSamples Watcher ===")
    ensure_dir(ADDSAMPLES_DIR)
    ensure_dir(PROCESSED_DIR)
    ensure_dir(DEFAULT_OUTDIR)

    client = ensure_client()
    print(f"[INIT] Using client: {client}")

    seen: Set[Path] = set()

    print(f"[WATCH] Monitoring directory: {ADDSAMPLES_DIR.resolve()}")
    while True:
        try:
            new_files = stable_new_files(ADDSAMPLES_DIR, seen)
            if new_files:
                print(f"[DETECT] Found {len(new_files)} new txt file(s): {[f.name for f in new_files]}")

            for txt in new_files:
                seen.add(txt)
                try:
                    process_txt_file(client, txt, DEFAULT_OUTDIR)
                    move_to_processed(txt, PROCESSED_DIR)
                except Exception as e:
                    print(f"[FATAL] Error processing {txt.name}: {e}")
                    # 默认 continue-on-error
                    move_to_processed(txt, PROCESSED_DIR)

            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\n[EXIT] KeyboardInterrupt. Exiting watcher.")
            break
        except Exception as e:
            print(f"[ERROR] Loop error: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()