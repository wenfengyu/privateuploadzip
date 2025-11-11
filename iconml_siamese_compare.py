import os
import time
import shutil
import random
import numpy as np
import tensorflow as tf
import json  # NEW
import re
from typing import Optional, Pattern



# ====== 你现有项目里的函数/模块 ======
# 假定存在以下接口：LoadImage.e_load_image(fn) -> np.ndarray[h,w,3] (float或uint8均可)
# 假定存在：build_siamese_model(input_shape) -> Keras model，输入为 [img1, img2]，输出为相似度（0~1）
from LoadImageLevelSiamese import e_load_image as load_img
from siamese import build_siamese_model  # 如果你的函数在别处，请改成正确的导入

IMAGE_SIZE = 32  # 固定输入尺寸；你的 e_load_image 内部也会用到这个
BASE_DIR = "./images"
UPLOAD_DIR = "./uploadimages"
DONE_DIR = "./doneimages"
WEIGHTS_PATH = "./model/icon/traindata1"
INFO_DIR = "./info"

BATCH_SIZE = 512           # 可按显存调大/调小
SIM_THRESHOLD = 0.996        # 打印相似的阈值
POLL_INTERVAL = 1.0        # 轮询间隔秒
RESULTS_DIR = "./imageresults"  # NEW
IMAGE_EXTS = {".png", ".jpg", ".webp", ".PNG", ".JPG", ".WEBP"}

def _safe_read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""

def _extract_first(regex: Pattern, text: str) -> str:
    if not text:
        return ""
    m = regex.search(text)
    return m.group(1).strip() if m else ""

def _parse_permissions(text: str) -> str:
    """
    解析 Permissions 区块里形如:
        name='android.permission.XYZ
    的行；容忍缺右引号/单双引号/空格等格式问题。
    返回分号拼接的字符串（若没有则为空字符串）。
    """
    if not text:
        return ""
    p_idx = text.lower().find("permissions")
    if p_idx < 0:
        return ""
    window = text[p_idx:p_idx+3000]  # 防止扫太大
    perms = []
    for line in window.splitlines():
        if "name" in line.lower():
            m = re.search(r"name\s*=\s*['\"]?([A-Za-z0-9._]+)['\"]?", line)
            if m:
                perms.append(m.group(1).strip())
    # 去重保序
    seen, out = set(), []
    for p in perms:
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return ";".join(out)

def _parse_icon_hash_for_icon(text: str, package: str, icon_filename: str) -> str:
    """
    在 'Icon hash:' 区段中，查找与 icon_filename 完全同名的行 (<name>=<HEX>) 的 HEX。
    若找不到，返回空字符串。
    """
    if not text:
        return ""
    idx = text.lower().find("icon hash")
    if idx < 0:
        return ""
    window = text[idx:idx+5000]
    target = os.path.basename(icon_filename).strip().lower()
    for line in window.splitlines():
        if "=" not in line:
            continue
        left, right = line.split("=", 1)
        if left.strip().lower() == target:
            m = re.match(r"([0-9A-Fa-f]+)", right.strip())
            return (m.group(1).upper() if m else right.strip())
    return ""

def _parse_apk_hash(text: str) -> str:
    """
    从行：
      [+] Analyzing APK: /path/.../<HEX>.bin
    中抓取 <HEX>（去掉 .bin）。若不是纯 HEX 也返回主体。
    """
    if not text:
        return ""
    m = re.search(r"(?i)\[\+\]\s*Analyzing\s+APK:\s*(\S+)", text)
    if not m:
        return ""
    p = m.group(1).strip()
    base = os.path.basename(p)
    name, _ = os.path.splitext(base)
    if re.fullmatch(r"[0-9a-fA-F]{32,128}", name):
        return name.lower()
    return name

def parse_info_file(package: str, icon_filename: str) -> dict:
    """
    从 ./info/<package>.txt 尽力解析：
      - packagename（来自 'package:' 行）
      - devid（来自 'devid:' 行）
      - label（来自 'label:' 行）
      - permissions（来自 'Permissions' 区块）
      - hash（来自 '[+] Analyzing APK:' 行中的 .bin 文件名）
    任何项缺失则返回空字符串 ""。
    """
    path = os.path.join(INFO_DIR, f"{package}.txt")
    txt = _safe_read_text(path)
    if not txt:
        return {"packagename": "", "devid": "", "label": "", "permissions": "", "hash": ""}

    re_pkg   = re.compile(r"(?i)\bpackage\s*:\s*['\"]?([A-Za-z0-9._]+)['\"]?")
    re_devid = re.compile(r"(?i)\bdevid\s*:\s*([0-9a-fA-F]+)")
    re_label = re.compile(r"(?i)\blabel\s*:\s*['\"]?(.+?)\s*$", re.MULTILINE)

    packagename = _extract_first(re_pkg, txt)
    devid       = _extract_first(re_devid, txt)
    label       = _extract_first(re_label, txt)
    permissions = _parse_permissions(txt)
    apk_hash    = _parse_apk_hash(txt)  # 你要求的 hash 来源

    # 如需图标哈希可启用（当前不并入输出，仅示例）：
    # icon_hash   = _parse_icon_hash_for_icon(txt, package, icon_filename)

    return {
        "packagename": packagename or "",
        "devid": devid or "",
        "label": label or "",
        "permissions": permissions or "",
        "hash": apk_hash or "",
    }


def ensure_results_dir():
    ensure_dir(RESULTS_DIR)

def save_results_json(upload_path: str,
                      base_paths: list,
                      scores: np.ndarray,
                      threshold: float,
                      pairs_sorted: list):
    """
    保存 JSON 结果：
      - 若有匹配：仅输出最高分 best_match，并从 ./info/<package>.txt 解析字段并合并
      - 若无匹配：best_match = None（也落盘）
      - 文件：./imageresults/<upload_basename>.json
    """
    ensure_results_dir()
    up_base = os.path.basename(upload_path)
    name_no_ext, _ = os.path.splitext(up_base)
    out_path = os.path.join(RESULTS_DIR, f"{name_no_ext}.json")

    best_match = None
    if pairs_sorted:
        # 已按分数降序
        i, s = pairs_sorted[0]
        base_name = os.path.basename(base_paths[i])
        pkg_name = base_name.split("_")[0]

        # 解析 ./info/<package>.txt —— APK 哈希来自 "[+] Analyzing APK" 行
        icon_filename = os.path.basename(upload_path)
        info_fields = parse_info_file(pkg_name, icon_filename)

        best_match = {
            "package": pkg_name,
            "score": float(s),
            "packagename": info_fields.get("packagename", ""),
            "devid": info_fields.get("devid", ""),
            "label": info_fields.get("label", ""),
            "permissions": info_fields.get("permissions", ""),
            "hash": info_fields.get("hash", "")
        }

    payload = {
        "upload_filename": up_base,
        "total_candidates": int(len(base_paths)),
        "best_match": best_match,
        "timestamp": int(time.time())
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[RESULT] JSON saved -> {out_path}")

# ========= GPU 设置 =========
def setup_gpu():
    try:
        gpus = tf.config.list_physical_devices('GPU')
        if gpus:
            for g in gpus:
                tf.config.experimental.set_memory_growth(g, True)
        print(f"[GPU] Found {len(gpus)} GPU(s). Memory growth enabled.")
    except Exception as e:
        print(f"[GPU] Setup warning: {e}")


# ========= 文件/IO 工具 =========
def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def is_image_file(path: str) -> bool:
    _, ext = os.path.splitext(path)
    return ext in IMAGE_EXTS

def list_all_images(dir_path: str):
    files = []
    for root, _, fs in os.walk(dir_path):
        for f in fs:
            fp = os.path.join(root, f)
            if is_image_file(fp):
                files.append(fp)
    return files

def stable_new_files(upload_dir: str, known_set: set):
    """
    扫描 upload_dir，返回尚未处理过的“稳定”新文件列表。
    通过短暂等待确认文件大小未变化，避免读到半写入的文件。
    """
    cand = []
    for root, _, fs in os.walk(upload_dir):
        for f in fs:
            fp = os.path.join(root, f)
            if not is_image_file(fp):
                continue
            if fp in known_set:
                continue
            cand.append(fp)

    stable = []
    for p in cand:
        try:
            s1 = os.path.getsize(p)
            time.sleep(0.2)
            s2 = os.path.getsize(p)
            if s1 == s2 and s1 > 0:
                stable.append(p)
        except FileNotFoundError:
            pass
    return stable

def move_to_done(src_path: str, done_dir: str = DONE_DIR):
    ensure_dir(done_dir)
    base = os.path.basename(src_path)
    dst = os.path.join(done_dir, base)
    if os.path.exists(dst):
        name, ext = os.path.splitext(base)
        ts = int(time.time())
        dst = os.path.join(done_dir, f"{name}_{ts}{ext}")
    shutil.move(src_path, dst)
    return dst


# ========= 预加载与模型 =========
def preload_base_images(base_dir: str):
    """
    预加载 ./images/ 下所有图片为 (N, 32, 32, 3) 的 float32 数组。
    与 e_load_image 对齐：其返回 (1,32,32,3) 的 tf.float32，这里取 [0] 去掉 batch 维。
    """
    paths = list_all_images(base_dir)
    ok_paths = []
    imgs = []

    print(f"[LOAD] Start preloading {len(paths)} images from '{base_dir}' ...")
    for i, p in enumerate(paths, 1):
        try:
            t = load_img(p)        # tf.Tensor, (1,32,32,3), float32, [0,1]
            arr = t.numpy()[0]     # (32,32,3)
            imgs.append(arr)
            ok_paths.append(p)
        except Exception as e:
            print(f"[WARN] Failed to load '{p}': {e}")

        if i % 500 == 0:
            print(f"[LOAD] Preloaded {i}/{len(paths)}")

    if not ok_paths:
        print("[LOAD] Done. Valid images: 0")
        return [], np.empty((0,), dtype=np.float32)

    base_imgs = np.stack(imgs, axis=0).astype(np.float32)  # (N,32,32,3)
    print(f"[LOAD] Done. Valid images: {len(ok_paths)}  Shape={base_imgs.shape}")
    return ok_paths, base_imgs


def build_and_load_model(example_image_shape):
    """
    根据样例图像 shape 构建 Siamese 模型并加载权重。
    example_image_shape 应为 (H,W,C) = (32,32,3)。
    """
    if len(example_image_shape) != 3:
        raise ValueError(f"Unexpected example_image_shape: {example_image_shape}")
    h, w, c = example_image_shape

    print(f"[MODEL] Building Siamese model with input shape: ({h},{w},{c})")
    model = build_siamese_model((h, w, c))
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=1e-4),
        loss='binary_crossentropy',
        metrics=['binary_accuracy']
    )
    print(f"[MODEL] Loading weights from: {WEIGHTS_PATH}")
    model.load_weights(WEIGHTS_PATH).expect_partial()

    # warmup
    dummy1 = np.zeros((1, h, w, c), dtype=np.float32)
    dummy2 = np.zeros((1, h, w, c), dtype=np.float32)
    _ = model([dummy1, dummy2], training=False)
    print("[MODEL] Warmup done.")
    return model


# ========= 推理与输出 =========
def predict_similarity_pairs(model, upload_img: np.ndarray, base_imgs: np.ndarray, batch_size: int = BATCH_SIZE):
    """
    将单张 upload_img (H,W,C) 与 base_imgs (N,H,W,C) 进行全量比对。
    分批将 [upload_img × bsz] 与 base_imgs[start:end] 喂入模型。
    返回 scores: (N,)  相似度(0~1)
    """
    N = base_imgs.shape[0]
    scores = np.empty((N,), dtype=np.float32)

    start = 0
    while start < N:
        end = min(start + batch_size, N)
        bsz = end - start

        left = np.repeat(upload_img[np.newaxis, ...], bsz, axis=0)   # (bsz,H,W,C)
        right = base_imgs[start:end]                                 # (bsz,H,W,C)

        t0 = time.time()
        out = model([left, right], training=False)
        out = np.array(out).reshape(-1)  # 兼容 Tensor/ndarray
        t1 = time.time()

        scores[start:end] = out
        print(f"[INFER] Compared {start}–{end} / {N} in {t1 - t0:.4f}s")
        start = end

    return scores


def print_matches(upload_path: str, base_paths: list, scores: np.ndarray,
                  threshold: float = SIM_THRESHOLD, max_show: Optional[int] = None):
    """
    打印匹配结果，并将全部命中(>threshold)按分数降序写入 ./imageresults/<same_name>.json
    - 生成的 JSON 中的 matched 列表包含所有命中(>threshold)的条目（不受 max_show 限制）
    - 控制台打印可用 max_show 截断展示
    """
    idx = np.where(scores > threshold)[0]

    # 构建“全部命中”的有序列表（用于 JSON）
    if idx.size == 0:
        all_pairs_sorted = []  # for JSON
        print(f"[RESULT] {os.path.basename(upload_path)}: No matches > {threshold}")
    else:
        all_pairs_sorted = [(int(i), float(scores[i])) for i in idx]
        all_pairs_sorted.sort(key=lambda x: x[1], reverse=True)

    # --- 保存 JSON（包括空命中时也会写出空列表） ---
    save_results_json(upload_path, base_paths, scores, threshold, all_pairs_sorted)

    # --- 控制台打印（可选择性截断） ---
    if not all_pairs_sorted:
        return

    pairs_to_show = all_pairs_sorted
    if max_show is not None:
        pairs_to_show = pairs_to_show[:max_show]

    print(f"[RESULT] {os.path.basename(upload_path)} matches (>{threshold}):")
    for i, s in pairs_to_show:
        print(f"  - {base_paths[i]}   score={s:.4f}")


# ========= 主循环 =========
def main():
    setup_gpu()

    ensure_dir(BASE_DIR)
    ensure_dir(UPLOAD_DIR)
    ensure_dir(DONE_DIR)

    # 1) 预加载基准库
    base_paths, base_imgs = preload_base_images(BASE_DIR)
    if base_imgs.shape[0] == 0:
        print("[FATAL] No valid images found in ./images . Exit.")
        return

    # 2) 构建并加载模型（输入为 (32,32,3)）
    model = build_and_load_model(base_imgs[0].shape)

    # 3) 循环监控 ./uploadimages
    seen = set()
    print(f"[WATCH] Start watching '{UPLOAD_DIR}' ... (poll interval {POLL_INTERVAL}s)")
    while True:
        try:
            new_files = stable_new_files(UPLOAD_DIR, seen)
            if new_files:
                print(f"[WATCH] Detected {len(new_files)} new file(s).")

            for up in new_files:
                seen.add(up)
                # 加载上传图：e_load_image -> (1,32,32,3) tf.float32
                try:
                    t = load_img(up)
                    uimg = t.numpy()[0]  # (32,32,3), float32, [0,1]
                except Exception as e:
                    print(f"[WARN] Failed to load upload '{up}': {e}")
                    dst = move_to_done(up)
                    print(f"[DONE] Moved '{up}' -> '{dst}'")
                    seen.discard(up)            # <<< 关键：处理完移出 seen
                    continue

                # 成功处理完
                # ...
                dst = move_to_done(up)
                print(f"[DONE] Moved '{up}' -> '{dst}'")
                seen.discard(up) 

                # 推理对比（分批）
                print(f"[INFER] Start comparing: {os.path.basename(up)}  vs  {len(base_paths)} base images ...")
                scores = predict_similarity_pairs(model, uimg, base_imgs, batch_size=BATCH_SIZE)

                # 打印匹配项
                print_matches(up, base_paths, scores, threshold=SIM_THRESHOLD, max_show=None)
                ensure_results_dir()
                # 移动到 done
                dst = move_to_done(up)
                print(f"[DONE] Moved '{up}' -> '{dst}'")

            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\n[EXIT] KeyboardInterrupt received. Bye.")
            break
        except Exception as e:
            print(f"[ERROR] Loop error: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()