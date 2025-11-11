#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import shutil
import logging
from typing import Dict, Optional, Set
import boto3
from botocore.exceptions import ClientError

# ============ 配置 ============
BUCKET = "mr-dev-iconml-request"

S3_PREFIX_REQUEST              = "iconml/request/"
S3_PREFIX_IMAGES               = "iconml/images/"
S3_PREFIX_PROCESSING           = "iconml/processing/"
S3_PREFIX_PROCESSED            = "iconml/processed/"
S3_PREFIX_RESULTS              = "iconml/inforesults/"
S3_PREFIX_IMAGERESULTS         = "iconml/imageresults/"
S3_PREFIX_ADDSAMPLES           = "iconml/addsamples/"
S3_PREFIX_ADDSAMPLEPROCESSED   = "iconml/addsampleprocessed/"
S3_PREFIX_REQUESTBYHASH        = "iconml/requestbyhash/"
S3_PREFIX_REQUESTBYHASHDONE    = "iconml/requestbyhashdone/"

LOCAL_DIR_REQUEST              = "request"
LOCAL_DIR_UPLOADIMAGES         = "uploadimages"
LOCAL_DIR_RESULTS              = "inforesults"
LOCAL_DIR_IMAGERESULTS         = "imageresults"
LOCAL_DIR_BAKRESULTS           = "bakresults"
LOCAL_DIR_BAKIMAGERESULTS      = "bakimageresults"
LOCAL_DIR_ADDSAMPLES           = "addsamples"
LOCAL_DIR_ADDSAMPLEPROCESSED   = "addsampleprocessed"
LOCAL_DIR_REQUESTBYHASH        = "requestbyhash"
LOCAL_DIR_REQUESTBYHASHDONE    = "requestbyhashdone"
LOCAL_DIR_BAKREQUESTBYHASHDONE = "bakrequestbyhashdone"

POLL_S3_INTERVAL    = 2.0
POLL_LOCAL_INTERVAL = 1.0
STABLE_WAIT_SEC     = 0.2

LOG_LEVEL = logging.INFO

# ============ 初始化 ============
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
s3 = boto3.client("s3")

for d in [
    LOCAL_DIR_REQUEST, LOCAL_DIR_UPLOADIMAGES,
    LOCAL_DIR_RESULTS, LOCAL_DIR_IMAGERESULTS,
    LOCAL_DIR_BAKRESULTS, LOCAL_DIR_BAKIMAGERESULTS,
    LOCAL_DIR_ADDSAMPLES, LOCAL_DIR_ADDSAMPLEPROCESSED,
    LOCAL_DIR_REQUESTBYHASH, LOCAL_DIR_REQUESTBYHASHDONE,
    LOCAL_DIR_BAKREQUESTBYHASHDONE,
]:
    os.makedirs(d, exist_ok=True)

# ============ 工具函数 ============
def s3_list_prefix(bucket: str, prefix: str):
    """分页列出所有对象，防止超过1000漏项"""
    keys = []
    token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        resp = s3.list_objects_v2(**params)
        for c in resp.get("Contents", []):
            if not c["Key"].endswith("/"):
                keys.append(c["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys

def s3_download(bucket: str, key: str, local_path: str):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)
    logging.info(f"S3 -> local: s3://{bucket}/{key} -> {local_path}")

def s3_upload(bucket: str, key: str, local_path: str):
    s3.upload_file(local_path, bucket, key)
    logging.info(f"local -> S3: {local_path} -> s3://{bucket}/{key}")

def s3_copy(bucket: str, src_key: str, dst_key: str):
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src_key}, Key=dst_key)
    logging.info(f"S3 copy: s3://{bucket}/{src_key} -> s3://{bucket}/{dst_key}")

def s3_delete(bucket: str, key: str):
    s3.delete_object(Bucket=bucket, Key=key)
    logging.info(f"S3 delete: s3://{bucket}/{key}")

def s3_move(bucket: str, src_key: str, dst_key: str):
    if src_key == dst_key:
        return
    s3_copy(bucket, src_key, dst_key)
    s3_delete(bucket, src_key)

def file_is_stable(path: str, wait_sec: float = STABLE_WAIT_SEC) -> bool:
    try:
        s1 = os.path.getsize(path)
        time.sleep(wait_sec)
        s2 = os.path.getsize(path)
        return s1 == s2 and s1 > 0
    except FileNotFoundError:
        return False

def move_local(src: str, dst_dir: str) -> str:
    os.makedirs(dst_dir, exist_ok=True)
    base = os.path.basename(src)
    dst = os.path.join(dst_dir, base)
    if os.path.exists(dst):
        name, ext = os.path.splitext(base)
        dst = os.path.join(dst_dir, f"{name}_{int(time.time())}{ext}")
    shutil.move(src, dst)
    logging.info(f"Local move: {src} -> {dst}")
    return dst

def load_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"load_json error: {e} ({path})")
        return None

def _head_last_modified_ts(bucket: str, key: str) -> Optional[float]:
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        return resp["LastModified"].timestamp()
    except ClientError:
        return None

# ============ 队列 & 版本缓存 ============
class Task:
    def __init__(self, request_json_name: str, icon_filename: str, s3_processing_key: Optional[str]):
        self.request_json_name = request_json_name
        self.icon_filename = icon_filename
        self.icon_base_noext = os.path.splitext(icon_filename)[0] if icon_filename else ""
        self.s3_processing_key = s3_processing_key
        self.created_ts = time.time()

    def __repr__(self):
        return f"Task(req={self.request_json_name}, icon={self.icon_filename})"

# 队列
pending_results: Dict[str, Task] = {}         # key: <request_json_name>
pending_imageresults: Dict[str, Task] = {}    # key: <icon_base>
pending_addsamples: Dict[str, str] = {}       # name -> s3_key
pending_requestbyhash: Dict[str, str] = {}    # txt_name -> s3_key
rbh_hash_to_txt: Dict[str, str] = {}          # <hash> -> txt_name

# 关键：把“是否处理过”改为“是否更新”
rbh_local_req_seen_mtime: Dict[str, float] = {}     # '<hash>.json' -> last_mtime（本地 request）
addsampes_s3_seen_mtime: Dict[str, float] = {}      # 'file.txt' -> last_modified（S3 addsamples）

# 兜底增量同步（本地结果目录）
inforesults_seen_mtime: Dict[str, float] = {}       # '<hash>.json' mtime
imageresults_seen_mtime: Dict[str, float] = {}      # '<icon_hash>.json' mtime

# ============ 标准结果处理（队列驱动） ============
def scan_and_handle_local_results():
    """按 pending 队列上传 inforesults（保留，用于 requestbyhash 触发）"""
    needed = set(pending_results.keys())
    if not needed:
        return
    for fname in os.listdir(LOCAL_DIR_RESULTS):
        if not fname.endswith(".json") or fname not in needed:
            continue
        local_path = os.path.join(LOCAL_DIR_RESULTS, fname)
        if not file_is_stable(local_path):
            continue
        task = pending_results.get(fname)
        if not task:
            continue
        s3_key = S3_PREFIX_RESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, local_path)
        except ClientError as e:
            logging.error(f"Upload results failed: {local_path} -> {s3_key}, err={e}")
            continue
        if task.s3_processing_key:
            try:
                s3_move(BUCKET, task.s3_processing_key, S3_PREFIX_PROCESSED + fname)
            except ClientError as e:
                logging.error(f"Move processing -> processed failed: {task.s3_processing_key}, err={e}")
        move_local(local_path, LOCAL_DIR_BAKRESULTS)
        pending_results.pop(fname, None)
        logging.info(f"[DEQUEUE] results done for {fname}")

def scan_and_handle_local_imageresults():
    """按 pending 队列上传 imageresults（保留，用于 requestbyhash 触发）"""
    needed = set(pending_imageresults.keys())
    if not needed:
        return
    for fname in os.listdir(LOCAL_DIR_IMAGERESULTS):
        if not fname.endswith(".json"):
            continue
        base = os.path.splitext(fname)[0]
        if base not in needed:
            continue
        local_path = os.path.join(LOCAL_DIR_IMAGERESULTS, fname)
        if not file_is_stable(local_path):
            continue
        task = pending_imageresults.get(base)
        if not task:
            continue
        s3_key = S3_PREFIX_IMAGERESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, local_path)
        except ClientError as e:
            logging.error(f"Upload imageresults failed: {local_path} -> {s3_key}, err={e}")
            continue
        move_local(local_path, LOCAL_DIR_BAKIMAGERESULTS)
        pending_imageresults.pop(base, None)
        logging.info(f"[DEQUEUE] imageresults done for {fname}")

# ============ 兜底：本地 inforesults/imageresults 增量同步 ============
def _sync_inforesults_incremental():
    for fname in os.listdir(LOCAL_DIR_RESULTS):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(LOCAL_DIR_RESULTS, fname)
        if not file_is_stable(fpath):
            continue
        try:
            mtime = os.path.getmtime(fpath)
        except FileNotFoundError:
            continue
        old = inforesults_seen_mtime.get(fname, 0.0)
        if mtime <= old:
            continue
        # 新或更新 -> 直接上传并归档
        s3_key = S3_PREFIX_RESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, fpath)
        except ClientError as e:
            logging.error(f"[RESULTS][INCR] upload failed: {fname}, err={e}")
            continue
        move_local(fpath, LOCAL_DIR_BAKRESULTS)
        inforesults_seen_mtime[fname] = mtime
        # 若本条在 pending 中，也顺便出队
        if fname in pending_results:
            pending_results.pop(fname, None)
        logging.info(f"[RESULTS][INCR] uploaded (untracked): {fname}")

def _sync_imageresults_incremental():
    for fname in os.listdir(LOCAL_DIR_IMAGERESULTS):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(LOCAL_DIR_IMAGERESULTS, fname)
        if not file_is_stable(fpath):
            continue
        try:
            mtime = os.path.getmtime(fpath)
        except FileNotFoundError:
            continue
        old = imageresults_seen_mtime.get(fname, 0.0)
        if mtime <= old:
            continue
        # 新或更新 -> 直接上传并归档
        s3_key = S3_PREFIX_IMAGERESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, fpath)
        except ClientError as e:
            logging.error(f"[IMAGERES][INCR] upload failed: {fname}, err={e}")
            continue
        move_local(fpath, LOCAL_DIR_BAKIMAGERESULTS)
        imageresults_seen_mtime[fname] = mtime
        # 若在 pending 中，也顺便出队
        base = os.path.splitext(fname)[0]
        if base in pending_imageresults:
            pending_imageresults.pop(base, None)
        logging.info(f"[IMAGERES][INCR] uploaded (untracked): {fname}")

# ============ addsamples 逻辑（支持同名覆盖更新） ============
def handle_new_addsamples_from_s3():
    keys = s3_list_prefix(BUCKET, S3_PREFIX_ADDSAMPLES)
    for key in keys:
        if not key.endswith(".txt"):
            continue
        name = os.path.basename(key)
        local_path = os.path.join(LOCAL_DIR_ADDSAMPLES, name)

        lm = _head_last_modified_ts(BUCKET, key)
        if lm is None:
            continue
        old = addsampes_s3_seen_mtime.get(name, 0.0)

        # S3 “第一次见到”或“LastModified 变新” -> 重新下载 + 重新入队
        if lm > old or not os.path.exists(local_path):
            try:
                s3_download(BUCKET, key, local_path)
            except ClientError as e:
                logging.error(f"Download addsample TXT failed: {key}, err={e}")
                continue
            addsampes_s3_seen_mtime[name] = lm
            pending_addsamples[name] = key
            logging.info(f"[ENQUEUE][ADDSAMPLE] {name} (lm={lm})")

def scan_and_handle_local_addsampleprocessed():
    if not pending_addsamples:
        return
    need_names = set(pending_addsamples.keys())
    for fname in os.listdir(LOCAL_DIR_ADDSAMPLEPROCESSED):
        if not fname.endswith(".txt") or fname not in need_names:
            continue
        local_path = os.path.join(LOCAL_DIR_ADDSAMPLEPROCESSED, fname)
        if not file_is_stable(local_path):
            continue
        s3_src_key = pending_addsamples.get(fname)
        s3_dst_key = S3_PREFIX_ADDSAMPLEPROCESSED + fname
        try:
            s3_move(BUCKET, s3_src_key, s3_dst_key)
        except ClientError as e:
            logging.error(f"Move addsample failed: {s3_src_key}, err={e}")
            continue
        pending_addsamples.pop(fname, None)
        logging.info(f"[DEQUEUE][ADDSAMPLE] done for {fname}")

# ============ requestbyhash 逻辑 ============
def _read_hashes_from_txt(local_path: str) -> Set[str]:
    out = set()
    try:
        with open(local_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                out.add(s)
    except Exception as e:
        logging.error(f"read hashes error: {e} ({local_path})")
    return out

def _need_redownload(bucket: str, key: str, local_path: str) -> bool:
    """S3 文件比本地更新就重新下载"""
    lm = _head_last_modified_ts(bucket, key)
    if lm is None:
        return False
    if not os.path.exists(local_path):
        return True
    local_mtime = os.path.getmtime(local_path)
    return (lm - local_mtime) > 1.0

def handle_new_requestbyhash_from_s3():
    keys = s3_list_prefix(BUCKET, S3_PREFIX_REQUESTBYHASH)
    for key in keys:
        if not key.endswith(".txt"):
            continue
        name = os.path.basename(key)
        local_path = os.path.join(LOCAL_DIR_REQUESTBYHASH, name)

        # 覆盖更新 -> 重新下载（或首次下载）
        if _need_redownload(BUCKET, key, local_path):
            try:
                s3_download(BUCKET, key, local_path)
            except ClientError as e:
                logging.error(f"Download requestbyhash TXT failed: {key}, err={e}")
                continue

        # 解析 hash -> txt 映射（覆盖写会被刷新）
        hashes = _read_hashes_from_txt(local_path)
        for h in hashes:
            rbh_hash_to_txt[h] = name

        if name not in pending_requestbyhash:
            pending_requestbyhash[name] = key
            logging.info(f"[ENQUEUE][RBH] {name} (hashes={len(hashes)})")

def _try_enqueue_request_task_from_local_request_json():
    """
    监听本地 request/<hash>.json：
    - 只托管那些来自 requestbyhash 的 <hash>（在 rbh_hash_to_txt 里）
    - 以 mtime 为准：第一次或更新后都会再次入队
    """
    for fname in os.listdir(LOCAL_DIR_REQUEST):
        if not fname.endswith(".json"):
            continue
        base = os.path.splitext(fname)[0]
        if base not in rbh_hash_to_txt:
            continue

        local_req_path = os.path.join(LOCAL_DIR_REQUEST, fname)
        if not file_is_stable(local_req_path):
            continue

        try:
            mtime = os.path.getmtime(local_req_path)
        except FileNotFoundError:
            continue
        old = rbh_local_req_seen_mtime.get(fname, 0.0)
        if mtime <= old:
            continue

        data = load_json(local_req_path)
        if not data:
            continue

        icon_filename = data.get("icon_filename", "") or ""
        t = Task(request_json_name=fname, icon_filename=icon_filename, s3_processing_key=None)
        pending_results[fname] = t
        if icon_filename:
            icon_base = os.path.splitext(icon_filename)[0]
            pending_imageresults[icon_base] = t

        rbh_local_req_seen_mtime[fname] = mtime
        logging.info(f"[ENQUEUE][RBH] {base} -> queues (icon={icon_filename}, mtime={mtime})")

def scan_and_handle_local_requestbyhashdone():
    """
    当本地 requestbyhashdone/<name>.txt 出现（稳定）：
      1) 上传到 S3 requestbyhashdone/
      2) 删除 S3 requestbyhash/<name>.txt（若还在）
      3) 本地 done/<name>.txt 归档到 bakrequestbyhashdone/
      4) 同名源 requestbyhash/<name>.txt 也归档
      5) 清理映射和已见记录
    """
    if not pending_requestbyhash:
        return
    need_names = set(pending_requestbyhash.keys())
    for fname in os.listdir(LOCAL_DIR_REQUESTBYHASHDONE):
        if not fname.endswith(".txt") or fname not in need_names:
            continue
        local_path = os.path.join(LOCAL_DIR_REQUESTBYHASHDONE, fname)
        if not file_is_stable(local_path):
            continue

        s3_dst_key = S3_PREFIX_REQUESTBYHASHDONE + fname
        s3_src_key = pending_requestbyhash.get(fname)

        try:
            s3_upload(BUCKET, s3_dst_key, local_path)
        except ClientError as e:
            logging.error(f"Upload RBH done failed: {fname}, err={e}")
            continue

        if s3_src_key:
            try:
                s3_delete(BUCKET, s3_src_key)
            except ClientError as e:
                logging.warning(f"Delete src warn: {s3_src_key}, err={e}")

        move_local(local_path, LOCAL_DIR_BAKREQUESTBYHASHDONE)

        # 同名源 requestbyhash/<name>.txt 也归档（如果还在）
        src_txt = os.path.join(LOCAL_DIR_REQUESTBYHASH, fname)
        if os.path.exists(src_txt):
            move_local(src_txt, LOCAL_DIR_BAKREQUESTBYHASHDONE)

        # 清理映射与已见
        pending_requestbyhash.pop(fname, None)
        batch_hashes = [h for h, tname in list(rbh_hash_to_txt.items()) if tname == fname]
        for h in batch_hashes:
            rbh_hash_to_txt.pop(h, None)
            rbh_local_req_seen_mtime.pop(f"{h}.json", None)

        logging.info(f"[DEQUEUE][RBH] done for {fname}")

# ============ 主循环 ============
def main_loop():
    logging.info("=== Start watcher (addsampes + requestbyhash + results sync, mtime-safe) ===")
    while True:
        try:
            # 1) S3 拉取 / 入队（支持同名覆盖更新）
            handle_new_addsamples_from_s3()
            handle_new_requestbyhash_from_s3()

            # 2) 本地 request/<hash>.json 入队（支持同名覆盖更新）
            _try_enqueue_request_task_from_local_request_json()

            # 3) 按队列上传（原有）
            scan_and_handle_local_results()
            scan_and_handle_local_imageresults()

            # 4) 兜底增量上传（不依赖队列，确保“任何新/更新的结果”都能上传并归档）
            _sync_inforesults_incremental()
            _sync_imageresults_incremental()

            # 5) 其它收尾
            scan_and_handle_local_addsampleprocessed()
            scan_and_handle_local_requestbyhashdone()

            time.sleep(POLL_LOCAL_INTERVAL)
            time.sleep(POLL_S3_INTERVAL)
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt, exit.")
            break
        except Exception as e:
            logging.exception(f"main_loop error: {e}")
            time.sleep(1.0)

if __name__ == "__main__":
    main_loop()
