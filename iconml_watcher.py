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

def _need_redownload(bucket: str, key: str, local_path: str) -> bool:
    """S3 文件比本地更新就重新下载"""
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        s3_mtime = resp["LastModified"].timestamp()
    except ClientError:
        return False
    if not os.path.exists(local_path):
        return True
    local_mtime = os.path.getmtime(local_path)
    return s3_mtime - local_mtime > 1.0

# ============ 队列结构 ============
class Task:
    def __init__(self, request_json_name: str, icon_filename: str, s3_processing_key: Optional[str]):
        self.request_json_name = request_json_name
        self.icon_filename = icon_filename
        self.icon_base_noext = os.path.splitext(icon_filename)[0] if icon_filename else ""
        self.s3_processing_key = s3_processing_key
        self.created_ts = time.time()

    def __repr__(self):
        return f"Task(req={self.request_json_name}, icon={self.icon_filename})"

# —— 标准 request 队列（用于自动上传 inforesults / imageresults）——
pending_results: Dict[str, Task] = {}        # key = "<request_json>.json"
pending_imageresults: Dict[str, Task] = {}   # key = "<icon_base>"

# —— addsamples 队列（已存在）——
pending_addsamples: Dict[str, str] = {}      # name -> s3_key

# —— requestbyhash 队列（新增）——
pending_requestbyhash: Dict[str, str] = {}   # txt_name -> s3_key
rbh_hash_to_txt: Dict[str, str] = {}         # hash -> txt_name
rbh_seen_request_json: Set[str] = set()      # '<hash>.json'

# —— 新增：本地结果“增量”监控缓存（修复同名再次生成不处理的问题）——
results_seen_mtime: Dict[str, float] = {}        # 'xxx.json' -> mtime
imageresults_seen_mtime: Dict[str, float] = {}   # 'iconhash.json' -> mtime

# ============ 标准结果处理（修复后：支持兜底增量上传） ============
def _sync_inforesults_incremental():
    """
    增量同步 inforesults/ 到 S3：
      - 只要发现新的/更新过的 *.json（mtime > 缓存），就上传到 S3，并归档到 bakresults/；
      - 若该文件也在 pending_results 中，按老流程移动 processing->processed；
      - 不在 pending 也照样上传（修复同名再次生成不处理的问题）。
    """
    for fname in os.listdir(LOCAL_DIR_RESULTS):
        if not fname.endswith(".json"):
            continue
        local_path = os.path.join(LOCAL_DIR_RESULTS, fname)
        try:
            mtime = os.path.getmtime(local_path)
        except FileNotFoundError:
            continue
        old = results_seen_mtime.get(fname, 0.0)
        if mtime <= old:
            continue
        if not file_is_stable(local_path):
            continue

        # 上传
        s3_key = S3_PREFIX_RESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, local_path)
        except ClientError as e:
            logging.error(f"[RESULTS] upload failed: {local_path} -> {s3_key}, err={e}")
            continue

        # 如果在 pending，则处理 processing -> processed
        task = pending_results.get(fname)
        if task and task.s3_processing_key:
            try:
                s3_move(BUCKET, task.s3_processing_key, S3_PREFIX_PROCESSED + fname)
            except ClientError as e:
                logging.error(f"[RESULTS] processing->processed failed: {task.s3_processing_key}, err={e}")
            # 出队
            pending_results.pop(fname, None)
            logging.info(f"[DEQUEUE] results done for {fname}")
        else:
            logging.info(f"[RESULTS] uploaded (untracked): {fname}")

        # 归档
        move_local(local_path, LOCAL_DIR_BAKRESULTS)
        results_seen_mtime[fname] = mtime

def _sync_imageresults_incremental():
    """
    增量同步 imageresults/ 到 S3：
      - 发现新的/更新过的 <icon_hash>.json 就上传并归档；
      - 若在 pending_imageresults 中按老流程出队；否则也上传（兜底）。
    """
    for fname in os.listdir(LOCAL_DIR_IMAGERESULTS):
        if not fname.endswith(".json"):
            continue
        local_path = os.path.join(LOCAL_DIR_IMAGERESULTS, fname)
        try:
            mtime = os.path.getmtime(local_path)
        except FileNotFoundError:
            continue
        old = imageresults_seen_mtime.get(fname, 0.0)
        if mtime <= old:
            continue
        if not file_is_stable(local_path):
            continue

        # 上传
        s3_key = S3_PREFIX_IMAGERESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, local_path)
        except ClientError as e:
            logging.error(f"[IMAGERES] upload failed: {local_path} -> {s3_key}, err={e}")
            continue

        # 如果在 pending_imageresults 中，按老流程出队
        base = os.path.splitext(fname)[0]
        task = pending_imageresults.get(base)
        if task:
            pending_imageresults.pop(base, None)
            logging.info(f"[DEQUEUE] imageresults done for {fname}")
        else:
            logging.info(f"[IMAGERES] uploaded (untracked): {fname}")

        # 归档
        move_local(local_path, LOCAL_DIR_BAKIMAGERESULTS)
        imageresults_seen_mtime[fname] = mtime

# ============ addsamples 逻辑 ============
def handle_new_addsamples_from_s3():
    keys = s3_list_prefix(BUCKET, S3_PREFIX_ADDSAMPLES)
    for key in keys:
        if not key.endswith(".txt"):
            continue
        name = os.path.basename(key)
        local_path = os.path.join(LOCAL_DIR_ADDSAMPLES, name)
        if not os.path.exists(local_path):
            try:
                s3_download(BUCKET, key, local_path)
            except ClientError as e:
                logging.error(f"Download addsample TXT failed: {key}, err={e}")
                continue
        if name not in pending_addsamples:
            pending_addsamples[name] = key
            logging.info(f"[ENQUEUE][ADDSAMPLE] {name}")

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

def handle_new_requestbyhash_from_s3():
    keys = s3_list_prefix(BUCKET, S3_PREFIX_REQUESTBYHASH)
    for key in keys:
        if not key.endswith(".txt"):
            continue
        name = os.path.basename(key)
        local_path = os.path.join(LOCAL_DIR_REQUESTBYHASH, name)

        # 若S3较新或本地不存在，重新下载
        if _need_redownload(BUCKET, key, local_path):
            try:
                s3_download(BUCKET, key, local_path)
            except ClientError as e:
                logging.error(f"Download requestbyhash TXT failed: {key}, err={e}")
                continue

        # 解析
        hashes = _read_hashes_from_txt(local_path)
        for h in hashes:
            rbh_hash_to_txt[h] = name

        if name not in pending_requestbyhash:
            pending_requestbyhash[name] = key
            logging.info(f"[ENQUEUE][RBH] {name} (hashes={len(hashes)})")

def _try_enqueue_request_task_from_local_request_json():
    """
    当 request/<hash>.json 出现后：
      - pending_results 以 '<hash>.json' 为键入队；
      - 如果 json 中有 icon_filename，则以 icon 的 base 入队 pending_imageresults。
    """
    for fname in os.listdir(LOCAL_DIR_REQUEST):
        if not fname.endswith(".json"):
            continue
        base = os.path.splitext(fname)[0]
        if base not in rbh_hash_to_txt or fname in rbh_seen_request_json:
            continue
        local_req_path = os.path.join(LOCAL_DIR_REQUEST, fname)
        if not file_is_stable(local_req_path):
            continue
        data = load_json(local_req_path)
        if not data:
            continue
        icon_filename = data.get("icon_filename", "") or ""
        t = Task(request_json_name=fname, icon_filename=icon_filename, s3_processing_key=None)
        pending_results.setdefault(fname, t)
        if icon_filename:
            icon_base = os.path.splitext(icon_filename)[0]
            pending_imageresults.setdefault(icon_base, t)
        rbh_seen_request_json.add(fname)
        logging.info(f"[ENQUEUE][RBH] {base} -> queues (icon={icon_filename})")

def scan_and_handle_local_requestbyhashdone():
    """
    当本地 requestbyhashdone/<name>.txt 出现时：
      - 上传同名到 S3 requestbyhashdone/；
      - 删除 S3 源 requestbyhash/<name>.txt；
      - 本地归档；
      - 清理映射（包括 rbh_seen_request_json）。
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

        # 归档源 requestbyhash txt
        src_txt = os.path.join(LOCAL_DIR_REQUESTBYHASH, fname)
        if os.path.exists(src_txt):
            move_local(src_txt, LOCAL_DIR_BAKREQUESTBYHASHDONE)

        # 清理映射与 seen
        pending_requestbyhash.pop(fname, None)
        batch_hashes = [h for h, tname in list(rbh_hash_to_txt.items()) if tname == fname]
        for h in batch_hashes:
            rbh_hash_to_txt.pop(h, None)
            rbh_seen_request_json.discard(f"{h}.json")
        logging.info(f"[DEQUEUE][RBH] done for {fname}")

# ============ 主循环 ============
def main_loop():
    logging.info("=== Start watcher (addsampes + requestbyhash + results sync, fixed reprocessing) ===")
    while True:
        try:
            # 1) S3拉取任务
            handle_new_addsamples_from_s3()
            handle_new_requestbyhash_from_s3()

            # 2) 发现本地 request/<hash>.json 后入队
            _try_enqueue_request_task_from_local_request_json()

            # 3) **先做增量同步**：无论是否在pending，只要本地有新的/更新的结果文件，一律上传并归档
            _sync_inforesults_incremental()
            _sync_imageresults_incremental()

            # 4) 仍兼容原有 addsamples / requestbyhash 完成收尾
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
