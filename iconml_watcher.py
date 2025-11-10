import os
import json
import time
import shutil
import logging
from typing import Dict, Optional
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
# 新增：入库任务相关
S3_PREFIX_ADDSAMPLES           = "iconml/addsamples/"
S3_PREFIX_ADDSAMPLEPROCESSED   = "iconml/addsampleprocessed/"

LOCAL_DIR_REQUEST              = "request"
LOCAL_DIR_UPLOADIMAGES         = "uploadimages"
LOCAL_DIR_RESULTS              = "inforesults"
LOCAL_DIR_IMAGERESULTS         = "imageresults"
LOCAL_DIR_BAKRESULTS           = "bakresults"
LOCAL_DIR_BAKIMAGERESULTS      = "bakimageresults"
# 新增：入库任务本地目录
LOCAL_DIR_ADDSAMPLES           = "addsamples"
LOCAL_DIR_ADDSAMPLEPROCESSED   = "addsampleprocessed"

POLL_S3_INTERVAL    = 2.0
POLL_LOCAL_INTERVAL = 1.0
STABLE_WAIT_SEC     = 0.2

LOG_LEVEL = logging.INFO

# ============ 初始化 ============
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
s3 = boto3.client("s3")

for d in [
    LOCAL_DIR_REQUEST, LOCAL_DIR_UPLOADIMAGES,
    LOCAL_DIR_RESULTS, LOCAL_DIR_IMAGERESULTS,
    LOCAL_DIR_BAKRESULTS, LOCAL_DIR_BAKIMAGERESULTS,
    LOCAL_DIR_ADDSAMPLES, LOCAL_DIR_ADDSAMPLEPROCESSED
]:
    os.makedirs(d, exist_ok=True)

# ============ 工具函数 ============
def s3_list_prefix(bucket: str, prefix: str):
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = resp.get("Contents", [])
        return [c["Key"] for c in contents if not c["Key"].endswith("/")]
    except ClientError as e:
        logging.error(f"s3_list_prefix error: {e}")
        return []

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

# ============ 队列项 ============
class Task:
    def __init__(self, request_json_name: str, icon_filename: str, s3_processing_key: str):
        self.request_json_name = request_json_name
        self.icon_filename = icon_filename
        self.icon_base_noext = os.path.splitext(icon_filename)[0]
        self.s3_processing_key = s3_processing_key
        self.created_ts = time.time()

    def __repr__(self):
        return f"Task(req={self.request_json_name}, icon={self.icon_filename})"

pending_results: Dict[str, Task] = {}
pending_imageresults: Dict[str, Task] = {}
pending_addsamples: Dict[str, str] = {}

# ============ Addsamples（监控 .txt 文件） ============
def handle_new_addsamples_from_s3():
    """
    监控 S3 iconml/addsamples/：
      - 若发现新的 .txt 文件：
        1) 下载到本地 ./addsamples/<name>.txt
        2) 将其加入 pending_addsamples 队列，等待本地 addsampleprocessed/<name>.txt 出现后再移动 S3 文件
    """
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
        else:
            logging.info(f"Addsample TXT already exists locally: {local_path}")

        if name not in pending_addsamples:
            pending_addsamples[name] = key
            logging.info(f"[ENQUEUE][ADDSAMPLE] {name} -> pending_addsamples")

def scan_and_handle_local_addsampleprocessed():
    """
    监控本地 ./addsampleprocessed/：
      - 若出现与 pending_addsamples 中同名的 .txt：
        1) 将 S3 上 iconml/addsamples/<name>.txt 移动到 iconml/addsampleprocessed/<name>.txt
        2) 从 pending_addsamples 出队
    """
    if not pending_addsamples:
        return

    need_names = set(pending_addsamples.keys())
    for fname in os.listdir(LOCAL_DIR_ADDSAMPLEPROCESSED):
        if not fname.endswith(".txt"):
            continue
        if fname not in need_names:
            continue

        local_path = os.path.join(LOCAL_DIR_ADDSAMPLEPROCESSED, fname)
        if not file_is_stable(local_path):
            continue

        s3_src_key = pending_addsamples.get(fname)
        s3_dst_key = S3_PREFIX_ADDSAMPLEPROCESSED + fname

        try:
            s3_move(BUCKET, s3_src_key, s3_dst_key)
            logging.info(f"[S3 MOVE][ADDSAMPLE] {s3_src_key} -> {s3_dst_key}")
        except ClientError as e:
            logging.error(f"Move S3 addsamples -> addsampleprocessed failed: {s3_src_key}, err={e}")
            continue

        pending_addsamples.pop(fname, None)
        logging.info(f"[DEQUEUE][ADDSAMPLE] done for {fname}")

# ============ 主循环 ============
def main_loop():
    logging.info("=== Start watcher (addsamples .txt mode) ===")
    while True:
        try:
            handle_new_addsamples_from_s3()
            scan_and_handle_local_addsampleprocessed()
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
