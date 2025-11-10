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

POLL_S3_INTERVAL    = 2.0    # 轮询 S3 新 request/addsamples 的间隔
POLL_LOCAL_INTERVAL = 1.0    # 轮询本地目录的间隔
STABLE_WAIT_SEC     = 0.2    # 本地文件大小稳定检测

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
    """列出指定前缀下的对象键列表（单页按 1000 上限）。"""
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
    """S3 “移动”：copy + delete（幂等）"""
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

# ============ 队列项 ============
class Task:
    """
    表示一个 request 任务：
    - request_json_name: 例如 xxx.json（不含路径）
    - icon_filename: 例如 xcwewsss.png
    - s3_processing_key: processing/ 下对应 JSON 的 key
    """
    def __init__(self, request_json_name: str, icon_filename: str, s3_processing_key: str):
        self.request_json_name = request_json_name
        self.icon_filename = icon_filename
        self.icon_base_noext = os.path.splitext(icon_filename)[0]
        self.s3_processing_key = s3_processing_key
        self.created_ts = time.time()

    def __repr__(self):
        return f"Task(req={self.request_json_name}, icon={self.icon_filename})"

# 等待本地 inforesults 的队列：key = request_json_name
pending_results: Dict[str, Task] = {}
# 等待本地 imageresults 的队列：key = icon_base_noext
pending_imageresults: Dict[str, Task] = {}
# 新增：等待 addsample 本地处理完成（本地 addsampleprocessed/ 出现同名文件）
# key = json 文件名（例如 123456.json），val = s3 上的完整 key（iconml/addsamples/<name>.json）
pending_addsamples: Dict[str, str] = {}

# ============ 逻辑：request -> results / imageresults ============
def handle_new_request_from_s3():
    """
    扫描 S3 iconml/request/ ，发现新 JSON：
      1) 下载到本地 request/
      2) 将 S3 该 JSON 移动到 processing/
      3) 解析 icon_filename 并下载 S3 images/<icon_filename> 到本地 uploadimages/
      4) 将任务加入两个待处理队列
    """
    keys = s3_list_prefix(BUCKET, S3_PREFIX_REQUEST)
    for key in keys:
        if not key.endswith(".json"):
            continue

        req_name = os.path.basename(key)  # xxx.json
        local_req_path = os.path.join(LOCAL_DIR_REQUEST, req_name)

        # 跳过本地已有（幂等）
        if os.path.exists(local_req_path):
            try:
                s3_move(BUCKET, key, S3_PREFIX_PROCESSING + req_name)
            except ClientError as e:
                logging.warning(f"Move existing request to processing warn: {e}")
            continue

        # 1) 下载到本地 request/
        try:
            s3_download(BUCKET, key, local_req_path)
        except ClientError as e:
            logging.error(f"Download request JSON failed: {key}, err={e}")
            continue

        # 2) S3 移动到 processing/
        processing_key = S3_PREFIX_PROCESSING + req_name
        try:
            s3_move(BUCKET, key, processing_key)
        except ClientError as e:
            logging.error(f"Move request -> processing failed: {key} -> {processing_key}, err={e}")

        # 3) 解析 JSON，下载 icon 文件到本地 uploadimages/
        data = load_json(local_req_path)
        if not data:
            logging.error(f"Invalid JSON: {local_req_path}")
            continue

        icon_filename = data.get("icon_filename")
        if not icon_filename:
            logging.error(f"JSON missing icon_filename: {local_req_path}")
            continue

        s3_icon_key = S3_PREFIX_IMAGES + icon_filename
        local_icon_path = os.path.join(LOCAL_DIR_UPLOADIMAGES, icon_filename)
        if not os.path.exists(local_icon_path):
            try:
                s3_download(BUCKET, s3_icon_key, local_icon_path)
            except ClientError as e:
                logging.error(f"Download icon failed: {s3_icon_key}, err={e}")
        else:
            logging.info(f"Icon already exists locally: {local_icon_path}")

        # 4) 入队：results 与 imageresults
        t = Task(req_name, icon_filename, processing_key)
        pending_results.setdefault(req_name, t)
        pending_imageresults.setdefault(t.icon_base_noext, t)
        logging.info(f"[ENQUEUE] {t} -> results & imageresults queues")

def scan_and_handle_local_results():
    """
    监控本地 inforesults/：
      若出现与 request_json 同名的结果 JSON：
        - 上传到 S3 inforesults/
        - 将 S3 processing/ 同名 JSON 移动到 processed/
        - 将本地结果 JSON 移动到 bakresults/
        - 从 pending_results 队列移除
    """
    needed = set(pending_results.keys())
    if not needed:
        return

    for fname in os.listdir(LOCAL_DIR_RESULTS):
        if not fname.endswith(".json"):
            continue
        if fname not in needed:
            continue

        local_path = os.path.join(LOCAL_DIR_RESULTS, fname)
        if not file_is_stable(local_path):
            continue

        task = pending_results.get(fname)
        if not task:
            continue

        # 上传到 S3 inforesults/
        s3_key = S3_PREFIX_RESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, local_path)
        except ClientError as e:
            logging.error(f"Upload results failed: {local_path} -> {s3_key}, err={e}")
            continue

        # 将 S3 processing/ 中的同名 JSON 移动到 processed/
        try:
            s3_move(BUCKET, task.s3_processing_key, S3_PREFIX_PROCESSED + fname)
        except ClientError as e:
            logging.error(f"Move processing -> processed failed: {task.s3_processing_key}, err={e}")

        # 本地移动到 bakresults/
        move_local(local_path, LOCAL_DIR_BAKRESULTS)

        # 出队
        pending_results.pop(fname, None)
        logging.info(f"[DEQUEUE] results done for {fname}")

def scan_and_handle_local_imageresults():
    """
    监控本地 imageresults/：
      若出现 <icon_filename无后缀>.json：
        - 上传到 S3 imageresults/
        - 删除 S3 images/ 中的 icon_filename
        - 本地移动到 bakimageresults/
        - 从 pending_imageresults 队列移除
    """
    needed = set(pending_imageresults.keys())
    if not needed:
        return

    for fname in os.listdir(LOCAL_DIR_IMAGERESULTS):
        if not fname.endswith(".json"):
            continue

        base = os.path.splitext(fname)[0]  # 期望与 icon_filename 去后缀一致
        if base not in needed:
            continue

        local_path = os.path.join(LOCAL_DIR_IMAGERESULTS, fname)
        if not file_is_stable(local_path):
            continue

        task = pending_imageresults.get(base)
        if not task:
            continue

        # 上传到 S3 imageresults/
        s3_key = S3_PREFIX_IMAGERESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, local_path)
        except ClientError as e:
            logging.error(f"Upload imageresults failed: {local_path} -> {s3_key}, err={e}")
            continue

        # 删除 S3 images/<icon_filename>
        s3_icon_key = S3_PREFIX_IMAGES + task.icon_filename
        try:
            s3_delete(BUCKET, s3_icon_key)
        except ClientError as e:
            logging.warning(f"Delete icon warn: {s3_icon_key}, err={e}")

        # 本地移至备份目录
        move_local(local_path, LOCAL_DIR_BAKIMAGERESULTS)

        # 出队
        pending_imageresults.pop(base, None)
        logging.info(f"[DEQUEUE] imageresults done for {fname}")

# ============ 新增逻辑：addsamples -> addsampleprocessed ============
def handle_new_addsamples_from_s3():
    """
    监控 S3 iconml/addsamples/：
      - 若发现新的 .json 文件：
        1) 下载到本地 ./addsamples/<name>.json （幂等，已存在则跳过下载）
        2) 将其加入 pending_addsamples 队列，等待本地 addsampleprocessed/<name>.json 出现后再移动 S3 文件
    """
    keys = s3_list_prefix(BUCKET, S3_PREFIX_ADDSAMPLES)
    for key in keys:
        if not key.endswith(".json"):
            continue
        name = os.path.basename(key)
        local_path = os.path.join(LOCAL_DIR_ADDSAMPLES, name)

        if not os.path.exists(local_path):
            try:
                s3_download(BUCKET, key, local_path)
            except ClientError as e:
                logging.error(f"Download addsample JSON failed: {key}, err={e}")
                continue
        else:
            logging.info(f"Addsample JSON already exists locally: {local_path}")

        if name not in pending_addsamples:
            pending_addsamples[name] = key
            logging.info(f"[ENQUEUE][ADDSAMPLE] {name} -> pending_addsamples")

def scan_and_handle_local_addsampleprocessed():
    """
    监控本地 ./addsampleprocessed/：
      - 若出现与 pending_addsamples 中同名的 .json：
        1) 将 S3 上 iconml/addsamples/<name>.json 移动到 iconml/addsampleprocessed/<name>.json
        2) 从 pending_addsamples 出队
    """
    if not pending_addsamples:
        return

    need_names = set(pending_addsamples.keys())
    for fname in os.listdir(LOCAL_DIR_ADDSAMPLEPROCESSED):
        if not fname.endswith(".json"):
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
    logging.info("=== Start watcher ===")
    while True:
        try:
            # 1) 扫描 S3 新 request
            handle_new_request_from_s3()

            # 2) 扫描 S3 新 addsamples 任务
            handle_new_addsamples_from_s3()

            # 3) 扫描本地 inforesults（队列1）
            scan_and_handle_local_results()

            # 4) 扫描本地 imageresults（队列2）
            scan_and_handle_local_imageresults()

            # 5) 扫描本地 addsampleprocessed（队列3）
            scan_and_handle_local_addsampleprocessed()

            # 两个不同的节奏
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