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
# 入库任务（已存在）
S3_PREFIX_ADDSAMPLES           = "iconml/addsamples/"
S3_PREFIX_ADDSAMPLEPROCESSED   = "iconml/addsampleprocessed/"
# 新增：批量 hash 任务（request-by-hash）
S3_PREFIX_REQUESTBYHASH        = "iconml/requestbyhash/"
S3_PREFIX_REQUESTBYHASHDONE    = "iconml/requestbyhashdone/"

LOCAL_DIR_REQUEST              = "request"
LOCAL_DIR_UPLOADIMAGES         = "uploadimages"
LOCAL_DIR_RESULTS              = "inforesults"
LOCAL_DIR_IMAGERESULTS         = "imageresults"
LOCAL_DIR_BAKRESULTS           = "bakresults"
LOCAL_DIR_BAKIMAGERESULTS      = "bakimageresults"
# 入库任务本地目录（已存在）
LOCAL_DIR_ADDSAMPLES           = "addsamples"
LOCAL_DIR_ADDSAMPLEPROCESSED   = "addsampleprocessed"
# 新增：request-by-hash 本地目录
LOCAL_DIR_REQUESTBYHASH        = "requestbyhash"
LOCAL_DIR_REQUESTBYHASHDONE    = "requestbyhashdone"

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
    LOCAL_DIR_ADDSAMPLES, LOCAL_DIR_ADDSAMPLEPROCESSED,
    LOCAL_DIR_REQUESTBYHASH, LOCAL_DIR_REQUESTBYHASHDONE
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

def load_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"load_json error: {e} ({path})")
        return None

# ============ 队列结构 ============
class Task:
    """
    兼容标准 request 流程的任务对象。
    - request_json_name: 例如 'abcd.json'
    - icon_filename:     例如 'sha256abcdef.png'
    - s3_processing_key: 对 requestbyhash 生成的本地请求，可能为 None
    """
    def __init__(self, request_json_name: str, icon_filename: str, s3_processing_key: Optional[str]):
        self.request_json_name = request_json_name
        self.icon_filename = icon_filename
        self.icon_base_noext = os.path.splitext(icon_filename)[0] if icon_filename else ""
        self.s3_processing_key = s3_processing_key
        self.created_ts = time.time()

    def __repr__(self):
        return f"Task(req={self.request_json_name}, icon={self.icon_filename})"

# —— 标准 request 队列（用于自动上传 inforesults / imageresults）——
pending_results: Dict[str, Task] = {}        # key = request_json_name
pending_imageresults: Dict[str, Task] = {}   # key = icon_base_noext

# —— addsamples 队列（已存在）——
pending_addsamples: Dict[str, str] = {}      # name -> s3_key

# —— requestbyhash 队列（新增）——
# 1) 记录 S3 上的 txt 以便完成时移动
pending_requestbyhash: Dict[str, str] = {}   # txt_name -> s3_key
# 2) 记录 txt 中的 hash 属于哪个 txt，用于识别本地生成的 request/<hash>.json
rbh_hash_to_txt: Dict[str, str] = {}         # hash -> txt_name
# 3) 避免重复入队（对 request/<hash>.json）
rbh_seen_request_json: Set[str] = set()      # set of '<hash>.json'

# ============ 标准 request 的本地结果处理（沿用原逻辑） ============
def scan_and_handle_local_results():
    """
    监控本地 inforesults/：
      若出现与 pending_results 同名（<request_json>.json）的结果：
        - 上传到 S3 inforesults/
        - （仅当有 s3_processing_key 时）S3 processing/<req> -> processed/<req>
        - 本地移动到 bakresults/
        - 出队
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

        # 仅当有 processing key 才移动到 processed（requestbyhash 生成的 local req 没有 processing 阶段）
        if task.s3_processing_key:
            try:
                s3_move(BUCKET, task.s3_processing_key, S3_PREFIX_PROCESSED + fname)
            except ClientError as e:
                logging.error(f"Move processing -> processed failed: {task.s3_processing_key}, err={e}")

        move_local(local_path, LOCAL_DIR_BAKRESULTS)
        pending_results.pop(fname, None)
        logging.info(f"[DEQUEUE] results done for {fname}")

def scan_and_handle_local_imageresults():
    """
    监控本地 imageresults/：
      若出现 <icon_base>.json：
        - 上传到 S3 imageresults/
        - 删除 S3 images/<icon_filename>
        - 本地移动到 bakimageresults/
        - 出队
    """
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

        # 上传到 S3 imageresults/
        s3_key = S3_PREFIX_IMAGERESULTS + fname
        try:
            s3_upload(BUCKET, s3_key, local_path)
        except ClientError as e:
            logging.error(f"Upload imageresults failed: {local_path} -> {s3_key}, err={e}")
            continue

        # 删除 S3 images/<icon_filename>（存在即删，不存在忽略）

        #if task.icon_filename:
        #    s3_icon_key = S3_PREFIX_IMAGES + task.icon_filename
        #    try:
        #        s3_delete(BUCKET, s3_icon_key)
        #    except ClientError as e:
        #        logging.warning(f"Delete icon warn: {s3_icon_key}, err={e}")

        move_local(local_path, LOCAL_DIR_BAKIMAGERESULTS)
        pending_imageresults.pop(base, None)
        logging.info(f"[DEQUEUE] imageresults done for {fname}")

# ============ addsamples（保持不变） ============
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
        else:
            logging.info(f"Addsample TXT already exists locally: {local_path}")

        if name not in pending_addsamples:
            pending_addsamples[name] = key
            logging.info(f"[ENQUEUE][ADDSAMPLE] {name} -> pending_addsamples")

def scan_and_handle_local_addsampleprocessed():
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

# ============ NEW: requestbyhash 逻辑 ============
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
    """
    监控 S3 iconml/requestbyhash/：
      - 新 .txt -> 下载到 ./requestbyhash/
      - 解析 hash 列表，登记到 rbh_hash_to_txt，并把 txt 加入 pending_requestbyhash
    """
    keys = s3_list_prefix(BUCKET, S3_PREFIX_REQUESTBYHASH)
    for key in keys:
        if not key.endswith(".txt"):
            continue
        name = os.path.basename(key)
        local_path = os.path.join(LOCAL_DIR_REQUESTBYHASH, name)

        # 下载
        if not os.path.exists(local_path):
            try:
                s3_download(BUCKET, key, local_path)
            except ClientError as e:
                logging.error(f"Download requestbyhash TXT failed: {key}, err={e}")
                continue

        # 解析 hash，并登记映射
        hashes = _read_hashes_from_txt(local_path)
        for h in hashes:
            rbh_hash_to_txt[h] = name

        if name not in pending_requestbyhash:
            pending_requestbyhash[name] = key
            logging.info(f"[ENQUEUE][RBH] {name} -> pending_requestbyhash, hashes={len(hashes)}")

def _try_enqueue_request_task_from_local_request_json():
    """
    监控本地 ./request/ 下是否生成了 <hash>.json
    若该 hash 属于某个 requestbyhash txt，读取 json 获取 icon_filename，
    并把该 hash 对应的 info / image 结果纳入标准队列 pending_results / pending_imageresults。
    """
    for fname in os.listdir(LOCAL_DIR_REQUEST):
        if not fname.endswith(".json"):
            continue
        # basename 作为 hash
        base = os.path.splitext(fname)[0]
        if base not in rbh_hash_to_txt:
            continue
        if fname in rbh_seen_request_json:
            continue

        local_req_path = os.path.join(LOCAL_DIR_REQUEST, fname)
        if not file_is_stable(local_req_path):
            continue

        data = load_json(local_req_path)
        if not data:
            continue
        icon_filename = data.get("icon_filename", "") or ""
        # 入队：inforesults 监控（以 request json 名为 key）
        t = Task(request_json_name=fname, icon_filename=icon_filename, s3_processing_key=None)
        pending_results.setdefault(fname, t)
        # 入队：imageresults 监控（以 icon base 为 key）
        if icon_filename:
            icon_base = os.path.splitext(icon_filename)[0]
            pending_imageresults.setdefault(icon_base, t)

        rbh_seen_request_json.add(fname)
        logging.info(f"[ENQUEUE][RBH] hash={base} -> results/imageresults queues (icon={icon_filename})")

def scan_and_handle_local_requestbyhashdone():
    """
    监控本地 ./requestbyhashdone/：
      - 若出现与 pending_requestbyhash 同名的 .txt，则视为整批完成（即便有 hash 失败也不阻塞），
        将 S3 上 requestbyhash/<txt> 移动到 requestbyhashdone/<txt> 并出队。
    """
    if not pending_requestbyhash:
        return
    need_names = set(pending_requestbyhash.keys())
    for fname in os.listdir(LOCAL_DIR_REQUESTBYHASHDONE):
        if not fname.endswith(".txt"):
            continue
        if fname not in need_names:
            continue
        local_path = os.path.join(LOCAL_DIR_REQUESTBYHASHDONE, fname)
        if not file_is_stable(local_path):
            continue

        s3_src_key = pending_requestbyhash.get(fname)
        s3_dst_key = S3_PREFIX_REQUESTBYHASHDONE + fname
        try:
            s3_move(BUCKET, s3_src_key, s3_dst_key)
            logging.info(f"[S3 MOVE][RBH] {s3_src_key} -> {s3_dst_key}")
        except ClientError as e:
            logging.error(f"Move S3 requestbyhash -> requestbyhashdone failed: {s3_src_key}, err={e}")
            continue

        # 出队 + 清理该 txt 下的 hash 映射（可选）
        pending_requestbyhash.pop(fname, None)
        # 移除 rbh_hash_to_txt 中属于该 txt 的所有 hash
        for h in [h for h, tname in list(rbh_hash_to_txt.items()) if tname == fname]:
            rbh_hash_to_txt.pop(h, None)

        logging.info(f"[DEQUEUE][RBH] done for {fname}")

# ============ 主循环 ============
def main_loop():
    logging.info("=== Start watcher (addsampes + requestbyhash + results sync) ===")
    while True:
        try:
            # 1) S3 pulls
            handle_new_addsamples_from_s3()
            handle_new_requestbyhash_from_s3()

            # 2) 本地 requestbyhash：当 request/<hash>.json 出现时，把该 hash 纳入标准队列
            _try_enqueue_request_task_from_local_request_json()

            # 3) 标准结果上传：inforesults / imageresults
            scan_and_handle_local_results()
            scan_and_handle_local_imageresults()

            # 4) addsamples 完成检测：本地 addsampleprocessed/<name>.txt -> S3 addsampleprocessed/
            scan_and_handle_local_addsampleprocessed()

            # 5) requestbyhash 完成检测：本地 requestbyhashdone/<name>.txt -> S3 requestbyhashdone/
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