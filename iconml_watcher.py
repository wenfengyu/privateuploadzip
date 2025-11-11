#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import shutil
import logging
import threading
from typing import Dict, Optional, Set, List
import boto3
from botocore.exceptions import ClientError

# =========================== 全局配置 ===========================
BUCKET = "mr-iconml-dev"

# --- request 流水线(S3) ---
S3_REQUEST        = "iconml/request/"
S3_IMAGES         = "iconml/images/"
S3_PROCESSING     = "iconml/processing/"
S3_PROCESSED      = "iconml/processed/"
S3_INFORESULTS    = "iconml/inforesults/"
S3_IMAGERESULTS   = "iconml/imageresults/"

# --- request 本地目录 ---
DIR_REQUEST           = "request"
DIR_UPLOADIMAGES      = "uploadimages"
DIR_INFORESULTS       = "inforesults"
DIR_IMAGERESULTS      = "imageresults"
DIR_BAKINFORESULTS    = "bakinforesults"
DIR_BAKIMAGERESULTS   = "bakimageresults"

# --- requestbyhash (RBH) 专用(S3) ---
S3_RBH        = "iconml/requestbyhash/"
S3_RBH_DONE   = "iconml/requestbyhashdone/"

# --- requestbyhash (RBH) 本地目录 ---
DIR_RBH         = "requestbyhash"
DIR_RBH_DONE    = "requestbyhashdone"
DIR_RBH_BAK     = "bakrequestbyhashdone"  # 归档 done 与源 txt

# --- addsample (AS) 专用(S3) ---
S3_AS_IN       = "iconml/addsamples/"
S3_AS_DONE     = "iconml/addsampleprocessed/"

# --- addsample (AS) 本地目录 ---
DIR_AS_IN      = "addsamples"            # 从 S3 下载的待处理 txt
DIR_AS_DONE    = "addsampleprocessed"    # 你本地产出的处理完成 txt
DIR_AS_BAK     = "bakaddsamples"         # 本地归档

# --- 轮询间隔 ---
POLL_S3_INTERVAL     = 2.0     # 扫 S3 的节奏
POLL_LOCAL_INTERVAL  = 1.0     # 扫本地目录的节奏
STABLE_WAIT_SEC      = 0.2     # 本地文件大小稳定检测
LOG_LEVEL            = logging.INFO

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")

# 统一确保目录存在
for d in [
    DIR_REQUEST, DIR_UPLOADIMAGES,
    DIR_INFORESULTS, DIR_IMAGERESULTS,
    DIR_BAKINFORESULTS, DIR_BAKIMAGERESULTS,
    DIR_RBH, DIR_RBH_DONE, DIR_RBH_BAK,
    DIR_AS_IN, DIR_AS_DONE, DIR_AS_BAK
]:
    os.makedirs(d, exist_ok=True)


# =========================== 公共 I/O 工具 ===========================
def new_s3():
    """每个线程独立创建 S3 client，避免跨线程复用连接。"""
    return boto3.client("s3")

def s3_list_prefix(s3, bucket: str, prefix: str) -> List[str]:
    keys, token = [], None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        try:
            resp = s3.list_objects_v2(**params)
        except ClientError as e:
            logging.error(f"s3_list_prefix error: {e} (prefix={prefix})")
            return keys
        for c in resp.get("Contents", []):
            if not c["Key"].endswith("/"):
                keys.append(c["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys

def s3_head_last_modified_ts(s3, bucket: str, key: str) -> Optional[float]:
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        return resp["LastModified"].timestamp()
    except ClientError:
        return None

def s3_download(s3, bucket: str, key: str, local_path: str):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)
    logging.info(f"S3 -> local: s3://{bucket}/{key} -> {local_path}")

def s3_upload(s3, bucket: str, key: str, local_path: str):
    s3.upload_file(local_path, bucket, key)
    logging.info(f"local -> S3: {local_path} -> s3://{bucket}/{key}")

def s3_copy(s3, bucket: str, src_key: str, dst_key: str):
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src_key}, Key=dst_key)
    logging.info(f"S3 copy: s3://{bucket}/{src_key} -> s3://{bucket}/{dst_key}")

def s3_delete(s3, bucket: str, key: str):
    s3.delete_object(Bucket=bucket, Key=key)
    logging.info(f"S3 delete: s3://{bucket}/{key}")

def s3_move(s3, bucket: str, src_key: str, dst_key: str):
    if src_key == dst_key:
        return
    s3_copy(s3, bucket, src_key, dst_key)
    s3_delete(s3, bucket, src_key)

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
    time.sleep(1.5)  # 给 FS 缓冲，避免紧跟读写抖动
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


# =========================== request 流水线（线程1） ===========================
class RequestTask:
    def __init__(self, req_json_name: str, icon_filename: str, s3_processing_key: str):
        self.req_json_name = req_json_name
        self.icon_filename = icon_filename
        self.icon_base_noext = os.path.splitext(icon_filename)[0]
        self.s3_processing_key = s3_processing_key
        self.created_ts = time.time()
    def __repr__(self):
        return f"Task(req={self.req_json_name}, icon={self.icon_filename})"

class RequestWatcher(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.s3 = new_s3()
        # 队列
        self.pending_inforesults: Dict[str, RequestTask] = {}    # key: request_json_name
        self.pending_imageresults: Dict[str, RequestTask] = {}   # key: icon_base
        # S3 最近更新时间缓存（处理同名覆盖）
        self.seen_s3_lm_request: Dict[str, float] = {}
        # 兜底增量同步缓存
        self.seen_local_inforesults_mtime: Dict[str, float] = {}
        self.seen_local_imageresults_mtime: Dict[str, float] = {}

    def _handle_new_request_from_s3(self):
        keys = s3_list_prefix(self.s3, BUCKET, S3_REQUEST)
        for key in keys:
            if not key.endswith(".json"):
                continue
            name = os.path.basename(key)
            local_path = os.path.join(DIR_REQUEST, name)

            s3lm = s3_head_last_modified_ts(self.s3, BUCKET, key)
            oldlm = self.seen_s3_lm_request.get(name, 0.0)
            local_mtime = os.path.getmtime(local_path) if os.path.exists(local_path) else 0.0

            # 首次或 S3 更新 -> 下载
            if (s3lm is not None) and (not os.path.exists(local_path) or s3lm > local_mtime + 1.0 or s3lm > oldlm + 1.0):
                try:
                    s3_download(self.s3, BUCKET, key, local_path)
                    self.seen_s3_lm_request[name] = s3lm or time.time()
                except ClientError as e:
                    logging.error(f"[REQ] download request failed: {key}, {e}")
                    continue

            # S3 移动到 processing（幂等）
            processing_key = S3_PROCESSING + name
            try:
                s3_move(self.s3, BUCKET, key, processing_key)
            except ClientError as e:
                logging.warning(f"[REQ] move request->processing warn: {e}")

            data = load_json(local_path)
            if not data:
                continue
            icon_filename = data.get("icon_filename")
            if not icon_filename:
                logging.error(f"[REQ] json missing icon_filename: {local_path}")
                continue

            # 下载 icon 到本地（若不存在）
            icon_key = S3_IMAGES + icon_filename
            icon_local = os.path.join(DIR_UPLOADIMAGES, icon_filename)
            if not os.path.exists(icon_local):
                try:
                    s3_download(self.s3, BUCKET, icon_key, icon_local)
                except ClientError as e:
                    logging.warning(f"[REQ] download icon warn (non-fatal): {e}")

            t = RequestTask(name, icon_filename, processing_key)
            self.pending_inforesults[name] = t
            self.pending_imageresults[t.icon_base_noext] = t
            logging.info(f"[ENQUEUE][REQ] {t}")

    def _scan_local_inforesults(self):
        if not self.pending_inforesults:
            return
        need = set(self.pending_inforesults.keys())
        for fname in os.listdir(DIR_INFORESULTS):
            if not fname.endswith(".json") or fname not in need:
                continue
            fpath = os.path.join(DIR_INFORESULTS, fname)
            if not file_is_stable(fpath):
                continue
            t = self.pending_inforesults.get(fname)
            if not t:
                continue

            try:
                s3_upload(self.s3, BUCKET, S3_INFORESULTS + fname, fpath)
            except ClientError as e:
                logging.error(f"[REQ] upload inforesults failed: {e}")
                continue
            try:
                s3_move(self.s3, BUCKET, t.s3_processing_key, S3_PROCESSED + fname)
            except ClientError as e:
                logging.warning(f"[REQ] processing->processed warn: {e}")

            move_local(fpath, DIR_BAKINFORESULTS)
            self.pending_inforesults.pop(fname, None)
            logging.info(f"[DEQUEUE][REQ] inforesults {fname}")

    def _scan_local_imageresults(self):
        if not self.pending_imageresults:
            return
        need = set(self.pending_imageresults.keys())
        for fname in os.listdir(DIR_IMAGERESULTS):
            if not fname.endswith(".json"):
                continue
            base = os.path.splitext(fname)[0]
            if base not in need:
                continue
            fpath = os.path.join(DIR_IMAGERESULTS, fname)
            if not file_is_stable(fpath):
                continue
            t = self.pending_imageresults.get(base)
            if not t:
                continue

            try:
                s3_upload(self.s3, BUCKET, S3_IMAGERESULTS + fname, fpath)
            except ClientError as e:
                logging.error(f"[REQ] upload imageresults failed: {e}")
                continue
            try:
                s3_delete(self.s3, BUCKET, S3_IMAGES + t.icon_filename)
            except ClientError as e:
                logging.warning(f"[REQ] delete icon warn: {e}")

            move_local(fpath, DIR_BAKIMAGERESULTS)
            self.pending_imageresults.pop(base, None)
            logging.info(f"[DEQUEUE][REQ] imageresults {fname}")

    # 兜底：任何新/更新的本地结果，都直接上传（不依赖队列）
    def _sync_inforesults_incremental(self):
        for fname in os.listdir(DIR_INFORESULTS):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(DIR_INFORESULTS, fname)
            if not file_is_stable(fpath):
                continue
            m = os.path.getmtime(fpath)
            if m <= self.seen_local_inforesults_mtime.get(fname, 0.0):
                continue
            try:
                s3_upload(self.s3, BUCKET, S3_INFORESULTS + fname, fpath)
            except ClientError as e:
                logging.error(f"[REQ][INCR] upload inforesults failed: {e}")
                continue
            move_local(fpath, DIR_BAKINFORESULTS)
            self.seen_local_inforesults_mtime[fname] = m
            self.pending_inforesults.pop(fname, None)
            logging.info(f"[REQ][INCR] inforesults {fname}")

    def _sync_imageresults_incremental(self):
        for fname in os.listdir(DIR_IMAGERESULTS):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(DIR_IMAGERESULTS, fname)
            if not file_is_stable(fpath):
                continue
            m = os.path.getmtime(fpath)
            if m <= self.seen_local_imageresults_mtime.get(fname, 0.0):
                continue
            try:
                s3_upload(self.s3, BUCKET, S3_IMAGERESULTS + fname, fpath)
            except ClientError as e:
                logging.error(f"[REQ][INCR] upload imageresults failed: {e}")
                continue
            move_local(fpath, DIR_BAKIMAGERESULTS)
            self.seen_local_imageresults_mtime[fname] = m
            self.pending_imageresults.pop(os.path.splitext(fname)[0], None)
            logging.info(f"[REQ][INCR] imageresults {fname}")

    def run(self):
        logging.info("=== RequestWatcher started ===")
        while True:
            try:
                self._handle_new_request_from_s3()
                self._scan_local_inforesults()
                self._scan_local_imageresults()
                # 兜底增量
                self._sync_inforesults_incremental()
                self._sync_imageresults_incremental()
                time.sleep(POLL_LOCAL_INTERVAL)
                time.sleep(POLL_S3_INTERVAL)
            except Exception as e:
                logging.exception(f"[REQ] loop error: {e}")
                time.sleep(1.0)


# =========================== requestbyhash 流水线（线程2） ===========================
class RBHWatcher(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.s3 = new_s3()
        # 记录批次 -> {hash集合}
        self.batches: Dict[str, Set[str]] = {}
        # S3 TXT 的 LastModified 记忆（支持同名覆盖）
        self.rbh_seen_s3_lm: Dict[str, float] = {}

    def _read_hashes(self, path: str) -> Set[str]:
        out = set()
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue
                    out.add(s)
        except Exception as e:
            logging.error(f"[RBH] read hashes error: {e} ({path})")
        return out

    def _pull_batches_from_s3(self):
        keys = s3_list_prefix(self.s3, BUCKET, S3_RBH)
        for key in keys:
            if not key.endswith(".txt"):
                continue
            name = os.path.basename(key)
            local = os.path.join(DIR_RBH, name)
            s3lm = s3_head_last_modified_ts(self.s3, BUCKET, key)
            old = self.rbh_seen_s3_lm.get(name, 0.0)
            local_m = os.path.getmtime(local) if os.path.exists(local) else 0.0

            if (s3lm is not None) and (not os.path.exists(local) or s3lm > local_m + 1.0 or s3lm > old + 1.0):
                try:
                    s3_download(self.s3, BUCKET, key, local)
                    self.rbh_seen_s3_lm[name] = s3lm or time.time()
                    self.batches[name] = self._read_hashes(local)
                    logging.info(f"[RBH] pull {name}, hashes={len(self.batches[name])}")
                except Exception as e:
                    logging.error(f"[RBH] download failed: {e}")

    def _info_exists_locally(self, h: str) -> bool:
        return os.path.exists(os.path.join(DIR_INFORESULTS, f"{h}.json"))

    def _icon_base_from_request(self, h: str) -> Optional[str]:
        """尝试从本地 request/<h>.json 读取 icon_filename -> icon_hash(base)。若无则返回 None。"""
        j = os.path.join(DIR_REQUEST, f"{h}.json")
        if not os.path.exists(j):
            return None
        data = load_json(j)
        if not data:
            return None
        icon_filename = data.get("icon_filename") or ""
        base, _ = os.path.splitext(icon_filename)
        return base or None

    def _image_exists_for_hash(self, h: str) -> bool:
        """若能解析出 icon_base，就检查 imageresults/<icon_base>.json；否则认为图像未就绪。"""
        base = self._icon_base_from_request(h)
        if not base:
            return False
        return os.path.exists(os.path.join(DIR_IMAGERESULTS, f"{base}.json"))

    def _all_done_for_batch(self, hashes: Set[str]) -> (List[str], List[str]):
        done, pending = [], []
        for h in hashes:
            info_ok = self._info_exists_locally(h)
            img_ok  = self._image_exists_for_hash(h)
            if info_ok and img_ok:
                done.append(h)
            else:
                pending.append(h)
        return done, pending

    def _write_summary(self, local_txt_name: str, done: List[str], pending: List[str], all_hashes: Set[str]) -> str:
        lines = []
        lines.append(f"SUMMARY FOR: {local_txt_name}")
        lines.append(f"TOTAL HASHES: {len(all_hashes)}")
        lines.append("")
        lines.append(f"[SUCCESS COMPLETED] ({len(done)})")
        for h in done: lines.append(f"  - {h}")
        lines.append("")
        lines.append(f"[PENDING_OR_TIMEOUT] ({len(pending)})")
        for h in pending: lines.append(f"  - {h}")
        lines.append("")
        lines.append("NOTE:")
        lines.append("  SUCCESS COMPLETED: inforesults/<hash>.json & imageresults/<icon_hash>.json both present.")
        lines.append("  PENDING_OR_TIMEOUT: missing either inforesult or imageresult (或 request/<hash>.json 缺失，无法确定 icon_hash)。")
        return "\n".join(lines)

    def _try_finalize_batches(self):
        for name, hashes in list(self.batches.items()):
            done, pending = self._all_done_for_batch(hashes)
            if pending:
                logging.info(f"[RBH] wait {name}: completed={len(done)} pending={len(pending)}")
                continue
            # 全部完成 -> 生成同名 done.txt
            out = self._write_summary(name, done, pending, hashes)
            local_done = os.path.join(DIR_RBH_DONE, name)  # 与源同名
            with open(local_done, "w", encoding="utf-8") as f:
                f.write(out)

            try:
                s3_upload(self.s3, BUCKET, S3_RBH_DONE + name, local_done)
            except Exception as e:
                logging.error(f"[RBH] upload done failed: {e}")
                continue

            move_local(local_done, DIR_RBH_BAK)
            src = os.path.join(DIR_RBH, name)
            if os.path.exists(src):
                move_local(src, DIR_RBH_BAK)
            self.batches.pop(name, None)
            logging.info(f"[RBH] finalized {name}")

    def run(self):
        logging.info("=== RBHWatcher started ===")
        while True:
            try:
                self._pull_batches_from_s3()
                self._try_finalize_batches()
                time.sleep(POLL_LOCAL_INTERVAL)
                time.sleep(POLL_S3_INTERVAL)
            except Exception as e:
                logging.exception(f"[RBH] loop error: {e}")
                time.sleep(1.0)


# =========================== addsample 流水线（线程3） ===========================
class AddSampleWatcher(threading.Thread):
    """
    addsample 逻辑：
      - 拉取 S3 iconml/addsamples/*.txt 到本地 addsamples/   （支持同名覆盖：用 LastModified 判定）
      - 你处理完毕后在本地 addSampleProcessed/ 产出同名 .txt
      - 监控到后：
            * 上传到 S3 iconml/addsampleprocessed/
            * 删除/清理 S3 源 addsamples/<name>.txt
            * 本地两个 txt 都归档到 bakaddsamples/
    """
    def __init__(self):
        super().__init__(daemon=True)
        self.s3 = new_s3()
        self.seen_s3_lm: Dict[str, float] = {}      # name -> s3 LastModified
        self.pending_names: Dict[str, str] = {}     # name -> s3_source_key

    def _pull_from_s3(self):
        keys = s3_list_prefix(self.s3, BUCKET, S3_AS_IN)
        for key in keys:
            if not key.endswith(".txt"):
                continue
            name = os.path.basename(key)
            local = os.path.join(DIR_AS_IN, name)
            s3lm = s3_head_last_modified_ts(self.s3, BUCKET, key)
            old  = self.seen_s3_lm.get(name, 0.0)
            local_m = os.path.getmtime(local) if os.path.exists(local) else 0.0

            if (s3lm is not None) and (not os.path.exists(local) or s3lm > local_m + 1.0 or s3lm > old + 1.0):
                try:
                    s3_download(self.s3, BUCKET, key, local)
                    self.seen_s3_lm[name] = s3lm or time.time()
                    self.pending_names[name] = key
                    logging.info(f"[AS] pull {name}")
                except ClientError as e:
                    logging.error(f"[AS] download failed: {e}")

    def _scan_local_done(self):
        if not self.pending_names:
            return
        need = set(self.pending_names.keys())
        for fname in os.listdir(DIR_AS_DONE):
            if not fname.endswith(".txt") or fname not in need:
                continue
            fpath = os.path.join(DIR_AS_DONE, fname)
            if not file_is_stable(fpath):
                continue
            s3_dst = S3_AS_DONE + fname
            s3_src = self.pending_names.get(fname)

            # 上传处理结果
            try:
                s3_upload(self.s3, BUCKET, s3_dst, fpath)
            except ClientError as e:
                logging.error(f"[AS] upload done failed: {e}")
                continue

            # 删除 S3 源（或也可改成 move 到一个历史目录）
            if s3_src:
                try:
                    s3_delete(self.s3, BUCKET, s3_src)
                except ClientError as e:
                    logging.warning(f"[AS] delete source warn: {e}")

            # 本地归档：done 与 in 各归档一份
            move_local(fpath, DIR_AS_BAK)
            src_txt = os.path.join(DIR_AS_IN, fname)
            if os.path.exists(src_txt):
                move_local(src_txt, DIR_AS_BAK)

            self.pending_names.pop(fname, None)
            logging.info(f"[AS] finalized {fname}")

    def run(self):
        logging.info("=== AddSampleWatcher started ===")
        while True:
            try:
                self._pull_from_s3()
                self._scan_local_done()
                time.sleep(POLL_LOCAL_INTERVAL)
                time.sleep(POLL_S3_INTERVAL)
            except Exception as e:
                logging.exception(f"[AS] loop error: {e}")
                time.sleep(1.0)


# =========================== 主函数：启动三个线程 ===========================
def main():
    req = RequestWatcher()
    rbh = RBHWatcher()
    ads = AddSampleWatcher()
    req.start()
    rbh.start()
    ads.start()
    logging.info("=== merged watcher running (request + requestbyhash + addsample) ===")
    # 主线程保持存活
    try:
        while True:
            time.sleep(60.0)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt, exit.")

if __name__ == "__main__":
    main()
