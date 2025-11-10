#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IconML S3 Client – Submit & Fetch Results

Modes:
  1) icon-json:
     - Upload icon -> iconml/images/<icon_filename>
     - Upload request json -> iconml/request/<hash>.json
     - Poll & fetch:
         imageresults/<icon_filename>.json
         inforesults/<hash>.json   (if request has app info fields)
  2) requestbyhash:
     - Upload txt -> iconml/requestbyhash/<txt_name>
     - Wait for same-named marker in iconml/requestbyhashdone/
     - For each hash in txt:
         fetch inforesults/<hash>.json
         if present, read request.icon_filename then fetch imageresults/<icon_filename>.json

Usage examples:
  python iconml_client.py icon-json \
      --bucket mr-dev-iconml-request \
      --icon ./xcwewsss.png \
      --hash 21cf2e...6724 \
      --request ./request.json

  python iconml_client.py requestbyhash \
      --bucket mr-dev-iconml-request \
      --txt ./batch_2025_11_10.txt
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, Dict, Any, List

import boto3
from botocore.exceptions import ClientError

# --------- S3 Layout (adjust if needed) ----------
PREFIX_IMAGES              = "iconml/images/"
PREFIX_REQUEST             = "iconml/request/"
PREFIX_PROCESSING          = "iconml/processing/"         # system-managed
PREFIX_PROCESSED           = "iconml/processed/"          # system-managed
PREFIX_IMAGERESULTS        = "iconml/imageresults/"
PREFIX_INFORESULTS         = "iconml/inforesults/"
PREFIX_REQUESTBYHASH       = "iconml/requestbyhash/"
PREFIX_REQUESTBYHASH_DONE  = "iconml/requestbyhashdone/"

# --------- Defaults for polling ----------
DEFAULT_POLL_SECS      = 3.0
DEFAULT_MAX_WAIT_SECS  = 30 * 60    # 30 minutes
HEAD_RETRY_INTERVAL    = 1.0

DEFAULT_BUCKET = "mr-dev-iconml-request"
DEFAULT_REGION = "us-west-2"

# =================================================
# S3 helpers
# =================================================

def s3_client(profile: Optional[str] = None, region: Optional[str] = None):
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
        return session.client("s3")
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")

def s3_upload_file(s3, bucket: str, local_path: str, key: str):
    s3.upload_file(local_path, bucket, key)
    print(f"[UPLOAD] {local_path} -> s3://{bucket}/{key}")

def s3_put_json(s3, bucket: str, key: str, obj: Dict[str, Any]):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    print(f"[PUT JSON] s3://{bucket}/{key}")

def s3_get_json(s3, bucket: str, key: str) -> Dict[str, Any]:
    r = s3.get_object(Bucket=bucket, Key=key)
    data = r["Body"].read()
    return json.loads(data.decode("utf-8", errors="replace"))

def s3_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return False
        # Some S3 providers raise 404 as 403; treat 4xx as not-exist except other errors
        code = e.response.get("Error", {}).get("Code", "")
        if str(code) in ("404", "NoSuchKey", "NotFound", "AccessDenied"):
            return False
        raise

def poll_until_exists(s3, bucket: str, key: str, max_wait: float, poll_secs: float) -> bool:
    """Return True if appears within max_wait, else False."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        if s3_exists(s3, bucket, key):
            return True
        time.sleep(poll_secs)
    return False

# =================================================
# Mode 1: icon + request json
# =================================================

def load_json_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def mode_icon_json(args):
    """
    Steps:
      1) upload icon to images/
      2) upload request json to request/<hash>.json
      3) wait imageresults/<icon_filename>.json   (always for icon mode)
      4) if request has app fields (hash, package/label/devid/permissions), also wait inforesults/<hash>.json
    """
    s3 = s3_client(args.profile, args.region)

    bucket = args.bucket
    icon_path = Path(args.icon).expanduser().resolve()
    req_path  = Path(args.request).expanduser().resolve()

    if not icon_path.exists():
        print(f"[FATAL] icon not found: {icon_path}")
        sys.exit(2)
    if not req_path.exists():
        print(f"[FATAL] request json not found: {req_path}")
        sys.exit(2)

    req = load_json_file(req_path)
    icon_filename = req.get("icon_filename") or icon_path.name
    # strongly recommend: request json must contain icon_filename consistent with uploaded icon
    if req.get("icon_filename") and req["icon_filename"] != icon_filename:
        print(f"[WARN] request.icon_filename != local icon filename, using request value: {req['icon_filename']}")
        icon_filename = req["icon_filename"]

    # 1) upload icon
    key_icon = PREFIX_IMAGES + icon_filename
    s3_upload_file(s3, bucket, str(icon_path), key_icon)

    # 2) upload request json (name by hash if present; else use icon_filename base)
    req_hash = req.get("hash")
    if req_hash:
        key_req = f"{PREFIX_REQUEST}{req_hash}.json"
    else:
        # icon-only mode without hash: allow request named by icon base + timestamp
        base = Path(icon_filename).stem
        key_req = f"{PREFIX_REQUEST}{base}_{int(time.time())}.json"

    s3_put_json(s3, bucket, key_req, req)

    # 3) wait for imageresults
    key_imgres = f"{PREFIX_IMAGERESULTS}{icon_filename}.json"
    print(f"[WAIT] imageresults: s3://{bucket}/{key_imgres}")
    ok = poll_until_exists(s3, bucket, key_imgres, args.max_wait, args.poll)
    if not ok:
        print(f"[TIMEOUT] imageresults not ready within {args.max_wait}s")
    else:
        imgres = s3_get_json(s3, bucket, key_imgres)
        print("\n=== ImageResults ===")
        print(json.dumps(imgres, ensure_ascii=False, indent=2))

    # 4) wait for inforesults (only if request had app info / hash)
    if req_hash:
        key_infores = f"{PREFIX_INFORESULTS}{req_hash}.json"
        print(f"[WAIT] inforesults: s3://{bucket}/{key_infores}")
        ok2 = poll_until_exists(s3, bucket, key_infores, args.max_wait, args.poll)
        if not ok2:
            print(f"[TIMEOUT] inforesults not ready within {args.max_wait}s")
        else:
            infores = s3_get_json(s3, bucket, key_infores)
            print("\n=== InfoResults ===")
            print(json.dumps(infores, ensure_ascii=False, indent=2))
    else:
        print("\n[NOTE] No 'hash' in request -> icon-only mode; inforesults not expected.")

# =================================================
# Mode 2: requestbyhash batch
# =================================================

def read_hash_lines(txt: Path) -> List[str]:
    out = []
    with txt.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                out.append(s)
    return out

def mode_requestbyhash(args):
    """
    Steps:
      1) upload local txt -> iconml/requestbyhash/<name>.txt
      2) wait same name appears in iconml/requestbyhashdone/
      3) for each hash in txt:
           - try fetch inforesults/<hash>.json
           - if present, read request.icon_filename, then fetch imageresults/<icon_filename>.json
    """
    s3 = s3_client(args.profile, args.region)
    bucket = args.bucket
    txt_path = Path(args.txt).expanduser().resolve()
    if not txt_path.exists():
        print(f"[FATAL] txt file not found: {txt_path}")
        sys.exit(2)

    hashes = read_hash_lines(txt_path)
    if not hashes:
        print(f"[FATAL] no hashes in txt: {txt_path}")
        sys.exit(2)

    # 1) upload txt
    name = txt_path.name
    key_reqhash = f"{PREFIX_REQUESTBYHASH}{name}"
    s3_upload_file(s3, bucket, str(txt_path), key_reqhash)

    # 2) wait for done marker
    key_done = f"{PREFIX_REQUESTBYHASH_DONE}{name}"
    print(f"[WAIT] requestbyhash done marker: s3://{bucket}/{key_done}")
    ok = poll_until_exists(s3, bucket, key_done, args.max_wait, args.poll)
    if not ok:
        print(f"[TIMEOUT] requestbyhash done marker not found within {args.max_wait}s. Exiting.")
        sys.exit(3)

    print("\n[INFO] Batch marked done. Now fetching results per hash...\n")

    # 3) per-hash results
    for h in hashes:
        print("="*70)
        print(f"[HASH] {h}")
        key_info = f"{PREFIX_INFORESULTS}{h}.json"
        if not s3_exists(s3, bucket, key_info):
            print(f"[MISS] inforesults not found for hash: {h}")
            continue

        infores = s3_get_json(s3, bucket, key_info)
        print("\n--- InfoResults ---")
        print(json.dumps(infores, ensure_ascii=False, indent=2))

        # Try get icon result by icon_filename inside info request section
        try:
            icon_fn = infores["request"]["icon_filename"]
        except Exception:
            icon_fn = None

        if icon_fn:
            key_imgres = f"{PREFIX_IMAGERESULTS}{icon_fn}.json"
            if s3_exists(s3, bucket, key_imgres):
                imgres = s3_get_json(s3, bucket, key_imgres)
                print("\n--- ImageResults ---")
                print(json.dumps(imgres, ensure_ascii=False, indent=2))
            else:
                print(f"[MISS] imageresults not found for icon: {icon_fn}")
        else:
            print("[NOTE] inforesults.request.icon_filename missing; skip imageresults fetch.")
    print("\n[DONE] requestbyhash batch fetch complete.")

# =================================================
# CLI
# =================================================

def build_parser():
    p = argparse.ArgumentParser(description="IconML S3 Client – submit & fetch results")
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"S3 bucket name (default: {DEFAULT_BUCKET})")
    p.add_argument("--profile", default=None, help="AWS profile name (optional)")
    p.add_argument("--region", default=DEFAULT_REGION, help=f"AWS region (default: {DEFAULT_REGION})")
    p.add_argument("--poll", type=float, default=DEFAULT_POLL_SECS, help="Polling interval seconds")
    p.add_argument("--max-wait", type=float, default=DEFAULT_MAX_WAIT_SECS, help="Max wait seconds")

    sub = p.add_subparsers(dest="mode", required=True)

    # mode 1: icon-json
    p1 = sub.add_parser("icon-json", help="Submit icon + request json, then fetch results")
    p1.add_argument("--icon", required=True, help="Local icon path (.png/.jpg/.webp)")
    p1.add_argument("--request", required=True, help="Local request json path")

    # mode 2: requestbyhash
    p2 = sub.add_parser("requestbyhash", help="Submit a txt of hashes, wait done marker, fetch each hash results")
    p2.add_argument("--txt", required=True, help="Local txt file with hashes (one per line)")
    return p

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.mode == "icon-json":
        mode_icon_json(args)
    elif args.mode == "requestbyhash":
        mode_requestbyhash(args)
    else:
        print(f"[FATAL] unknown mode: {args.mode}")
        sys.exit(2)

if __name__ == "__main__":
    main()
