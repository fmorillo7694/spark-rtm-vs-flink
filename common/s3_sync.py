#!/usr/bin/env python3
"""Upload a local directory (or one file, to key PREFIX) to s3://BUCKET/PREFIX/ (used by the in-cluster stateful runner,
which runs under IRSA, so boto3 picks the web-identity credentials up from the pod env).

Usage: s3_sync.py LOCAL_DIR BUCKET PREFIX
"""
import os
import sys

import boto3


def main():
    local, bucket, prefix = sys.argv[1], sys.argv[2], sys.argv[3].strip("/")
    s3 = boto3.client("s3")
    if os.path.isfile(local):
        s3.upload_file(local, bucket, prefix)
        print(f"[s3_sync] 1 file -> s3://{bucket}/{prefix}")
        return 0
    n = 0
    for root, _dirs, files in os.walk(local):
        for name in files:
            path = os.path.join(root, name)
            key = f"{prefix}/{os.path.relpath(path, local)}"
            s3.upload_file(path, bucket, key)
            n += 1
    print(f"[s3_sync] {n} files -> s3://{bucket}/{prefix}/")


if __name__ == "__main__":
    sys.exit(main())
