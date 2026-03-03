"""
S3 partition helpers.

Purpose:
- partition_exists(): idempotency guard for partitioned outputs
- list_partition_values(): discover available partition values under a dataset root
- get_latest_partition_value(): fetch max partition value (ISO date-friendly)

Conventions:
- Hive partitions like key=value (e.g., ingestion_date=2026-03-03)
- Dataset "root" is the prefix ABOVE the partition folders
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
import re

import boto3

def split_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """
    Split s3://bucket/prefix -> (bucket, prefix_with_trailing_slash_or_empty)
    """
    s = s3_uri.replace("s3://", "").strip("/")
    if "/" not in s:
        return s, ""
    bucket, prefix = s.split("/", 1)
    prefix = prefix.strip("/")
    return bucket, (prefix + "/") if prefix else ""

@dataclass
class S3Helper:
    """
    Thin boto3 wrapper for listing "directories" using Delimiter='/'.
    """
    client = boto3.client("s3")

    def prefix_has_objects(self, *, bucket: str, prefix: str) -> bool:
        resp = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return bool(resp.get("KeyCount", 0))

    def list_common_prefixes(self, *, bucket: str, prefix: str) -> List[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        out: List[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                p = cp.get("Prefix")
                if p:
                    out.append(p)
        return out


def partition_exists(partition_s3_uri: str) -> bool:
    """
    True if there is at least one object under the given partition prefix.
    Example: s3://bucket/.../ingestion_date=2026-03-03/
    """
    bucket, prefix = split_s3_uri(partition_s3_uri)
    return S3Helper().prefix_has_objects(bucket=bucket, prefix=prefix)


def list_partition_values(*, root_s3_uri: str, partition_key: str) -> List[str]:
    """
    List partition values under root_s3_uri for a given partition key.

    Example:
      root_s3_uri = s3://bucket/analytics/ebay_market_snapshot/
      partition_key = ingestion_date
      returns ['2026-03-01', '2026-03-02', ...] (sorted)

    Sorting:
    - Returned values are sorted lexicographically (works for ISO dates).
    """
    bucket, prefix = split_s3_uri(root_s3_uri)
    prefixes = S3Helper().list_common_prefixes(bucket=bucket, prefix=prefix)

    # Expect immediate children like .../ingestion_date=YYYY-MM-DD/
    pat = re.compile(rf"{re.escape(partition_key)}=([^/]+)/?$")

    values: List[str] = []
    for p in prefixes:
        # p is full key prefix; match on last path segment
        last = p.rstrip("/").split("/")[-1]
        m = pat.match(last)
        if m:
            values.append(m.group(1))

    return sorted(set(values))

def get_latest_partition_value(*, root_s3_uri: str, partition_key: str) -> Optional[str]:
    """
    Return the latest (max) partition value or None if no partitions exist.
    """
    values = list_partition_values(root_s3_uri=root_s3_uri, partition_key=partition_key)
    return values[-1] if values else None
