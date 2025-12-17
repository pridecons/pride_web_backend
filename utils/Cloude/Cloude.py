# utils/Cloude/Cloude.py
import csv
import json
import io
from typing import Any, Dict, List, Optional, Union, BinaryIO
import asyncio
import boto3
from config import (
    CF_R2_ACCESS_KEY_ID,
    CF_R2_ACCOUNT_ID,
    CF_R2_REGION,
    CF_R2_SECRET_ACCESS_KEY,
    BUCKET_NAME
)

R2_PUBLIC_BASE_URL=""
bucket_name = BUCKET_NAME

def _get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=CF_R2_ACCESS_KEY_ID,
        aws_secret_access_key=CF_R2_SECRET_ACCESS_KEY,
        region_name=CF_R2_REGION,
        endpoint_url=f"https://{CF_R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    )

# -----------------------
# UPLOAD Helpers (Fixed)
# -----------------------

def upload_bytes(
    file_key: str,
    data: bytes,
    bucket_name: str = bucket_name,
    content_type: Optional[str] = None,
    extra_args: Optional[Dict[str, Any]] = None,
):
    s3 = _get_s3_client()
    put_args: Dict[str, Any] = {
        "Bucket": bucket_name,
        "Key": file_key,
        "Body": data,
    }
    if content_type:
        put_args["ContentType"] = content_type
    if extra_args:
        put_args.update(extra_args)

    s3.put_object(**put_args)


def upload_fileobj(
    file_key: str,
    file_obj: BinaryIO,
    bucket_name: str = bucket_name,
    content_type: Optional[str] = None,
    extra_args: Optional[Dict[str, Any]] = None,
):
    s3 = _get_s3_client()
    put_args: Dict[str, Any] = {
        "Bucket": bucket_name,
        "Key": file_key,
        "Body": file_obj,
    }
    if content_type:
        put_args["ContentType"] = content_type
    if extra_args:
        put_args.update(extra_args)

    s3.put_object(**put_args)


def upload_file_path(
    file_key: str,
    file_path: str,
    bucket_name: str = bucket_name,
    extra_args: Optional[Dict[str, Any]] = None,
):
    s3 = _get_s3_client()
    if extra_args is None:
        extra_args = {}
    s3.upload_file(file_path, bucket_name, file_key, ExtraArgs=extra_args)


def list_objects(prefix: str = "", bucket_name: str = bucket_name, limit: int = 1000):
    """
    List objects in R2 under a prefix.
    Returns list of dicts: [{ "Key": "...", "LastModified": datetime, "Size": int, ... }, ...]
    """
    s3 = _get_s3_client()
    prefix = (prefix or "").lstrip("/")

    out = []
    token = None

    while True:
        kwargs = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "MaxKeys": min(limit, 1000),
        }
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get("Contents") or []
        out.extend(contents)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if len(out) >= limit:
                return out[:limit]
        else:
            break

    return out


def get_latest_key(prefix: str, bucket_name: str = bucket_name) -> str:
    """
    Find latest object key under prefix based on LastModified.
    """
    objs = list_objects(prefix=prefix, bucket_name=bucket_name, limit=2000)
    if not objs:
        raise FileNotFoundError(f"No files found under prefix: {prefix}")

    latest = max(objs, key=lambda x: x["LastModified"])
    return latest["Key"]


async def upload_bytes_to_cloud(
    data: bytes,
    key: str,
    content_type: str = "application/octet-stream",
    kind: str = "generic",
) -> str:
    """
    Upload raw bytes to R2 and return public URL.

    Args:
        data: File bytes
        key: Object key inside bucket (e.g. "chat/123/uuid__file.png")
        content_type: MIME type
        kind: Logical type (chat/invoice/etc) – abhi sirf future use ke liye

    Returns:
        Public URL string (e.g. "https://cdn.pridecons.com/chat/123/uuid__file.png")
        Agar R2_PUBLIC_BASE_URL set nahi hai to sirf "/{key}" return karega
    """
    s3 = _get_s3_client()

    def _put():
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

    # Blocking boto3 ko thread pe daal do
    await asyncio.to_thread(_put)

    # Public URL banao
    key_norm = key.lstrip("/")

    if R2_PUBLIC_BASE_URL:
        base = R2_PUBLIC_BASE_URL.rstrip("/")
        return f"{base}/{key_norm}"

    # Fallback: relative path (agar CDN base env set na ho)
    return f"/{key_norm}"

# -----------------------
# DOWNLOAD Helpers
# -----------------------

def get_object_bytes(file_key: str, bucket_name: str = bucket_name) -> bytes:
    s3 = _get_s3_client()
    try:
        resp = s3.get_object(Bucket=bucket_name, Key=file_key)
        return resp["Body"].read()
    except Exception as e:
        raise RuntimeError(f"Failed to download {bucket_name}/{file_key}: {e}")

def get_bytes(file_key: str) -> bytes:
    """
    Download object from R2 and return its raw bytes.
    file_key example: "invoices/INV_123.pdf"
    """
    s3 = _get_s3_client()
    resp = s3.get_object(Bucket=bucket_name, Key=file_key)
    return resp["Body"].read()

def get_text(file_key: str, bucket_name: str = bucket_name, encoding: str = "utf-8") -> str:
    raw = get_object_bytes(file_key, bucket_name)
    return raw.decode(encoding)


def delete_object(file_key: str, bucket_name: str = bucket_name):
    s3 = _get_s3_client()
    s3.delete_object(Bucket=bucket_name, Key=file_key)


def generate_presigned_url(
    file_key: str,
    bucket_name: str = bucket_name,
    expires_in: int = 3600,
    method: str = "get_object",
):
    s3 = _get_s3_client()
    return s3.generate_presigned_url(
        ClientMethod=method,
        Params={"Bucket": bucket_name, "Key": file_key},
        ExpiresIn=expires_in,
    )


# -----------------------
# JSON
# -----------------------

def upload_json(
    file_key: str,
    data: Union[Dict[str, Any], List[Any]],
    bucket_name: str = bucket_name,
    content_type: str = "application/json",
    **json_kwargs
):
    json_str = json.dumps(data, **json_kwargs)
    upload_bytes(
        file_key=file_key,
        data=json_str.encode("utf-8"),
        bucket_name=bucket_name,
        content_type=content_type
    )


def get_json(file_key: str, bucket_name: str = bucket_name):
    text = get_text(file_key, bucket_name)
    return json.loads(text)


# -----------------------
# CSV
# -----------------------

def get_csv_from_s3(file_key: str, bucket_name: str = bucket_name):
    text = get_text(file_key, bucket_name)
    csv_reader = csv.DictReader(text.splitlines())
    return list(csv_reader)


def upload_csv_to_s3(
    file_key: str,
    rows: List[Dict[str, Any]],
    bucket_name: str = bucket_name,
    fieldnames: Optional[List[str]] = None,
    content_type: str = "text/csv",
):
    if not rows:
        raise ValueError("CSV rows cannot be empty")

    if fieldnames is None:
        fieldnames = sorted({k for row in rows for k in row})

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    upload_bytes(
        file_key=file_key,
        data=buffer.getvalue().encode("utf-8"),
        bucket_name=bucket_name,
        content_type=content_type,
    )


# -----------------------
# Common File Types
# -----------------------

def upload_pdf(file_key: str, data: bytes):
    upload_bytes(file_key=file_key, data=data, content_type="application/pdf")


def upload_png(file_key: str, data: bytes):
    upload_bytes(file_key=file_key, data=data, content_type="image/png")


def upload_jpeg(file_key: str, data: bytes):
    upload_bytes(file_key=file_key, data=data, content_type="image/jpeg")


def upload_mp4(file_key: str, data: bytes):
    upload_bytes(file_key=file_key, data=data, content_type="video/mp4")


def upload_xlsx(file_key: str, data: bytes):
    upload_bytes(
        file_key=file_key,
        data=data,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
