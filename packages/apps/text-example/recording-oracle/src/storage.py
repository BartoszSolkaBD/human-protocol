"""Module for interaction with S3 storage."""
import json
from dataclasses import dataclass
from io import StringIO, BytesIO
from pathlib import Path
from urllib.parse import urlparse

from basemodels import Manifest

from src.config import Config


@dataclass
class S3Info:
    host: str
    bucket_name: str
    object_name: str


def s3_info_from_url(s3_url):
    parsed = urlparse(s3_url)
    bucket_name, object_name = parsed.path[1:].split("/", maxsplit=1)
    host = parsed.netloc
    return S3Info(host, bucket_name, object_name)


def download_object(s3_url):
    client = Config.storage_config.client()
    s3_info = s3_info_from_url(s3_url)
    return client.get_object(s3_info.bucket_name, s3_info.object_name)


def download_manifest(manifest_url):
    manifest = download_object(manifest_url)
    return Manifest(**json.loads(manifest.data))


def download_raw_results(results_url):
    res = download_object(results_url)
    with BytesIO(res.data) as f:
        return [json.loads(line) for line in f.readlines()]


def upload_data(
    path: Path,
    client=Config.storage_config.client(),
    bucket_name: str = Config.storage_config.results_bucket_name,
    glob_pattern: str = "*.txt",
    content_type: str = "text/plain",
):
    files = []
    if path.is_file():
        files.append(path)
    elif path.is_dir():
        files.extend(path.glob(glob_pattern))

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    for file_path in files:
        client.fput_object(
            bucket_name=bucket_name,
            object_name=file_path.name,
            content_type=content_type,
            file_path=file_path,
        )