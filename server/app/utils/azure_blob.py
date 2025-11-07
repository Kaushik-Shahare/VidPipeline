import base64
import os
from functools import lru_cache
from typing import List

from azure.core.exceptions import AzureError, ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
import mimetypes
import datetime


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable '{name}' is required for Azure Blob Storage")
    return value


@lru_cache(maxsize=1)
def _blob_service_client() -> BlobServiceClient:
    connection_string = _require_env("AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(connection_string)


def _container_name() -> str:
    return os.getenv("AZURE_STORAGE_CONTAINER", "videos")


def _base_url() -> str:
    return os.getenv("AZURE_STORAGE_URL", "")


def _container_client():
    client = _blob_service_client().get_container_client(_container_name())
    try:
        client.create_container()
    except ResourceExistsError:
        pass
    return client


def _blob_name(video_hash: str) -> str:
    return f"{video_hash}/source.mp4"


def _block_id(chunk_index: int) -> str:
    return base64.b64encode(f"{chunk_index:06}".encode()).decode()


def stage_video_chunk(video_hash: str, chunk_index: int, chunk_bytes: bytes) -> str:
    """Stage a video chunk as a block within Azure Blob Storage."""

    blob_client = _container_client().get_blob_client(_blob_name(video_hash))
    try:
        blob_client.stage_block(block_id=_block_id(chunk_index), data=chunk_bytes)
    except AzureError as exc:
        raise RuntimeError("Failed to upload chunk to Azure Blob Storage") from exc
    return blob_client.blob_name


def commit_video_upload(video_hash: str, total_chunks: int) -> str:
    """Commit the staged blocks for a video upload and return the blob URL."""

    blob_client = _container_client().get_blob_client(_blob_name(video_hash))
    block_list: List[str] = [_block_id(i) for i in range(total_chunks)]
    try:
        blob_client.commit_block_list(
            block_list,
            content_settings=ContentSettings(content_type="video/mp4"),
        )
    except AzureError as exc:
        raise RuntimeError("Failed to finalize video upload in Azure Blob Storage") from exc
    base_url = _base_url().rstrip("/")
    if base_url:
        return f"{base_url}/{blob_client.blob_name}"
    return blob_client.url


def download_video_blob(video_hash: str, download_path: str) -> str:
    """Download the committed video blob to a local path."""

    blob_client = _container_client().get_blob_client(_blob_name(video_hash))
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    try:
        with open(download_path, "wb") as file_obj:
            stream = blob_client.download_blob()
            file_obj.write(stream.readall())
    except AzureError as exc:
        raise RuntimeError("Failed to download video from Azure Blob Storage") from exc
    return download_path


def _parse_conn_string() -> dict:
    parts = {}
    conn = _require_env("AZURE_STORAGE_CONNECTION_STRING")
    for kv in conn.split(";"):
        if not kv:
            continue
        if "=" in kv:
            k, v = kv.split("=", 1)
            parts[k] = v
    return parts


def _guess_content_type(path: str) -> str:
    ctype, _ = mimetypes.guess_type(path)
    return ctype or "application/octet-stream"


def upload_file(video_hash: str, local_path: str, blob_path: str) -> str:
    client = _container_client()
    with open(local_path, "rb") as f:
        client.upload_blob(
            name=blob_path,
            data=f,
            overwrite=True,
            content_settings=ContentSettings(content_type=_guess_content_type(local_path)),
        )
    base_url = _base_url().rstrip("/")
    return f"{base_url}/{blob_path}" if base_url else client.get_blob_client(blob_path).url


def upload_directory(video_hash: str, local_dir: str, dest_prefix: str = "", exclude: List[str] | None = None) -> None:
    """Upload a local directory tree to Azure Blob under the video's folder.

    - If dest_prefix is empty, files are uploaded directly under `{video_hash}/`.
    - The `exclude` list contains relative paths (from `local_dir`) to skip.
    """
    # Normalize exclusion list to a set of POSIX-style relative paths
    exclude_set = set((p.replace("\\", "/") for p in (exclude or [])))

    # Normalize prefix (avoid double slashes when empty)
    norm_prefix = dest_prefix.strip("/")

    for root, _dirs, files in os.walk(local_dir):
        for fname in files:
            lp = os.path.join(root, fname)
            rel = os.path.relpath(lp, local_dir).replace("\\", "/")

            # Skip excluded files
            if rel in exclude_set:
                continue

            if norm_prefix:
                blob_path = f"{video_hash}/{norm_prefix}/{rel}"
            else:
                blob_path = f"{video_hash}/{rel}"

            upload_file(video_hash, lp, blob_path)


def signed_url(blob_path: str, minutes_valid: int = 120) -> str:
    parts = _parse_conn_string()
    account = parts.get("AccountName")
    key = parts.get("AccountKey")
    if not account or not key:
        raise RuntimeError("Invalid Azure connection string: missing AccountName/AccountKey")

    sas = generate_blob_sas(
        account_name=account,
        container_name=_container_name(),
        blob_name=blob_path,
        account_key=key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes_valid),
    )
    base = _base_url().rstrip("/")
    base_url = f"{base}/{blob_path}" if base else _container_client().get_blob_client(blob_path).url
    return f"{base_url}?{sas}"


