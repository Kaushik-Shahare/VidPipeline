import base64
import logging
import os
from functools import lru_cache
from typing import Dict, List

from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.blob import (
    generate_container_sas,
    ContainerSasPermissions,
    CorsRule,
)
import mimetypes
import datetime
from urllib.parse import urlparse


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

from azure.core.exceptions import ResourceExistsError, HttpResponseError

@lru_cache(maxsize=1)
def _container_client():
    client = _blob_service_client().get_container_client(_container_name())
    try:
        client.create_container()
    except ResourceExistsError:
        pass
    except HttpResponseError as exc:
        # 409 means already exists — ignore; otherwise re-raise or log
        if getattr(exc, "status_code", None) == 409:
            pass
        else:
            logging.getLogger(__name__).exception("Failed creating container")
            raise
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


def upload_directory(video_hash: str, local_dir: str, dest_prefix: str = "", exclude: List[str] | None = None) -> Dict[str, Dict[str, str]]:
    """Upload a local directory tree to Azure Blob under the video's folder.

    - If dest_prefix is empty, files are uploaded directly under `{video_hash}/`.
    - The `exclude` list contains relative paths (from `local_dir`) to skip.
    """
    # Normalize exclusion list to a set of POSIX-style relative paths
    exclude_set = set((p.replace("\\", "/") for p in (exclude or [])))

    # Normalize prefix (avoid double slashes when empty)
    norm_prefix = dest_prefix.strip("/")

    uploaded: Dict[str, Dict[str, str]] = {}

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

            url = upload_file(video_hash, lp, blob_path)
            uploaded[rel] = {"blob_path": blob_path, "url": url}

    return uploaded


def _full_blob_url(blob_path: str) -> str:
    base = _base_url().rstrip("/")
    container = _container_name()

    if not base:
        return _container_client().get_blob_client(blob_path).url

    # Ensure the container segment is present exactly once
    if base.endswith(f"/{container}"):
        return f"{base}/{blob_path}"

    return f"{base}/{container}/{blob_path}"


def generate_container_sas_token(minutes_valid: int = 120) -> str:
    ensure_blob_cors()
    parts = _parse_conn_string()
    account = parts.get("AccountName")
    key = parts.get("AccountKey")
    if not account or not key:
        raise RuntimeError("Invalid Azure connection string: missing AccountName/AccountKey")

    return generate_container_sas(
        account_name=account,
        container_name=_container_name(),
        account_key=key,
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes_valid),
    )


def signed_url(blob_path: str, minutes_valid: int = 120, sas_token: str | None = None) -> str:
    token = sas_token or generate_container_sas_token(minutes_valid)
    base_url = _full_blob_url(blob_path)
    if not token:
        return base_url
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{token}"



def blob_path_from_location(location: str | None) -> str:
    """Extract the blob path (relative to the container) from a stored location."""

    if not location:
        return ""

    location = location.strip()

    # If no scheme, treat as already being the blob path
    if "://" not in location:
        path = location.lstrip("/")
        # Handle legacy local paths like media/uploads/<hash>/...
        legacy_prefix = "media/uploads/"
        if path.startswith(legacy_prefix):
            path = path[len(legacy_prefix):]
        return path

    parsed = urlparse(location)
    path = parsed.path.lstrip("/")

    container = _container_name()
    container_prefix = f"{container}/"
    if path.startswith(container_prefix):
        return path[len(container_prefix):]

    base = _base_url().rstrip("/")
    if base and location.startswith(f"{base}/"):
        return location[len(base) + 1 :]

    return path


_CORS_CONFIGURED = False


def ensure_blob_cors() -> None:
    global _CORS_CONFIGURED
    if _CORS_CONFIGURED:
        return

    origins_env = os.getenv("AZURE_BLOB_CORS_ORIGINS") or "*"
    allowed_origins = [origin.strip() for origin in origins_env.split(",") if origin.strip()] or ["*"]

    cors_rule = CorsRule(
        allowed_origins=allowed_origins,
        allowed_methods=["GET","HEAD","OPTIONS","PUT"],
        allowed_headers="*",
        exposed_headers="*",
        max_age_in_seconds=3600,
    )

    service_client = _blob_service_client()

    try:
        current_props = service_client.get_service_properties()
        existing_rules = current_props.get("cors") or []
        # If an identical rule already exists, skip update
        for rule in existing_rules:
            if (
                rule.allowed_origins == cors_rule.allowed_origins
                and rule.allowed_methods == cors_rule.allowed_methods
                and rule.allowed_headers == cors_rule.allowed_headers
                and rule.exposed_headers == cors_rule.exposed_headers
                and rule.max_age_in_seconds == cors_rule.max_age_in_seconds
            ):
                _CORS_CONFIGURED = True
                return

        new_rules = list(existing_rules) + [cors_rule]
        service_client.set_service_properties(cors=new_rules)
        _CORS_CONFIGURED = True
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Unable to configure Azure Blob CORS: %s", exc
        )


def generate_block_upload_sas(video_hash: str, chunk_index: int, minutes_valid: int = 30) -> str:
    """Generate a presigned URL for uploading a specific block/chunk directly to Azure Blob.
    
    This enables client-side direct uploads without data passing through the backend.
    The URL grants write-only permission for a specific block ID.
    """
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions
    from urllib.parse import quote
    
    ensure_blob_cors()
    parts = _parse_conn_string()
    account = parts.get("AccountName")
    key = parts.get("AccountKey")
    if not account or not key:
        raise RuntimeError("Invalid Azure connection string: missing AccountName/AccountKey")
    
    blob_name = _blob_name(video_hash)
    block_id = _block_id(chunk_index)
    
    # Generate SAS token with ALL necessary permissions for block operations
    # add=True is required for PutBlock (staging blocks)
    # write=True is required for PutBlockList (committing blocks)
    # create=True allows creating the blob if it doesn't exist
    sas_token = generate_blob_sas(
        account_name=account,
        container_name=_container_name(),
        blob_name=blob_name,
        account_key=key,
        permission=BlobSasPermissions(read=False, add=True, create=True, write=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes_valid),
    )
    
    # Build the presigned URL with URL-encoded block_id parameter
    base_url = _full_blob_url(blob_name)
    # URL encode the block_id to handle special characters in base64
    encoded_block_id = quote(block_id, safe='')
    return f"{base_url}?comp=block&blockid={encoded_block_id}&{sas_token}"


def verify_block_staged(video_hash: str, chunk_index: int) -> bool:
    """Verify if a specific block has been successfully staged."""
    try:
        blob_client = _container_client().get_blob_client(_blob_name(video_hash))
        # Get uncommitted blocks
        block_list_result = blob_client.get_block_list(block_list_type="uncommitted")
        
        # Azure SDK returns tuple: (committed_blocks, uncommitted_blocks)
        if isinstance(block_list_result, tuple):
            _, uncommitted = block_list_result
        else:
            uncommitted = getattr(block_list_result, 'uncommitted_blocks', [])
        
        expected_block_id = _block_id(chunk_index)
        return any(block.id == expected_block_id for block in uncommitted)
    except AzureError:
        return False


def get_staged_chunks(video_hash: str) -> list[int]:
    """Get list of chunk indices that have been successfully staged.
    
    Returns:
        List of chunk indices (e.g., [0, 1, 2, 5, 7])
    """
    import time
    
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            blob_client = _container_client().get_blob_client(_blob_name(video_hash))
            
            # Check if blob exists before getting block list
            try:
                block_list_result = blob_client.get_block_list(block_list_type="uncommitted")
            except ResourceNotFoundError:
                return []
            except AzureError as e:
                # Blob doesn't exist yet (no chunks uploaded) - return empty list
                error_code = getattr(e, 'error_code', None)
                if error_code == 'BlobNotFound':
                    return []
                # For other errors, raise to trigger retry
                raise
            
            # Azure SDK returns tuple: (committed_blocks, uncommitted_blocks)
            # We only care about uncommitted blocks (chunks not yet finalized)
            if isinstance(block_list_result, tuple):
                _, uncommitted = block_list_result
            else:
                uncommitted = getattr(block_list_result, 'uncommitted_blocks', [])
            
            # Extract chunk indices from block IDs
            staged_indices = []
            for block in uncommitted:
                try:
                    # Decode base64 block_id back to chunk index
                    decoded = base64.b64decode(block.id).decode()
                    chunk_index = int(decoded)
                    staged_indices.append(chunk_index)
                except (ValueError, UnicodeDecodeError):
                    continue
            
            return sorted(staged_indices)
            
        except AzureError as e:
            if attempt < max_retries - 1:
                # Exponential backoff
                time.sleep(retry_delay * (2 ** attempt))
                continue
            # Last attempt failed, return empty list rather than crash
            logging.getLogger(__name__).warning(
                f"Failed to get staged chunks for {video_hash} after {max_retries} attempts: {e}"
            )
            return []
    
    return []


def get_uploaded_chunks(video_hash: str) -> list[int]:
    """Get list of chunk indices that have already been uploaded to Azure.
    
    This enables resumable uploads - client can skip already-uploaded chunks.
    
    Args:
        video_hash: Unique identifier for the video
    
    Returns:
        Sorted list of chunk indices (e.g., [0, 1, 2, 5, 7])
    """
    container_client = _container_client()
    prefix = f"{video_hash}/chunk_"
    
    try:
        # List all blobs with the chunk prefix
        blobs = container_client.list_blobs(name_starts_with=prefix)
        uploaded_indices = []
        
        for blob in blobs:
            # Extract chunk index from blob name: {video_hash}/chunk_{index}
            blob_name = blob.name
            if blob_name.startswith(prefix):
                try:
                    chunk_index = int(blob_name[len(prefix):])
                    uploaded_indices.append(chunk_index)
                except ValueError:
                    # Skip blobs that don't match the pattern
                    continue
        
        return sorted(uploaded_indices)
    except AzureError as e:
        logging.getLogger(__name__).warning(
            f"Failed to list uploaded chunks for {video_hash}: {e}"
        )
        return []


async def upload_chunk_to_azure(video_hash: str, chunk_index: int, chunk_bytes: bytes) -> str:
    """Upload a video chunk as a separate blob in Azure Storage.
    
    Chunks are stored under: {video_hash}/chunk_{chunk_index}
    This allows for independent chunk uploads without block staging complexity.
    
    Args:
        video_hash: Unique identifier for the video
        chunk_index: Index of this chunk (0-based)
        chunk_bytes: Raw chunk data
    
    Returns:
        Blob path where chunk was stored
    """
    blob_path = f"{video_hash}/chunk_{chunk_index}"
    blob_client = _container_client().get_blob_client(blob_path)
    
    try:
        blob_client.upload_blob(
            chunk_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream")
        )
    except AzureError as exc:
        raise RuntimeError(f"Failed to upload chunk {chunk_index} to Azure: {str(exc)}") from exc
    
    return blob_path


async def merge_chunks_to_source(video_hash: str, total_chunks: int) -> str:
    """Merge all uploaded chunks into a single source.mp4 file.
    
    Downloads all chunks, concatenates them in order, and uploads the result
    as {video_hash}/source.mp4.
    
    Args:
        video_hash: Unique identifier for the video
        total_chunks: Total number of chunks to merge
    
    Returns:
        URL of the merged source.mp4 blob
    """
    import tempfile
    import os
    
    container_client = _container_client()
    source_blob_path = f"{video_hash}/source.mp4"
    
    # Create a temporary file to write merged content
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.mp4') as temp_file:
        temp_path = temp_file.name
        
        try:
            # Download and concatenate all chunks in order
            for chunk_index in range(total_chunks):
                chunk_blob_path = f"{video_hash}/chunk_{chunk_index}"
                chunk_blob_client = container_client.get_blob_client(chunk_blob_path)
                
                try:
                    # Download chunk data
                    chunk_data = chunk_blob_client.download_blob().readall()
                    temp_file.write(chunk_data)
                except ResourceNotFoundError:
                    raise RuntimeError(f"Chunk {chunk_index} not found in Azure. Upload may have failed.")
                except AzureError as exc:
                    raise RuntimeError(f"Failed to download chunk {chunk_index}: {str(exc)}") from exc
            
            # Close the temp file before uploading
            temp_file.close()
            
            # Upload merged file as source.mp4
            source_blob_client = container_client.get_blob_client(source_blob_path)
            with open(temp_path, 'rb') as merged_file:
                source_blob_client.upload_blob(
                    merged_file,
                    overwrite=True,
                    content_settings=ContentSettings(content_type="video/mp4")
                )
            
            # Clean up chunks after successful merge
            for chunk_index in range(total_chunks):
                chunk_blob_path = f"{video_hash}/chunk_{chunk_index}"
                try:
                    container_client.get_blob_client(chunk_blob_path).delete_blob()
                except Exception as e:
                    # Log but don't fail if cleanup fails
                    logging.getLogger(__name__).warning(
                        f"Failed to delete chunk {chunk_index} after merge: {e}"
                    )
            
            # Return the URL
            base_url = _base_url().rstrip("/")
            if base_url:
                return f"{base_url}/{source_blob_path}"
            return source_blob_client.url
            
        finally:
            # Always clean up temp file
            if os.path.exists(temp_path):
                os.unlink(temp_path)
