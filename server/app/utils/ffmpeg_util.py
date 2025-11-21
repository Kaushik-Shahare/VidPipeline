import ffmpeg
import os
import logging
import subprocess
import json

logging.basicConfig(level=logging.INFO, filename='logs/ffmpeg.log')


def extract_video_metadata(video_path: str) -> dict:
    """Extract video metadata using ffprobe.
    
    Returns dictionary with:
    - width: int - Video width in pixels
    - height: int - Video height in pixels  
    - duration: float - Duration in seconds
    - codec: str - Video codec name (e.g., 'h264', 'hevc')
    - format: str - Container format (e.g., 'mov,mp4,m4a,3gp,3g2,mj2')
    - mime_type: str - Detected MIME type (e.g., 'video/mp4')
    - bit_rate: int - Overall bit rate in bits/s
    
    Raises:
        RuntimeError: If ffprobe fails or video is invalid
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Run ffprobe to get JSON metadata
        probe = ffmpeg.probe(video_path)
        
        # Get video stream (first video stream)
        video_stream = next(
            (stream for stream in probe['streams'] if stream['codec_type'] == 'video'),
            None
        )
        
        if not video_stream:
            raise RuntimeError("No video stream found in file")
        
        # Extract format info
        format_info = probe.get('format', {})
        
        # Map format to MIME type
        format_name = format_info.get('format_name', '').lower()
        mime_type_map = {
            'mov,mp4,m4a,3gp,3g2,mj2': 'video/mp4',
            'matroska,webm': 'video/x-matroska',
            'avi': 'video/x-msvideo',
            'webm': 'video/webm',
            'mpeg': 'video/mpeg',
            'quicktime': 'video/quicktime',
        }
        
        mime_type = 'video/mp4'  # default
        for fmt_key, mime in mime_type_map.items():
            if fmt_key in format_name:
                mime_type = mime
                break
        
        metadata = {
            'width': int(video_stream.get('width', 0)),
            'height': int(video_stream.get('height', 0)),
            'duration': float(format_info.get('duration', 0)),
            'codec': video_stream.get('codec_name', 'unknown'),
            'format': format_name,
            'mime_type': mime_type,
            'bit_rate': int(format_info.get('bit_rate', 0)),
        }
        
        logger.info(f"Extracted metadata from {video_path}: {metadata}")
        return metadata
        
    except ffmpeg.Error as e:
        stderr = e.stderr.decode() if e.stderr else str(e)
        logger.error(f"ffprobe failed for {video_path}: {stderr}")
        raise RuntimeError(f"Failed to extract video metadata: {stderr}") from e
    except Exception as e:
        logger.error(f"Metadata extraction failed for {video_path}: {e}")
        raise RuntimeError(f"Failed to extract video metadata: {str(e)}") from e


def compress_video(input_path: str, output_path: str, target_bitrate: str = '2500k') -> dict:
    """Compress video using ffmpeg with efficient H.264 encoding.
    
    This reduces file size while maintaining acceptable quality.
    Useful for reducing storage costs and processing time.
    
    Args:
        input_path: Path to input video file
        output_path: Path to save compressed video
        target_bitrate: Target video bitrate (e.g., '2500k', '5000k')
                       Lower = smaller file, lower quality
                       
    Returns:
        dict with compression stats:
        - original_size: int - Original file size in bytes
        - compressed_size: int - Compressed file size in bytes
        - compression_ratio: float - Ratio (e.g., 0.65 = 65% of original)
        - saved_bytes: int - Bytes saved
        
    Raises:
        RuntimeError: If compression fails
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Compressing video: {input_path} -> {output_path} (target bitrate: {target_bitrate})")
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    original_size = os.path.getsize(input_path)
    
    try:
        # Use ffmpeg to compress with H.264
        # Fast preset for quick compression before heavy transcoding
        cmd = (
            ffmpeg
            .input(input_path)
            .output(
                output_path,
                **{
                    'c:v': 'libx264',
                    'preset': 'veryfast',  # Much faster encoding (was 'medium')
                    'crf': '28',  # Higher CRF = lower quality but faster (was 23)
                    'b:v': target_bitrate,
                    'maxrate': target_bitrate,
                    'c:a': 'aac',
                    'b:a': '128k',
                    'movflags': '+faststart',  # Enable streaming
                    'threads': '0'  # Use all available cores
                }
            )
            .overwrite_output()
            .compile()
        )
        
        logger.info(f"Running compression: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Wait for compression with timeout (max 45 minutes to match Celery limit)
        try:
            stdout, stderr = process.communicate(timeout=2700)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            raise RuntimeError("Video compression timed out after 45 minutes")
        
        if process.returncode != 0:
            logger.error(f"FFmpeg compression failed:\nSTDERR:\n{stderr}")
            raise RuntimeError(f"Video compression failed: {stderr}")
        
        # Calculate compression stats
        compressed_size = os.path.getsize(output_path)
        compression_ratio = compressed_size / original_size
        saved_bytes = original_size - compressed_size
        
        stats = {
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': round(compression_ratio, 3),
            'saved_bytes': saved_bytes,
            'saved_mb': round(saved_bytes / (1024 * 1024), 2),
            'compression_percent': round((1 - compression_ratio) * 100, 1)
        }
        
        logger.info(f"Compression complete: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Video compression failed: {e}")
        # Clean up output file if it exists
        if os.path.exists(output_path):
            os.unlink(output_path)
        raise RuntimeError(f"Video compression failed: {str(e)}") from e


def transcode_hls_profile(input_file, output_dir, profile: str):
    """Transcode a single HLS variant (profile) into a profile-specific folder.

    Output layout:
    {output_dir}/hls/{profile}/playlist.m3u8
    """
    logging.info(f"Starting HLS transcoding for profile={profile} for {input_file} into {output_dir}")

    profiles = {
        '144p': {'scale': '256:144', 'b:v': '200k', 'resolution': '256x144', 'preset': 'ultrafast', 'fps': None},
        '360p': {'scale': '640:360', 'b:v': '800k', 'resolution': '640x360', 'preset': 'ultrafast', 'fps': None},
        '480p': {'scale': '854:480', 'b:v': '1200k', 'resolution': '854x480', 'preset': 'ultrafast', 'fps': None},
        '720p': {'scale': '1280:720', 'b:v': '2000k', 'resolution': '1280x720', 'preset': 'ultrafast', 'fps': 30},
        '1080p': {'scale': '1920:1080', 'b:v': '2500k', 'resolution': '1920x1080', 'preset': 'ultrafast', 'fps': 30},
    }

    if profile not in profiles:
        raise ValueError(f"Unknown profile '{profile}'")

    info = profiles[profile]

    hls_root = os.path.join(output_dir, 'hls')
    profile_dir = os.path.join(hls_root, profile)
    os.makedirs(profile_dir, exist_ok=True)

    playlist_path = os.path.join(profile_dir, 'playlist.m3u8')
    segment_pattern = os.path.join(profile_dir, 'segment_%03d.ts')

    try:
        # Build ffmpeg command with ARM-optimized settings
        # Build video filter: scale + optional fps limit
        vf_parts = [f"scale={info['scale']}"]
        if info.get('fps'):
            vf_parts.append(f"fps={info['fps']}")
        vf_string = ','.join(vf_parts)
        
        output_options = {
            'c:v': 'libx264',
            'preset': info['preset'],
            'b:v': info['b:v'],
            'c:a': 'aac',
            'b:a': '128k',
            'threads': '0',
            'movflags': '+faststart'
        }
        
        cmd = (
            ffmpeg
            .input(input_file)
            .output(
                playlist_path,
                vf=vf_string,
                format='hls',
                hls_time=6,
                hls_playlist_type='vod',
                hls_segment_filename=segment_pattern,
                **output_options
            )
            .overwrite_output()
            .compile()
        )
        
        # Run ffmpeg as subprocess with timeout protection
        logging.info(f"Running FFmpeg command for {profile}: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Wait with timeout (profile-specific)
        timeout_map = {
            '144p': 300,   # 5 minutes
            '360p': 600,   # 10 minutes
            '480p': 900,   # 15 minutes
            '720p': 1500,  # 25 minutes
            '1080p': 3000, # 50 minutes
        }
        
        try:
            stdout, stderr = process.communicate(timeout=timeout_map.get(profile, 1800))
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            raise RuntimeError(f"FFmpeg timeout after {timeout_map.get(profile, 1800)}s for {profile}")
        
        if process.returncode != 0:
            logging.error(f"FFmpeg failed for {profile}:\nSTDERR:\n{stderr}")
            raise RuntimeError(f"FFmpeg transcode failed for {profile}: {stderr}")
        
        logging.info(f"FFmpeg completed successfully for {profile}")
        
    except Exception as e:
        logging.error(f"HLS transcode for profile {profile} failed: {e}")
        raise

    logging.info(f"HLS profile {profile} completed: {playlist_path}")
    return playlist_path


def generate_hls_master_playlist(output_dir, profiles_available=None):
    """Generate the HLS master playlist after all profiles are completed.
    
    Args:
        output_dir: Base output directory (e.g., /path/to/video_hash)
        profiles_available: List of profile names that exist (e.g., ['144p', '360p'])
                           If None, assumes all standard profiles exist
    """
    logging.info(f"Generating HLS master playlist in {output_dir}")
    
    profiles = {
        '144p': {'scale': '256:144', 'b:v': '200k', 'resolution': '256x144', 'bandwidth': 200000},
        '360p': {'scale': '640:360', 'b:v': '800k', 'resolution': '640x360', 'bandwidth': 800000},
        '480p': {'scale': '854:480', 'b:v': '1500k', 'resolution': '854x480', 'bandwidth': 1500000},
        '720p': {'scale': '1280:720', 'b:v': '3000k', 'resolution': '1280x720', 'bandwidth': 3000000},
        '1080p': {'scale': '1920:1080', 'b:v': '5000k', 'resolution': '1920x1080', 'bandwidth': 5000000},
    }
    
    hls_root = os.path.join(output_dir, 'hls')
    os.makedirs(hls_root, exist_ok=True)
    
    master_playlist = os.path.join(hls_root, 'master.m3u8')
    
    # If no profiles specified, check which ones actually exist
    if profiles_available is None:
        profiles_available = []
        for profile in ['144p', '360p', '480p', '720p', '1080p']:
            profile_playlist = os.path.join(hls_root, profile, 'playlist.m3u8')
            if os.path.exists(profile_playlist):
                profiles_available.append(profile)
    
    with open(master_playlist, 'w') as f:
        f.write('#EXTM3U\n')
        f.write('#EXT-X-VERSION:3\n')
        
        # Write entries for available profiles, highest to lowest
        for p in ['1080p', '720p', '480p', '360p', '144p']:
            if p in profiles_available and p in profiles:
                info = profiles[p]
                f.write(f"#EXT-X-STREAM-INF:BANDWIDTH={info['bandwidth']},RESOLUTION={info['resolution']}\n")
                f.write(f"{p}/playlist.m3u8\n")
    
    logging.info(f"HLS master playlist generated: {master_playlist}")
    return master_playlist

# def transcode_to_dash(input_file, output_dir):
#     """DEPRECATED: DASH transcoding is disabled in favor of HLS-only approach.
#     
#     This function is kept for reference but is not used in the current pipeline.
#     """
#     logging.info(f"Starting DASH transcoding for {input_file} into {output_dir}")
#     dash_dir = os.path.join(output_dir, "dash")
#     os.makedirs(dash_dir, exist_ok=True)
#     output_path = os.path.join(dash_dir, "manifest.mpd")
#
#     # Prepare parameters: keep hardware-encoder params minimal and compatible
#     output_params_hw = {
#         'c:v': 'h264_videotoolbox',  # HW accel on macOS
#         'c:a': 'aac',
#         'b:a': '128k',
#         'b:v': '3000k',
#         'maxrate': '3000k',
#         'bufsize': '6000k',
#         'f': 'dash',
#         'seg_duration': 4,
#         'use_template': 1,
#         'use_timeline': 1
#     }
#     # Software encoder (libx264) can use GOP/scene-change/b-frames tuning
#     output_params_sw = {
#         'c:v': 'libx264',
#         'c:a': 'aac',
#         'b:a': '128k',
#         'bf': 1,
#         'keyint_min': 120,
#         'g': 120,
#         'sc_threshold': 0,
#         'b:v': '3000k',
#         'maxrate': '3000k',
#         'bufsize': '6000k',
#         'f': 'dash',
#         'seg_duration': 4,
#         'use_template': 1,
#         'use_timeline': 1
#     }
#
#     # Call ffmpeg with dict unpacking, with hw->sw fallback
#     try:
#         ffmpeg.input(input_file).output(output_path, **output_params_hw).run(overwrite_output=True)
#     except ffmpeg.Error as e:
#         logging.warning(f"DASH hw-encode failed, falling back to libx264: {e}")
#         ffmpeg.input(input_file).output(output_path, **output_params_sw).run(overwrite_output=True)
#
#     logging.info(f"DASH transcoding completed: {output_path}")
#     return output_path

def video_thumbnail(input_file, output_dir, time_offset="00:00:01"):
    logging.info(f"Generating thumbnail for {input_file} at {time_offset}")

    output_file = os.path.join(output_dir, 'thumbnail.jpg')

    try:
        # Build command
        cmd = (
            ffmpeg.input(input_file, ss=time_offset)
            .output(output_file, vframes=1)
            .overwrite_output()
            .compile()
        )
        
        # Run as subprocess
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            logging.error(f"Thumbnail generation failed:\nSTDERR:\n{stderr}")
            raise RuntimeError(f"Thumbnail generation failed: {stderr}")
            
        logging.info(f"Thumbnail generated: {output_file}")
    except Exception as e:
        logging.error(f"Thumbnail generation failed: {e}")
        raise

    return output_file
