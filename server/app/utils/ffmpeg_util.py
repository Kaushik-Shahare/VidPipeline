import ffmpeg
import os
import logging

logging.basicConfig(level=logging.INFO, filename='logs/ffmpeg.log')

def transcode_to_hls(input_file, output_dir):
    logging.info(f"Starting HLS transcoding for {input_file} into {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    hls_dir = os.path.join(output_dir, "hls")
    os.makedirs(hls_dir, exist_ok=True)
    output_path = os.path.join(hls_dir, "playlist.m3u8")
    params_hw = {
        "c:v": "h264_videotoolbox",
        "c:a": "aac",
        "b:a": "128k",
        "hls_time": 6,
        "hls_playlist_type": "vod",
        "hls_segment_filename": os.path.join(hls_dir, "segment_%03d.ts"),
    }
    params_sw = dict(params_hw, **{"c:v": "libx264"})
    try:
        ffmpeg.input(input_file).output(output_path, **params_hw).run(overwrite_output=True)
    except ffmpeg.Error as e:
        logging.warning(f"HLS hw-encode failed, falling back to libx264: {e}")
        ffmpeg.input(input_file).output(output_path, **params_sw).run(overwrite_output=True)
    logging.info(f"HLS transcoding completed: {os.path.join(output_dir, 'hls', 'playlist.m3u8')}")

    return os.path.join(hls_dir, "playlist.m3u8")

def transcode_to_dash(input_file, output_dir):
    logging.info(f"Starting DASH transcoding for {input_file} into {output_dir}")
    dash_dir = os.path.join(output_dir, "dash")
    os.makedirs(dash_dir, exist_ok=True)
    output_path = os.path.join(dash_dir, "manifest.mpd")

    # Prepare parameters: keep hardware-encoder params minimal and compatible
    output_params_hw = {
        'c:v': 'h264_videotoolbox',  # HW accel on macOS
        'c:a': 'aac',
        'b:a': '128k',
        'b:v': '3000k',
        'maxrate': '3000k',
        'bufsize': '6000k',
        'f': 'dash',
        'seg_duration': 4,
        'use_template': 1,
        'use_timeline': 1
    }
    # Software encoder (libx264) can use GOP/scene-change/b-frames tuning
    output_params_sw = {
        'c:v': 'libx264',
        'c:a': 'aac',
        'b:a': '128k',
        'bf': 1,
        'keyint_min': 120,
        'g': 120,
        'sc_threshold': 0,
        'b:v': '3000k',
        'maxrate': '3000k',
        'bufsize': '6000k',
        'f': 'dash',
        'seg_duration': 4,
        'use_template': 1,
        'use_timeline': 1
    }

    # Call ffmpeg with dict unpacking, with hw->sw fallback
    try:
        ffmpeg.input(input_file).output(output_path, **output_params_hw).run(overwrite_output=True)
    except ffmpeg.Error as e:
        logging.warning(f"DASH hw-encode failed, falling back to libx264: {e}")
        ffmpeg.input(input_file).output(output_path, **output_params_sw).run(overwrite_output=True)

    logging.info(f"DASH transcoding completed: {output_path}")
    return output_path

def video_thumbnail(input_file, output_dir, time_offset="00:00:01"):
    logging.info(f"Generating thumbnail for {input_file} at {time_offset}")

    output_file = os.path.join(output_dir, 'thumbnail.jpg')

    try:
        # Use proper parameters for single image output
        ffmpeg.input(input_file, ss=time_offset).output(output_file, **{'vframes': 1}).run(overwrite_output=True)
        logging.info(f"Thumbnail generated: {output_file}")
    except ffmpeg.Error as e:
        logging.error(f"Thumbnail generation failed: {e}")
        raise

    return output_file
