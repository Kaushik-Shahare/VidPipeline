import ffmpeg
import os
import logging

logging.basicConfig(level=logging.INFO, filename='ffmpeg.log')

def transcode_to_hls(input_file, output_dir):
    logging.info(f"Starting HLS transcoding for {input_file} into {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    hls_dir = os.path.join(output_dir, "hls")
    os.makedirs(hls_dir, exist_ok=True)
   
    ffmpeg.input(input_file).output(
        os.path.join(hls_dir, "playlist.m3u8"),
        **{
            "c:v": "h264_videotoolbox",   # hardware accel on M1, use 'libx264' if issues
            "c:a": "aac",
            "b:a": "128k",
            "hls_time": 6,
            "hls_playlist_type": "vod",
        "hls_segment_filename": os.path.join(hls_dir, "segment_%03d.ts"),
        }
    ).run(overwrite_output=True)
    logging.info(f"HLS transcoding completed: {os.path.join(output_dir, 'hls', 'playlist.m3u8')}")

    return os.path.join(hls_dir, "playlist.m3u8")

def transcode_to_dash(input_file, output_dir):
    logging.info(f"Starting DASH transcoding for {input_file} into {output_dir}")
    dash_dir = os.path.join(output_dir, "dash")
    os.makedirs(dash_dir, exist_ok=True)
    output_path = os.path.join(dash_dir, "manifest.mpd")

    # Prepare parameters as a dictionary
    output_params = {
        'c:v': 'h264_videotoolbox',  # Use 'libx264' if hardware accel fails
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

    # Call ffmpeg with dict unpacking
    ffmpeg.input(input_file).output(output_path, **output_params).run(overwrite_output=True)

    logging.info(f"DASH transcoding completed: {output_path}")
    return output_path
