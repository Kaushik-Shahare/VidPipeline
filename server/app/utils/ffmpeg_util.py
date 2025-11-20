import ffmpeg
import os
import logging

logging.basicConfig(level=logging.INFO, filename='logs/ffmpeg.log')

def transcode_hls_profile(input_file, output_dir, profile: str):
    """Transcode a single HLS variant (profile) into a profile-specific folder.

    Output layout:
    {output_dir}/hls/master.m3u8
    {output_dir}/hls/{profile}/playlist.m3u8
    """
    logging.info(f"Starting HLS transcoding for profile={profile} for {input_file} into {output_dir}")

    profiles = {
        '144p': {'scale': '256:144', 'b:v': '200k', 'resolution': '256x144'},
        '360p': {'scale': '640:360', 'b:v': '800k', 'resolution': '640x360'},
        '480p': {'scale': '854:480', 'b:v': '1500k', 'resolution': '854x480'},
        '720p': {'scale': '1280:720', 'b:v': '3000k', 'resolution': '1280x720'},
        '1080p': {'scale': '1920:1080', 'b:v': '5000k', 'resolution': '1920x1080'},
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
        # Use software encoder libx264 for broader compatibility; hardware fallback can be added
        (
            ffmpeg
            .input(input_file)
            .output(
                playlist_path,
                vf=f"scale={info['scale']}",
                format='hls',
                hls_time=6,
                hls_playlist_type='vod',
                hls_segment_filename=segment_pattern,
                **{'b:v': info['b:v'], 'c:v': 'libx264', 'c:a': 'aac'}
            )
            .run(overwrite_output=True)
        )
    except ffmpeg.Error as e:
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
        # single image output
        ffmpeg.input(input_file, ss=time_offset).output(output_file, **{'vframes': 1}).run(overwrite_output=True)
        logging.info(f"Thumbnail generated: {output_file}")
    except ffmpeg.Error as e:
        logging.error(f"Thumbnail generation failed: {e}")
        raise

    return output_file
