import subprocess
import logging
import os

logger = logging.getLogger(__name__)


def scan_file_for_viruses(file_path: str) -> dict:
    """Scan a file for viruses using ClamAV.
    
    Requires ClamAV to be installed and clamd daemon running:
    - macOS: brew install clamav
    - Ubuntu: apt-get install clamav clamav-daemon
    - Start daemon: clamd or freshclam (updates virus definitions)
    
    Args:
        file_path: Path to file to scan
        
    Returns:
        dict with keys:
        - clean: bool - True if no viruses found
        - scan_result: str - Scan output
        - threat: str | None - Name of detected threat if any
        
    Raises:
        RuntimeError: If ClamAV is not installed or scanning fails
    """
    logger.info(f"Starting virus scan for: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Check if clamscan is available
    try:
        subprocess.run(
            ['which', 'clamscan'],
            check=True,
            capture_output=True
        )
    except subprocess.CalledProcessError:
        logger.warning("ClamAV (clamscan) not found. Skipping virus scan.")
        return {
            'clean': True,
            'scan_result': 'ClamAV not installed - scan skipped',
            'threat': None
        }
    
    try:
        # Run clamscan on the file
        # --no-summary: Don't print summary
        # --infected: Only print infected files
        result = subprocess.run(
            ['clamscan', '--no-summary', file_path],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        output = result.stdout + result.stderr
        logger.info(f"ClamAV scan result for {file_path}: {output}")
        
        # Return code 0 = clean, 1 = virus found, 2 = error
        if result.returncode == 0:
            return {
                'clean': True,
                'scan_result': output.strip() or 'No threats found',
                'threat': None
            }
        elif result.returncode == 1:
            # Virus detected - extract threat name from output
            # Format: "filename: Threat.Name FOUND"
            threat_name = None
            for line in output.split('\n'):
                if 'FOUND' in line:
                    parts = line.split(':')
                    if len(parts) >= 2:
                        threat_name = parts[1].replace('FOUND', '').strip()
                        break
            
            return {
                'clean': False,
                'scan_result': output.strip(),
                'threat': threat_name or 'Unknown threat'
            }
        else:
            # Scan error
            raise RuntimeError(f"ClamAV scan failed with code {result.returncode}: {output}")
            
    except subprocess.TimeoutExpired:
        raise RuntimeError("Virus scan timed out after 5 minutes")
    except Exception as e:
        logger.error(f"Virus scan error for {file_path}: {e}")
        raise RuntimeError(f"Virus scan failed: {str(e)}") from e
