from pathlib import Path
import shutil
import logging

logger = logging.getLogger(__name__)

DOWNLOAD_ROOT = Path("tmp_downloads")


def cleanup_job_files(job_id: str):
    """
    Remove temp and partial files for a job safely.
    """
    if not DOWNLOAD_ROOT.exists():
        return

    for path in DOWNLOAD_ROOT.glob(f"{job_id}*"):
        try:
            if path.is_file():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)

            logger.info("Cleaned up file %s", path)

        except Exception:
            logger.exception("Failed to clean up %s", path)
