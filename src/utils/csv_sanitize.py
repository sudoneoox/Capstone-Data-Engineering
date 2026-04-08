from pathlib import Path
import logging
import csv

logger = logging.getLogger(__name__)


def sanitize_csv_file(path: Path) -> None:
    """
    Normalize CSV to avoid malformed quote / parsing issues.
    - Removes null bytes
    - Standardizes quoting
    - Rewrites file safely
    """

    logger.info("Sanitizing CSV: %s", path)

    tmp_path = path.with_suffix(".cleaned")

    with (
        open(path, "r", encoding="utf-8", errors="replace") as infile,
        open(tmp_path, "w", newline="", encoding="utf-8") as outfile,
    ):

        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

        expected_len = None

        for i, row in enumerate(reader):
            # Remove null bytes if present
            row = [col.replace("\x00", "") if col else col for col in row]

            if i == 0:
                expected_len = len(row)

            # Detect broken rows
            if expected_len is not None and len(row) != expected_len:
                logger.warning(
                    "Row %d has %d columns (expected %d): %s",
                    i,
                    len(row),
                    expected_len,
                    row[:5],  # don't spam full row
                )
                continue

            writer.writerow(row)

    # Backup original file
    backup_path = path.with_suffix(".bak")
    if not backup_path.exists():
        path.rename(backup_path)

    tmp_path.rename(path)

    logger.info("Sanitized CSV written: %s (backup at %s)", path, backup_path)


def sanitize_dataset_csvs(dataset_dir: Path) -> None:
    """
    Run CSV sanitization over all CSV files in dataset directory.
    """
    for csv_file in dataset_dir.rglob("*.csv"):
        try:
            sanitize_csv_file(csv_file)
        except Exception:
            logger.exception("Failed to sanitize %s", csv_file)
