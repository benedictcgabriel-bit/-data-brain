import csv
import hashlib
import io
from typing import Any, Dict, List, Tuple


def compute_dataset_id(raw_content: bytes) -> str:
    """
    Computes a deterministic SHA-256 hash of the CSV content.
    Normalizes line endings (\r\n -> \n) so identical CSVs from different
    operating systems yield the exact same dataset_id.
    """
    normalized = raw_content.replace(b"\r\n", b"\n").strip()
    return hashlib.sha256(normalized).hexdigest()


def parse_and_validate_csv(
    raw_content: bytes, filename: str
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Validates CSV file type and content.
    Returns (dataset_id, rows) where rows is a list of dictionary representations of data rows.
    Raises ValueError for invalid, empty, or malformed inputs.
    """
    # 1. Filename extension validation
    if not filename.lower().endswith(".csv"):
        raise ValueError("Invalid file type: File must have a .csv extension.")

    # 2. Empty or corrupted byte validation
    if not raw_content or len(raw_content.strip()) == 0:
        raise ValueError("Invalid CSV: Uploaded file is empty.")

    if b"\x00" in raw_content:
        raise ValueError("Malformed CSV: Binary NUL bytes detected.")

    # 3. Decoding
    try:
        text_content = raw_content.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text_content = raw_content.decode("latin-1")
        except Exception as e:
            raise ValueError(f"Encoding error: Unable to decode file as UTF-8: {e}")

    # 4. CSV parsing and validation
    f = io.StringIO(text_content)
    try:
        reader = csv.DictReader(f, strict=True)
        if not reader.fieldnames:
            raise ValueError("Invalid CSV: No headers found.")

        # Clean fieldnames (strip whitespace)
        fieldnames = [fn.strip() for fn in reader.fieldnames if fn is not None]
        if not fieldnames:
            raise ValueError("Invalid CSV: Empty column headers.")

        rows: List[Dict[str, Any]] = []
        for line_num, raw_row in enumerate(reader, start=2):
            cleaned_row: Dict[str, Any] = {}
            has_data = False
            for k, v in raw_row.items():
                if k is None:
                    continue
                k_clean = k.strip()
                v_clean = v.strip() if isinstance(v, str) else v
                if v_clean not in (None, ""):
                    has_data = True
                # Attempt light numeric conversion if strictly integer or float
                converted_val = v_clean
                if isinstance(v_clean, str) and v_clean != "":
                    try:
                        if v_clean.isdigit() or (v_clean.startswith("-") and v_clean[1:].isdigit()):
                            converted_val = int(v_clean)
                        else:
                            f_val = float(v_clean)
                            if "." in v_clean:
                                converted_val = f_val
                    except (ValueError, OverflowError):
                        converted_val = v_clean
                cleaned_row[k_clean] = converted_val

            if has_data:
                rows.append(cleaned_row)

    except csv.Error as e:
        raise ValueError(f"Malformed CSV syntax: {e}")
    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"CSV processing failed: {e}")

    dataset_id = compute_dataset_id(raw_content)
    return dataset_id, rows
