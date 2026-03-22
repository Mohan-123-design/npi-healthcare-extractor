# modules/sheet_csv_handler.py

import pandas as pd
import os
from dotenv import load_dotenv
from .web_utils import logger

load_dotenv()


def read_input_csv(path):
    """
    Read the input CSV as text, handling encoding issues gracefully.

    - Try UTF-8 first.
    - If that fails with UnicodeDecodeError, fall back to latin-1 so the
      pipeline can continue without crashing.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input CSV not found at: {path}")

    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8")
    except UnicodeDecodeError:
        logger.warning(
            f"UTF-8 decode failed for {path}; falling back to latin-1 encoding."
        )
        df = pd.read_csv(path, dtype=str, encoding="latin-1")

    df = df.fillna("")

    required = {"npi_id", "email"}
    cols_lower = {c.lower(): c for c in df.columns}
    if not required.issubset(set(cols_lower.keys())):
        raise ValueError(
            "Input CSV must include headers 'npi_id' and 'email' (case-insensitive)."
        )

    # normalize
    df = df.rename(
        columns={cols_lower["npi_id"]: "npi_id", cols_lower["email"]: "email"}
    )
    df = df.rename(columns={c: c.lower() for c in df.columns})

    return df.to_dict(orient="records")


def write_output_csv(rows, out_dir, filename=None):
    import datetime

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if not filename:
        filename = (
            f"results_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
        )

    out_path = os.path.join(out_dir, filename)
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)

    logger.info(f"Wrote results to {out_path}")
    return out_path
