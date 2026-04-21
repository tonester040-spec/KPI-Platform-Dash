"""
LibreOffice-based .xls → .xlsx converter.

Salon Ultimate exports legacy .xls files that Python's native readers
(xlrd, openpyxl) cannot parse directly — in some cases because the
OLE compound document is malformed at the exporter level.  LibreOffice's
import filter can read many of these files where xlrd/openpyxl fail.

Use this helper BEFORE handing a file to openpyxl.

Usage:
    from parsers.utils.xls_converter import convert_xls_to_xlsx, needs_conversion

    if needs_conversion(path):
        path = convert_xls_to_xlsx(path)
    wb = openpyxl.load_workbook(path)

Requirements:
    - LibreOffice installed and `soffice` on PATH
      (override with SOFFICE_PATH env var if needed).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Union


__all__ = ["convert_xls_to_xlsx", "needs_conversion"]


def needs_conversion(file_path: Union[str, Path]) -> bool:
    """Return True if the file is a legacy .xls that should be converted."""
    return Path(file_path).suffix.lower() == ".xls"


def convert_xls_to_xlsx(
    xls_path: Union[str, Path],
    output_dir: Union[str, Path, None] = None,
    reuse_existing: bool = True,
) -> Path:
    """
    Convert a .xls file to .xlsx using LibreOffice in headless mode.

    Args:
        xls_path       : Path to the .xls file.
        output_dir     : Where to write the .xlsx. Defaults to the .xls file's directory.
        reuse_existing : If True and a matching .xlsx already exists in the output dir,
                         return that path instead of re-running LibreOffice.

    Returns:
        Path to the generated .xlsx file.

    Raises:
        FileNotFoundError : Source .xls does not exist.
        ValueError        : File extension is not .xls.
        RuntimeError      : LibreOffice is not installed, the conversion failed,
                            or the expected output was not produced.
    """
    xls_path = Path(xls_path)

    if not xls_path.exists():
        raise FileNotFoundError(f"File not found: {xls_path}")

    if xls_path.suffix.lower() != ".xls":
        raise ValueError(
            f"Expected .xls file, got: {xls_path.suffix} ({xls_path.name})"
        )

    output_dir = Path(output_dir) if output_dir else xls_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    expected_xlsx = output_dir / f"{xls_path.stem}.xlsx"

    # Short-circuit: previously converted file is still on disk
    if reuse_existing and expected_xlsx.exists():
        return expected_xlsx

    soffice = os.environ.get("SOFFICE_PATH", "soffice")

    try:
        result = subprocess.run(
            [
                soffice,
                "--headless",
                "--convert-to", "xlsx",
                "--outdir", str(output_dir),
                str(xls_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "LibreOffice (soffice) is not installed or not on PATH. "
            "Install with: sudo apt-get install libreoffice  "
            "or: brew install --cask libreoffice  "
            "or: set SOFFICE_PATH env var to the binary."
        ) from exc

    if result.returncode != 0:
        error_msg = (result.stderr or result.stdout or "Unknown error").strip()
        raise RuntimeError(
            f"LibreOffice conversion failed for {xls_path} "
            f"(exit {result.returncode}): {error_msg}"
        )

    if not expected_xlsx.exists():
        raise RuntimeError(
            f"LibreOffice reported success but output file not found: {expected_xlsx}. "
            f"stdout: {result.stdout.strip()}"
        )

    return expected_xlsx
