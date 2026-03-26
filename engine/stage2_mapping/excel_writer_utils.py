from __future__ import annotations


def preferred_excel_writer_engine() -> str:
    """Use xlsxwriter when available, otherwise fall back to openpyxl."""
    try:
        import xlsxwriter  # noqa: F401

        return "xlsxwriter"
    except Exception:
        return "openpyxl"
