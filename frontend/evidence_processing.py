"""
Evidence file validation and optimization helpers (disk paths only; no Flask/db imports).

PDF: prefers Ghostscript (gswin64c / gs) when available; falls back to copying bytes unchanged.
Images: resize wide sides, strip EXIF, encode optimized WEBP (quality ~78).
Thumbnails: small WEBP preview (PDF via PyMuPDF when installed).
"""
from __future__ import annotations

import shutil
import subprocess
import zipfile
from pathlib import Path
from shutil import which

from PIL import Image, ImageOps

# --- Validation ---

MAX_UPLOAD_BYTES = 25 * 1024 * 1024

_ALLOWED_BINARY_SIGNATURES: dict[str, tuple[bytes, ...]] = {
    "pdf": (b"%PDF",),
    "png": (b"\x89PNG\r\n\x1a\n",),
    "jpeg": (b"\xff\xd8\xff",),
    "webp": (b"RIFF",),  # WEBP is RIFF....WEBP — checked separately
}

_ZIP_MAGIC = b"PK\x03\x04"


def sniff_kind(header: bytes) -> str | None:
    if len(header) < 12:
        return None
    if header.startswith(_ALLOWED_BINARY_SIGNATURES["pdf"]):
        return "pdf"
    if header.startswith(_ALLOWED_BINARY_SIGNATURES["png"]):
        return "png"
    if header.startswith(_ALLOWED_BINARY_SIGNATURES["jpeg"]):
        return "jpeg"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "webp"
    if header.startswith(_ZIP_MAGIC):
        # heuristic for OOXML
        return "zip"
    return None


def normalize_extension(raw: str | None) -> str:
    if not raw:
        return ""
    ext = Path(raw).suffix.lower().lstrip(".")
    aliases = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "webp": "webp", "pdf": "pdf", "docx": "docx", "xlsx": "xlsx"}
    return aliases.get(ext, ext)


def allowed_upload_ext(ext: str) -> bool:
    return ext in {"pdf", "png", "jpeg", "webp", "docx", "xlsx"}


def infer_mime(ext: str, kind: str | None) -> str:
    if ext == "pdf" or kind == "pdf":
        return "application/pdf"
    if ext in {"png", "jpeg", "webp"} or kind in {"png", "jpeg", "webp"}:
        mapping = {"png": "image/png", "jpeg": "image/jpeg", "webp": "image/webp"}
        return mapping.get(ext, "application/octet-stream")
    if ext == "docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if ext == "xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return "application/octet-stream"


def validate_upload_file(path: Path, declared_ext: str, declared_size: int) -> tuple[str, str]:
    """
    Returns (normalized_extension, mime_type). Raises ValueError on rejection.
    """
    if declared_size <= 0:
        raise ValueError("Empty file")
    if declared_size > MAX_UPLOAD_BYTES:
        raise ValueError("File exceeds 25 MB limit")
    ext = normalize_extension(declared_ext)
    if not allowed_upload_ext(ext):
        raise ValueError("Unsupported file type")
    with path.open("rb") as f:
        header = f.read(4096)
    kind = sniff_kind(header)
    if ext == "pdf" and kind != "pdf":
        raise ValueError("File content does not match PDF")
    if ext in {"png", "jpeg", "webp"}:
        ne = ext
        if kind not in {"png", "jpeg", "webp"}:
            raise ValueError("File content does not match image type")
        if ne == "jpeg" and kind != "jpeg":
            raise ValueError("File content does not match JPEG")
        if ne == "png" and kind != "png":
            raise ValueError("File content does not match PNG")
        if ne == "webp" and kind != "webp":
            raise ValueError("File content does not match WEBP")
    if ext == "docx":
        if kind != "zip":
            raise ValueError("Invalid DOCX container")
        # minimal OOXML check
        try:
            zf = zipfile.ZipFile(path)
            names = zf.namelist()
            zf.close()
        except Exception:
            raise ValueError("Invalid DOCX") from None
        if "[Content_Types].xml" not in names:
            raise ValueError("Invalid DOCX structure")
    if ext == "xlsx":
        if kind != "zip":
            raise ValueError("Invalid XLSX container")
        try:
            zf = zipfile.ZipFile(path)
            names = zf.namelist()
            zf.close()
        except Exception:
            raise ValueError("Invalid XLSX") from None
        if "[Content_Types].xml" not in names:
            raise ValueError("Invalid XLSX structure")

    mime = infer_mime(ext, kind)
    return ext, mime


def sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def find_ghostscript_binary() -> str | None:
    for name in ("gswin64c", "gswin32c", "gs"):
        p = which(name)
        if p:
            return p
    return None


def optimize_pdf_gs(inp: Path, outp: Path) -> bool:
    gs = find_ghostscript_binary()
    if not gs:
        return False
    outp.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        gs,
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.5",
        "-dPDFSETTINGS=/ebook",
        "-dDetectDuplicateImages=true",
        "-dCompressFonts=true",
        "-dSubsetFonts=true",
        "-dEmbedAllFonts=true",
        "-dNOPAUSE",
        "-dQUIET",
        "-dBATCH",
        "-dFastWebView=true",
        f"-sOutputFile={outp}",
        str(inp),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=600)
        return outp.is_file() and outp.stat().st_size > 0
    except Exception:
        try:
            if outp.is_file():
                outp.unlink()
        except Exception:
            pass
        return False


def optimize_image_to_webp(inp: Path, outp: Path, *, max_side: int = 2200, quality: int = 78) -> None:
    outp.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(inp) as im:
        im = ImageOps.exif_transpose(im)
        im = im.convert("RGB")
        w, h = im.size
        if max(w, h) > max_side:
            scale = max_side / float(max(w, h))
            nw = max(1, int(round(w * scale)))
            nh = max(1, int(round(h * scale)))
            im = im.resize((nw, nh), Image.Resampling.LANCZOS)
        im.save(outp, format="WEBP", quality=quality, method=6)


def tiny_thumbnail_webp(inp_path: Path, outp: Path, *, box: int = 220) -> None:
    """Raster thumbnail from image file."""
    outp.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(inp_path) as im:
        im = ImageOps.exif_transpose(im)
        im.thumbnail((box, box), Image.Resampling.LANCZOS)
        if im.mode not in ("RGB", "RGBA"):
            im = im.convert("RGB")
        else:
            im = im.convert("RGB")
        im.save(outp, format="WEBP", quality=72, method=6)


def pdf_first_page_thumbnail(pdf_path: Path, outp: Path, *, box: int = 220) -> bool:
    try:
        import fitz  # type: ignore
    except Exception:
        return False
    outp.parent.mkdir(parents=True, exist_ok=True)
    try:
        doc = fitz.open(str(pdf_path))
        if doc.page_count < 1:
            doc.close()
            return False
        page = doc.load_page(0)
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        img.thumbnail((box, box), Image.Resampling.LANCZOS)
        img.save(outp, format="WEBP", quality=72, method=6)
        doc.close()
        return outp.is_file()
    except Exception:
        try:
            if outp.is_file():
                outp.unlink()
        except Exception:
            pass
        return False


def rezip_store_archive(inp: Path, outp: Path) -> None:
    """Lightweight OOXML 'optimization': store with DEFLATED members."""
    outp.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(inp, "r") as zin:
        with zipfile.ZipFile(outp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zout:
            for item in zin.infolist():
                zout.writestr(item, zin.read(item.filename))


def _relative_upload_path(upload_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(upload_root.resolve()).as_posix()
    except Exception:
        return path.name


def process_evidence_file_with_storage(
    *,
    storage: "StorageProvider",
    staging_path: Path,
    sha256_hex: str,
    normalized_ext: str,
    base_ts,
) -> dict:
    """
    Optimize staging bytes and persist via StorageProvider.
    Returns dict with storage_path / thumbnail_storage_path as relative POSIX keys.
    """
    import tempfile

    y = base_ts.strftime("%Y")
    m = base_ts.strftime("%m")

    with tempfile.TemporaryDirectory() as td:
        work = Path(td)
        final_dir = work / "final"
        thumb_dir = work / "thumb"
        final_dir.mkdir(parents=True, exist_ok=True)
        thumb_dir.mkdir(parents=True, exist_ok=True)

        original_size = staging_path.stat().st_size
        safe_hash = sha256_hex.lower()
        tmp_final = final_dir / f"{safe_hash}_opt.tmp"

        output_ext = normalized_ext
        with staging_path.open("rb") as sf:
            hdr = sf.read(16)
        mime_out = infer_mime(normalized_ext, sniff_kind(hdr))

        if normalized_ext == "pdf":
            ok = optimize_pdf_gs(staging_path, tmp_final)
            if not ok:
                shutil.copy2(staging_path, tmp_final)
                output_ext = "pdf"
                mime_out = "application/pdf"
        elif normalized_ext in {"png", "jpeg", "webp"}:
            tmp_webp = final_dir / f"{safe_hash}_opt.webp"
            optimize_image_to_webp(staging_path, tmp_webp)
            tmp_final = tmp_webp
            output_ext = "webp"
            mime_out = "image/webp"
        elif normalized_ext in {"docx", "xlsx"}:
            rezip_store_archive(staging_path, tmp_final)
            mime_out = infer_mime(normalized_ext, "zip")
        else:
            shutil.copy2(staging_path, tmp_final)

        stored_filename = f"{safe_hash}.{output_ext}"
        final_path = final_dir / stored_filename
        tmp_final.replace(final_path)
        optimized_size = final_path.stat().st_size

        thumb_name = f"{safe_hash}_thumb.webp"
        thumb_work = thumb_dir / thumb_name
        thumb_rel: str | None = None

        if output_ext == "pdf":
            if pdf_first_page_thumbnail(final_path, thumb_work):
                thumb_rel = storage.generate_path("evidence", "_thumbs", y, m, thumb_name)
                storage.save_file(thumb_work, thumb_rel)
        elif output_ext == "webp":
            tiny_thumbnail_webp(final_path, thumb_work)
            thumb_rel = storage.generate_path("evidence", "_thumbs", y, m, thumb_name)
            storage.save_file(thumb_work, thumb_rel)

        rel_storage = storage.generate_path("evidence", y, m, stored_filename)
        storage.save_file(final_path, rel_storage)

        return {
            "original_size": original_size,
            "optimized_size": optimized_size,
            "stored_filename": stored_filename,
            "storage_path": rel_storage.replace("\\", "/"),
            "thumbnail_storage_path": thumb_rel.replace("\\", "/") if thumb_rel else None,
            "file_extension": output_ext,
            "mime_type": mime_out,
        }


def process_evidence_file(
    *,
    upload_root: Path,
    staging_path: Path,
    sha256_hex: str,
    normalized_ext: str,
    final_dir: Path,
    thumb_dir: Path,
) -> dict:
    """
    Produce optimized artifact under final_dir using stored filename <sha>.<ext>.
    Same layout as historical direct-disk behavior (tests / callers).
    """
    final_dir.mkdir(parents=True, exist_ok=True)
    thumb_dir.mkdir(parents=True, exist_ok=True)
    original_size = staging_path.stat().st_size
    safe_hash = sha256_hex.lower()
    tmp_final = final_dir / f"{safe_hash}_opt.tmp"

    output_ext = normalized_ext
    mime_out = infer_mime(normalized_ext, sniff_kind(staging_path.read_bytes()[:16]))

    if normalized_ext == "pdf":
        ok = optimize_pdf_gs(staging_path, tmp_final)
        if not ok:
            shutil.copy2(staging_path, tmp_final)
            output_ext = "pdf"
            mime_out = "application/pdf"
    elif normalized_ext in {"png", "jpeg", "webp"}:
        tmp_webp = final_dir / f"{safe_hash}_opt.webp"
        optimize_image_to_webp(staging_path, tmp_webp)
        tmp_final = tmp_webp
        output_ext = "webp"
        mime_out = "image/webp"
    elif normalized_ext in {"docx", "xlsx"}:
        rezip_store_archive(staging_path, tmp_final)
        mime_out = infer_mime(normalized_ext, "zip")
    else:
        shutil.copy2(staging_path, tmp_final)

    stored_filename = f"{safe_hash}.{output_ext}"
    final_path = final_dir / stored_filename
    tmp_final.replace(final_path)

    optimized_size = final_path.stat().st_size
    rel_storage = _relative_upload_path(upload_root, final_path)

    thumb_name = f"{safe_hash}_thumb.webp"
    thumb_path_full = thumb_dir / thumb_name
    thumb_rel: str | None = None

    if output_ext == "pdf":
        if pdf_first_page_thumbnail(final_path, thumb_path_full):
            thumb_rel = _relative_upload_path(upload_root, thumb_path_full)
    elif output_ext == "webp":
        tiny_thumbnail_webp(final_path, thumb_path_full)
        thumb_rel = _relative_upload_path(upload_root, thumb_path_full)

    return {
        "original_size": original_size,
        "optimized_size": optimized_size,
        "stored_filename": stored_filename,
        "storage_path": rel_storage.replace("\\", "/"),
        "thumbnail_storage_path": thumb_rel.replace("\\", "/") if thumb_rel else None,
        "file_extension": output_ext,
        "mime_type": mime_out,
    }


def evidence_destination_dirs(upload_root: Path, utc_now) -> tuple[Path, Path]:
    y = utc_now.strftime("%Y")
    m = utc_now.strftime("%m")
    final_dir = upload_root / "evidence" / y / m
    thumb_dir = upload_root / "evidence" / "_thumbs" / y / m
    return final_dir, thumb_dir

