import warnings
import importlib.util
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, send_from_directory, Response, session, abort, g
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from openpyxl import load_workbook, Workbook
import pandas as pd
import os
import sys
import threading
import subprocess
import uuid
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from types import SimpleNamespace
import hashlib
import json
from collections import defaultdict, Counter
import csv
from io import BytesIO
import re
import time
import math
import calendar
import ipaddress
import secrets
import smtplib
import ssl
from functools import lru_cache
from urllib import error as urllib_error
from urllib import request as urllib_request
from email.message import EmailMessage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    CCC_API_BASE_URL,
    CCC_GET_ENDPOINTS_PATH,
    CCC_API_PAGE_SIZE,
    CCC_PASSWORD,
    CCC_SHEET_MAPPING_PATH,
    CCC_USERNAME,
    FRONTEND_DB_PATH,
    FRONTEND_INSTANCE_DIR,
    FRONTEND_UPLOAD_DIR,
    MAIL_DEFAULT_SENDER,
    MAIL_PASSWORD,
    MAIL_PORT,
    MAIL_SERVER,
    MAIL_USERNAME,
    PIPELINE_RUNS_DIR,
    PIPELINE_TEMPLATE_DIR,
    PROJECT_ROOT,
    PUBLIC_APP_BASE_URL,
    SECRET_KEY,
    STAGE1_INPUT_BACKUP_DIR,
    STAGE1_INPUT_DIR,
    STAGE1_KLARAKARBON_OUTPUT_DIR,
    STAGE2_EF_XLSX,
    STAGE2_MAPPING_DIR,
    STAGE2_OUTPUT_DIR,
    STAGE2_TRAVEL_DIR,
)
from company_slug import company_slug
from preprocess_jobs import (
    klarakarbon_entry_headers,
    klarakarbon_company_supported,
    run_travel_preprocess,
    validate_travel_upload,
)
from services import messaging_service, notification_service, search_service

APP_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = FRONTEND_INSTANCE_DIR

# Use centralized configuration so file-system paths do not change between
# local development and server deployments.
app = Flask(__name__, instance_path=str(INSTANCE_DIR), instance_relative_config=True)
app.config['SECRET_KEY'] = SECRET_KEY
INSTANCE_DIR.mkdir(parents=True, exist_ok=True)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + str(FRONTEND_DB_PATH)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = str(FRONTEND_UPLOAD_DIR)

TEMPLATES_PATH = APP_DIR / "data" / "templates.json"
ISO_COUNTRIES_PATH = APP_DIR / "data" / "iso_countries.json"
ALLOWED_EMAIL_DOMAIN = "cts-nordics.com"
PROFILE_PHOTO_ALLOWED_EXT = frozenset({".png", ".jpg", ".jpeg", ".webp"})
COMPANY_LOGO_ALLOWED_EXT = frozenset({".png"})
TRAVEL_ALLOWED_EXT = frozenset({".xlsb"})


def _email_domain_allowed(email: str) -> bool:
    e = (email or "").strip().lower()
    if e.count("@") != 1:
        return False
    local, domain = e.split("@", 1)
    return bool(local) and domain == ALLOWED_EMAIL_DOMAIN


def _hash_password_reset_token(raw_token: str) -> str:
    return hashlib.sha256((raw_token or "").encode("utf-8")).hexdigest()


def _send_plain_email(to_addr: str, subject: str, body: str) -> bool:
    if not MAIL_SERVER:
        app.logger.warning("Password reset: MAIL_SERVER not configured; email not sent to %s", to_addr)
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = MAIL_DEFAULT_SENDER
        msg["To"] = to_addr
        msg.set_content(body)
        ctx = ssl.create_default_context()
        if MAIL_PORT == 465:
            with smtplib.SMTP_SSL(MAIL_SERVER, MAIL_PORT, context=ctx, timeout=30) as smtp:
                if MAIL_USERNAME:
                    smtp.login(MAIL_USERNAME, MAIL_PASSWORD)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(MAIL_SERVER, MAIL_PORT, timeout=30) as smtp:
                smtp.ehlo()
                smtp.starttls(context=ctx)
                smtp.ehlo()
                if MAIL_USERNAME:
                    smtp.login(MAIL_USERNAME, MAIL_PASSWORD)
                smtp.send_message(msg)
        return True
    except Exception:
        app.logger.exception("Password reset email failed for %s", to_addr)
        return False


_FORGOT_PW_WINDOW_SEC = 600
_FORGOT_PW_MAX_PER_IP = 5
_FORGOT_PW_MAX_PER_EMAIL = 3
_forgot_pw_lock = threading.Lock()
_forgot_pw_ip_ts: dict[str, list[float]] = {}
_forgot_pw_email_ts: dict[str, list[float]] = {}


def _client_ip_for_rate_limit() -> str:
    xff = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    if xff:
        return xff[:200]
    return str(request.remote_addr or "unknown")[:200]


def _prune_ts_list(ts_list: list[float], now: float) -> None:
    cutoff = now - _FORGOT_PW_WINDOW_SEC
    ts_list[:] = [t for t in ts_list if t > cutoff]


def _forgot_pw_rate_allow_ip(client_ip: str) -> bool:
    now = time.time()
    with _forgot_pw_lock:
        lst = _forgot_pw_ip_ts.setdefault(client_ip, [])
        _prune_ts_list(lst, now)
        if len(lst) >= _FORGOT_PW_MAX_PER_IP:
            return False
        lst.append(now)
        return True


def _forgot_pw_rate_allow_email(email_norm: str) -> bool:
    now = time.time()
    with _forgot_pw_lock:
        lst = _forgot_pw_email_ts.setdefault(email_norm, [])
        _prune_ts_list(lst, now)
        if len(lst) >= _FORGOT_PW_MAX_PER_EMAIL:
            return False
        lst.append(now)
        return True


def _audit_password_reset(event: str, **kwargs: object) -> None:
    extra = " ".join(f"{k}={kwargs[k]}" for k in sorted(kwargs))
    app.logger.info("[AUDIT] %s %s", event, extra)


def _password_reset_meets_policy(pw: str) -> bool:
    if len(pw) < 8:
        return False
    if not re.search(r"[A-Za-z]", pw):
        return False
    if not re.search(r"\d", pw):
        return False
    return True


def _delete_expired_password_reset_tokens() -> None:
    expired = PasswordResetToken.query.filter(PasswordResetToken.expires_at < datetime.utcnow()).all()
    for row in expired:
        db.session.delete(row)


def _load_iso_countries() -> list[tuple[str, str]]:
    try:
        with ISO_COUNTRIES_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return []
    out: list[tuple[str, str]] = []
    if not isinstance(raw, list):
        return out
    for row in raw:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").strip().upper()
        name = str(row.get("name") or "").strip()
        if code and name:
            out.append((code, name))
    out.sort(key=lambda x: x[1].casefold())
    return out


ISO_COUNTRIES = _load_iso_countries()
ISO_COUNTRY_NAME_BY_CODE = {code: name for code, name in ISO_COUNTRIES}
ISO_COUNTRY_CODE_BY_NAME = {name.casefold(): code for code, name in ISO_COUNTRIES}


def _normalize_template_key(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _load_templates_from_json() -> dict[str, dict[str, list[str]]]:
    try:
        with TEMPLATES_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        raw = {}

    templates: dict[str, dict[str, list[str]]] = {}
    if not isinstance(raw, dict):
        return templates

    for company_name, sheets in raw.items():
        if not isinstance(company_name, str) or not isinstance(sheets, dict):
            continue
        clean_company = company_name.strip()
        if not clean_company:
            continue
        clean_sheets: dict[str, list[str]] = {}
        for sheet_name, headers in sheets.items():
            if not isinstance(sheet_name, str) or not isinstance(headers, list):
                continue
            clean_sheet = sheet_name.strip()
            if not clean_sheet:
                continue
            clean_headers = [str(h).strip() for h in headers if str(h).strip()]
            clean_sheets[clean_sheet] = clean_headers
        templates[clean_company] = clean_sheets
    return templates


def _build_template_index() -> dict[str, dict[str, object]]:
    index: dict[str, dict[str, object]] = {}
    for company_name, sheets in TEMPLATES.items():
        norm_company = _normalize_template_key(company_name)
        sheet_index: dict[str, dict[str, object]] = {}
        for sheet_name, headers in sheets.items():
            sheet_index[_normalize_template_key(sheet_name)] = {
                "name": sheet_name,
                "headers": headers,
            }
        index[norm_company] = {"name": company_name, "sheets": sheet_index}
    return index


TEMPLATES = _load_templates_from_json()
TEMPLATE_INDEX = _build_template_index()
print("Templates loaded:", len(TEMPLATES))

KLARAKARBON_SHEET_NAME = "Klarakarbon"
TRAVEL_SHEET_NAME = "Travel"

# ---- Stage2 mapping (web single-company runner) ----
STAGE2_MAPPING_OUTPUT_DIR = STAGE2_OUTPUT_DIR
_STAGE2_MAP_LOCK = threading.Lock()
_MAPPING_RUNS: dict[str, dict[str, object]] = {}  # legacy in-memory (kept for backward compatibility)

# ---- Company canonical names + countries (web mapping) ----
# User-provided source-of-truth list (deduplicated).
_COMPANY_COUNTRY_CANONICAL: dict[str, str] = {
    "BIMMS": "Portugal",
    "CTS Finland": "Finland",
    "CTS-VDC Services": "Ireland",
    "DC Piping": "Portugal",
    "GT Nordics": "Norway",
    "Mecwide Nordics": "Norway",
    "Porvelox": "Portugal",
    "Caerus Nordics": "Norway",
    "CTS Sweden": "Sweden",
    "Navitas Portugal": "Portugal",
    "NEP Switchboards": "Norway",
    "CTS Denmark": "Denmark",
    "CTS Group": "Switzerland",
    "CTS Nordics": "Norway",
    "Fortica": "Norway",
    "MC Prefab": "Sweden",
    "Navitas Norway": "Norway",
    "QEC": "Norway",
    "SD Nordics": "Norway",
    "CTS Security Solutions": "Sweden",
    "Velox": "Norway",
    "CTS EU": "Portugal",
    "Gapit": "Norway",
}


def _canonical_company_name_and_country(name: str) -> tuple[str, str | None]:
    raw = (name or "").strip()
    if not raw:
        return "", None

    def norm(s: str) -> str:
        return "".join(ch.lower() for ch in (s or "") if ch.isalnum())

    n = norm(raw)
    # Build synonyms (file stems vs display names)
    synonyms: dict[str, str] = {
        norm("CTS-VDC"): "CTS-VDC Services",
        norm("CTS VDC"): "CTS-VDC Services",
        norm("CTS-VDC Services"): "CTS-VDC Services",
        norm("CTS Group HQ"): "CTS Group",
        norm("CTS Group"): "CTS Group",
        norm("NordicEPOD"): "Nordic EPOD",
        norm("Nordic EPOD"): "Nordic EPOD",
        norm("Caerus Nordics"): "Caerus Nordics",
        norm("CTS EU"): "CTS EU",
        norm("GT Nordics"): "GT Nordics",
        norm("Mecwide Nordics"): "Mecwide Nordics",
        norm("Navitas Norway"): "Navitas Norway",
        norm("Navitas Portugal"): "Navitas Portugal",
        norm("CTS Denmark"): "CTS Denmark",
        norm("CTS Finland"): "CTS Finland",
        norm("CTS Sweden"): "CTS Sweden",
        norm("CTS Nordics"): "CTS Nordics",
        norm("CTS Security Solutions"): "CTS Security Solutions",
        norm("DC Piping"): "DC Piping",
        norm("MC Prefab"): "MC Prefab",
        norm("Porvelox"): "Porvelox",
        norm("Fortica"): "Fortica",
        norm("Gapit"): "Gapit",
        norm("QEC"): "QEC",
        norm("SD Nordics"): "SD Nordics",
        norm("Velox"): "Velox",
        norm("BIMMS"): "BIMMS",
        norm("NEP Switchboards"): "NEP Switchboards",
        norm("NEP Switchboards AS"): "NEP Switchboards",
    }

    canonical = synonyms.get(n, raw)
    # Country lookup uses canonical keys; if missing, best-effort match by normalized canonical names
    country = _COMPANY_COUNTRY_CANONICAL.get(canonical)
    if country is None:
        want = norm(canonical)
        for k, v in _COMPANY_COUNTRY_CANONICAL.items():
            if norm(k) == want:
                country = v
                canonical = k
                break

    return canonical, country

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

_ENDPOINTS_WITHOUT_PROFILE = frozenset(
    {
        "login",
        "logout",
        "register",
        "profile_setup",
        "profile_page",
        "static",
        "mouse_symbol_image",
        "index",
        "who_we_are",
        "platform",
        "methodology",
        "trust",
        "forgot_password",
        "reset_password",
        "impact",
        "impact_approach",
        "impact_operations",
        "impact_lca_epd",
        "impact_collaboration",
        "impact_corporate",
        "impact_materiality",
        "impact_carbon",
        "impact_stakeholders",
        "impact_sdgs",
        "impact_governance",
        "impact_reporting",
        "esg",
        "csrd",
        "lca",
        "lca_tool",
        "csrd_policies",
        "csrd_policy_add",
        "csrd_policy_update",
        "csrd_policy_delete",
        "csrd_policy_file",
    }
)


@app.before_request
def _require_complete_profile_for_app():
    if not current_user.is_authenticated:
        return
    if _user_profile_complete(current_user):
        return
    ep = request.endpoint
    if ep in _ENDPOINTS_WITHOUT_PROFILE:
        return
    if request.path.startswith("/api/"):
        return jsonify({"error": "Complete profile setup before using this feature."}), 403
    return redirect(url_for("profile_setup"))


@app.before_request
def _ensure_activity_session_for_authenticated_user():
    if current_user.is_authenticated:
        _activity_session_id(create_if_missing=True)


@app.after_request
def _log_authenticated_activity(response: Response):
    try:
        if not current_user.is_authenticated:
            return response
        if request.method.upper() == "OPTIONS":
            return response
        if _is_static_or_ignored_activity_path(request.path):
            return response
        if request.environ.get("skip_activity_log"):
            return response
        _write_activity_log_for_user(current_user, action=_classify_activity_action())
    except Exception:
        pass
    return response


@app.context_processor
def _nav_profile_context():
    def nav_profile_photo_url() -> str | None:
        try:
            if not current_user.is_authenticated:
                return None
            rel = getattr(current_user, "profile_photo_path", None)
            if not rel:
                return None
            return url_for("static", filename=str(rel))
        except Exception:
            return None

    return dict(nav_profile_photo_url=nav_profile_photo_url)


# Database models
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    company_name = db.Column(db.String(200), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    # owner | super_admin | admin | manager | user — kept in sync with is_admin via sync_user_admin_flag()
    role = db.Column(db.String(32), nullable=True, default="user")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    first_name = db.Column(db.String(100), nullable=True)
    last_name = db.Column(db.String(100), nullable=True)
    job_title = db.Column(db.String(200), nullable=True)
    phone = db.Column(db.String(40), nullable=True)
    company_country = db.Column(db.String(100), nullable=True)
    profile_photo_path = db.Column(db.String(500), nullable=True)
    is_profile_complete = db.Column(db.Boolean, default=False)


USER_ROLES: tuple[str, ...] = ("owner", "super_admin", "admin", "manager", "user")
USER_ROLES_SET = frozenset(USER_ROLES)
# Roles that may use existing admin routes (is_admin=True)
ROLES_WITH_ADMIN_ACCESS = frozenset({"owner", "super_admin", "admin", "manager"})


def normalize_user_role(raw: str | None) -> str:
    r = (raw or "user").strip().lower()
    return r if r in USER_ROLES_SET else "user"


def sync_user_admin_flag(user: "User") -> None:
    """Set role + is_admin from canonical role (call after changing role)."""
    r = normalize_user_role(getattr(user, "role", None))
    user.role = r
    user.is_admin = r in ROLES_WITH_ADMIN_ACCESS


def _user_public_dict_for_admin(u: "User") -> dict:
    """Safe user fields for admin panel JSON (no password)."""
    rel = getattr(u, "profile_photo_path", None)
    photo = None
    if rel:
        try:
            photo = url_for("static", filename=str(rel))
        except Exception:
            photo = None
    r = normalize_user_role(getattr(u, "role", None))
    raw_role = (getattr(u, "role", None) or "").strip() or r
    fn = (getattr(u, "first_name", None) or "").strip()
    ln = (getattr(u, "last_name", None) or "").strip()
    full = " ".join(x for x in (fn, ln) if x).strip()
    return {
        "id": u.id,
        "email": u.email,
        "first_name": fn or None,
        "last_name": ln or None,
        "full_name": full or None,
        "phone": (getattr(u, "phone", None) or "").strip() or None,
        "company_name": (u.company_name or "").strip(),
        "company_country": (getattr(u, "company_country", None) or "").strip() or None,
        "role": r,
        "role_display": raw_role,
        "profile_photo_url": photo,
        "created_at": u.created_at.strftime("%Y-%m-%d") if getattr(u, "created_at", None) else None,
    }


def _user_display_name(u: "User" | None) -> str:
    if u is None:
        return ""
    first = (getattr(u, "first_name", None) or "").strip()
    last = (getattr(u, "last_name", None) or "").strip()
    full = " ".join(part for part in (first, last) if part).strip()
    return full or (getattr(u, "email", None) or "").strip() or f"User {getattr(u, 'id', '')}"


def _notification_payload(row: "Notification") -> dict[str, object]:
    created_at = row.created_at.strftime("%Y-%m-%d %H:%M") if getattr(row, "created_at", None) else ""
    return {
        "id": int(row.id),
        "title": row.title,
        "message": row.message,
        "type": row.type,
        "link": row.link,
        "is_read": bool(row.is_read),
        "created_at": created_at,
    }


def _message_payload(row: "Message", *, viewer_id: int) -> dict[str, object]:
    sender = User.query.get(int(row.sender_id))
    receiver = User.query.get(int(row.receiver_id))
    return {
        "id": int(row.id),
        "thread_id": row.thread_id,
        "sender_id": int(row.sender_id),
        "receiver_id": int(row.receiver_id),
        "sender_name": _user_display_name(sender),
        "receiver_name": _user_display_name(receiver),
        "message": row.message,
        "created_at": row.created_at.strftime("%Y-%m-%d %H:%M") if getattr(row, "created_at", None) else "",
        "is_read": bool(row.is_read),
        "is_mine": int(row.sender_id) == int(viewer_id),
    }


def _contact_payload(u: "User") -> dict[str, object]:
    return {
        "id": int(u.id),
        "name": _user_display_name(u),
        "email": u.email,
        "company_name": (getattr(u, "company_name", None) or "").strip(),
    }


class Company(db.Model):
    __tablename__ = "companies"
    id = db.Column(db.Integer, primary_key=True)
    company_name = db.Column(db.String(200), unique=True, nullable=False)
    company_logo_path = db.Column(db.String(500), nullable=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)


class PasswordResetToken(db.Model):
    __tablename__ = "password_reset_tokens"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    token_hash = db.Column(db.String(64), nullable=False, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    used = db.Column(db.Boolean, nullable=False, default=False)


class EmissionFactor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(200), nullable=False)
    subcategory = db.Column(db.String(200), nullable=False)
    factor = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String(100))
    year = db.Column(db.Integer)
    description = db.Column(db.Text)
    extra_data = db.Column(db.Text)  # Tüm satır verisi JSON olarak


class PipelineRun(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(30), default="queued")  # queued, running, succeeded, failed
    input_dir = db.Column(db.String(800), nullable=False)
    run_dir = db.Column(db.String(800), nullable=False)
    stage1_output = db.Column(db.String(800))
    stage2_output_dir = db.Column(db.String(800))
    log_path = db.Column(db.String(800))
    exit_code = db.Column(db.Integer)
    error_message = db.Column(db.Text)


class MappingRun(db.Model):
    """
    Web-triggered single-company Stage2 mapping runs (persisted).
    """
    id = db.Column(db.String(32), primary_key=True)  # run_id (hex)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    company_name = db.Column(db.String(200), nullable=False)
    sheet_name = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(30), default="queued")  # queued, running, succeeded, failed
    input_path = db.Column(db.String(900))
    output_path = db.Column(db.String(900))
    error_message = db.Column(db.Text)
    # When set, this run mapped only the given DataEntry batch (entry_group).
    source_entry_group = db.Column(db.String(40), nullable=True)


class MappingRunSummary(db.Model):
    """
    Lightweight persisted totals derived from a MappingRun output workbook.
    Used to power Home / Carbon Accounting dashboards.
    """
    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.String(32), unique=True, nullable=False)
    company_name = db.Column(db.String(200), nullable=False)
    sheet_name = db.Column(db.String(200), nullable=False)
    scope = db.Column(db.Integer)  # 1/2/3 or NULL
    tco2e_total = db.Column(db.Float, default=0.0)
    rows_count = db.Column(db.Integer, default=0)
    mapped_categories_count = db.Column(db.Integer, default=0)
    total_categories = db.Column(db.Integer, default=0)
    coverage_pct = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class DataEntry(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_name = db.Column(db.String(200), nullable=False)
    sheet_name = db.Column(db.String(200), nullable=False)
    entry_group = db.Column(db.String(32), nullable=False, default="")
    uploaded_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    row_index = db.Column(db.Integer, nullable=False, default=1)
    column_name = db.Column(db.String(200), nullable=False)
    value = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class CsrdPolicy(db.Model):
    __tablename__ = "csrd_policies"
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(500), nullable=False)
    short_description = db.Column(db.Text, nullable=False)
    # Relative to FRONTEND_UPLOAD_DIR, e.g. csrd_policies/<uuid>_<name>.pdf
    file_relpath = db.Column(db.String(600), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Notification(db.Model):
    __tablename__ = "notifications"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    title = db.Column(db.String(255), nullable=False)
    message = db.Column(db.Text, nullable=False)
    type = db.Column(db.String(50), nullable=False, default="info")
    link = db.Column(db.String(500), nullable=True)
    is_read = db.Column(db.Boolean, nullable=False, default=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class Message(db.Model):
    __tablename__ = "messages"
    id = db.Column(db.Integer, primary_key=True)
    thread_id = db.Column(db.String(64), nullable=False, index=True)
    sender_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    receiver_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    message = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    is_read = db.Column(db.Boolean, nullable=False, default=False, index=True)


class UserActivityLog(db.Model):
    __tablename__ = "user_activity_logs"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True, index=True)
    company_name = db.Column(db.String(200), nullable=True, index=True)
    email = db.Column(db.String(120), nullable=True, index=True)
    action = db.Column(db.String(80), nullable=False, index=True)
    page = db.Column(db.String(500), nullable=True)
    endpoint = db.Column(db.String(120), nullable=True, index=True)
    method = db.Column(db.String(16), nullable=True)
    ip_address = db.Column(db.String(80), nullable=True, index=True)
    country = db.Column(db.String(120), nullable=True, index=True)
    city = db.Column(db.String(120), nullable=True, index=True)
    device = db.Column(db.String(80), nullable=True, index=True)
    browser = db.Column(db.String(80), nullable=True, index=True)
    os = db.Column(db.String(80), nullable=True, index=True)
    referrer = db.Column(db.String(500), nullable=True)
    session_id = db.Column(db.String(64), nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


def _csrd_policy_upload_dir() -> Path:
    d = FRONTEND_UPLOAD_DIR / "csrd_policies"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _csrd_filename_is_pdf(filename: str) -> bool:
    return Path(secure_filename(filename or "")).suffix.lower() == ".pdf"


def _ensure_db_tables() -> None:
    """
    Best-effort table creation for environments that don't run app.py directly.
    Safe to call within request context.
    """
    try:
        db.create_all()
        _ensure_mapping_run_summary_columns()
        _ensure_data_entry_columns()
        _ensure_mapping_run_source_entry_group_column()
        _ensure_user_profile_columns()
    except Exception:
        pass


def _is_static_or_ignored_activity_path(path: str | None) -> bool:
    raw = str(path or "")
    return (
        not raw
        or raw.startswith("/static/")
        or raw.startswith("/assets/")
        or raw.startswith("/favicon")
    )


def _client_ip_address() -> str:
    forwarded = str(request.headers.get("X-Forwarded-For", "") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return str(request.remote_addr or "").strip() or "Unknown"


def _normalize_referrer(value: str | None) -> str | None:
    ref = str(value or "").strip()
    return ref[:500] if ref else None


def _normalize_page_path() -> str:
    path = str(request.path or "").strip() or "/"
    query_string = str(request.query_string.decode("utf-8", errors="ignore") if request.query_string else "").strip()
    if request.endpoint == "search_page" and query_string:
        return f"{path}?{query_string}"[:500]
    return path[:500]


def _activity_session_id(create_if_missing: bool = True) -> str | None:
    sid = str(session.get("activity_session_id", "") or "").strip()
    if sid or not create_if_missing:
        return sid or None
    sid = uuid.uuid4().hex
    session["activity_session_id"] = sid
    return sid


def _parse_user_agent(user_agent: str | None) -> tuple[str, str, str]:
    ua = str(user_agent or "").lower()
    if not ua:
        return ("Unknown", "Unknown", "Unknown")

    if "bot" in ua or "spider" in ua or "crawl" in ua:
        device = "Bot"
    elif "tablet" in ua or "ipad" in ua:
        device = "Tablet"
    elif "mobile" in ua or "iphone" in ua or "android" in ua:
        device = "Mobile"
    else:
        device = "Desktop"

    if "edg/" in ua:
        browser = "Edge"
    elif "opr/" in ua or "opera" in ua:
        browser = "Opera"
    elif "chrome/" in ua and "edg/" not in ua:
        browser = "Chrome"
    elif "safari/" in ua and "chrome/" not in ua:
        browser = "Safari"
    elif "firefox/" in ua:
        browser = "Firefox"
    elif "msie" in ua or "trident/" in ua:
        browser = "Internet Explorer"
    else:
        browser = "Other"

    if "windows" in ua:
        os_name = "Windows"
    elif "mac os" in ua or "macintosh" in ua:
        os_name = "macOS"
    elif "android" in ua:
        os_name = "Android"
    elif "iphone" in ua or "ipad" in ua or "ios" in ua:
        os_name = "iOS"
    elif "linux" in ua:
        os_name = "Linux"
    else:
        os_name = "Other"
    return (device, browser, os_name)


@lru_cache(maxsize=512)
def _geo_lookup_for_ip(ip_address_raw: str) -> tuple[str, str]:
    ip = str(ip_address_raw or "").strip()
    if not ip:
        return ("Unknown", "Unknown")
    try:
        addr = ipaddress.ip_address(ip)
        if addr.is_private or addr.is_loopback or addr.is_reserved or addr.is_multicast:
            return ("Local", "Local")
    except ValueError:
        return ("Unknown", "Unknown")

    url = f"https://ipapi.co/{ip}/json/"
    try:
        with urllib_request.urlopen(url, timeout=0.6) as response:
            payload = json.loads(response.read().decode("utf-8"))
        country = str(payload.get("country_name") or payload.get("country") or "Unknown").strip() or "Unknown"
        city = str(payload.get("city") or "Unknown").strip() or "Unknown"
        return (country, city)
    except (urllib_error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError):
        return ("Unknown", "Unknown")


def _classify_activity_action() -> str:
    endpoint = str(request.endpoint or "").strip()
    path = str(request.path or "").strip().lower()
    method = str(request.method or "GET").upper()

    if endpoint == "search_page":
        return "search_usage"
    if endpoint == "api_mapping_run":
        return "mapping_run"
    if endpoint == "data_sources_ccc_api" and method == "POST":
        return "ccc_api_sync"
    if endpoint == "analytics_forecasting" and method == "POST":
        return "forecasting_run"
    if endpoint == "analytics_decarbonization" and method == "POST":
        return "decarbonization_run"
    if endpoint == "governance_audit_ready_output" and method == "POST":
        return "audit_dataset_generation"
    if endpoint == "analytics_mapped_window_output" and method == "POST":
        return "mapped_window_generation"
    if endpoint == "analytics_emissions_totals" and method == "POST":
        return "totals_generation"
    if endpoint == "analytics_share_analysis" and method == "POST":
        return "share_analysis_generation"
    if endpoint == "governance_double_counting_check" and method == "POST":
        return "double_counting_generation"
    if "upload" in path or endpoint in {"admin", "dashboard"} and method == "POST":
        return "data_upload"
    if path.startswith("/api/"):
        return "api_request"
    return "page_visit"


def _activity_log_payload_for_user(user: "User", *, action: str) -> dict[str, object]:
    ip_address = _client_ip_address()
    country, city = _geo_lookup_for_ip(ip_address)
    device, browser, os_name = _parse_user_agent(request.headers.get("User-Agent"))
    return {
        "user_id": int(getattr(user, "id", 0) or 0) or None,
        "company_name": (getattr(user, "company_name", None) or "").strip() or None,
        "email": (getattr(user, "email", None) or "").strip() or None,
        "action": action,
        "page": _normalize_page_path(),
        "endpoint": str(request.endpoint or "").strip() or None,
        "method": str(request.method or "").upper() or None,
        "ip_address": ip_address,
        "country": country or "Unknown",
        "city": city or "Unknown",
        "device": device or "Unknown",
        "browser": browser or "Unknown",
        "os": os_name or "Unknown",
        "referrer": _normalize_referrer(request.referrer),
        "session_id": _activity_session_id(create_if_missing=True),
        "created_at": datetime.utcnow(),
    }


def _write_activity_log_for_user(user: "User", *, action: str) -> None:
    if user is None or not getattr(user, "id", None):
        return
    try:
        payload = _activity_log_payload_for_user(user, action=action)
        with db.engine.begin() as conn:
            conn.execute(UserActivityLog.__table__.insert().values(**payload))
    except Exception:
        pass


def _extract_dataset_name(path_or_ref: str | None) -> str | None:
    raw = str(path_or_ref or "").strip()
    if not raw:
        return None
    cleaned = raw.split("?", 1)[0].rstrip("/")
    name = Path(cleaned).name
    if any(name.lower().endswith(ext) for ext in (".xlsx", ".xls", ".csv", ".json", ".pdf", ".xlsb")):
        return name
    return None


def _owner_analytics_context() -> dict[str, object]:
    now = datetime.utcnow()
    logs = UserActivityLog.query.order_by(UserActivityLog.created_at.asc()).all()
    total_users = int(User.query.count())
    if not logs:
        empty_metrics = {
            "total_users": total_users,
            "active_users_24h": 0,
            "active_users_7d": 0,
            "total_logins": 0,
            "total_page_views": 0,
            "ccc_api_usage_count": 0,
            "mapping_runs_count": 0,
            "forecast_runs_count": 0,
            "audit_dataset_runs": 0,
            "avg_session_duration_minutes": 0.0,
            "feature_adoption_rate": 0.0,
            "top_company_engagement_score": 0.0,
            "most_used_dataset": "None yet",
            "most_visited_pages": [],
            "most_active_companies": [],
            "top_countries": [],
            "top_browsers": [],
            "top_devices": [],
            "most_active_hours": [],
            "feature_usage_frequency": [],
            "dataset_usage_frequency": [],
            "company_engagement": [],
        }
        return {"metrics": empty_metrics, "chart_data": empty_metrics, "activity_rows": 0}

    rows: list[dict[str, object]] = []
    for row in logs:
        rows.append(
            {
                "user_id": row.user_id,
                "company_name": row.company_name or "Unknown",
                "email": row.email or "",
                "action": row.action or "",
                "page": row.page or "",
                "endpoint": row.endpoint or "",
                "method": row.method or "",
                "country": row.country or "Unknown",
                "city": row.city or "Unknown",
                "device": row.device or "Unknown",
                "browser": row.browser or "Unknown",
                "os": row.os or "Unknown",
                "referrer": row.referrer or "",
                "session_id": row.session_id or "",
                "created_at": row.created_at or now,
                "dataset_name": _extract_dataset_name(row.page) or _extract_dataset_name(row.referrer) or "",
            }
        )
    frame = pd.DataFrame(rows)
    frame["created_at"] = pd.to_datetime(frame["created_at"])
    frame["date"] = frame["created_at"].dt.strftime("%Y-%m-%d")
    frame["hour"] = frame["created_at"].dt.hour

    last_24h = frame[frame["created_at"] >= (now - timedelta(hours=24))]
    last_7d = frame[frame["created_at"] >= (now - timedelta(days=7))]
    active_users_24h = int(last_24h["user_id"].dropna().nunique())
    active_users_7d = int(last_7d["user_id"].dropna().nunique())

    def _top_pairs(series: pd.Series, *, limit: int = 8, exclude_unknown: bool = False) -> list[dict[str, object]]:
        cleaned = series.fillna("").astype(str).str.strip()
        if exclude_unknown:
            cleaned = cleaned[~cleaned.isin(["", "Unknown"])]
        else:
            cleaned = cleaned[cleaned != ""]
        return [{"name": str(idx), "value": int(val)} for idx, val in cleaned.value_counts().head(limit).items()]

    total_logins = int((frame["action"] == "login").sum())
    total_page_views = int(frame["action"].isin(["page_visit", "search_usage"]).sum())
    ccc_api_usage_count = int((frame["action"] == "ccc_api_sync").sum())
    mapping_runs_count = int((frame["action"] == "mapping_run").sum())
    forecast_runs_count = int((frame["action"] == "forecasting_run").sum())
    audit_dataset_runs = int((frame["action"] == "audit_dataset_generation").sum())

    session_frame = frame[frame["session_id"].astype(str).str.strip() != ""].copy()
    session_durations: list[float] = []
    if not session_frame.empty:
        grouped_sessions = session_frame.groupby("session_id")["created_at"].agg(["min", "max"])
        session_durations = [
            max(0.0, float((row["max"] - row["min"]).total_seconds() / 60.0))
            for _, row in grouped_sessions.iterrows()
        ]
    avg_session_duration_minutes = round(sum(session_durations) / len(session_durations), 1) if session_durations else 0.0

    feature_user_sets = {
        "mapping": set(frame.loc[frame["action"] == "mapping_run", "user_id"].dropna().astype(int).tolist()),
        "ccc_api": set(frame.loc[frame["action"] == "ccc_api_sync", "user_id"].dropna().astype(int).tolist()),
        "forecasting": set(frame.loc[frame["action"] == "forecasting_run", "user_id"].dropna().astype(int).tolist()),
        "audit_export": set(frame.loc[frame["action"] == "audit_dataset_generation", "user_id"].dropna().astype(int).tolist()),
        "search": set(frame.loc[frame["action"] == "search_usage", "user_id"].dropna().astype(int).tolist()),
    }
    feature_adoption = [
        {
            "name": key.replace("_", " ").title(),
            "value": round((len(user_ids) / total_users) * 100.0, 1) if total_users else 0.0,
        }
        for key, user_ids in feature_user_sets.items()
    ]
    feature_adoption_rate = round(sum(item["value"] for item in feature_adoption) / len(feature_adoption), 1) if feature_adoption else 0.0

    upload_actions = {"data_upload"}
    company_engagement: list[dict[str, object]] = []
    for company_name, group in frame.groupby("company_name"):
        company_sessions = [sid for sid in group["session_id"].astype(str).tolist() if sid]
        session_minutes = 0.0
        if company_sessions:
            company_session_frame = session_frame[session_frame["session_id"].isin(company_sessions)]
            if not company_session_frame.empty:
                local_group = company_session_frame.groupby("session_id")["created_at"].agg(["min", "max"])
                local_minutes = [
                    max(0.0, float((row["max"] - row["min"]).total_seconds() / 60.0))
                    for _, row in local_group.iterrows()
                ]
                session_minutes = sum(local_minutes) / len(local_minutes) if local_minutes else 0.0
        score = (
            int((group["action"] == "login").sum()) * 3
            + int(group["action"].isin(upload_actions).sum()) * 4
            + int((group["action"] == "mapping_run").sum()) * 5
            + int((group["action"] == "ccc_api_sync").sum()) * 4
            + float(session_minutes)
            + int(group["user_id"].dropna().nunique()) * 6
        )
        company_engagement.append(
            {
                "name": company_name,
                "value": round(score, 1),
            }
        )
    company_engagement.sort(key=lambda item: float(item["value"]), reverse=True)
    top_company_engagement_score = float(company_engagement[0]["value"]) if company_engagement else 0.0

    dataset_usage_frequency = _top_pairs(frame["dataset_name"], limit=8, exclude_unknown=True)
    most_used_dataset = dataset_usage_frequency[0]["name"] if dataset_usage_frequency else "None yet"

    feature_usage_counter = Counter(
        {
            "Login": total_logins,
            "Search": int((frame["action"] == "search_usage").sum()),
            "Mapping": mapping_runs_count,
            "CCC API": ccc_api_usage_count,
            "Forecasting": forecast_runs_count,
            "Audit Export": audit_dataset_runs,
            "Page Views": total_page_views,
        }
    )
    feature_usage_frequency = [{"name": name, "value": int(value)} for name, value in feature_usage_counter.items()]

    most_visited_pages = _top_pairs(frame["page"], limit=8)
    most_active_companies = _top_pairs(frame["company_name"], limit=8, exclude_unknown=True)
    top_countries = _top_pairs(frame["country"], limit=8, exclude_unknown=True)
    top_browsers = _top_pairs(frame["browser"], limit=8, exclude_unknown=True)
    top_devices = _top_pairs(frame["device"], limit=8, exclude_unknown=True)
    most_active_hours = [{"name": f"{int(idx):02d}:00", "value": int(val)} for idx, val in frame["hour"].value_counts().sort_index().items()]

    daily_active_users = (
        frame.groupby("date")["user_id"].nunique().sort_index()
    )
    session_duration_distribution = [
        {"name": "0-5 min", "value": sum(1 for value in session_durations if value <= 5)},
        {"name": "5-15 min", "value": sum(1 for value in session_durations if 5 < value <= 15)},
        {"name": "15-30 min", "value": sum(1 for value in session_durations if 15 < value <= 30)},
        {"name": "30-60 min", "value": sum(1 for value in session_durations if 30 < value <= 60)},
        {"name": "60+ min", "value": sum(1 for value in session_durations if value > 60)},
    ]

    metrics = {
        "total_users": total_users,
        "active_users_24h": active_users_24h,
        "active_users_7d": active_users_7d,
        "total_logins": total_logins,
        "total_page_views": total_page_views,
        "ccc_api_usage_count": ccc_api_usage_count,
        "mapping_runs_count": mapping_runs_count,
        "forecast_runs_count": forecast_runs_count,
        "audit_dataset_runs": audit_dataset_runs,
        "avg_session_duration_minutes": avg_session_duration_minutes,
        "feature_adoption_rate": feature_adoption_rate,
        "top_company_engagement_score": top_company_engagement_score,
        "most_used_dataset": most_used_dataset,
        "most_visited_pages": most_visited_pages,
        "most_active_companies": most_active_companies,
        "top_countries": top_countries,
        "top_browsers": top_browsers,
        "top_devices": top_devices,
        "most_active_hours": most_active_hours,
        "feature_usage_frequency": feature_usage_frequency,
        "dataset_usage_frequency": dataset_usage_frequency,
        "company_engagement": company_engagement[:8],
    }
    chart_data = {
        "daily_active_users": [{"name": str(idx), "value": int(val)} for idx, val in daily_active_users.items()],
        "activity_by_hour": most_active_hours,
        "top_pages": most_visited_pages,
        "country_distribution": top_countries,
        "browser_distribution": top_browsers,
        "company_distribution": most_active_companies,
        "feature_usage": feature_usage_frequency,
        "dataset_usage": dataset_usage_frequency,
        "session_duration_distribution": session_duration_distribution,
    }
    return {
        "metrics": metrics,
        "chart_data": chart_data,
        "activity_rows": int(len(frame.index)),
    }


def _create_user_notification(
    user_id: int,
    *,
    title: str,
    message: str,
    notification_type: str = "info",
    link: str | None = None,
) -> None:
    try:
        notification_service.create_notification(
            db.session,
            Notification,
            user_id=int(user_id),
            title=title,
            message=message,
            notification_type=notification_type,
            link=link,
        )
        db.session.commit()
    except Exception:
        db.session.rollback()


def _user_profile_complete(u: object) -> bool:
    if u is None or not getattr(u, "is_authenticated", False):
        return True
    v = getattr(u, "is_profile_complete", None)
    if v is None:
        return True
    return bool(v)


def _company_logo_slug_filename(company_key: str) -> str:
    raw = (company_key or "").strip()
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", raw).strip("_") or "company"
    return f"{slug}.png"


def _static_subdir(*parts: str) -> Path:
    p = APP_DIR / "static"
    for part in parts:
        p = p / part
    p.mkdir(parents=True, exist_ok=True)
    return p


def _save_upload_image(
    storage,
    dest_dir: Path,
    base_name: str,
    allowed_ext: frozenset[str],
) -> str | None:
    if not storage or not getattr(storage, "filename", None):
        return None
    fn = secure_filename(storage.filename or "")
    ext = Path(fn).suffix.lower()
    if ext not in allowed_ext:
        return None
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{base_name}{ext}"
    storage.save(str(dest))
    rel = dest.relative_to(APP_DIR / "static").as_posix()
    return rel


def _company_has_logo_for_key(template_company_key: str) -> bool:
    row = Company.query.filter_by(company_name=template_company_key).first()
    if not row or not row.company_logo_path:
        return False
    try:
        return (APP_DIR / "static" / row.company_logo_path).is_file()
    except Exception:
        return bool(row.company_logo_path)


def _company_logo_static_rel(template_company_key: str) -> str | None:
    row = Company.query.filter_by(company_name=template_company_key).first()
    if not row or not row.company_logo_path:
        return None
    p = APP_DIR / "static" / row.company_logo_path
    try:
        if p.is_file():
            return row.company_logo_path
    except Exception:
        pass
    return None


def _save_company_logo_png(storage, company_key: str) -> str | None:
    if not storage or not getattr(storage, "filename", None):
        return None
    ext = Path(secure_filename(storage.filename or "")).suffix.lower()
    if ext not in COMPANY_LOGO_ALLOWED_EXT:
        return None
    dest_dir = _static_subdir("company_logos")
    dest_name = _company_logo_slug_filename(company_key)
    dest = dest_dir / dest_name
    storage.save(str(dest))
    return f"company_logos/{dest_name}"


def _save_profile_photo_file(storage, user_id: int) -> str | None:
    if not storage or not getattr(storage, "filename", None):
        return None
    fn = secure_filename(storage.filename or "")
    ext = Path(fn).suffix.lower()
    if ext not in PROFILE_PHOTO_ALLOWED_EXT:
        return None
    dest_dir = _static_subdir("profile_photos")
    dest = dest_dir / f"user_{user_id}_{uuid.uuid4().hex[:10]}{ext}"
    storage.save(str(dest))
    return dest.relative_to(APP_DIR / "static").as_posix()


def _ensure_user_profile_columns() -> None:
    try:
        from sqlalchemy import inspect, text

        inspector = inspect(db.engine)
        if not inspector.has_table("user"):
            return
        existing = {col["name"] for col in inspector.get_columns("user")}
        alters: list[str] = []
        backfill_profile_complete = False
        if "first_name" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN first_name VARCHAR(100)")
        if "last_name" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN last_name VARCHAR(100)")
        if "job_title" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN job_title VARCHAR(200)")
        if "phone" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN phone VARCHAR(40)")
        if "company_country" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN company_country VARCHAR(100)")
        if "profile_photo_path" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN profile_photo_path VARCHAR(500)")
        if "is_profile_complete" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN is_profile_complete BOOLEAN")
            backfill_profile_complete = True
        role_added = False
        if "role" not in existing:
            alters.append("ALTER TABLE user ADD COLUMN role VARCHAR(32)")
            role_added = True
        if not alters:
            return
        with db.engine.begin() as conn:
            for stmt in alters:
                conn.execute(text(stmt))
            if backfill_profile_complete:
                conn.execute(text("UPDATE user SET is_profile_complete = 1 WHERE is_profile_complete IS NULL"))
            if role_added:
                conn.execute(text("UPDATE user SET role = 'admin' WHERE is_admin = 1"))
                conn.execute(text("UPDATE user SET role = 'user' WHERE role IS NULL"))
    except Exception:
        pass


def _ensure_mapping_run_source_entry_group_column() -> None:
    try:
        from sqlalchemy import inspect, text

        inspector = inspect(db.engine)
        if not inspector.has_table("mapping_run"):
            return
        existing_columns = {col["name"] for col in inspector.get_columns("mapping_run")}
        if "source_entry_group" in existing_columns:
            return
        with db.engine.begin() as conn:
            conn.execute(text("ALTER TABLE mapping_run ADD COLUMN source_entry_group VARCHAR(40)"))
    except Exception:
        pass


def _resolve_template_company_name(company_name: str) -> str | None:
    raw = (company_name or "").strip()
    candidates = [raw]
    canon, _country = _canonical_company_name_and_country(raw)
    if canon and canon not in candidates:
        candidates.append(canon)

    for candidate in candidates:
        norm_candidate = _normalize_template_key(candidate)
        entry = TEMPLATE_INDEX.get(norm_candidate)
        if entry:
            return str(entry["name"])
    return None


def _resolve_template_sheet_name(company_name: str, sheet_name: str) -> str | None:
    company_key = _resolve_template_company_name(company_name)
    if not company_key:
        return None
    if str(sheet_name or "").strip().lower() == KLARAKARBON_SHEET_NAME.lower():
        return KLARAKARBON_SHEET_NAME
    company_entry = TEMPLATE_INDEX.get(_normalize_template_key(company_key)) or {}
    sheets = company_entry.get("sheets") or {}
    if not isinstance(sheets, dict):
        return None
    sheet_entry = sheets.get(_normalize_template_key(sheet_name))
    if not isinstance(sheet_entry, dict):
        return None
    return str(sheet_entry.get("name") or "")


def _list_template_companies_for_user() -> list[dict[str, str]]:
    if bool(getattr(current_user, "is_admin", False)):
        return [{"key": name, "label": name} for name in TEMPLATES.keys()]

    resolved = _resolve_template_company_name(getattr(current_user, "company_name", "") or "")
    if not resolved:
        return []
    return [{"key": resolved, "label": resolved}]


def _get_template_company_sheets(company_name: str) -> list[str]:
    resolved_company = _resolve_template_company_name(company_name)
    if not resolved_company:
        return []
    sheets = list((TEMPLATES.get(resolved_company) or {}).keys())
    if klarakarbon_company_supported(resolved_company) and KLARAKARBON_SHEET_NAME not in sheets:
        sheets.append(KLARAKARBON_SHEET_NAME)
    return sheets


def _get_template_sheet_headers(company_name: str, sheet_name: str) -> list[str]:
    resolved_company = _resolve_template_company_name(company_name)
    if not resolved_company:
        return []
    if str(sheet_name or "").strip().lower() == KLARAKARBON_SHEET_NAME.lower():
        return list(klarakarbon_entry_headers(resolved_company))
    resolved_sheet = _resolve_template_sheet_name(company_name, sheet_name)
    if not resolved_sheet:
        return []
    return list((TEMPLATES.get(resolved_company) or {}).get(resolved_sheet) or [])


def _build_template_workbook(company_name: str) -> BytesIO:
    resolved_company = _resolve_template_company_name(company_name)
    wb = Workbook()
    try:
        wb.remove(wb.active)
    except Exception:
        pass

    for sheet_name in _get_template_company_sheets(resolved_company or company_name):
        ws = wb.create_sheet(title=str(sheet_name)[:31])
        for idx, header in enumerate(_get_template_sheet_headers(resolved_company or company_name, sheet_name), start=1):
            ws.cell(row=1, column=idx).value = header

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio


def _user_can_access_company(company_name: str) -> bool:
    if bool(getattr(current_user, "is_admin", False)):
        return True
    resolved_requested = _resolve_template_company_name(company_name)
    resolved_owned = _resolve_template_company_name(getattr(current_user, "company_name", "") or "")
    if not resolved_requested or not resolved_owned:
        return False
    return _normalize_template_key(resolved_requested) == _normalize_template_key(resolved_owned)


def _get_data_entry_template_schema(company_name: str, sheet_name: str) -> tuple[list[str], dict[str, dict[str, str]]]:
    headers = _get_template_sheet_headers(company_name, sheet_name)
    if not headers:
        return [], {}
    rules = {h: _infer_column_rule(h) for h in headers}
    return headers, rules


def _clean_data_entry_rows(headers: list[str], rows: list[object]) -> tuple[list[list[str]], list[str]]:
    cleaned_rows: list[list[str]] = []
    for r in rows:
        if not isinstance(r, list):
            continue
        rr = [("" if v is None else str(v)) for v in r]
        if not any(v.strip() for v in rr):
            continue
        if len(rr) < len(headers):
            rr = rr + [""] * (len(headers) - len(rr))
        elif len(rr) > len(headers):
            rr = rr[: len(headers)]
        cleaned_rows.append(rr)
    return _validate_and_normalize_rows(headers, cleaned_rows)


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _is_data_entry_editable(created_at: datetime | None) -> bool:
    if bool(getattr(current_user, "is_admin", False)):
        return True
    if not isinstance(created_at, datetime):
        return True
    return created_at >= (datetime.utcnow() - timedelta(days=7))


def _normalize_data_entry_rows(headers: list[str], rows: list[object]) -> tuple[list[dict[str, object]], list[str]]:
    normalized_rows: list[dict[str, object]] = []
    raw_values: list[list[str]] = []
    for row_idx, r in enumerate(rows, start=1):
        if isinstance(r, dict):
            cells = r.get("cells") or r.get("values") or []
            entry_group = str(r.get("entry_group") or "").strip()
            created_at = _parse_iso_datetime(r.get("created_at"))
            is_persisted = bool(r.get("is_persisted"))
            source_row_index = int(r.get("row_index") or row_idx)
        elif isinstance(r, list):
            cells = r
            entry_group = ""
            created_at = None
            is_persisted = False
            source_row_index = row_idx
        else:
            continue

        if not isinstance(cells, list):
            continue
        rr = [("" if v is None else str(v)) for v in cells]
        if not any(v.strip() for v in rr):
            continue
        if len(rr) < len(headers):
            rr = rr + [""] * (len(headers) - len(rr))
        elif len(rr) > len(headers):
            rr = rr[: len(headers)]
        normalized_rows.append(
            {
                "entry_group": entry_group,
                "created_at": created_at,
                "is_persisted": is_persisted,
                "row_index": source_row_index,
                "cells": rr,
            }
        )
        raw_values.append(rr)

    cleaned_values, validation_errors = _validate_and_normalize_rows(headers, raw_values)
    for meta, cleaned in zip(normalized_rows, cleaned_values):
        meta["cells"] = cleaned
    return normalized_rows, validation_errors


def _data_entry_content_fingerprint(headers: list[str], cells: list[str]) -> str:
    """Stable fingerprint for duplicate detection (full row vector, trimmed)."""
    padded = list(cells[: len(headers)])
    while len(padded) < len(headers):
        padded.append("")
    normalized = [str(c or "").strip() for c in padded]
    return json.dumps(normalized, ensure_ascii=True, separators=(",", ":"))


def _validate_data_entry_row_requirements(headers: list[str], rows: list[dict[str, object]]) -> list[str]:
    """Required-field validation after normalization (types already checked)."""
    errors: list[str] = []
    rules = [_infer_column_rule(h) for h in headers]
    for ridx, meta in enumerate(rows, start=1):
        cells = list(meta.get("cells") or [])
        while len(cells) < len(headers):
            cells.append("")
        if not any(str(c or "").strip() for c in cells):
            continue
        for cidx, h in enumerate(headers):
            rule = rules[cidx] if cidx < len(rules) else {"type": "text"}
            val = str(cells[cidx]).strip() if cidx < len(cells) else ""
            if rule.get("required") == "1" and not val:
                errors.append(f"Row {ridx}, column '{h}': required field is empty")
    return errors


def _delete_data_entry_group(
    company_name: str,
    sheet_name: str,
    entry_group: str,
    created_at: datetime | None,
    row_index: int,
) -> None:
    if entry_group and not entry_group.startswith("legacy:"):
        DataEntry.query.filter_by(
            company_name=company_name,
            sheet_name=sheet_name,
            entry_group=entry_group,
        ).delete(synchronize_session=False)
        return

    query = DataEntry.query.filter_by(
        company_name=company_name,
        sheet_name=sheet_name,
        row_index=row_index,
    )
    if created_at is not None:
        query = query.filter(DataEntry.created_at == created_at)
    query.delete(synchronize_session=False)


def _upsert_data_entries(company_name: str, sheet_name: str, headers: list[str], rows: list[dict[str, object]]) -> dict[str, object]:
    """
    Save rows with duplicate detection (company + sheet + full row content).
    Returns counts and entry_group ids that were newly written.
    """
    grid_snapshot = _load_data_entry_grid_rows(company_name, sheet_name, headers)
    entry_group_to_fp: dict[str, str] = {}
    all_fps: set[str] = set()
    for r in grid_snapshot:
        eg = str(r.get("entry_group") or "").strip()
        if not eg:
            continue
        fp = _data_entry_content_fingerprint(headers, list(r.get("cells") or []))
        entry_group_to_fp[eg] = fp
        all_fps.add(fp)

    saved_rows_count = 0
    duplicate_rows_count = 0
    saved_entry_groups: list[str] = []
    batch_fps: set[str] = set()

    next_row_index = (
        db.session.query(db.func.max(DataEntry.row_index))
        .filter_by(company_name=company_name, sheet_name=sheet_name)
        .scalar()
        or 0
    )

    for row in rows:
        cells = list(row.get("cells") or [])
        if not any(str(v or "").strip() for v in cells):
            continue

        fp = _data_entry_content_fingerprint(headers, cells)
        entry_group = str(row.get("entry_group") or "").strip()
        created_at = row.get("created_at")
        if not isinstance(created_at, datetime):
            created_at = None
        source_row_index = int(row.get("row_index") or 0)
        is_persisted = bool(row.get("is_persisted"))

        check = set(all_fps) | set(batch_fps)
        old_fp = entry_group_to_fp.get(entry_group) if entry_group else None
        if is_persisted and old_fp is not None:
            check.discard(old_fp)
            if fp == old_fp:
                duplicate_rows_count += 1
                continue

        if fp in check:
            duplicate_rows_count += 1
            continue

        if is_persisted:
            if not _is_data_entry_editable(created_at):
                continue
            if old_fp is not None:
                all_fps.discard(old_fp)
            entry_group_to_fp.pop(entry_group, None)
            _delete_data_entry_group(company_name, sheet_name, entry_group, created_at, source_row_index)
            effective_created_at = created_at or datetime.utcnow()
            effective_row_index = source_row_index or (next_row_index + 1)
            effective_entry_group = uuid.uuid4().hex[:12]
        else:
            next_row_index += 1
            effective_created_at = datetime.utcnow()
            effective_row_index = next_row_index
            effective_entry_group = uuid.uuid4().hex[:12]

        for column_name, value in zip(headers, cells):
            vv = (value or "").strip()
            if vv == "":
                continue
            db.session.add(
                DataEntry(
                    company_name=company_name,
                    sheet_name=sheet_name,
                    entry_group=effective_entry_group,
                    uploaded_by_user_id=getattr(current_user, "id", None),
                    row_index=effective_row_index,
                    column_name=column_name,
                    value=vv,
                    created_at=effective_created_at,
                )
            )

        all_fps.add(fp)
        batch_fps.add(fp)
        entry_group_to_fp[effective_entry_group] = fp
        saved_rows_count += 1
        saved_entry_groups.append(effective_entry_group)

    return {
        "saved_rows_count": saved_rows_count,
        "duplicate_rows_count": duplicate_rows_count,
        "saved_entry_groups": saved_entry_groups,
    }


def _load_data_entry_grid_rows(company_name: str, sheet_name: str, headers: list[str]) -> list[dict[str, object]]:
    entries = (
        DataEntry.query.filter_by(company_name=company_name, sheet_name=sheet_name)
        .order_by(DataEntry.created_at.asc(), DataEntry.row_index.asc(), DataEntry.id.asc())
        .all()
    )
    grouped: dict[str, dict[str, object]] = {}
    order: list[str] = []
    for entry in entries:
        created_at = getattr(entry, "created_at", None)
        row_index = int(getattr(entry, "row_index", 0) or 0)
        entry_group = str(getattr(entry, "entry_group", "") or "").strip()
        group_key = entry_group or f"legacy:{created_at.isoformat() if created_at else ''}:{row_index}"
        if group_key not in grouped:
            grouped[group_key] = {
                "entry_group": group_key,
                "row_index": row_index,
                "created_at": created_at,
                "values": {},
            }
            order.append(group_key)
        values = grouped[group_key]["values"]
        if isinstance(values, dict):
            values[str(entry.column_name)] = "" if entry.value is None else str(entry.value)

    rows: list[dict[str, object]] = []
    for key in order:
        row = grouped[key]
        values = row.get("values") if isinstance(row.get("values"), dict) else {}
        created_at = row.get("created_at") if isinstance(row.get("created_at"), datetime) else None
        rows.append(
            {
                "entry_group": row.get("entry_group") or "",
                "row_index": int(row.get("row_index") or 0),
                "created_at": created_at.isoformat() if created_at else "",
                "is_editable": _is_data_entry_editable(created_at),
                "is_persisted": True,
                "cells": [values.get(header, "") for header in headers],
            }
        )
    return rows


def _load_data_entries_dataframe(company_name: str, sheet_name: str, headers: list[str]) -> "pd.DataFrame":
    rows = _load_data_entry_grid_rows(company_name, sheet_name, headers)
    values = [list(row.get("cells") or []) for row in rows]
    return pd.DataFrame(values, columns=headers)


def _load_data_entries_dataframe_for_entry_groups(
    company_name: str, sheet_name: str, headers: list[str], entry_groups: set[str]
) -> "pd.DataFrame":
    """Build a dataframe containing only logical rows whose entry_group is in entry_groups."""
    if not entry_groups:
        return pd.DataFrame(columns=headers)
    rows = _load_data_entry_grid_rows(company_name, sheet_name, headers)
    filtered = [r for r in rows if str(r.get("entry_group") or "").strip() in entry_groups]
    values = [list(row.get("cells") or []) for row in filtered]
    if not values:
        return pd.DataFrame(columns=headers)
    return pd.DataFrame(values, columns=headers)


def _batch_action_type_for_sheet(sheet_name: str) -> str:
    sheet_key = str(sheet_name or "").strip().lower()
    if sheet_key in {KLARAKARBON_SHEET_NAME.lower(), TRAVEL_SHEET_NAME.lower()}:
        return "append_run"
    return "map"


def _batch_action_label_for_sheet(sheet_name: str) -> str:
    return "Append & Run pipeline" if _batch_action_type_for_sheet(sheet_name) == "append_run" else "Map"


def _display_name_for_user(user: "User | None") -> str:
    if user is None:
        return "Unknown"
    first = str(getattr(user, "first_name", "") or "").strip()
    last = str(getattr(user, "last_name", "") or "").strip()
    full = " ".join(part for part in [first, last] if part).strip()
    if full:
        return full
    email = str(getattr(user, "email", "") or "").strip()
    if email and "@" in email:
        return email.split("@", 1)[0]
    return email or "Unknown"


def _list_admin_data_entry_batches() -> list[dict[str, object]]:
    """
    Uploaded data grouped to one row per (company, sheet).
    """
    from sqlalchemy import func

    _ensure_db_tables()

    co_company = func.trim(func.coalesce(DataEntry.company_name, ""))
    co_sheet = func.trim(func.coalesce(DataEntry.sheet_name, ""))

    rows = (
        db.session.query(
            co_company.label("company_name"),
            co_sheet.label("sheet_name"),
            func.max(DataEntry.created_at).label("uploaded_at"),
            func.count(func.distinct(DataEntry.row_index)).label("row_count"),
        )
        .group_by(co_company, co_sheet)
        .order_by(func.max(DataEntry.created_at).desc())
        .limit(500)
        .all()
    )

    latest_merged_workbook = _find_latest_merged_mapping_workbook()
    latest_merged_mtime = None
    try:
        latest_merged_mtime = latest_merged_workbook.stat().st_mtime if latest_merged_workbook else None
    except Exception:
        latest_merged_mtime = None

    seen_keys: set[tuple[str, str]] = set()
    out: list[dict[str, object]] = []
    for r in rows:
        company = str(r.company_name or "").strip()
        sheet = str(r.sheet_name or "").strip()
        dedup_key = (company, sheet)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        if _is_hidden_schema_sheet(sheet):
            continue

        action_type = _batch_action_type_for_sheet(sheet)
        mapped_run = (
            MappingRun.query.filter_by(
                company_name=company,
                sheet_name=sheet,
                status="succeeded",
            )
            .order_by(MappingRun.created_at.desc())
            .first()
        )
        mapper_email = ""
        mapped_at_dt = getattr(mapped_run, "created_at", None) if mapped_run else None
        is_mapped = False

        if action_type == "map":
            if mapped_run is not None and mapped_at_dt is not None:
                try:
                    is_mapped = bool(r.uploaded_at) and mapped_at_dt >= r.uploaded_at
                except Exception:
                    is_mapped = True
        else:
            mapped_run = (
                mapped_run
            )
            if latest_merged_mtime is not None and getattr(r, "uploaded_at", None) is not None:
                try:
                    is_mapped = latest_merged_mtime >= r.uploaded_at.timestamp()
                except Exception:
                    is_mapped = False

        if mapped_run is not None:
            u = db.session.get(User, mapped_run.user_id)
            if u is not None:
                mapper_email = str(getattr(u, "email", "") or "")
        latest_entry = (
            DataEntry.query.filter_by(company_name=company, sheet_name=sheet)
            .order_by(DataEntry.created_at.desc(), DataEntry.id.desc())
            .first()
        )
        uploaded_by_user_id = int(getattr(latest_entry, "uploaded_by_user_id", 0) or 0)
        uploaded_by_name = ""
        if uploaded_by_user_id:
            uploaded_user = db.session.get(User, uploaded_by_user_id)
            uploaded_by_name = _display_name_for_user(uploaded_user)
        uid = hashlib.sha1(f"{company}\x00{sheet}".encode("utf-8")).hexdigest()[:16]
        out.append(
            {
                "batch_uid": uid,
                "company_name": company,
                "sheet_name": sheet,
                "entry_group": "",
                "uploaded_at": r.uploaded_at,
                "row_count": int(r.row_count or 0),
                "mappable": True,
                "action_type": action_type,
                "action_label": _batch_action_label_for_sheet(sheet),
                "mapped": is_mapped,
                "mapped_at": mapped_at_dt,
                "mapped_by": mapper_email,
                "uploaded_by_user": uploaded_by_name,
                "download_run_id": str(getattr(mapped_run, "id", "") or "") if mapped_run else "",
            }
        )

    travel_path = STAGE2_TRAVEL_DIR / "analysis_summary.xlsx"
    if travel_path.exists():
        companies_for_travel = sorted({str(b.get("company_name") or "").strip() for b in out if str(b.get("company_name") or "").strip()})
        uploaded_at = None
        try:
            uploaded_at = datetime.utcfromtimestamp(travel_path.stat().st_mtime)
        except Exception:
            uploaded_at = None
        existing_travel = {(str(b.get("company_name") or "").strip(), str(b.get("sheet_name") or "").strip()) for b in out}
        for company in companies_for_travel:
            key = (company, TRAVEL_SHEET_NAME)
            if key in existing_travel:
                continue
            uid = hashlib.sha1(f"{company}\x00{TRAVEL_SHEET_NAME}".encode("utf-8")).hexdigest()[:16]
            is_mapped = False
            if latest_merged_mtime is not None and uploaded_at is not None:
                try:
                    is_mapped = latest_merged_mtime >= uploaded_at.timestamp()
                except Exception:
                    is_mapped = False
            out.append(
                {
                    "batch_uid": uid,
                    "company_name": company,
                    "sheet_name": TRAVEL_SHEET_NAME,
                    "entry_group": "",
                    "uploaded_at": uploaded_at,
                    "row_count": 0,
                    "mappable": True,
                    "action_type": "append_run",
                    "action_label": _batch_action_label_for_sheet(TRAVEL_SHEET_NAME),
                    "mapped": is_mapped,
                    "mapped_at": None,
                    "mapped_by": "",
                    "uploaded_by_user": "System",
                    "download_run_id": "",
                }
            )
    return out


def _batches_for_admin_mapping_json(batches: list[dict[str, object]]) -> list[dict[str, object]]:
    """JSON-serializable batch list for the admin mapping page scripts."""
    out: list[dict[str, object]] = []
    for b in batches:
        ua = b.get("uploaded_at")
        ma = b.get("mapped_at")
        out.append(
            {
                "batch_uid": str(b.get("batch_uid") or ""),
                "company_name": str(b.get("company_name") or ""),
                "sheet_name": str(b.get("sheet_name") or ""),
                "entry_group": str(b.get("entry_group") or ""),
                "row_count": int(b.get("row_count") or 0),
                "mappable": bool(b.get("mappable")),
                "action_type": str(b.get("action_type") or ""),
                "action_label": str(b.get("action_label") or ""),
                "mapped": bool(b.get("mapped")),
                "uploaded_at": ua.isoformat() + "Z" if isinstance(ua, datetime) else "",
                "mapped_at": ma.isoformat() + "Z" if isinstance(ma, datetime) else "",
                "mapped_by": str(b.get("mapped_by") or ""),
                "uploaded_by_user": str(b.get("uploaded_by_user") or ""),
                "download_run_id": str(b.get("download_run_id") or ""),
            }
        )
    return out


def _ensure_mapping_run_summary_columns() -> None:
    try:
        from sqlalchemy import inspect, text

        inspector = inspect(db.engine)
        if not inspector.has_table("mapping_run_summary"):
            return

        existing_columns = {col["name"] for col in inspector.get_columns("mapping_run_summary")}
        alter_statements = []
        if "mapped_categories_count" not in existing_columns:
            alter_statements.append(
                "ALTER TABLE mapping_run_summary ADD COLUMN mapped_categories_count INTEGER DEFAULT 0"
            )
        if "total_categories" not in existing_columns:
            alter_statements.append(
                "ALTER TABLE mapping_run_summary ADD COLUMN total_categories INTEGER DEFAULT 0"
            )
        if "coverage_pct" not in existing_columns:
            alter_statements.append(
                "ALTER TABLE mapping_run_summary ADD COLUMN coverage_pct FLOAT DEFAULT 0"
            )

        if not alter_statements:
            return

        with db.engine.begin() as conn:
            for stmt in alter_statements:
                conn.execute(text(stmt))
    except Exception:
        pass


def _ensure_data_entry_columns() -> None:
    try:
        from sqlalchemy import inspect, text

        inspector = inspect(db.engine)
        if not inspector.has_table("data_entry"):
            return

        existing_columns = {col["name"] for col in inspector.get_columns("data_entry")}
        alters: list[str] = []
        if "entry_group" not in existing_columns:
            alters.append("ALTER TABLE data_entry ADD COLUMN entry_group VARCHAR(32) DEFAULT ''")
        if "uploaded_by_user_id" not in existing_columns:
            alters.append("ALTER TABLE data_entry ADD COLUMN uploaded_by_user_id INTEGER")
        if not alters:
            return
        with db.engine.begin() as conn:
            for stmt in alters:
                conn.execute(text(stmt))
    except Exception:
        pass


def _infer_scope_from_sheet(sheet_name: str) -> int | None:
    s = (sheet_name or "").strip().lower()
    if "scope 1" in s or s.startswith("scope1"):
        return 1
    if "scope 2" in s or s.startswith("scope2"):
        return 2
    if "scope 3" in s or s.startswith("scope3"):
        return 3
    return None


def _count_company_mapped_categories(company_name: str) -> int:
    company_keys = _company_candidate_keys(company_name)
    if not company_keys:
        return 0

    runs = (
        MappingRun.query.filter(
            MappingRun.status == "succeeded",
            MappingRun.company_name.in_(company_keys),
        )
        .order_by(MappingRun.created_at.desc())
        .all()
    )
    seen: set[str] = set()
    for run in runs:
        sheet_key = (getattr(run, "sheet_name", "") or "").strip().lower()
        if sheet_key:
            seen.add(sheet_key)
    return len(seen)


def _calculate_mapping_coverage(company_name: str) -> tuple[int, int, float]:
    mapped_categories_count = int(_count_company_mapped_categories(company_name) or 0)
    total_categories = int(_count_company_schema_sheets(company_name) or 0)
    coverage_pct = round((mapped_categories_count / total_categories) * 100, 2) if total_categories > 0 else 0.0
    return mapped_categories_count, total_categories, coverage_pct


def _upsert_mapping_run_summary(
    run_id: str,
    company_name: str,
    sheet_name: str,
    mapped_df: "pd.DataFrame",
    output_path: str | Path | None,
) -> None:
    total_tco2e = 0.0
    rows_count = 0
    used_col = None
    if output_path:
        total_tco2e, rows_count, used_col = _sum_tco2e_from_xlsx(output_path, sheet_name)
    if used_col is None:
        total_tco2e, rows_count, used_col = _sum_tco2e(mapped_df)

    mapped_categories_count, total_categories, coverage_pct = _calculate_mapping_coverage(company_name)
    print("SUMMARY:", mapped_categories_count, total_categories, coverage_pct)

    summ = MappingRunSummary.query.filter_by(run_id=run_id).first()
    if not summ:
        company_key = (company_name or "").strip()
        sheet_key = (sheet_name or "").strip().lower()
        existing_for_sheet = (
            MappingRunSummary.query.filter_by(company_name=company_key)
            .order_by(MappingRunSummary.created_at.desc())
            .all()
        )
        for row in existing_for_sheet:
            row_sheet_key = (getattr(row, "sheet_name", "") or "").strip().lower()
            if row_sheet_key == sheet_key:
                summ = row
                break

    if not summ:
        summ = MappingRunSummary(run_id=run_id)
        db.session.add(summ)

    summ.run_id = run_id
    summ.company_name = company_name
    summ.sheet_name = sheet_name
    summ.scope = _infer_scope_from_sheet(sheet_name)
    summ.tco2e_total = float(total_tco2e or 0.0)
    summ.rows_count = int(rows_count or 0)
    summ.mapped_categories_count = int(mapped_categories_count or 0)
    summ.total_categories = int(total_categories or 0)
    summ.coverage_pct = float(coverage_pct or 0.0)
    summ.created_at = datetime.now()


def _find_tco2e_column(df: "pd.DataFrame") -> str | None:
    if df is None or getattr(df, "columns", None) is None:
        return None

    def norm(x: str) -> str:
        return "".join(ch.lower() for ch in (x or "") if ch.isalnum())

    candidates: list[str] = []
    for c in list(df.columns):
        n = norm(str(c))
        if n in {"emissionstco2e", "emissionstco2etonnes", "tco2e", "totalemissionstco2e"}:
            candidates.append(str(c))
            continue
        if n in ("co2e", "co2et", "co2etonnes", "co2eton", "co2etonne"):
            candidates.append(str(c))
            continue
        if n.startswith("emissionstco2e") or n.startswith("co2et") or n.startswith("co2e"):
            candidates.append(str(c))
            continue
        if ("co2e" in n) and ("calculated" in n):
            candidates.append(str(c))

    if not candidates:
        return None

    best_col = None
    best_score = (-1, -1.0)
    for c in candidates:
        try:
            ser = df[c]
        except Exception:
            continue

        numeric_count = 0
        total_abs = 0.0
        try:
            for v in ser.tolist():
                f = _parse_float_loose(v)
                if f is None:
                    continue
                numeric_count += 1
                total_abs += abs(float(f))
        except Exception:
            continue

        if (numeric_count, total_abs) > best_score:
            best_col = str(c)
            best_score = (numeric_count, total_abs)

    return best_col


def _sum_tco2e(df: "pd.DataFrame") -> tuple[float, int, str | None]:
    """
    Returns (total_tco2e, rows_count, used_column).
    """
    col = _find_tco2e_column(df)
    if not col:
        return 0.0, int(len(df.index)) if df is not None else 0, None
    try:
        ser_raw = df[col]
        # Robust numeric parsing for Excel exports (comma decimals, NBSP, etc.)
        try:
            if getattr(ser_raw, "dtype", None) == "object":
                ser_raw = (
                    ser_raw.astype(str)
                    .str.replace("\u00a0", "", regex=False)
                    .str.replace(" ", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )
        except Exception:
            pass
        ser = pd.to_numeric(ser_raw, errors="coerce")
        total = float(ser.fillna(0).sum())
    except Exception:
        total = 0.0
    return total, int(len(df.index)) if df is not None else 0, col


def _parse_float_loose(v) -> float | None:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (int, float)):
        try:
            out = float(v)
            if math.isnan(out) or math.isinf(out):
                return None
            return out
        except Exception:
            return None
    try:
        s = str(v).strip()
    except Exception:
        return None
    if not s:
        return None
    s = s.replace("\u00a0", "").replace(" ", "")
    # Extract first numeric token (handles "12.3t", "1,23", "≈ 5.1", etc.)
    import re as _re
    m = _re.search(r"[+-]?\d+(?:[.,]\d+)?(?:e[+-]?\d+)?", s, flags=_re.IGNORECASE)
    if not m:
        return None
    token = m.group(0).replace(",", ".")
    try:
        out = float(token)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _sum_tco2e_from_xlsx(xlsx_path: str | Path, sheet_name: str | None) -> tuple[float, int, str | None]:
    """
    Sum tCO2e values from an Excel workbook using openpyxl with data_only=True.
    This is more reliable than pandas when cells contain formulas (Excel-visible numbers).
    Returns (total_tco2e, rows_count, used_header_name).
    """
    try:
        p = Path(xlsx_path)
    except Exception:
        p = None
    if not p or not str(p) or not os.path.exists(str(p)):
        return 0.0, 0, None

    try:
        wb = load_workbook(str(p), read_only=True, data_only=True, keep_links=False)
    except Exception:
        return 0.0, 0, None

    target_ws = None
    want = (sheet_name or "").strip().lower()
    if want:
        for n in wb.sheetnames:
            if str(n).strip().lower() == want:
                target_ws = wb[n]
                break
    if target_ws is None and wb.sheetnames:
        target_ws = wb[wb.sheetnames[0]]
    if target_ws is None:
        return 0.0, 0, None

    def norm(x: str) -> str:
        return "".join(ch.lower() for ch in (x or "") if ch.isalnum())

    header_row, headers = _detect_header_row_and_headers(target_ws, max_scan_rows=50)
    candidates: list[tuple[int, str]] = []
    for i, h in enumerate(headers, start=1):
        nh = norm(str(h))
        if nh in (
            "emissionstco2e",
            "emissionstco2etonnes",
            "tco2e",
            "totalemissionstco2e",
            "co2e",
            "co2et",
            "co2etonnes",
        ):
            candidates.append((i, str(h)))
            continue
        if nh.startswith("emissionstco2e") or nh.startswith("co2et") or nh.startswith("co2e"):
            candidates.append((i, str(h)))
            continue
        # Some providers include long headers containing "Calculated CO2e"
        if ("co2e" in nh) and ("calculated" in nh):
            candidates.append((i, str(h)))

    if not candidates:
        return 0.0, 0, None

    rows_count = 0
    max_r = int(getattr(target_ws, "max_row", 1) or 1)
    # First pass: count non-empty rows (for display) + compute per-candidate sums.
    best = {"used": None, "total": 0.0, "numeric_count": 0}
    for r in range(header_row + 1, max_r + 1):
        # Skip fully empty rows to keep counts consistent
        row_has_any = False
        try:
            for c in range(1, min(int(getattr(target_ws, "max_column", 1) or 1), 40) + 1):
                v2 = target_ws.cell(row=r, column=c).value
                if v2 is not None and str(v2).strip() != "":
                    row_has_any = True
                    break
        except Exception:
            row_has_any = True
        if not row_has_any:
            continue

        rows_count += 1
        for col_idx, used in candidates:
            val = target_ws.cell(row=r, column=col_idx).value
            f = _parse_float_loose(val)
            if f is None:
                continue
            # Accumulate into a temporary bucket per candidate by encoding in dict key
            k = f"{col_idx}:{used}"
            # stash totals in best dict dynamically
            # (avoid allocating large per-column arrays)
            tot_key = f"tot::{k}"
            cnt_key = f"cnt::{k}"
            best[tot_key] = float(best.get(tot_key, 0.0)) + float(f)
            best[cnt_key] = int(best.get(cnt_key, 0)) + 1

    # Choose the candidate column with the highest numeric_count; ties broken by total magnitude.
    chosen = None
    chosen_total = 0.0
    chosen_cnt = 0
    for col_idx, used in candidates:
        k = f"{col_idx}:{used}"
        cnt = int(best.get(f"cnt::{k}", 0))
        tot = float(best.get(f"tot::{k}", 0.0))
        if cnt > chosen_cnt or (cnt == chosen_cnt and abs(tot) > abs(chosen_total)):
            chosen = used
            chosen_total = tot
            chosen_cnt = cnt

    if chosen is None:
        return 0.0, int(rows_count), None

    return float(chosen_total), int(rows_count), str(chosen)


def _detect_data_year_from_xlsx(xlsx_path: str | Path, sheet_name: str | None, scan_rows: int = 500) -> int | None:
    """
    Best-effort: infer reporting year from a period column like
    'Reporting period (month, year)' with values like \"Jan'-2025\".
    Returns the most common year found (or max if tie), else None.
    """
    try:
        p = Path(xlsx_path)
    except Exception:
        return None
    if not p or not os.path.exists(str(p)):
        return None
    try:
        wb = load_workbook(str(p), read_only=True, data_only=True, keep_links=False)
    except Exception:
        return None

    want = (sheet_name or "").strip().lower()
    ws = None
    if want:
        for n in wb.sheetnames:
            if str(n).strip().lower() == want:
                ws = wb[n]
                break
    if ws is None and wb.sheetnames:
        ws = wb[wb.sheetnames[0]]
    if ws is None:
        return None

    def norm(x: str) -> str:
        return "".join(ch.lower() for ch in (x or "") if ch.isalnum())

    header_row, headers = _detect_header_row_and_headers(ws, max_scan_rows=50)
    period_col = None
    for i, h in enumerate(headers, start=1):
        nh = norm(str(h))
        if nh in ("reportingperiodmonthyear", "reportingperiod", "reportingperiodmonth", "monthyear", "periodmonthyear"):
            period_col = i
            break
        if ("period" in nh and "year" in nh) or nh.startswith("reportingperiod"):
            period_col = i
            break
    if period_col is None:
        return None

    import re as _re
    counts: dict[int, int] = {}
    max_r = int(getattr(ws, "max_row", 1) or 1)
    end = min(max_r, header_row + int(scan_rows))
    for r in range(header_row + 1, end + 1):
        v = ws.cell(row=r, column=period_col).value
        if v is None:
            continue
        s = str(v)
        m = _re.search(r"(19|20)\d{2}", s)
        if not m:
            continue
        y = int(m.group(0))
        counts[y] = counts.get(y, 0) + 1
    if not counts:
        return None
    best = sorted(counts.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)[0][0]
    return int(best)


def _parse_period_value(value) -> datetime | None:
    if value is None:
        return None
    try:
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return datetime(int(value.year), int(value.month), 1)
    except Exception:
        pass
    if isinstance(value, datetime):
        return datetime(value.year, value.month, 1)

    s = str(value).strip()
    if not s:
        return None

    import re as _re
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }

    # Many uploaded workbooks store "reporting period (month, year)"
    # as the first day of each month, e.g. 01/03/2025 for Mar 2025.
    m = _re.match(r"^\s*(\d{1,2})[/-](\d{1,2})[/-]((?:19|20)\d{2})\s*$", s)
    if m:
        first = int(m.group(1))
        second = int(m.group(2))
        year = int(m.group(3))
        if first == 1 and 1 <= second <= 12:
            return datetime(year, second, 1)
        if second == 1 and 1 <= first <= 12:
            return datetime(year, first, 1)

    m = _re.search(r"(?i)\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[\'\s\-_/,]*(19|20)\d{2}\b", s)
    if m:
        mon = month_map[m.group(1).lower()[:3]]
        year_match = _re.search(r"(19|20)\d{2}", s)
        if year_match:
            return datetime(int(year_match.group(0)), mon, 1)

    m = _re.search(r"\b((?:19|20)\d{2})[-/](0?[1-9]|1[0-2])\b", s)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), 1)

    try:
        dt = pd.to_datetime([s], errors="coerce", dayfirst=False)[0]
        if pd.isna(dt):
            return None
        return datetime(int(dt.year), int(dt.month), 1)
    except Exception:
        return None


def _find_period_column(df: "pd.DataFrame", sample_rows: int = 120) -> str | None:
    if df is None or getattr(df, "columns", None) is None or len(df.columns) == 0:
        return None

    def norm(x: str) -> str:
        return "".join(ch.lower() for ch in (x or "") if ch.isalnum())

    preferred_exact = {
        "date",
        "reportingperiodmonthyear",
        "reportingperiod",
        "reportingperiodmonth",
        "monthyear",
        "periodmonthyear",
        "reportingmonth",
    }

    for c in list(df.columns):
        n = norm(str(c))
        if n in preferred_exact:
            return str(c)
        if ("date" in n) or ("period" in n and ("month" in n or "year" in n)):
            return str(c)

    first_col = str(df.columns[0])
    try:
        sample = [v for v in df[first_col].tolist()[:sample_rows] if str(v).strip() != ""]
        if sample:
            parsed = sum(1 for v in sample if _parse_period_value(v) is not None)
            if parsed >= max(2, int(len(sample) * 0.5)):
                return first_col
    except Exception:
        pass

    best_col = None
    best_score = (-1, -1.0)
    for c in list(df.columns):
        try:
            sample = [v for v in df[str(c)].tolist()[:sample_rows] if str(v).strip() != ""]
        except Exception:
            continue
        if not sample:
            continue
        parsed = sum(1 for v in sample if _parse_period_value(v) is not None)
        ratio = parsed / max(len(sample), 1)
        if parsed >= 3 and ratio >= 0.55 and (parsed, ratio) > best_score:
            best_col = str(c)
            best_score = (parsed, ratio)
    return best_col


def _read_sheet_df_from_workbook(xlsx_path: str | Path, sheet_name: str | None) -> "pd.DataFrame | None":
    try:
        sheets = pd.read_excel(xlsx_path, sheet_name=None, engine="openpyxl")
    except Exception:
        return None

    want = (sheet_name or "").strip().lower()
    if want:
        for k, v in sheets.items():
            if str(k).strip().lower() == want:
                return v

    if len(sheets) == 1:
        return next(iter(sheets.values()))

    if want:
        want_prefix = want[:20]
        for k, v in sheets.items():
            if str(k).strip().lower().startswith(want_prefix):
                return v

    return next(iter(sheets.values())) if sheets else None


def _build_period_profile_from_df(df: "pd.DataFrame") -> dict[str, object]:
    total_tco2e, rows_count, used_col = _sum_tco2e(df)
    if not used_col:
        return {
            "points": [],
            "total": 0.0,
            "rows_count": int(rows_count or 0),
            "min_date": None,
            "max_date": None,
        }

    period_col = _find_period_column(df)
    if not period_col:
        return {
            "points": [],
            "total": float(total_tco2e or 0.0),
            "rows_count": int(rows_count or 0),
            "min_date": None,
            "max_date": None,
        }

    buckets: dict[str, float] = defaultdict(float)
    for _, row in df.iterrows():
        try:
            raw_date = row.get(period_col)
            raw_value = row.get(used_col)
        except Exception:
            continue
        dt = _parse_period_value(raw_date)
        val = _parse_float_loose(raw_value)
        if dt is None or val is None:
            continue
        key = dt.strftime("%Y-%m")
        buckets[key] += float(val)

    if not buckets:
        return {
            "points": [],
            "total": float(total_tco2e or 0.0),
            "rows_count": int(rows_count or 0),
            "min_date": None,
            "max_date": None,
        }

    points = []
    for key in sorted(buckets.keys()):
        year, month = key.split("-")
        dt = datetime(int(year), int(month), 1)
        points.append(
            {
                "key": key,
                "label": dt.strftime("%b %Y"),
                "date": dt,
                "value": round(float(buckets[key]), 6),
            }
        )

    return {
        "points": points,
        "total": round(sum(float(p["value"]) for p in points), 6),
        "rows_count": int(rows_count or 0),
        "min_date": points[0]["date"] if points else None,
        "max_date": points[-1]["date"] if points else None,
    }


def _format_period_label(points: list[dict[str, object]], fallback_dt: datetime | None) -> str:
    if points:
        start = points[0].get("date")
        end = points[-1].get("date")
        if isinstance(start, datetime) and isinstance(end, datetime):
            if start.year == end.year and start.month == end.month:
                return start.strftime("%Y-%m")
            return f"{start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}"
    if isinstance(fallback_dt, datetime):
        return fallback_dt.strftime("%Y-%m-%d")
    return ""


def _period_profile_for_summary(sub: "MappingRunSummary", run_cache: dict[str, "MappingRun | None"] | None = None) -> dict[str, object]:
    rid = str(getattr(sub, "run_id", "") or "")
    cache_key = "|".join(
        [
            rid,
            str(getattr(sub, "sheet_name", "") or ""),
            str(getattr(sub, "created_at", "") or ""),
            str(getattr(sub, "tco2e_total", "") or ""),
            str(getattr(sub, "rows_count", "") or ""),
        ]
    )
    with _PERIOD_PROFILE_CACHE_LOCK:
        cached = _PERIOD_PROFILE_CACHE.get(cache_key)
        if cached is not None:
            return cached

    # Avoid reopening mapped workbooks during requests. Those reads were causing
    # slow responses and proxy timeouts on report/dashboard pages.
    profile = _fallback_period_profile_for_summary(sub)
    with _PERIOD_PROFILE_CACHE_LOCK:
        _PERIOD_PROFILE_CACHE[cache_key] = profile
    return profile


def _company_candidate_keys(raw_company_name: str) -> list[str]:
    keys: list[str] = []
    raw = (raw_company_name or "").strip()
    if raw:
        keys.append(raw)
    canon, _country = _canonical_company_name_and_country(raw)
    if canon and canon not in keys:
        keys.append(canon)
    try:
        p = _resolve_company_file(canon or raw)
        if p and p.stem and p.stem not in keys:
            keys.append(p.stem)
    except Exception:
        pass
    return keys


def _count_company_schema_sheets(company_name: str) -> int:
    sheets = [s for s in _get_template_company_sheets(company_name) if str(s).strip() != KLARAKARBON_SHEET_NAME]
    return int(len(sheets))


HIDDEN_SCHEMA_SHEETS = {"readme", "company information"}


def _is_hidden_schema_sheet(sheet_name: str) -> bool:
    return (sheet_name or "").strip().lower() in HIDDEN_SCHEMA_SHEETS


def _infer_column_rule(header: str) -> dict[str, str]:
    raw = (header or "").strip()
    low = raw.lower()
    compact = "".join(ch for ch in low if ch.isalnum())

    date_markers = (
        "reportingperiod",
        "release date",
        "attachment date",
        "start date",
        "end date",
        "travel date",
        "date duration",
        "purchase date",
    )
    number_keywords = (
        "spend",
        "amount",
        "consumption",
        "activity volume",
        "activity amount",
        "fuel consumption",
        "distance",
        "km",
        "kwh",
        "mwh",
        "litre",
        "liter",
        "volume",
        "quantity",
        "qty",
        "weight",
        "ton",
        "tonne",
        "hours",
        "days",
        "months",
        "employee count",
        "headcount",
        "ef_value",
    )
    text_exclusions = ("id", "unit", "currency", "supplier", "source", "description", "name", "country")

    if any(marker in low for marker in date_markers) or compact == "date":
        return {"type": "date", "format": "YYYY-MM-DD", "placeholder": "YYYY-MM-DD", "required": "1"}

    if any(ex in low for ex in text_exclusions):
        return {"type": "text"}

    if any(key in low for key in number_keywords):
        return {"type": "number", "format": "decimal", "placeholder": "0"}

    return {"type": "text"}


def _normalize_date_like(value: str) -> str | None:
    s = (value or "").strip()
    if not s:
        return ""
    try:
        dt = pd.to_datetime([s], errors="coerce", dayfirst=False)[0]
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _normalize_number_like(value: str) -> str | None:
    s = (value or "").strip()
    if not s:
        return ""
    f = _parse_float_loose(s)
    if f is None:
        return None
    return str(f)


def _validate_and_normalize_rows(headers: list[str], rows: list[list[str]]) -> tuple[list[list[str]], list[str]]:
    rules = [_infer_column_rule(h) for h in headers]
    out: list[list[str]] = []
    errors: list[str] = []
    for ridx, row in enumerate(rows, start=1):
        normalized: list[str] = []
        for cidx, value in enumerate(row):
            rule = rules[cidx] if cidx < len(rules) else {"type": "text"}
            text = "" if value is None else str(value).strip()
            if rule.get("type") == "date":
                norm = _normalize_date_like(text)
                if norm is None:
                    errors.append(f"Row {ridx}, column '{headers[cidx]}': expected date format YYYY-MM-DD")
                    normalized.append(text)
                else:
                    normalized.append(norm)
            elif rule.get("type") == "number":
                norm = _normalize_number_like(text)
                if norm is None:
                    errors.append(f"Row {ridx}, column '{headers[cidx]}': expected a numeric value")
                    normalized.append(text)
                else:
                    normalized.append(norm)
            else:
                normalized.append(text)
        out.append(normalized)
    return out, errors


def _latest_sheet_totals_for_company(company_keys: list[str]) -> list[MappingRunSummary]:
    """
    For a company (possibly multiple aliases), return the latest summary per sheet.
    """
    if not company_keys:
        return []
    q = (
        MappingRunSummary.query.filter(MappingRunSummary.company_name.in_(company_keys))
        .order_by(MappingRunSummary.created_at.desc())
        .all()
    )
    seen: set[str] = set()
    out: list[MappingRunSummary] = []
    for r in q:
        k = (r.sheet_name or "").strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def _backfill_mapping_summaries(max_runs: int = 200) -> None:
    return    """
    Best-effort backfill for existing MappingRun rows created before summaries existed.
    Safe to call frequently; only fills missing summaries.
    """
    try:
        _ensure_db_tables()
        if not _should_attempt_backfill_mapping_summaries():
            return
        runs = (
            MappingRun.query.filter_by(status="succeeded")
            .order_by(MappingRun.created_at.desc())
            .limit(int(max_runs))
            .all()
        )
        changed = False
        for mr in runs:
            rid = getattr(mr, "id", None)
            if not rid:
                continue
            summ_existing = MappingRunSummary.query.filter_by(run_id=rid).first()
            if summ_existing:
                continue
            p = getattr(mr, "output_path", None)
            if not p or not os.path.exists(str(p)):
                continue
            total_tco2e, rows_count, _used_col = _sum_tco2e_from_xlsx(p, getattr(mr, "sheet_name", None))
            if _used_col is None:
                # Fallback: try pandas if workbook read fails
                try:
                    sheets = pd.read_excel(p, sheet_name=None, engine="openpyxl")
                except Exception:
                    continue
                mapped_df = None
                want = (getattr(mr, "sheet_name", "") or "").strip().lower()
                if want:
                    for k, v in sheets.items():
                        if str(k).strip().lower() == want:
                            mapped_df = v
                            break
                if mapped_df is None:
                    mapped_df = next(iter(sheets.values())) if sheets else None
                if mapped_df is None:
                    continue
                total_tco2e, rows_count, _used_col = _sum_tco2e(mapped_df)
            scope = _infer_scope_from_sheet(getattr(mr, "sheet_name", "") or "")
            if summ_existing:
                summ_existing.company_name = getattr(mr, "company_name", "") or summ_existing.company_name
                summ_existing.sheet_name = getattr(mr, "sheet_name", "") or summ_existing.sheet_name
                summ_existing.scope = scope
                summ_existing.tco2e_total = float(total_tco2e or 0.0)
                summ_existing.rows_count = int(rows_count or 0)
                if not summ_existing.created_at:
                    summ_existing.created_at = getattr(mr, "created_at", None) or datetime.utcnow()
                changed = True
            else:
                summ = MappingRunSummary(
                    run_id=rid,
                    company_name=getattr(mr, "company_name", "") or "",
                    sheet_name=getattr(mr, "sheet_name", "") or "",
                    scope=scope,
                    tco2e_total=float(total_tco2e or 0.0),
                    rows_count=int(rows_count or 0),
                    created_at=getattr(mr, "created_at", None) or datetime.utcnow(),
                )
                db.session.add(summ)
                changed = True
        if changed:
            db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


def _norm_name(s: str) -> str:
    return "".join(ch.lower() for ch in (s or "") if ch.isalnum())


def _list_stage1_input_files() -> list[Path]:
    out: list[Path] = []
    try:
        for p in sorted(STAGE1_INPUT_DIR.glob("*.xls*"), key=lambda x: x.name.lower()):
            if p.name.startswith("~$"):
                continue
            if p.suffix.lower() not in (".xlsx", ".xlsm"):
                continue
            out.append(p)
    except Exception:
        out = []
    return out


def _company_file_map() -> dict[str, Path]:
    """
    Map a stable company key -> input Excel path.
    Key is the filename stem (e.g. "BIMMS" from "BIMMS.xlsx").
    """
    m: dict[str, Path] = {}
    for p in _list_stage1_input_files():
        m[p.stem] = p
    return m


def _company_canonical_file_map() -> dict[str, Path]:
    """
    Map canonical company name -> input Excel path.
    If multiple files map to the same canonical name, keep the first.
    """
    out: dict[str, Path] = {}
    for stem, p in _company_file_map().items():
        canon, _country = _canonical_company_name_and_country(stem)
        key = canon or stem
        if key not in out:
            out[key] = p
    return out


def _resolve_company_file(company_key: str) -> Path | None:
    company_key = (company_key or "").strip()
    if not company_key:
        return None

    # First: canonical map (supports web display names)
    canon, _country = _canonical_company_name_and_country(company_key)
    cm = _company_canonical_file_map()
    if canon and canon in cm:
        return cm[canon]

    m = _company_file_map()
    if company_key in m:
        return m[company_key]

    want = _norm_name(company_key)
    if not want:
        return None

    # Best-effort normalized match (handles spaces/dashes differences)
    for k, p in m.items():
        if _norm_name(k) == want:
            return p

    return None


def _detect_header_row_and_headers(ws, max_scan_rows: int = 20) -> tuple[int, list[str]]:
    """
    Return (header_row_index_1based, headers[]).
    Scans first N rows and picks the earliest row with the highest number of non-empty cells (>=2).
    This helps when sheets have a few instruction rows before the actual header row.
    """
    best_row_idx = 1
    best_values = None
    best_score = -1

    max_r = min(int(getattr(ws, "max_row", 1) or 1), max_scan_rows)
    max_c = int(getattr(ws, "max_column", 1) or 1)
    max_c = min(max_c, 200)  # guard

    for r in range(1, max_r + 1):
        values = []
        score = 0
        last_non_empty = 0
        for c in range(1, max_c + 1):
            v = ws.cell(row=r, column=c).value
            s = "" if v is None else str(v).strip()
            values.append(s)
            if s:
                score += 1
                last_non_empty = c
        if score >= 2 and score > best_score:
            best_score = score
            best_row_idx = r
            best_values = values[:last_non_empty] if last_non_empty else values

    if best_values is None:
        # Fallback: use first row up to last non-empty cell
        r = 1
        last_non_empty = 0
        values = []
        for c in range(1, max_c + 1):
            v = ws.cell(row=r, column=c).value
            s = "" if v is None else str(v).strip()
            values.append(s)
            if s:
                last_non_empty = c
        best_values = values[:last_non_empty] if last_non_empty else values

    headers: list[str] = []
    for i, h in enumerate(best_values, start=1):
        hh = (h or "").strip()
        headers.append(hh if hh else f"Column {i}")

    return best_row_idx, headers


def _read_header_row_raw(ws, header_row: int, max_columns: int = 200) -> list[str]:
    """
    Read header row values as strings, up to the last non-empty cell.
    Does not invent placeholder names.
    """
    max_c = int(getattr(ws, "max_column", 1) or 1)
    max_c = min(max_c, max_columns)
    values: list[str] = []
    last_non_empty = 0
    for c in range(1, max_c + 1):
        v = ws.cell(row=header_row, column=c).value
        s = "" if v is None else str(v).strip()
        values.append(s)
        if s:
            last_non_empty = c
    return values[:last_non_empty] if last_non_empty else values


def _write_schema_only_workbook(source_file: Path, dest_file: Path) -> None:
    """
    Create a schema-only workbook:
    - same sheet names
    - blank sheet contents except the detected header row (written at the same row index)
    This avoids slow row-deletion on very large historical files.
    """
    src_wb = load_workbook(source_file, read_only=True, data_only=True, keep_links=False)
    out_wb = Workbook()
    try:
        out_wb.remove(out_wb.active)
    except Exception:
        pass

    for name in src_wb.sheetnames:
        src_ws = src_wb[name]
        header_row, _headers = _detect_header_row_and_headers(src_ws)
        raw_headers = _read_header_row_raw(src_ws, header_row)
        ws = out_wb.create_sheet(title=name)
        for j, v in enumerate(raw_headers, start=1):
            vv = (v or "").strip()
            ws.cell(row=header_row, column=j).value = vv if vv else None

    out_wb.save(dest_file)


def _ensure_company_backup_and_initialize(company_file: Path) -> None:
    """
    One-time operation per company file:
    - Create a backup of the original (historical 2025 filled) workbook
    - Clear all data rows below detected headers (keeps instruction rows + header row)
    This ensures Stage1 pipeline reads only newly submitted web rows.
    """
    backup_path = STAGE1_INPUT_BACKUP_DIR / company_file.name
    if not backup_path.exists():
        try:
            shutil.copy2(company_file, backup_path)
        except Exception:
            # If backup fails, do not proceed with destructive changes.
            raise

        tmp_path = company_file.with_suffix(".webtmp.xlsx")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

        _write_schema_only_workbook(backup_path, tmp_path)
        os.replace(str(tmp_path), str(company_file))


_EF_CACHE: dict[str, object] = {"mtime_ns": None, "rows": None, "sheets": None, "scopes": None, "sources": None}
_SCHEMA_CACHE_LOCK = threading.Lock()
_SCHEMA_CACHE: dict[tuple[str, int | None], dict[str, object]] = {}
_BACKFILL_STATE_LOCK = threading.Lock()
_BACKFILL_STATE: dict[str, float] = {"last_attempt_at": 0.0}
_PERIOD_PROFILE_CACHE_LOCK = threading.Lock()
_PERIOD_PROFILE_CACHE: dict[str, dict[str, object]] = {}


def _schema_cache_key(company_file: Path) -> tuple[str, int | None]:
    try:
        st = company_file.stat()
        mtime_ns = getattr(st, "st_mtime_ns", None) or int(st.st_mtime * 1_000_000_000)
    except Exception:
        mtime_ns = None
    return (str(company_file.resolve()), mtime_ns)


def _invalidate_schema_cache(company_file: Path | None = None) -> None:
    global _SCHEMA_CACHE
    with _SCHEMA_CACHE_LOCK:
        if company_file is None:
            _SCHEMA_CACHE = {}
            return
        target = str(company_file.resolve())
        _SCHEMA_CACHE = {k: v for k, v in _SCHEMA_CACHE.items() if k[0] != target}


def _get_schema_cache_entry(company_file: Path) -> dict[str, object]:
    key = _schema_cache_key(company_file)
    with _SCHEMA_CACHE_LOCK:
        cached = _SCHEMA_CACHE.get(key)
        if cached is None:
            cached = {"sheets": None, "headers": {}}
            _SCHEMA_CACHE[key] = cached
        return cached


def _get_visible_sheet_names(company_file: Path) -> list[str]:
    cache_entry = _get_schema_cache_entry(company_file)
    cached_sheets = cache_entry.get("sheets")
    if isinstance(cached_sheets, list):
        return cached_sheets

    wb = load_workbook(company_file, read_only=True, data_only=True, keep_links=False)
    sheets = [s for s in list(wb.sheetnames) if not _is_hidden_schema_sheet(s)]
    cache_entry["sheets"] = sheets
    return sheets


def _get_sheet_headers_and_rules(company_file: Path, sheet: str) -> tuple[int, list[str], dict[str, str]]:
    cache_entry = _get_schema_cache_entry(company_file)
    headers_cache = cache_entry.setdefault("headers", {})
    if isinstance(headers_cache, dict) and sheet in headers_cache:
        cached = headers_cache[sheet]
        if isinstance(cached, tuple) and len(cached) == 3:
            return cached  # type: ignore[return-value]

    wb = load_workbook(company_file, read_only=True, data_only=True, keep_links=False)
    if sheet not in wb.sheetnames:
        raise KeyError(sheet)
    ws = wb[sheet]
    header_row, headers = _detect_header_row_and_headers(ws)
    rules = {h: _infer_column_rule(h) for h in headers}
    result = (header_row, headers, rules)
    if isinstance(headers_cache, dict):
        headers_cache[sheet] = result
    return result


def _should_attempt_backfill_mapping_summaries(ttl_seconds: int = 60) -> bool:
    now = time.time()
    with _BACKFILL_STATE_LOCK:
        last_attempt_at = float(_BACKFILL_STATE.get("last_attempt_at", 0.0) or 0.0)
        if (now - last_attempt_at) < float(ttl_seconds):
            return False
        _BACKFILL_STATE["last_attempt_at"] = now

    try:
        succeeded_count = MappingRun.query.filter_by(status="succeeded").count()
        summary_count = MappingRunSummary.query.count()
        return int(summary_count) < int(succeeded_count)
    except Exception:
        return False


def _fallback_period_profile_for_summary(sub: "MappingRunSummary") -> dict[str, object]:
    fallback_dt = getattr(sub, "created_at", None)
    fallback_point = None
    if isinstance(fallback_dt, datetime):
        fallback_point = {
            "key": fallback_dt.strftime("%Y-%m"),
            "label": fallback_dt.strftime("%b %Y"),
            "date": datetime(fallback_dt.year, fallback_dt.month, 1),
            "value": round(float(getattr(sub, "tco2e_total", 0.0) or 0.0), 6),
        }

    return {
        "points": [fallback_point] if fallback_point else [],
        "total": round(float(getattr(sub, "tco2e_total", 0.0) or 0.0), 6),
        "rows_count": int(getattr(sub, "rows_count", 0) or 0),
        "min_date": fallback_point["date"] if fallback_point else None,
        "max_date": fallback_point["date"] if fallback_point else None,
    }


def _load_stage2_emission_factors() -> dict[str, object]:
    """
    Load mapping emission factors from the Stage2 Excel file (each sheet = category).
    Returns a dict with:
      - rows: list[dict] where keys match the mapping headers
      - sheets: list[str]
      - scopes: list[str]
      - sources: list[str]
    """
    global _EF_CACHE
    try:
        st = STAGE2_EF_XLSX.stat()
        mtime_ns = getattr(st, "st_mtime_ns", None) or int(st.st_mtime * 1_000_000_000)
    except Exception:
        mtime_ns = None

    if _EF_CACHE.get("rows") is not None and _EF_CACHE.get("mtime_ns") == mtime_ns:
        return _EF_CACHE  # type: ignore[return-value]

    rows: list[dict[str, object]] = []
    sheets: list[str] = []
    scopes_set: set[str] = set()
    sources_set: set[str] = set()

    if not STAGE2_EF_XLSX.exists():
        _EF_CACHE = {"mtime_ns": mtime_ns, "rows": [], "sheets": [], "scopes": [], "sources": []}
        return _EF_CACHE  # type: ignore[return-value]

    wb = load_workbook(STAGE2_EF_XLSX, read_only=True, data_only=True, keep_links=False)
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        sheets.append(sheet_name)

        # Read headers from first row (fast in read_only mode)
        first = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
        headers = [("" if v is None else str(v).strip()) for v in (first or [])]
        while headers and headers[-1] == "":
            headers.pop()

        if not headers:
            continue

        # Expect the mapping headers; tolerate extra columns
        wanted = {
            "ef_name",
            "ef_description",
            "scope",
            "ef_category",
            "ef_id",
            "ef_value",
            "ef_unit",
            "ef_source",
            "Emission Factor Category",
        }

        col_idx = {h: i for i, h in enumerate(headers) if h}

        # Stream rows efficiently
        for row_idx, row_values in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            values = list(row_values[: len(headers)]) if row_values else []
            if not values:
                continue

            any_val = False
            for v in values:
                if v is None:
                    continue
                if str(v).strip() != "":
                    any_val = True
                    break
            if not any_val:
                continue

            row: dict[str, object] = {"sheet": sheet_name, "_row": row_idx}
            for h, idx in col_idx.items():
                if h in wanted:
                    row[h] = values[idx] if idx < len(values) else None

            # Normalize some fields to strings for filtering/search
            for k in ("ef_name", "ef_description", "scope", "ef_category", "ef_id", "ef_unit", "ef_source", "Emission Factor Category"):
                if k in row and row[k] is not None:
                    row[k] = str(row[k]).strip()

            if row.get("scope"):
                scopes_set.add(str(row["scope"]))
            if row.get("ef_source"):
                sources_set.add(str(row["ef_source"]))

            rows.append(row)

    scopes = sorted([s for s in scopes_set if s], key=lambda x: x.lower())
    sources = sorted([s for s in sources_set if s], key=lambda x: x.lower())
    sheets_sorted = sorted([s for s in sheets if s], key=lambda x: x.lower())

    _EF_CACHE = {"mtime_ns": mtime_ns, "rows": rows, "sheets": sheets_sorted, "scopes": scopes, "sources": sources}
    return _EF_CACHE  # type: ignore[return-value]


def list_pipeline_templates_for_user(user_company: str, is_admin: bool):
    files = []
    try:
        for p in sorted(PIPELINE_TEMPLATE_DIR.glob("*.xls*"), key=lambda x: x.name.lower()):
            if p.name.startswith("~$"):
                continue
            files.append(p)
    except Exception:
        files = []

    if is_admin:
        return files

    want = _norm_name(user_company)
    if not want:
        return []
    # Match by filename containing normalized company name (best-effort)
    out = []
    for p in files:
        if want in _norm_name(p.stem):
            out.append(p)
    return out


def _safe_user_can_access_run(run: "PipelineRun") -> bool:
    return bool(current_user.is_admin) or (run.user_id == current_user.id)


def _run_pipeline_background(run_id: int) -> None:
    with app.app_context():
        run = PipelineRun.query.get(run_id)
        if not run:
            return

        run.status = "running"
        db.session.commit()

        run_dir = Path(run.run_dir)
        input_dir = Path(run.input_dir)
        log_path = run_dir / "pipeline.log"
        run.log_path = str(log_path)
        db.session.commit()

        stage2_out_dir = PROJECT_ROOT / "engine" / "stage2_mapping" / "output"
        before = set()
        try:
            if stage2_out_dir.exists():
                before = {p.resolve() for p in stage2_out_dir.glob("*.xlsx")}
        except Exception:
            before = set()

        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "run_pipeline.py"),
            "all",
            "--stage1-input-folder",
            str(input_dir),
            "--stage1-work-dir",
            str(run_dir / "stage1"),
        ]

        rc = 1
        try:
            run_dir.mkdir(parents=True, exist_ok=True)
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("CMD: " + " ".join(cmd) + "\n")
                f.write("START: " + datetime.now().isoformat() + "\n\n")
                f.flush()
                proc = subprocess.run(
                    cmd,
                    cwd=str(PROJECT_ROOT),
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env={
                        **os.environ,
                        "PYTHONUTF8": os.environ.get("PYTHONUTF8", "1"),
                        "PYTHONIOENCODING": os.environ.get("PYTHONIOENCODING", "utf-8"),
                        "PYTHONUNBUFFERED": os.environ.get("PYTHONUNBUFFERED", "1"),
                    },
                )
                rc = int(proc.returncode)
        except Exception as exc:
            run.status = "failed"
            run.exit_code = -1
            run.error_message = str(exc)
            db.session.commit()
            return

        run.exit_code = rc

        # Stage1 output: last chained file (if present)
        try:
            stage1_final = run_dir / "stage1" / "stage1_05_translated.xlsx"
            if stage1_final.exists():
                run.stage1_output = str(stage1_final)
        except Exception:
            pass

        # Collect new Stage2 outputs and copy into run dir
        try:
            after = set()
            if stage2_out_dir.exists():
                after = {p.resolve() for p in stage2_out_dir.glob("*.xlsx")}
            new_files = sorted([p for p in after if p not in before], key=lambda p: p.stat().st_mtime)
            if new_files:
                dst = run_dir / "stage2_output"
                dst.mkdir(parents=True, exist_ok=True)
                for p in new_files:
                    shutil.copy2(p, dst / p.name)
                run.stage2_output_dir = str(dst)
        except Exception:
            pass

        run.status = "succeeded" if rc == 0 else "failed"
        db.session.commit()

# Registration dropdown: exact template.json top-level keys only
COMPANIES = list(TEMPLATES.keys())

# Utility: Calculate emissions from excel data (veritabanından faktör çeker)
def calculate_emissions_from_excel(file_path, template_name):
    from sqlalchemy import and_
    total_emission = 0
    by_category = {}
    try:
        xls = pd.ExcelFile(file_path)
        # Önce 'Activity Based' sheet var mı kontrol et
        if 'Activity Based' in xls.sheet_names:
            sheet = 'Activity Based'
        else:
            sheet = 0  # ilk sheet
        header_row = 0
        if template_name == 'Scope 1 Fuel Usage (Mobile Combustion)':
            header_row = 4
        elif template_name == 'Scope 2 Electricity':
            header_row = 0
        elif template_name == 'Scope 2 District Heating':
            header_row = 0
        elif template_name == 'Scope 3 Category 7 Employee Commuting':
            header_row = 0
        elif template_name.startswith('Scope 3'):
            header_row = 0
        elif template_name == 'Water Tracker':
            header_row = 0
        else:
            header_row = 0
        df = xls.parse(sheet, header=header_row)
        if not isinstance(df, pd.DataFrame):
            return {
                'total_emission': 0,
                'by_category': {}
            }
        df.columns = [str(c).strip().lower() for c in df.columns]
        # Scope 1 Fuel Usage
        if template_name == 'Scope 1 Fuel Usage (Mobile Combustion)':
            fuel_col = next((c for c in df.columns if 'fuel' in c), None)
            litre_col = next((c for c in df.columns if 'litre' in c), None)
            if fuel_col and litre_col:
                for _, row in df.iterrows():
                    fuel = str(row[fuel_col]).strip()
                    try:
                        litres = float(row[litre_col])
                    except (ValueError, TypeError):
                        litres = 0
                    # Veritabanından faktör çek
                    factor_obj = EmissionFactor.query.filter_by(category=template_name, subcategory=fuel).first()
                    factor = factor_obj.factor if factor_obj else 0
                    emission = litres * factor
                    if fuel not in by_category:
                        by_category[fuel] = 0
                    by_category[fuel] += emission
                    total_emission += emission
        # Scope 2 Electricity
        elif template_name == 'Scope 2 Electricity':
            cons_col = next((c for c in df.columns if 'consumption' in c or 'kwh' in c), None)
            factor_col = next((c for c in df.columns if 'emission factor' in c), None)
            if cons_col:
                for _, row in df.iterrows():
                    try:
                        cons = float(row[cons_col])
                    except (ValueError, TypeError):
                        cons = 0
                    if factor_col and row.get(factor_col):
                        factor = float(row[factor_col])
                    else:
                        factor_obj = EmissionFactor.query.filter_by(category=template_name, subcategory='Electricity').first()
                        factor = factor_obj.factor if factor_obj else 0
                    emission = cons * factor
                    by_category['Electricity'] = by_category.get('Electricity', 0) + emission
                    total_emission += emission
        # Scope 2 District Heating
        elif template_name == 'Scope 2 District Heating':
            cons_col = next((c for c in df.columns if 'consumption' in c), None)
            factor_col = next((c for c in df.columns if 'emission factor' in c), None)
            if cons_col:
                for _, row in df.iterrows():
                    try:
                        cons = float(row[cons_col])
                    except (ValueError, TypeError):
                        cons = 0
                    if factor_col and row.get(factor_col):
                        factor = float(row[factor_col])
                    else:
                        factor_obj = EmissionFactor.query.filter_by(category=template_name, subcategory='District Heating').first()
                        factor = factor_obj.factor if factor_obj else 0
                    emission = cons * factor
                    by_category['District Heating'] = by_category.get('District Heating', 0) + emission
                    total_emission += emission
        # Scope 3 Category 7 Employee Commuting
        elif template_name == 'Scope 3 Category 7 Employee Commuting':
            mode_col = next((c for c in df.columns if 'mode of transport' in c), None)
            km_col = next((c for c in df.columns if 'km travelled per month' in c), None)
            if mode_col and km_col:
                for _, row in df.iterrows():
                    mode = str(row[mode_col]).strip().lower()
                    try:
                        km = float(row[km_col])
                    except (ValueError, TypeError):
                        km = 0
                    factor_obj = EmissionFactor.query.filter_by(category=template_name, subcategory=mode).first()
                    factor = factor_obj.factor if factor_obj else 0
                    emission = km * factor
                    if mode not in by_category:
                        by_category[mode] = 0
                    by_category[mode] += emission
                    total_emission += emission
        # Scope 3 (Spend-based veya Activity-based)
        elif template_name.startswith('Scope 3'):
            spend_col = next((c for c in df.columns if 'spend' in c), None)
            factor_col = next((c for c in df.columns if 'emission factor' in c), None)
            cons_col = next((c for c in df.columns if 'consumption' in c or 'amount' in c), None)
            if spend_col and factor_col:
                for _, row in df.iterrows():
                    try:
                        spend = float(row[spend_col])
                        factor = float(row[factor_col])
                        emission = spend * factor
                        by_category['Spend-based'] = by_category.get('Spend-based', 0) + emission
                        total_emission += emission
                    except (ValueError, TypeError):
                        continue
            elif cons_col and factor_col:
                for _, row in df.iterrows():
                    try:
                        cons = float(row[cons_col])
                        factor = float(row[factor_col])
                        emission = cons * factor
                        by_category['Activity-based'] = by_category.get('Activity-based', 0) + emission
                        total_emission += emission
                    except (ValueError, TypeError):
                        continue
        # Water Tracker
        elif template_name == 'Water Tracker':
            cons_col = next((c for c in df.columns if 'consumption' in c or 'amount' in c), None)
            factor_col = next((c for c in df.columns if 'emission factor' in c), None)
            if cons_col:
                for _, row in df.iterrows():
                    try:
                        cons = float(row[cons_col])
                    except (ValueError, TypeError):
                        cons = 0
                    if factor_col and row.get(factor_col):
                        factor = float(row[factor_col])
                    else:
                        factor_obj = EmissionFactor.query.filter_by(category=template_name, subcategory='Water').first()
                        factor = factor_obj.factor if factor_obj else 0
                    emission = cons * factor
                    by_category['Water'] = by_category.get('Water', 0) + emission
                    total_emission += emission
        else:
            # Diğer template'ler için dummy değer
            return {
                'total_emission': 1234.56,
                'by_category': {
                    'Dummy': 1234.56
                }
            }
    except Exception as e:
        print(f'Excel emission calculation error: {e}')
        return {'total_emission': 0, 'by_category': {}}
    return {'total_emission': round(total_emission, 2), 'by_category': {k: round(v, 2) for k, v in by_category.items()}}

def _safe_float(v) -> float:
    try:
        out = float(v or 0)
        if math.isnan(out) or math.isinf(out):
            return 0.0
        return out
    except Exception:
        return 0.0

def _infer_scopes(template_name: str, total_emission: float) -> tuple[float, float, float]:
    """Infer scope1/2/3 totals for a single submission based on template name."""
    name = (template_name or "").lower()
    s1 = s2 = s3 = 0.0
    if "scope 1" in name:
        s1 = total_emission
    elif "scope 2" in name:
        s2 = total_emission
    elif "scope 3" in name:
        s3 = total_emission
    return s1, s2, s3

def _parse_date_yyyy_mm_dd(s: str | None) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None

def get_emission_factor_name(extra_data_json):
    if not extra_data_json or extra_data_json == 'null':
        return ''
    try:
        data = json.loads(extra_data_json) if isinstance(extra_data_json, str) else extra_data_json
        for key in data:
            if key.strip().lower() == 'item prettyid':
                return data[key]
        return ''
    except Exception:
        return ''

app.jinja_env.filters['get_emission_factor_name'] = get_emission_factor_name

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/who-we-are')
def who_we_are():
    return render_template('who_we_are.html')

@app.route('/platform')
def platform():
    return render_template('platform.html')

@app.route('/methodology')
def methodology():
    return render_template('methodology.html')

@app.route('/trust')
def trust():
    return render_template('trust.html')


@app.route('/impact')
def impact():
    return render_template('impact.html')


@app.route('/impact/approach')
def impact_approach():
    return render_template('impact_approach.html')


@app.route('/impact/operations')
def impact_operations():
    return render_template('impact_operations.html')


@app.route('/impact/lca-epd')
def impact_lca_epd():
    return render_template('impact_lca_epd.html')


@app.route('/impact/collaboration')
def impact_collaboration():
    return render_template('impact_collaboration.html')


@app.route('/impact/corporate')
def impact_corporate():
    return render_template('impact_corporate.html')


@app.route('/impact/materiality')
def impact_materiality():
    return render_template('impact_materiality.html')


@app.route('/impact/carbon')
def impact_carbon():
    return render_template('impact_carbon.html')


@app.route('/impact/stakeholders')
def impact_stakeholders():
    return render_template('impact_stakeholders.html')


@app.route('/impact/sdgs')
def impact_sdgs():
    return render_template('impact_sdgs.html')


@app.route('/impact/governance')
def impact_governance():
    return render_template('impact_governance.html')


@app.route('/impact/reporting')
def impact_reporting():
    return render_template('impact_reporting.html')


@app.route('/esg', endpoint='esg')
def esg():
    return render_template('esg.html')


@app.route('/esg/csrd', endpoint='csrd')
def csrd():
    return render_template('csrd_overview.html')


@app.route('/lca')
def lca():
    return render_template('lca.html')


@app.route('/lca/tool')
def lca_tool():
    return render_template('lca_tool.html')


@app.route('/lca-tool')
def lca_tool_legacy_redirect():
    return redirect(url_for('lca_tool'), 301)


@app.route('/csrd')
def csrd_legacy_redirect():
    return redirect(url_for('csrd_policies'), 301)


@app.route('/esg/csrd/policies', endpoint='csrd_policies')
@login_required
def csrd_policies():
    _ensure_db_tables()
    edit_id = request.args.get('edit', type=int)
    edit_policy = None
    if edit_id and current_user.is_admin:
        edit_policy = CsrdPolicy.query.get(edit_id)
    policies = CsrdPolicy.query.order_by(CsrdPolicy.created_at.desc()).all()
    return render_template(
        'csrd_policies.html',
        policies=policies,
        edit_policy=edit_policy,
    )


@app.route('/esg/csrd/policies/add', methods=['POST'])
@app.route('/csrd/add', methods=['POST'])
@login_required
def csrd_policy_add():
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('csrd_policies'))
    _ensure_db_tables()
    title = (request.form.get('title') or '').strip()
    short_description = (request.form.get('short_description') or '').strip()
    upload = request.files.get('file')
    if not title or not short_description:
        flash('Title and description are required.')
        return redirect(url_for('csrd_policies'))
    if not upload or not getattr(upload, 'filename', None):
        flash('Please upload a PDF file.')
        return redirect(url_for('csrd_policies'))
    if not _csrd_filename_is_pdf(upload.filename):
        flash('Only PDF files are allowed.')
        return redirect(url_for('csrd_policies'))
    upload_dir = _csrd_policy_upload_dir()
    base = secure_filename(upload.filename) or 'policy.pdf'
    if not base.lower().endswith('.pdf'):
        base = f'{base}.pdf'
    unique_name = f'{uuid.uuid4().hex}_{base}'
    dest = upload_dir / unique_name
    upload.save(str(dest))
    rel = f'csrd_policies/{unique_name}'
    row = CsrdPolicy(title=title, short_description=short_description, file_relpath=rel)
    db.session.add(row)
    db.session.commit()
    flash('Policy saved.')
    return redirect(url_for('csrd_policies'))


@app.route('/esg/csrd/policies/<int:policy_id>/update', methods=['POST'])
@app.route('/csrd/<int:policy_id>/update', methods=['POST'])
@login_required
def csrd_policy_update(policy_id: int):
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('csrd_policies'))
    _ensure_db_tables()
    row = CsrdPolicy.query.get_or_404(policy_id)
    title = (request.form.get('title') or '').strip()
    short_description = (request.form.get('short_description') or '').strip()
    if not title or not short_description:
        flash('Title and description are required.')
        return redirect(url_for('csrd_policies', edit=policy_id))
    row.title = title
    row.short_description = short_description
    upload = request.files.get('file')
    if upload and getattr(upload, 'filename', None):
        if not _csrd_filename_is_pdf(upload.filename):
            flash('Only PDF files are allowed.')
            return redirect(url_for('csrd_policies', edit=policy_id))
        old_path = FRONTEND_UPLOAD_DIR / row.file_relpath
        try:
            if old_path.is_file():
                old_path.unlink()
        except Exception:
            pass
        upload_dir = _csrd_policy_upload_dir()
        base = secure_filename(upload.filename) or 'policy.pdf'
        if not base.lower().endswith('.pdf'):
            base = f'{base}.pdf'
        unique_name = f'{uuid.uuid4().hex}_{base}'
        dest = upload_dir / unique_name
        upload.save(str(dest))
        row.file_relpath = f'csrd_policies/{unique_name}'
    row.updated_at = datetime.utcnow()
    db.session.commit()
    flash('Policy updated.')
    return redirect(url_for('csrd_policies'))


@app.route('/esg/csrd/policies/<int:policy_id>/delete', methods=['POST'])
@app.route('/csrd/<int:policy_id>/delete', methods=['POST'])
@login_required
def csrd_policy_delete(policy_id: int):
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('csrd_policies'))
    _ensure_db_tables()
    row = CsrdPolicy.query.get_or_404(policy_id)
    disk_path = FRONTEND_UPLOAD_DIR / row.file_relpath
    db.session.delete(row)
    db.session.commit()
    try:
        if disk_path.is_file():
            disk_path.unlink()
    except Exception:
        pass
    flash('Policy removed.')
    return redirect(url_for('csrd_policies'))


@app.route('/esg/csrd/policies/<int:policy_id>/file')
@app.route('/csrd/<int:policy_id>/file')
@login_required
def csrd_policy_file(policy_id: int):
    _ensure_db_tables()
    row = CsrdPolicy.query.get_or_404(policy_id)
    path = FRONTEND_UPLOAD_DIR / row.file_relpath
    if not path.is_file():
        return ('File not found', 404)
    dl_name = Path(row.file_relpath).name
    return send_file(str(path), mimetype='application/pdf', as_attachment=False, download_name=dl_name)


@app.route('/assets/mouse-symbol')
def mouse_symbol_image():
    symbol_path = APP_DIR / "images" / "Symbol for mouse.png"
    if not symbol_path.exists():
        return ("Mouse symbol image not found.", 404)
    return send_file(str(symbol_path), mimetype="image/png")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''
        if not _email_domain_allowed(email):
            flash('Only CTS company email addresses are allowed')
            return render_template('login.html')

        user = User.query.filter(db.func.lower(User.email) == email).first()
        if user and check_password_hash(user.password_hash, password):
            login_user(user)
            session["activity_session_id"] = uuid.uuid4().hex
            request.environ["skip_activity_log"] = True
            _write_activity_log_for_user(user, action="login")
            if not _user_profile_complete(user):
                return redirect(url_for('profile_setup'))
            return redirect(url_for('dashboard'))
        flash('Invalid email or password')

    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        _ensure_db_tables()
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''
        company_name = (request.form.get('company_name') or '').strip()

        if not _email_domain_allowed(email):
            flash('Only CTS company email addresses are allowed')
            return render_template('register.html', companies=COMPANIES)

        if company_name not in TEMPLATES:
            flash('Invalid company selection')
            return render_template('register.html', companies=COMPANIES)

        if User.query.filter(db.func.lower(User.email) == email).first():
            flash('Email already registered')
            return render_template('register.html', companies=COMPANIES)

        user = User(
            email=email,
            password_hash=generate_password_hash(password),
            company_name=company_name,
            is_profile_complete=False,
            role="user",
        )
        sync_user_admin_flag(user)
        db.session.add(user)
        db.session.commit()

        flash('Registration successful! Please login.')
        return redirect(url_for('login'))
    return render_template('register.html', companies=COMPANIES)


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    _ensure_db_tables()
    generic_ok = "If the email exists, a reset link will be sent."
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        client_ip = _client_ip_for_rate_limit()
        if not _forgot_pw_rate_allow_ip(client_ip):
            _audit_password_reset("password_reset_request_blocked", reason="rate_limit_ip", ip=client_ip)
            flash("Too many password reset attempts. Please try again later.")
            return redirect(url_for("forgot_password"))
        if not _email_domain_allowed(email):
            flash("Only CTS company email addresses are allowed")
            return render_template("forgot_password.html")
        if not _forgot_pw_rate_allow_email(email):
            _audit_password_reset("password_reset_request_blocked", reason="rate_limit_email", ip=client_ip, email=email)
            flash("Too many password reset attempts. Please try again later.")
            return redirect(url_for("forgot_password"))
        user = User.query.filter(db.func.lower(User.email) == email).first()
        if user:
            _delete_expired_password_reset_tokens()
            raw = secrets.token_urlsafe(32)
            token_hash = _hash_password_reset_token(raw)
            for old in PasswordResetToken.query.filter_by(user_id=user.id).all():
                db.session.delete(old)
            db.session.add(
                PasswordResetToken(
                    user_id=user.id,
                    token_hash=token_hash,
                    expires_at=datetime.utcnow() + timedelta(hours=1),
                    used=False,
                )
            )
            db.session.commit()
            reset_url = f"{PUBLIC_APP_BASE_URL}/reset-password/{raw}"
            body = (
                "You requested a password reset.\n\n"
                "Click the link below to create a new password:\n\n"
                f"{reset_url}\n\n"
                "This link expires in 1 hour.\n\n"
                "If you did not request this, ignore this email.\n"
            )
            _send_plain_email(str(user.email), "Password reset request", body)
            _audit_password_reset("password_reset_requested", ip=client_ip, email=email, user_id=user.id)
        else:
            _audit_password_reset("password_reset_requested", ip=client_ip, email=email, user_found="false")
        flash(generic_ok)
        return redirect(url_for("forgot_password"))
    return render_template("forgot_password.html")


@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    _ensure_db_tables()
    raw = (token or "").strip()
    if len(raw) < 20:
        flash("This reset link is invalid or has expired.")
        return redirect(url_for("login"))
    th = _hash_password_reset_token(raw)
    row = (
        PasswordResetToken.query.filter_by(token_hash=th, used=False)
        .filter(PasswordResetToken.expires_at > datetime.utcnow())
        .first()
    )
    if request.method == "GET":
        if not row:
            flash("This reset link is invalid or has expired.")
            return redirect(url_for("login"))
        return render_template("reset_password.html", token=raw)

    if not row:
        flash("This reset link is invalid or has expired.")
        return redirect(url_for("login"))

    p1 = request.form.get("password") or ""
    p2 = request.form.get("password_confirm") or ""
    if not _password_reset_meets_policy(p1):
        flash("Password must be at least 8 characters and include at least one letter and one number.")
        return render_template("reset_password.html", token=raw)
    if p1 != p2:
        flash("Passwords do not match.")
        return render_template("reset_password.html", token=raw)

    user = User.query.filter_by(id=row.user_id).first()
    if not user:
        flash("This reset link is invalid or has expired.")
        return redirect(url_for("login"))

    client_ip = _client_ip_for_rate_limit()
    user.password_hash = generate_password_hash(p1)
    row.used = True
    db.session.commit()
    _audit_password_reset("password_reset_completed", ip=client_ip, user_id=user.id)
    flash("Password updated successfully.")
    return redirect(url_for("login"))


@app.route("/profile/setup", methods=["GET", "POST"])
@login_required
def profile_setup():
    _ensure_db_tables()
    if _user_profile_complete(current_user):
        return redirect(url_for("dashboard"))

    companies = list(TEMPLATES.keys())
    resolved_company = _resolve_template_company_name(current_user.company_name or "") or (current_user.company_name or "").strip()
    show_logo_upload = bool(resolved_company) and not _company_has_logo_for_key(resolved_company)

    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        job_title = (request.form.get("job_title") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        co = (request.form.get("company_name") or "").strip()
        country_code = (request.form.get("company_country") or "").strip().upper()

        if not first_name or not last_name or not job_title:
            flash("First name, last name, and job title are required.")
            return render_template(
                "profile_setup.html",
                companies=companies,
                iso_countries=ISO_COUNTRIES,
                show_logo_upload=show_logo_upload,
                resolved_company=resolved_company,
            )

        if co not in TEMPLATES:
            flash("Invalid company selection.")
            return render_template(
                "profile_setup.html",
                companies=companies,
                iso_countries=ISO_COUNTRIES,
                show_logo_upload=show_logo_upload,
                resolved_company=resolved_company,
            )

        valid_codes = {c for c, _n in ISO_COUNTRIES}
        if country_code not in valid_codes:
            flash("Please select a valid country.")
            return render_template(
                "profile_setup.html",
                companies=companies,
                iso_countries=ISO_COUNTRIES,
                show_logo_upload=show_logo_upload,
                resolved_company=resolved_company,
            )

        logo_rel_to_set: str | None = None
        if show_logo_upload:
            lfile = request.files.get("company_logo")
            if lfile and lfile.filename:
                logo_rel_to_set = _save_company_logo_png(lfile, co)
                if not logo_rel_to_set:
                    flash("Company logo must be a PNG file.")
                    return render_template(
                        "profile_setup.html",
                        companies=companies,
                        iso_countries=ISO_COUNTRIES,
                        show_logo_upload=show_logo_upload,
                        resolved_company=resolved_company,
                    )

        current_user.first_name = first_name
        current_user.last_name = last_name
        current_user.job_title = job_title
        current_user.phone = phone or None
        current_user.company_name = co
        current_user.company_country = country_code

        pfile = request.files.get("profile_photo")
        rel_photo = _save_profile_photo_file(pfile, int(current_user.id))
        if rel_photo:
            current_user.profile_photo_path = rel_photo

        if logo_rel_to_set:
            row = Company.query.filter_by(company_name=co).first()
            if not row:
                row = Company(company_name=co, created_by_user_id=current_user.id)
                db.session.add(row)
            row.company_logo_path = logo_rel_to_set
            row.created_by_user_id = row.created_by_user_id or current_user.id

        current_user.is_profile_complete = True
        db.session.commit()
        flash("Profile saved.")
        return redirect(url_for("dashboard"))

    return render_template(
        "profile_setup.html",
        companies=companies,
        iso_countries=ISO_COUNTRIES,
        show_logo_upload=show_logo_upload,
        resolved_company=resolved_company,
    )


@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile_page():
    _ensure_db_tables()
    if not _user_profile_complete(current_user):
        return redirect(url_for("profile_setup"))

    if request.method == "POST":
        current_user.first_name = (request.form.get("first_name") or "").strip() or None
        current_user.last_name = (request.form.get("last_name") or "").strip() or None
        current_user.job_title = (request.form.get("job_title") or "").strip() or None
        current_user.phone = (request.form.get("phone") or "").strip() or None

        if current_user.is_admin:
            country_code = (request.form.get("company_country") or "").strip().upper()
            valid_codes = {c for c, _n in ISO_COUNTRIES}
            if country_code in valid_codes:
                current_user.company_country = country_code

        pfile = request.files.get("profile_photo")
        rel_photo = _save_profile_photo_file(pfile, int(current_user.id))
        if rel_photo:
            current_user.profile_photo_path = rel_photo

        db.session.commit()
        flash("Profile updated.")
        return redirect(url_for("profile_page"))

    cc = (current_user.company_country or "").strip().upper()
    country_readonly_label = cc
    for code, name in ISO_COUNTRIES:
        if code == cc:
            country_readonly_label = f"{name} ({code})"
            break

    return render_template(
        "profile.html",
        iso_countries=ISO_COUNTRIES,
        company_display=(current_user.company_name or ""),
        country_readonly_label=country_readonly_label,
    )





def _render_dashboard_admin_analytics():
    company_filter = request.args.get('company', '').strip().lower()
    template_filter = request.args.get('template', '').strip().lower()
    include_categories = (request.args.get('include_categories', '1').strip() != '0')
    all_time = (request.args.get('all_time', '').strip() in ('1', 'true', 'on', 'yes'))
    date_from = _parse_date_yyyy_mm_dd(request.args.get('from'))
    date_to = _parse_date_yyyy_mm_dd(request.args.get('to'))

    # Default window: last 365 days (unless all_time is explicitly requested)
    if not all_time and not date_from and not date_to:
        date_to = datetime.now()
        date_from = date_to - timedelta(days=365)

    q = MappingRunSummary.query.order_by(MappingRunSummary.created_at.desc())
    if company_filter:
        q = q.filter(MappingRunSummary.company_name.ilike(f"%{company_filter}%"))
    if template_filter:
        q = q.filter(MappingRunSummary.sheet_name.ilike(f"%{template_filter}%"))

    summaries = q.all()
    # Only count the latest run per company+sheet
    latest_summaries: list[MappingRunSummary] = []
    seen_latest: set[tuple[str, str]] = set()
    for s in summaries:
        key = ((s.company_name or "").strip().lower(), (s.sheet_name or "").strip().lower())
        if not key[0] or not key[1] or key in seen_latest:
            continue
        seen_latest.add(key)
        latest_summaries.append(s)

    company_totals = defaultdict(lambda: {"total": 0.0, "scope1": 0.0, "scope2": 0.0, "scope3": 0.0, "count": 0})
    month_totals = defaultdict(float)  # YYYY-MM -> total
    year_totals = defaultdict(float)   # YYYY -> total
    category_totals = defaultdict(float)
    run_cache: dict[str, MappingRun | None] = {}

    grand = {"total": 0.0, "scope1": 0.0, "scope2": 0.0, "scope3": 0.0, "count": 0}
    for sub in latest_summaries:
        profile = _period_profile_for_summary(sub, run_cache=run_cache)
        points = list(profile.get("points") or [])
        if date_from:
            points = [p for p in points if isinstance(p.get("date"), datetime) and p["date"] >= date_from]
        if date_to:
            points = [p for p in points if isinstance(p.get("date"), datetime) and p["date"] < (date_to + timedelta(days=1))]
        if (date_from or date_to) and not points:
            continue

        total = _safe_float(sum(float(p.get("value") or 0.0) for p in points) if points else profile.get("total", 0.0))
        scope_num = int(getattr(sub, "scope", 0) or 0)
        s1 = total if scope_num == 1 else 0.0
        s2 = total if scope_num == 2 else 0.0
        s3 = total if scope_num == 3 else 0.0

        company = (sub.company_name or "").strip() or "(unknown)"
        company_totals[company]["total"] += total
        company_totals[company]["scope1"] += s1
        company_totals[company]["scope2"] += s2
        company_totals[company]["scope3"] += s3
        company_totals[company]["count"] += 1

        grand["total"] += total
        grand["scope1"] += s1
        grand["scope2"] += s2
        grand["scope3"] += s3
        grand["count"] += 1

        for point in points:
            dt = point.get("date")
            if not isinstance(dt, datetime):
                continue
            month_key = dt.strftime("%Y-%m")
            year_key = dt.strftime("%Y")
            month_totals[month_key] += float(point.get("value") or 0.0)
            year_totals[year_key] += float(point.get("value") or 0.0)

        if include_categories:
            category_totals[str(sub.sheet_name or "(unknown)")] += total

    company_rows = [
        {
            "company": k,
            "total": round(v["total"], 2),
            "scope1": round(v["scope1"], 2),
            "scope2": round(v["scope2"], 2),
            "scope3": round(v["scope3"], 2),
            "count": v["count"],
        }
        for k, v in company_totals.items()
    ]
    company_rows.sort(key=lambda r: r["total"], reverse=True)

    month_rows = [{"month": k, "total": round(v, 2)} for k, v in month_totals.items()]
    month_rows.sort(key=lambda r: r["month"])

    year_rows = [{"year": k, "total": round(v, 2)} for k, v in year_totals.items()]
    year_rows.sort(key=lambda r: r["year"])

    category_rows = [{"category": k, "total": round(v, 2)} for k, v in category_totals.items()]
    category_rows.sort(key=lambda r: r["total"], reverse=True)

    # Chart payloads (keep top N for readability)
    top_companies = company_rows[:12]
    top_categories = category_rows[:12]
    chart_payload = {
        "companies": {
            "labels": [r["company"] for r in top_companies],
            "values": [r["total"] for r in top_companies],
        },
        "months": {
            "labels": [r["month"] for r in month_rows][-24:],
            "values": [r["total"] for r in month_rows][-24:],
        },
        "years": {
            "labels": [r["year"] for r in year_rows],
            "values": [r["total"] for r in year_rows],
        },
        "categories": {
            "labels": [r["category"] for r in top_categories],
            "values": [r["total"] for r in top_categories],
        },
    }

    return render_template(
        "dashboard_admin.html",
        user=current_user,
        filters={
            "company": request.args.get("company", ""),
            "template": request.args.get("template", ""),
            "from": request.args.get("from", ""),
            "to": request.args.get("to", ""),
            "include_categories": "1" if include_categories else "0",
            "all_time": "1" if all_time else "0",
        },
        grand={
            "total": round(grand["total"], 2),
            "scope1": round(grand["scope1"], 2),
            "scope2": round(grand["scope2"], 2),
            "scope3": round(grand["scope3"], 2),
            "count": grand["count"],
            "companies": len(company_totals),
        },
        company_rows=company_rows,
        month_rows=month_rows,
        year_rows=year_rows,
        category_rows=category_rows,
        chart_payload=chart_payload,
    )


@app.route("/dashboard/analytics")
@login_required
def dashboard_analytics():
    if not current_user.is_admin:
        return redirect(url_for("dashboard"))
    _ensure_db_tables()
    _backfill_mapping_summaries()
    return _render_dashboard_admin_analytics()


@app.route('/dashboard')
@login_required
def dashboard():
    _ensure_db_tables()
    if current_user.is_admin:
        _backfill_mapping_summaries()

    mapping_runs = (
        MappingRun.query.filter_by(user_id=current_user.id)
        .order_by(MappingRun.created_at.desc())
        .limit(10)
        .all()
    )
    total_mapping_runs = MappingRun.query.filter_by(user_id=current_user.id).count()
    thirty_days_ago = datetime.now() - timedelta(days=30)
    recent_mapping_runs_count = MappingRun.query.filter(
        MappingRun.user_id == current_user.id,
        MappingRun.created_at >= thirty_days_ago
    ).count()

    companies = _list_template_companies_for_user()
    default_company = companies[0]["key"] if companies else None

    if not current_user.is_admin:
        rk = _resolve_template_company_name(current_user.company_name or "")
    else:
        rk = _resolve_template_company_name(default_company) if default_company else None
    company_logo_rel = _company_logo_static_rel(rk) if rk else None
    klarakarbon_supported = klarakarbon_company_supported(rk or "")

    return render_template(
        "dashboard.html",
        user=current_user,
        companies=companies,
        default_company=default_company,
        mapping_runs=mapping_runs,
        total_mapping_runs=total_mapping_runs,
        recent_mapping_runs_count=recent_mapping_runs_count,
        company_logo_rel=company_logo_rel,
        klarakarbon_company=rk,
        klarakarbon_supported=klarakarbon_supported,
    )


@app.route("/preprocess/klarakarbon/upload", methods=["POST"])
@login_required
def upload_klarakarbon_preprocess():
    flash("Direct Klarakarbon Excel upload is disabled. Use the 'Klarakarbon' category in Data Entry.")
    return redirect(url_for("dashboard"))


def _user_can_access_company_file(company_file: Path) -> bool:
    if not company_file:
        return False
    if bool(getattr(current_user, "is_admin", False)):
        return True
    user_file = _resolve_company_file(getattr(current_user, "company_name", "") or "")
    if not user_file:
        return False
    try:
        return user_file.resolve() == company_file.resolve()
    except Exception:
        return str(user_file) == str(company_file)


@app.route("/api/excel_schema/companies", methods=["GET"])
@login_required
def api_excel_schema_companies():
    if current_user.is_admin:
        companies = list(TEMPLATES.keys())
        return jsonify({"companies": companies})

    company_name = _resolve_template_company_name(getattr(current_user, "company_name", "") or "")
    companies = [company_name] if company_name else []
    return jsonify({"companies": companies})


@app.route("/api/excel_schema/sheets", methods=["GET"])
@login_required
def api_excel_schema_sheets():
    company = request.args.get("company", "").strip()
    if not _user_can_access_company(company):
        return jsonify({"error": "Access denied"}), 403
    resolved_company = _resolve_template_company_name(company)
    if not resolved_company:
        return jsonify({"error": "Company not found"}), 404
    sheets = _get_template_company_sheets(resolved_company)
    return jsonify({"company": resolved_company, "sheets": sheets})


@app.route("/api/excel_schema/headers", methods=["GET"])
@login_required
def api_excel_schema_headers():
    company = request.args.get("company", "").strip()
    sheet = request.args.get("sheet", "").strip()
    if not sheet:
        return jsonify({"error": "sheet is required"}), 400
    if _is_hidden_schema_sheet(sheet):
        return jsonify({"error": "This sheet is not available for web data entry"}), 403

    if not _user_can_access_company(company):
        return jsonify({"error": "Access denied"}), 403
    resolved_company = _resolve_template_company_name(company)
    if not resolved_company:
        return jsonify({"error": "Company not found"}), 404
    resolved_sheet = _resolve_template_sheet_name(resolved_company, sheet)
    if not resolved_sheet:
        return jsonify({"error": "Sheet not found"}), 404
    headers = _get_template_sheet_headers(resolved_company, resolved_sheet)
    return jsonify(
        {
            "company": resolved_company,
            "sheet": resolved_sheet,
            "header_row": 1,
            "headers": headers,
            "rules": {h: _infer_column_rule(h) for h in headers},
        }
    )


@app.route("/api/data_entry/rows", methods=["GET"])
@login_required
def api_data_entry_rows():
    company = request.args.get("company", "").strip()
    sheet = request.args.get("sheet", "").strip()
    if not company or not sheet:
        return jsonify({"error": "company and sheet are required"}), 400
    if not _user_can_access_company(company):
        return jsonify({"error": "Access denied"}), 403

    resolved_company = _resolve_template_company_name(company)
    if not resolved_company:
        return jsonify({"error": "Company not found"}), 404
    resolved_sheet = _resolve_template_sheet_name(resolved_company, sheet)
    if not resolved_sheet:
        return jsonify({"error": "Sheet not found"}), 404

    headers, _rules = _get_data_entry_template_schema(resolved_company, resolved_sheet)
    return jsonify(
        {
            "company": resolved_company,
            "sheet": resolved_sheet,
            "headers": headers,
            "rows": _load_data_entry_grid_rows(resolved_company, resolved_sheet, headers),
        }
    )


@app.route("/api/excel_schema/download", methods=["GET"])
@login_required
def api_excel_schema_download():
    company = request.args.get("company", "").strip()
    if not _user_can_access_company(company):
        return jsonify({"error": "Access denied"}), 403
    resolved_company = _resolve_template_company_name(company)
    if not resolved_company:
        return jsonify({"error": "Company not found"}), 404
    bio = _build_template_workbook(resolved_company)
    filename = secure_filename(f"{resolved_company}_template.xlsx") or "template.xlsx"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/excel_schema/save", methods=["POST"])
@login_required
def api_excel_schema_save():
    payload = request.get_json(silent=True) or {}
    company = (payload.get("company") or "").strip()
    sheet = (payload.get("sheet") or "").strip()
    rows = payload.get("rows") or []

    if not company or not sheet:
        return jsonify({"error": "company and sheet are required"}), 400
    if not isinstance(rows, list):
        return jsonify({"error": "rows must be a list"}), 400
    if _is_hidden_schema_sheet(sheet):
        return jsonify({"error": "This sheet is not available for web data entry"}), 403
    if not _user_can_access_company(company):
        return jsonify({"error": "Access denied"}), 403
    resolved_company = _resolve_template_company_name(company)
    if not resolved_company:
        return jsonify({"error": "Company not found"}), 404
    resolved_sheet = _resolve_template_sheet_name(resolved_company, sheet)
    if not resolved_sheet:
        return jsonify({"error": "Sheet not found"}), 404
    headers, _rules = _get_data_entry_template_schema(resolved_company, resolved_sheet)
    if not headers:
        return jsonify({"error": "Sheet not found"}), 404
    normalized_rows, validation_errors = _normalize_data_entry_rows(headers, rows)
    if validation_errors:
        return jsonify({"error": validation_errors[0], "validation_errors": validation_errors[:20]}), 400
    requirement_errors = _validate_data_entry_row_requirements(headers, normalized_rows)
    if requirement_errors:
        return jsonify({"error": requirement_errors[0], "validation_errors": requirement_errors[:20]}), 400
    try:
        result = _upsert_data_entries(resolved_company, resolved_sheet, headers, normalized_rows)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Save failed: {e}"}), 500

    saved_rows_count = int(result.get("saved_rows_count") or 0)
    duplicate_rows_count = int(result.get("duplicate_rows_count") or 0)
    saved_entry_groups = list(result.get("saved_entry_groups") or [])
    return jsonify(
        {
            "ok": True,
            "company": resolved_company,
            "sheet": resolved_sheet,
            "saved_rows": saved_rows_count,
            "saved_rows_count": saved_rows_count,
            "duplicate_rows_count": duplicate_rows_count,
            "saved_entry_groups": saved_entry_groups,
            "message": f"{saved_rows_count} rows saved, {duplicate_rows_count} duplicates skipped",
        }
    )


@app.route("/api/mapping/run", methods=["POST"])
@login_required
def api_mapping_run():
    if not bool(getattr(current_user, "is_admin", False)):
        return jsonify({"error": "Mapping is only available for administrators"}), 403
    _ensure_db_tables()
    payload = request.get_json(silent=True) or {}
    company = (payload.get("company") or "").strip()
    sheet = (payload.get("sheet") or "").strip()
    rows = payload.get("rows") or []
    entry_group_filter = (payload.get("entry_group") or "").strip()

    if not company or not sheet:
        return jsonify({"error": "company and sheet are required"}), 400
    if not isinstance(rows, list):
        return jsonify({"error": "rows must be a list"}), 400
    if _is_hidden_schema_sheet(sheet):
        return jsonify({"error": "This sheet is not available for web data entry"}), 403
    if not _user_can_access_company(company):
        return jsonify({"error": "Access denied"}), 403

    resolved_company = _resolve_template_company_name(company)
    if not resolved_company:
        return jsonify({"error": "Company not found"}), 404
    resolved_sheet = _resolve_template_sheet_name(resolved_company, sheet)
    if not resolved_sheet:
        return jsonify({"error": "Sheet not found"}), 404
    if resolved_sheet == KLARAKARBON_SHEET_NAME:
        return jsonify({"error": "Klarakarbon entries are not mapped from the Data Entry page."}), 403
    headers, _rules = _get_data_entry_template_schema(resolved_company, resolved_sheet)
    if not headers:
        return jsonify({"error": "Sheet not found"}), 404

    if entry_group_filter:
        existing_batch_map = MappingRun.query.filter_by(
            company_name=resolved_company,
            sheet_name=resolved_sheet,
            status="succeeded",
            source_entry_group=entry_group_filter,
        ).first()
        if existing_batch_map is not None:
            ma = existing_batch_map.created_at.isoformat() if existing_batch_map.created_at else ""
            return jsonify(
                {
                    "error": "This batch was already mapped",
                    "already_mapped": True,
                    "mapped_at": ma,
                }
            ), 409

    if rows:
        normalized_rows, validation_errors = _normalize_data_entry_rows(headers, rows)
        if validation_errors:
            return jsonify({"error": validation_errors[0], "validation_errors": validation_errors[:20]}), 400
        requirement_errors = _validate_data_entry_row_requirements(headers, normalized_rows)
        if requirement_errors:
            return jsonify({"error": requirement_errors[0], "validation_errors": requirement_errors[:20]}), 400
        try:
            _upsert_data_entries(resolved_company, resolved_sheet, headers, normalized_rows)
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": f"Save failed: {e}"}), 500

    if entry_group_filter:
        df = _load_data_entries_dataframe_for_entry_groups(
            resolved_company, resolved_sheet, headers, {entry_group_filter}
        )
        if df.empty:
            return jsonify({"error": "No saved rows found for this entry batch"}), 400
    else:
        df = _load_data_entries_dataframe(resolved_company, resolved_sheet, headers)
        if df.empty:
            return jsonify({"error": "No saved rows found for this company and sheet"}), 400

    run_id = uuid.uuid4().hex[:12]
    mr = MappingRun(
        id=run_id,
        user_id=current_user.id,
        company_name=resolved_company,
        sheet_name=resolved_sheet,
        status="running",
        created_at=datetime.utcnow(),
        source_entry_group=entry_group_filter or None,
    )
    try:
        db.session.add(mr)
        db.session.commit()
    except Exception:
        pass

    try:
        mapped_df, out_path, input_path = run_mapping(resolved_company, resolved_sheet, df)
    except Exception as e:
        try:
            mr.status = "failed"
            mr.error_message = str(e)
            db.session.commit()
        except Exception:
            pass
        return jsonify({"error": f"Mapping failed: {e}"}), 500

    try:
        mr.status = "succeeded"
        mr.output_path = str(out_path)
        mr.input_path = str(input_path)
        db.session.commit()
    except Exception:
        pass

    # Persist summary totals for dashboards
    try:
        _upsert_mapping_run_summary(
            run_id=run_id,
            company_name=resolved_company,
            sheet_name=resolved_sheet,
            mapped_df=mapped_df,
            output_path=out_path,
        )
        db.session.commit()
    except Exception:
        pass

    _create_user_notification(
        current_user.id,
        title="Mapping run completed",
        message=f"{resolved_company} / {resolved_sheet} mapping finished successfully.",
        notification_type="success",
        link=url_for("home"),
    )

    preview = mapped_df.head(40).fillna("").to_dict(orient="records")
    resp: dict[str, object] = {
        "ok": True,
        "run_id": run_id,
        "company": resolved_company,
        "sheet": resolved_sheet,
        "mapped_columns": list(mapped_df.columns),
        "preview": preview,
        "preview_rows": len(preview),
        "mapped_at": mr.created_at.isoformat() + "Z" if getattr(mr, "created_at", None) else "",
        "mapped_by": str(getattr(current_user, "email", "") or ""),
    }
    if entry_group_filter:
        resp["entry_group"] = entry_group_filter
    return jsonify(resp)


@app.route("/api/pipeline/append_run", methods=["POST"])
@login_required
def api_pipeline_append_run():
    if not bool(getattr(current_user, "is_admin", False)):
        return jsonify({"error": "Append & Run pipeline is only available for administrators"}), 403

    payload = request.get_json(silent=True) or {}
    company = (payload.get("company") or "").strip()
    sheet = (payload.get("sheet") or "").strip()
    if not company or not sheet:
        return jsonify({"error": "company and sheet are required"}), 400
    if not _user_can_access_company(company):
        return jsonify({"error": "Access denied"}), 403

    resolved_company = _resolve_template_company_name(company)
    if not resolved_company:
        return jsonify({"error": "Company not found"}), 404

    sheet_key = str(sheet).strip()
    if _batch_action_type_for_sheet(sheet_key) != "append_run":
        return jsonify({"error": "This category must use the normal Map action."}), 400
    if sheet_key == KLARAKARBON_SHEET_NAME:
        resolved_sheet = KLARAKARBON_SHEET_NAME
    elif sheet_key == TRAVEL_SHEET_NAME:
        resolved_sheet = TRAVEL_SHEET_NAME
    else:
        return jsonify({"error": "Unsupported pipeline category."}), 400

    try:
        result = _run_append_and_pipeline(resolved_company, resolved_sheet)
    except Exception as e:
        return jsonify({"error": f"Pipeline failed: {e}"}), 500
    return jsonify(result)


def _find_latest_merged_mapping_workbook() -> Path | None:
    patterns = [
        "mapped_results_merged_*.xlsx",
        "mapped_results_merged.xlsx",
    ]
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(STAGE2_OUTPUT_DIR.glob(pattern))
    candidates = [p for p in candidates if p.is_file() and not p.name.startswith("~$")]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _find_latest_stage2_output(*patterns: str) -> Path | None:
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(STAGE2_OUTPUT_DIR.rglob(pattern))
    files = [p for p in candidates if p.is_file() and not p.name.startswith("~$")]
    if not files:
        return None
    deduped = list({str(p.resolve()): p for p in files}.values())
    deduped.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return deduped[0]


def _load_stage2_module(module_basename: str):
    module_path = STAGE2_MAPPING_DIR / f"{module_basename}.py"
    module_name = f"ui_stage2_{module_basename}_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {module_path.name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_stage1_api_source_module(module_basename: str):
    module_path = PROJECT_ROOT / "engine" / "stage1_preprocess" / "api_sources" / f"{module_basename}.py"
    module_name = f"ui_stage1_api_{module_basename}_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {module_path.name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _format_output_preview_value(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float):
        if math.isfinite(value):
            if value.is_integer():
                return f"{value:,.0f}"
            return f"{value:,.4f}".rstrip("0").rstrip(".")
        return ""
    return str(value)


def _dataframe_preview_payload(sheet_name: str, df: pd.DataFrame, limit: int = 20) -> dict[str, object]:
    preview = df.head(limit).copy()
    columns = [str(c) for c in preview.columns]
    rows: list[list[str]] = []
    for ridx in range(len(preview.index)):
        rows.append([
            _format_output_preview_value(preview.iloc[ridx, cidx])
            for cidx in range(len(columns))
        ])
    return {
        "name": sheet_name,
        "columns": columns,
        "rows": rows,
        "row_count": int(len(df.index)),
        "column_count": int(len(df.columns)),
        "truncated": bool(len(df.index) > limit),
    }


ANALYTICS_OUTPUT_VIEWS: dict[str, dict[str, object]] = {
    "forecasting": {
        "title": "Forecasting",
        "group": "Analytics",
        "summary": "Run the existing forecasting script on demand and preview the newly generated workbook.",
        "preferred_sheets": ("Forecast_Total", "Forecast_Company", "Forecast_Sheet", "Backtest", "Meta"),
        "empty_message": "No forecasting workbook has been generated from the UI in this environment yet.",
    },
    "decarbonization": {
        "title": "Decarbonization",
        "group": "Analytics",
        "summary": "Run the existing decarbonization scripts with UI-supplied parameters and preview the fresh scenario output.",
        "preferred_sheets": ("Yearly", "Monthly", "Delta_vs_BAU", "Meta"),
        "empty_message": "No decarbonization workbook has been generated from the UI in this environment yet.",
    },
    "ccc_api_source": {
        "title": "CCC API",
        "group": "Data Sources",
        "summary": "Test the CCC connection and ingest any configured GET endpoint into stage1-compatible workbook outputs without changing pipeline calculations.",
        "preferred_sheets": ("CCC Purchase Orders Raw", "Scope 3 Cat 1 Common Purchases", "Scope 3 Cat 1 Services Spend", "CCC Waste Suppliers Review"),
        "empty_message": "No CCC API workbook has been generated from the UI in this environment yet.",
    },
    "mapped_window_output": {
        "title": "Mapped Window Output",
        "group": "Data Outputs",
        "summary": "Generate a fresh auditor-style mapped window workbook for a selected reporting period using the existing stage2 filter step.",
        "preferred_sheets": ("Company Totals Window", "Company by GHGP Totals Window", "GHGP sheet Totals Window", "Company Stacked Months Window"),
        "empty_message": "No mapped window workbook has been generated from the UI in this environment yet.",
    },
    "emissions_totals": {
        "title": "Totals Tables",
        "group": "Data Outputs",
        "summary": "Generate fresh totals tables on demand using the existing window workbook and totals-table script.",
        "preferred_sheets": ("Company Totals Window", "Company by GHGP Totals Window", "GHGP sheet Totals Window"),
        "empty_message": "No totals workbook has been generated from the UI in this environment yet.",
    },
    "share_analysis": {
        "title": "Share Analysis",
        "group": "Data Outputs",
        "summary": "Generate fresh share-analysis tables using the existing totals-table stage and preview the resulting workbook.",
        "preferred_sheets": ("Company Totals Window", "Company by GHGP Totals Window", "Company Stacked Data Window", "Company Stacked Months Window"),
        "empty_message": "No share-analysis workbook has been generated from the UI in this environment yet.",
    },
    "double_counting_check": {
        "title": "Double Counting Check",
        "group": "Governance",
        "summary": "Generate a fresh double-counting report using the existing stage2 review script.",
        "preferred_sheets": ("DC Log", "Anomalies"),
        "empty_message": "No double-counting workbook has been generated from the UI in this environment yet.",
    },
    "audit_ready_output": {
        "title": "Audit Ready Dataset",
        "group": "Governance",
        "summary": "Generate a fresh mapped window dataset for external audit review using the existing filtering step only.",
        "preferred_sheets": ("Company Totals Window", "Company by GHGP Totals Window", "GHGP sheet Totals Window", "Company Stacked Months Window"),
        "empty_message": "No audit-ready dataset has been generated from the UI in this environment yet.",
    },
}

ANALYTICS_RUN_LOG_CONFIG: dict[str, dict[str, object]] = {
    "forecasting": {
        "path": APP_DIR / "run_logs" / "forecasting_runs.json",
        "types": {"forecasting"},
    },
    "decarbonization": {
        "path": APP_DIR / "run_logs" / "decarbonization_runs.json",
        "types": {"decarbonization"},
    },
    "ccc_api_source": {
        "path": APP_DIR / "run_logs" / "ccc_api_sync.json",
        "types": {"ccc_api_sync", "ccc_api_test"},
    },
    "mapped_window_output": {
        "path": APP_DIR / "run_logs" / "data_output_runs.json",
        "types": {"mapped_window"},
    },
    "emissions_totals": {
        "path": APP_DIR / "run_logs" / "data_output_runs.json",
        "types": {"totals_tables"},
    },
    "share_analysis": {
        "path": APP_DIR / "run_logs" / "data_output_runs.json",
        "types": {"share_analysis"},
    },
    "double_counting_check": {
        "path": APP_DIR / "run_logs" / "data_output_runs.json",
        "types": {"double_counting"},
    },
    "audit_ready_output": {
        "path": APP_DIR / "run_logs" / "audit_runs.json",
        "types": {"audit_output"},
    },
}


def _analytics_history_download_url(filename: str) -> str | None:
    safe_name = Path(str(filename or "")).name
    if not safe_name:
        return None
    return url_for("analytics_output_download_file", filename=safe_name)


def _analytics_run_log_config(view_key: str) -> dict[str, object] | None:
    cfg = ANALYTICS_RUN_LOG_CONFIG.get(view_key)
    if cfg is not None:
        return cfg
    return None


def _read_run_history(view_key: str, limit: int = 20) -> list[dict[str, object]]:
    cfg = _analytics_run_log_config(view_key)
    path = Path(cfg.get("path")) if cfg and cfg.get("path") else None
    allowed_types = {str(v) for v in (cfg.get("types") or set())} if cfg else set()
    if path is None or not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    rows: list[dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        row_type = str(row.get("type") or "")
        if allowed_types and row_type not in allowed_types:
            continue
        output_file = str(row.get("output_file") or "")
        row["download_url"] = _analytics_history_download_url(output_file) if output_file else None
        rows.append(row)
    rows.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    return rows[:limit]


def _append_run_history(view_key: str, payload: dict[str, object]) -> None:
    cfg = _analytics_run_log_config(view_key)
    path = Path(cfg.get("path")) if cfg and cfg.get("path") else None
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    except Exception:
        existing = []
    rows = existing if isinstance(existing, list) else []
    clean_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        r = dict(row)
        r.pop("download_url", None)
        clean_rows.append(r)
    clean_rows.insert(0, payload)
    path.write_text(json.dumps(clean_rows[:100], indent=2), encoding="utf-8")


def _latest_run_history_entry(view_key: str, *, statuses: set[str] | None = None, types: set[str] | None = None) -> dict[str, object] | None:
    rows = _read_run_history(view_key, limit=100)
    for row in rows:
        row_status = str(row.get("status") or "")
        row_type = str(row.get("type") or "")
        if statuses and row_status not in statuses:
            continue
        if types and row_type not in types:
            continue
        return row
    return None


def _load_output_sheet_previews(path: Path, preferred_sheets: tuple[str, ...], *, max_rows: int = 20, max_sheets: int = 4) -> tuple[list[str], list[dict[str, object]]]:
    xls = pd.ExcelFile(path, engine="openpyxl")
    available_sheets = [str(name) for name in xls.sheet_names]
    selected: list[str] = []
    for name in preferred_sheets:
        if name in available_sheets and name not in selected:
            selected.append(name)
    if not selected:
        selected = available_sheets[:max_sheets]
    previews: list[dict[str, object]] = []
    for sheet_name in selected[:max_sheets]:
        df = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl")
        previews.append(_dataframe_preview_payload(sheet_name, df, limit=max_rows))
    return available_sheets, previews


def _find_output_file_by_name(filename: str) -> Path | None:
    safe_name = Path(str(filename or "")).name
    if not safe_name:
        return None
    candidates: list[Path] = []
    for root in (STAGE2_OUTPUT_DIR, STAGE1_INPUT_DIR):
        candidates.extend([p for p in root.rglob(safe_name) if p.is_file()])
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _latest_logged_output_path(view_key: str) -> Path | None:
    history = _read_run_history(view_key, limit=100)
    for row in history:
        output_file = str(row.get("output_file") or "")
        if not output_file:
            continue
        path = _find_output_file_by_name(output_file)
        if path is not None:
            return path
    return None


def _analytics_output_context_for_path(view_key: str, path: Path | None) -> dict[str, object]:
    cfg = ANALYTICS_OUTPUT_VIEWS[view_key]
    preferred_sheets = tuple(str(s) for s in (cfg.get("preferred_sheets") or ()))
    context: dict[str, object] = {
        "page_key": view_key,
        "page_title": str(cfg.get("title") or "Output"),
        "page_group": str(cfg.get("group") or "Analytics"),
        "page_summary": str(cfg.get("summary") or ""),
        "empty_message": str(cfg.get("empty_message") or "No output found."),
        "output_exists": False,
        "output_file_name": "",
        "output_file_mtime": "",
        "output_sheet_count": 0,
        "output_sheet_names": [],
        "sheet_previews": [],
        "download_url": None,
        "run_notice": "",
        "run_error": "",
        "form_state": {},
        "companion_outputs": [],
        "run_history": _read_run_history(view_key),
    }
    if path is None:
        return context
    try:
        available_sheets, previews = _load_output_sheet_previews(path, preferred_sheets)
    except Exception as exc:
        context["empty_message"] = f"Output file was found but could not be read: {exc}"
        return context
    stat = path.stat()
    context.update(
        {
            "output_exists": True,
            "output_file_name": path.name,
            "output_file_mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
            "output_sheet_count": len(available_sheets),
            "output_sheet_names": available_sheets,
            "sheet_previews": previews,
            "download_url": url_for("analytics_output_download", kind=view_key),
        }
    )
    return context


def _analytics_output_context(view_key: str) -> dict[str, object]:
    return _analytics_output_context_for_path(view_key, _latest_logged_output_path(view_key))


def _build_companion_output_payload(title: str, path: Path, preferred_sheets: tuple[str, ...]) -> dict[str, object]:
    available_sheets, previews = _load_output_sheet_previews(path, preferred_sheets)
    return {
        "title": title,
        "file_name": path.name,
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
        "sheet_count": len(available_sheets),
        "sheet_names": available_sheets,
        "sheet_previews": previews,
    }


def _parse_int_form(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = str(request.form.get(name, "") or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _parse_int_arg(name: str, default: int | None, *, minimum: int | None = None, maximum: int | None = None) -> int | None:
    raw = str(request.args.get(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _parse_float_form(name: str, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    raw = str(request.form.get(name, "") or "").strip()
    try:
        value = float(raw)
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _scenario_growth_profile(scenario_type: str) -> tuple[float, float]:
    st = str(scenario_type or "").strip().lower()
    if st == "expansion":
        return 0.10, 2.5
    if st == "growth":
        return 0.05, 2.0
    if st == "decarbonization":
        return 0.0, 1.5
    return 0.0, 2.0


def _decarbonization_scope_mapping(scope_key: str) -> tuple[list[str], bool]:
    scope = str(scope_key or "").strip().lower()
    if scope == "scope1":
        return (["Scope 1"], False)
    if scope == "scope2":
        return (["Scope 2"], False)
    if scope == "scope3":
        return (["S3 Cat 1 Purchased G&S"], False)
    return ([], True)


def _write_ui_lever_csv(*, target_year: int, reduction_pct: float, scope_key: str, scenario_type: str) -> Path:
    applies_to_cols, scale_all = _decarbonization_scope_mapping(scope_key)
    ramp_years = max(1, target_year - 2025)
    out_path = STAGE2_OUTPUT_DIR / f"ui_decarb_levers_{uuid.uuid4().hex}.csv"
    row = {
        "lever_key": "ui_custom",
        "name": f"UI custom {scenario_type}",
        "reduction_pct": max(0.0, min(1.0, reduction_pct)),
        "start_year_offset": 0,
        "ramp_years": ramp_years,
        "scale_all": bool(scale_all),
        "applies_to_cols": ", ".join(applies_to_cols),
        "notes": f"Generated from UI for scope={scope_key}, target_year={target_year}",
    }
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "lever_key",
                "name",
                "reduction_pct",
                "start_year_offset",
                "ramp_years",
                "scale_all",
                "applies_to_cols",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerow(row)
    return out_path


def _run_forecasting_from_ui(*, target_year: int) -> Path:
    forecasting = _load_stage2_module("Forecasting")
    cfg = forecasting.ForecastConfig(target_year=int(target_year))
    out_path = forecasting.run_forecasting(None, cfg)
    if out_path is None:
        raise RuntimeError("Forecasting did not produce an output workbook.")
    return Path(out_path)


def _run_decarbonization_from_ui(*, target_year: int, reduction_pct: float, scope_key: str, scenario_type: str) -> tuple[Path, Path]:
    years = max(1, int(target_year) - 2025)
    annual_growth, pod_multiplier_end = _scenario_growth_profile(scenario_type)
    levers_csv = _write_ui_lever_csv(
        target_year=int(target_year),
        reduction_pct=float(reduction_pct),
        scope_key=scope_key,
        scenario_type=scenario_type,
    )

    decarb = _load_stage2_module("Decarbonization")
    scenario_output = decarb.run_scenarios(
        years=years,
        annual_growth=annual_growth,
        pod_multiplier_end=pod_multiplier_end,
        baseline_source="additive_tabs",
        levers_csv=str(levers_csv),
        s3_cat1_multiplier=1.0,
        dirty_region_multiplier=1.0,
    )

    decarb_scenarios = _load_stage2_module("Decarbonization_Scenarios")
    rollout_end = f"{int(target_year)}-01-01"
    companion_output = decarb_scenarios.run(
        None,
        input_window=None,
        user_scenarios=True,
        annual_growth_default=annual_growth,
        annual_growth_10pct=max(annual_growth, 0.10),
        annual_growth_expansion=max(annual_growth, 0.15),
        rollout_end=rollout_end,
    )
    return Path(scenario_output), Path(companion_output)


def _window_period_end_date(year: int, month: int) -> str:
    month = max(1, min(12, int(month)))
    _, day_count = calendar.monthrange(int(year), month)
    return f"{int(year)}-{int(month):02d}-{day_count:02d}"


def _run_mapped_window_from_ui(*, year: int, start_month: int = 1, end_month: int = 12) -> Path:
    year = int(year)
    start_month = max(1, min(12, int(start_month)))
    end_month = max(start_month, min(12, int(end_month)))
    window = _load_stage2_module("filter_run_output_by_period")
    out_path = window.filter_workbook(
        f"{year}-{start_month:02d}-01",
        _window_period_end_date(year, end_month),
        None,
        None,
    )
    if out_path is None:
        raise RuntimeError("Mapped window generation did not produce an output workbook.")
    return Path(out_path)


def _run_totals_tables_from_ui(*, year: int, start_month: int = 1, end_month: int = 12) -> Path:
    window_path = _run_mapped_window_from_ui(year=year, start_month=start_month, end_month=end_month)
    totals = _load_stage2_module("Window_total_tables")
    out_path = totals.main(str(window_path))
    return Path(out_path) if out_path else window_path


def _capture_existing_output_names(*patterns: str) -> set[str]:
    names: set[str] = set()
    for pattern in patterns:
        for path in STAGE2_OUTPUT_DIR.rglob(pattern):
            if path.is_file():
                names.add(path.name)
    return names


def _find_new_output_after_run(before_names: set[str], *patterns: str) -> Path | None:
    candidates: list[Path] = []
    for pattern in patterns:
        for path in STAGE2_OUTPUT_DIR.rglob(pattern):
            if path.is_file():
                candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for path in candidates:
        if path.name not in before_names:
            return path
    return candidates[0]


def _run_double_counting_from_ui() -> Path:
    before_names = _capture_existing_output_names("mapped_results_merged_dc_*.xlsx", "mapped_results_merged_dc.xlsx")
    double_counting = _load_stage2_module("double_countin_booklets")
    double_counting.main()
    out_path = _find_new_output_after_run(before_names, "mapped_results_merged_dc_*.xlsx", "mapped_results_merged_dc.xlsx")
    if out_path is None:
        raise RuntimeError("Double counting report did not produce an output workbook.")
    return out_path


def _ccc_runtime_defaults() -> dict[str, object]:
    return {
        "base_url": str(CCC_API_BASE_URL or "").strip(),
        "username": str(CCC_USERNAME or "").strip(),
        "page_size": int(CCC_API_PAGE_SIZE or 100),
        "has_password": bool(str(CCC_PASSWORD or "").strip()),
    }


def _load_ccc_mapping_rules() -> dict[str, object]:
    try:
        data = json.loads(CCC_SHEET_MAPPING_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    return data if isinstance(data, dict) else {}


def _load_ccc_get_endpoints() -> dict[str, str]:
    try:
        data = json.loads(CCC_GET_ENDPOINTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in data.items()
        if str(key).strip() and str(value).strip()
    }


def _parse_json_object_form_field(name: str) -> dict[str, object]:
    raw = str(request.form.get(name, "") or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"{name.replace('_', ' ').title()} must be valid JSON.") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"{name.replace('_', ' ').title()} must be a JSON object.")
    return data


def _ccc_connection_status_payload(*, state: str = "unknown", message: str = "", tested_at: str = "") -> dict[str, str]:
    labels = {
        "connected": "Connected",
        "configured": "Configured",
        "missing": "Missing credentials",
        "failed": "Connection failed",
        "unknown": "Not tested",
    }
    tones = {
        "connected": "success",
        "configured": "neutral",
        "missing": "error",
        "failed": "error",
        "unknown": "neutral",
    }
    return {
        "state": state,
        "label": labels.get(state, "Unknown"),
        "tone": tones.get(state, "neutral"),
        "message": message,
        "tested_at": tested_at,
    }


def _ccc_ui_context() -> dict[str, object]:
    context = _analytics_output_context("ccc_api_source")
    defaults = _ccc_runtime_defaults()
    endpoints = _load_ccc_get_endpoints()
    latest_sync = _latest_run_history_entry("ccc_api_source", statuses={"success"}, types={"ccc_api_sync"})
    latest_test = _latest_run_history_entry("ccc_api_source", types={"ccc_api_test", "ccc_api_sync"})
    if latest_test:
        test_status = _ccc_connection_status_payload(
            state="connected" if str(latest_test.get("status") or "") == "success" else "failed",
            message=str(latest_test.get("message") or ""),
            tested_at=str(latest_test.get("timestamp") or ""),
        )
    elif defaults["base_url"] and defaults["username"] and defaults["has_password"]:
        test_status = _ccc_connection_status_payload(
            state="configured",
            message="Stored credentials found in environment configuration.",
        )
    else:
        test_status = _ccc_connection_status_payload(
            state="missing",
            message="Set CCC credentials in config/api_credentials.env or environment variables.",
        )
    context.update(
        {
            "form_state": {
                "base_url": defaults["base_url"],
                "username": defaults["username"],
                "page_size": defaults["page_size"],
                "has_password": defaults["has_password"],
                "endpoint_name": next(iter(endpoints.keys()), "purchase_order"),
                "path_params": "",
                "query_params": "",
            },
            "ccc_connection_status": test_status,
            "ccc_last_sync": latest_sync,
            "ccc_mapping_rules": _load_ccc_mapping_rules(),
            "ccc_get_endpoints": endpoints,
        }
    )
    return context


def _test_ccc_connection_from_ui(*, base_url: str, username: str, password: str) -> dict[str, object]:
    ccc_client = _load_stage1_api_source_module("ccc_client")
    return dict(
        ccc_client.test_connection(
            base_url=str(base_url or "").strip(),
            username=str(username or "").strip(),
            password=str(password or ""),
        )
    )


def _run_ccc_generic_ingest_from_ui(*, endpoint_name: str, base_url: str, username: str, password: str, page_size: int, path_params: dict[str, object], query_params: dict[str, object]) -> tuple[Path, dict[str, object]]:
    ccc_ingest = _load_stage1_api_source_module("ccc_generic_ingest")
    result = ccc_ingest.ingest_endpoint(
        str(endpoint_name or "").strip(),
        base_url=str(base_url or "").strip(),
        username=str(username or "").strip(),
        password=str(password or ""),
        page_size=int(page_size or 100),
        path_params=path_params,
        query_params=query_params,
    )
    output_path = Path(result.get("output_path"))
    return output_path, dict(result)


def _run_ccc_purchase_orders_from_ui(*, base_url: str, username: str, password: str, page_size: int) -> tuple[Path, dict[str, object]]:
    ccc_api = _load_stage1_api_source_module("ccc_purchase_orders")
    result = ccc_api.sync_purchase_orders(
        base_url=str(base_url or "").strip(),
        username=str(username or "").strip(),
        password=str(password or ""),
        page_size=int(page_size or 100),
    )
    output_path = Path(result.get("output_path"))
    return output_path, dict(result)


def _scope_label(scope_key: str) -> str:
    labels = {
        "scope1": "Scope 1",
        "scope2": "Scope 2",
        "scope3": "Scope 3",
        "total": "Total footprint",
    }
    return labels.get(str(scope_key or "").strip().lower(), str(scope_key or ""))


def _resolve_country_code_and_name(raw_country: str | None) -> tuple[str | None, str | None]:
    raw = str(raw_country or "").strip()
    if not raw:
        return None, None
    code = raw.upper()
    if code in ISO_COUNTRY_NAME_BY_CODE:
        return code, ISO_COUNTRY_NAME_BY_CODE[code]
    by_name = ISO_COUNTRY_CODE_BY_NAME.get(raw.casefold())
    if by_name:
        return by_name, ISO_COUNTRY_NAME_BY_CODE.get(by_name)
    return None, raw


def _company_country_lookup() -> dict[str, tuple[str | None, str | None]]:
    lookup: dict[str, tuple[str | None, str | None]] = {}
    rows = User.query.filter(User.company_name.isnot(None)).all()
    for user in rows:
        company_name = str(getattr(user, "company_name", "") or "").strip()
        if not company_name:
            continue
        keys = {_normalize_template_key(company_name)}
        canon, inferred_country = _canonical_company_name_and_country(company_name)
        if canon:
            keys.add(_normalize_template_key(canon))
        country_code, country_name = _resolve_country_code_and_name(getattr(user, "company_country", None))
        if country_code is None and inferred_country:
            country_code, country_name = _resolve_country_code_and_name(inferred_country)
        if country_code is None and country_name is None:
            continue
        for key in keys:
            if key and key not in lookup:
                lookup[key] = (country_code, country_name)
    return lookup


def _load_emissions_map_points() -> dict[str, object]:
    path = _latest_logged_output_path("emissions_totals")
    points: list[dict[str, object]] = []
    context: dict[str, object] = {
        "output_exists": False,
        "output_file_name": "",
        "output_file_mtime": "",
        "points": points,
        "total_emissions": 0.0,
        "companies_count": 0,
        "countries_count": 0,
        "empty_message": "No totals workbook has been generated from the UI yet. Run Totals Tables first to refresh the map.",
    }
    if path is None or not path.exists():
        return context
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
        preferred_sheet = None
        for candidate in ("Company Totals Window", "Company Totals"):
            if candidate in xls.sheet_names:
                preferred_sheet = candidate
                break
        if preferred_sheet is None:
            raise RuntimeError("No company totals sheet found in the latest workbook.")
        df = pd.read_excel(xls, sheet_name=preferred_sheet, engine="openpyxl")
    except Exception as exc:
        context["empty_message"] = f"Map output was found but could not be read: {exc}"
        return context

    company_col = next((c for c in df.columns if str(c).strip().lower() == "company"), None)
    emissions_col = next((c for c in df.columns if str(c).strip().lower() == "co2e (t)"), None)
    share_col = next((c for c in df.columns if str(c).strip().lower() == "share (%)"), None)
    if company_col is None or emissions_col is None:
        context["empty_message"] = "Latest workbook is missing the expected Company Totals columns."
        return context

    country_lookup = _company_country_lookup()
    country_seen: set[str] = set()
    total_emissions = 0.0
    for _, row in df.iterrows():
        company_name = str(row.get(company_col) or "").strip()
        if not company_name:
            continue
        emissions = _safe_float(row.get(emissions_col) or 0.0)
        if emissions <= 0:
            continue
        share_pct = _safe_float(row.get(share_col) or 0.0) if share_col else 0.0
        norm = _normalize_template_key(company_name)
        country_code, country_name = country_lookup.get(norm, (None, None))
        if country_code is None and country_name is None:
            _canon, inferred_country = _canonical_company_name_and_country(company_name)
            country_code, country_name = _resolve_country_code_and_name(inferred_country)
        if not country_name:
            continue
        total_emissions += emissions
        if country_code:
            country_seen.add(country_code)
        points.append(
            {
                "company_name": company_name,
                "country_code": country_code or "",
                "country_name": country_name,
                "emissions": emissions,
                "share_pct": share_pct,
            }
        )

    if not points:
        return context
    stat = path.stat()
    context.update(
        {
            "output_exists": True,
            "output_file_name": path.name,
            "output_file_mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
            "points": points,
            "total_emissions": total_emissions,
            "companies_count": len(points),
            "countries_count": len(country_seen),
            "empty_message": "",
        }
    )
    return context


@app.route("/api/mapping/download_merged", methods=["GET"])
@login_required
def api_mapping_download_merged():
    if not bool(getattr(current_user, "is_admin", False)):
        flash("Access denied")
        return redirect(url_for("dashboard"))
    p = _find_latest_merged_mapping_workbook()
    if p is None or not p.exists():
        flash("Merged mapping output file not found.")
        return redirect(url_for("admin_mapping_panel"))
    return send_file(str(p), as_attachment=True, download_name=p.name)


@app.route("/api/admin/upload_notifications", methods=["GET"])
@login_required
def api_admin_upload_notifications():
    if not bool(getattr(current_user, "is_admin", False)):
        return jsonify({"error": "Access denied"}), 403

    _ensure_db_tables()
    one_week_ago = datetime.utcnow() - timedelta(days=7)
    batches = _list_admin_data_entry_batches()
    notifications: list[dict[str, object]] = []
    for b in batches:
        uploaded_at = b.get("uploaded_at")
        if not isinstance(uploaded_at, datetime):
            continue
        if uploaded_at < one_week_ago:
            continue

        mapped = bool(b.get("mapped"))
        mapped_at = b.get("mapped_at")
        mapped_by = str(b.get("mapped_by") or "").strip()
        if mapped and mapped_at:
            mapping_status = f"Mapped by {mapped_by or 'admin'} at {mapped_at.strftime('%Y-%m-%d %H:%M')}"
        elif mapped:
            mapping_status = "Mapped"
        else:
            mapping_status = "Not mapped yet"

        notifications.append(
            {
                "company_name": str(b.get("company_name") or ""),
                "uploaded_by_user": str(b.get("uploaded_by_user") or "Unknown"),
                "upload_timestamp": uploaded_at.strftime("%Y-%m-%d %H:%M"),
                "category": str(b.get("sheet_name") or ""),
                "row_count": int(b.get("row_count") or 0),
                "mapping_status": mapping_status,
                "mapped_by_admin": mapped_by,
                "mapping_timestamp": mapped_at.strftime("%Y-%m-%d %H:%M") if isinstance(mapped_at, datetime) else "",
                "mapped": mapped,
            }
        )

    notifications.sort(key=lambda item: str(item.get("upload_timestamp") or ""), reverse=True)
    return jsonify({"notifications": notifications, "count": len(notifications)})


@app.route("/api/mapping/download/<run_id>", methods=["GET"])
@login_required
def api_mapping_download(run_id: str):
    _ensure_db_tables()
    mr = MappingRun.query.get(run_id)
    if not mr:
        # Fallback to legacy in-memory if present
        meta = _MAPPING_RUNS.get(run_id)
        if not meta:
            flash("Mapping output not found (expired).")
            return redirect(url_for("dashboard"))
        if not bool(getattr(current_user, "is_admin", False)) and meta.get("user_id") != current_user.id:
            flash("Access denied")
            return redirect(url_for("dashboard"))
        p = meta.get("path")
        if not p or not os.path.exists(str(p)):
            flash("Mapping output file not found.")
            return redirect(url_for("dashboard"))
        fn = f"mapped_{meta.get('company')}_{meta.get('sheet')}.xlsx"
        fn = secure_filename(fn) or "mapped_results.xlsx"
        return send_file(str(p), as_attachment=True, download_name=fn)

    if not bool(getattr(current_user, "is_admin", False)) and mr.user_id != current_user.id:
        flash("Access denied")
        return redirect(url_for("dashboard"))
    if not mr.output_path or not os.path.exists(mr.output_path):
        flash("Mapping output file not found.")
        return redirect(url_for("dashboard"))

    fn = secure_filename(f"mapped_{mr.company_name}_{mr.sheet_name}.xlsx") or "mapped_results.xlsx"
    return send_file(str(mr.output_path), as_attachment=True, download_name=fn)


@app.route("/mappings", methods=["GET"])
@login_required
def mapping_runs():
    _ensure_db_tables()
    runs = (
        MappingRun.query.filter_by(user_id=current_user.id)
        .order_by(MappingRun.created_at.desc())
        .limit(200)
        .all()
    )
    return render_template("mapping_runs.html", user=current_user, runs=runs)


@app.route("/admin/mapping_runs", methods=["GET"])
@login_required
def admin_mapping_runs():
    _ensure_db_tables()
    if not bool(getattr(current_user, "is_admin", False)):
        flash("Access denied")
        return redirect(url_for("dashboard"))

    rows = (
        db.session.query(MappingRun, User.email, User.company_name)
        .join(User, MappingRun.user_id == User.id)
        .order_by(MappingRun.created_at.desc())
        .limit(500)
        .all()
    )
    return render_template("mapping_runs_admin.html", user=current_user, rows=rows)


@app.route("/admin/mapping", methods=["GET"])
@login_required
def admin_mapping_panel():
    _ensure_db_tables()
    if not bool(getattr(current_user, "is_admin", False)):
        flash("Access denied")
        return redirect(url_for("dashboard"))
    _ensure_mapping_run_source_entry_group_column()
    batches = _list_admin_data_entry_batches()
    batches_json = _batches_for_admin_mapping_json(batches)
    return render_template(
        "admin_mapping.html",
        user=current_user,
        batches=batches,
        batches_json=batches_json,
    )


@app.route("/analytics/output/download/<kind>", methods=["GET"])
@login_required
def analytics_output_download(kind: str):
    cfg = ANALYTICS_OUTPUT_VIEWS.get(kind)
    if not cfg:
        flash("Output not found.")
        return redirect(url_for("home"))
    path = _latest_logged_output_path(kind)
    if path is None or not path.exists():
        flash("No UI-generated output file found.")
        return redirect(url_for("home"))
    return send_file(str(path), as_attachment=True, download_name=path.name)


@app.route("/analytics/output/file/<path:filename>", methods=["GET"])
@login_required
def analytics_output_download_file(filename: str):
    safe_name = Path(str(filename or "")).name
    if not safe_name:
        flash("Output file not found.")
        return redirect(url_for("home"))
    candidates: list[Path] = []
    for root in (STAGE2_OUTPUT_DIR, STAGE1_INPUT_DIR):
        candidates.extend([p for p in root.rglob(safe_name) if p.is_file()])
    if not candidates:
        flash("Output file not found.")
        return redirect(url_for("home"))
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    path = candidates[0]
    return send_file(str(path), as_attachment=True, download_name=path.name)


@app.route("/data-sources/ccc-api", methods=["GET", "POST"])
@login_required
def data_sources_ccc_api():
    context = _ccc_ui_context()
    form_state = dict(context.get("form_state") or {})
    if request.method == "POST":
        action = str(request.form.get("action", "sync") or "sync").strip().lower()
        form_state = {
            "base_url": str(request.form.get("base_url", "") or "").strip(),
            "username": str(request.form.get("username", "") or "").strip(),
            "page_size": _parse_int_form("page_size", 100, minimum=1, maximum=500),
            "endpoint_name": str(request.form.get("endpoint_name", "") or "").strip(),
            "path_params": str(request.form.get("path_params", "") or "").strip(),
            "query_params": str(request.form.get("query_params", "") or "").strip(),
            "has_password": bool(str(request.form.get("password", "") or "").strip()) or bool(_ccc_runtime_defaults().get("has_password")),
        }
        password = str(request.form.get("password", "") or "")
        try:
            if action == "test_connection":
                result = _test_ccc_connection_from_ui(
                    base_url=form_state["base_url"],
                    username=form_state["username"],
                    password=password,
                )
                context = _ccc_ui_context()
                context["run_notice"] = "CCC API connection test succeeded. JWT token received."
                context["ccc_connection_status"] = _ccc_connection_status_payload(
                    state="connected",
                    message="JWT token received successfully.",
                    tested_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                )
                _append_run_history(
                    "ccc_api_source",
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "type": "ccc_api_test",
                        "status": "success",
                        "scenario": "Connection test",
                        "parameters_summary": f"Base URL: {form_state['base_url']}",
                        "base_url": form_state["base_url"],
                        "page_size": int(form_state["page_size"]),
                        "records_imported": 0,
                        "output_filename": "",
                        "output_file": "",
                        "message": str(result.get("token_received") and "JWT token received successfully." or "Connection test completed."),
                    },
                )
            else:
                path_params = _parse_json_object_form_field("path_params")
                query_params = _parse_json_object_form_field("query_params")
                endpoint_name = str(form_state["endpoint_name"] or "").strip()
                out_path, result = _run_ccc_generic_ingest_from_ui(
                    endpoint_name=endpoint_name,
                    base_url=form_state["base_url"],
                    username=form_state["username"],
                    password=password,
                    page_size=int(form_state["page_size"]),
                    path_params=path_params,
                    query_params=query_params,
                )
                context = _ccc_ui_context()
                context = {**context, **_analytics_output_context_for_path("ccc_api_source", out_path)}
                imported = int(result.get("records_imported") or 0)
                if not imported:
                    context["run_notice"] = f"Endpoint `{endpoint_name}` synced successfully but returned no rows."
                else:
                    context["run_notice"] = (
                        f"Endpoint `{endpoint_name}` synced successfully. "
                        f"Imported {imported} records and saved {out_path.name}."
                    )
                context["run_notice"] = (
                    context["run_notice"]
                )
                context["ccc_connection_status"] = _ccc_connection_status_payload(
                    state="connected",
                    message="Last sync authenticated successfully.",
                    tested_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                )
                _append_run_history(
                    "ccc_api_source",
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "type": "ccc_api_sync",
                        "status": "success",
                        "scenario": endpoint_name,
                        "parameters_summary": (
                            f"Base URL: {form_state['base_url']}, "
                            f"Endpoint: {endpoint_name}, "
                            f"Page size: {int(form_state['page_size'])}, "
                            f"Rows: {imported}"
                        ),
                        "base_url": form_state["base_url"],
                        "endpoint": endpoint_name,
                        "page_size": int(form_state["page_size"]),
                        "records_imported": imported,
                        "output_filename": out_path.name,
                        "output_file": out_path.name,
                        "message": f"Imported {imported} records.",
                    },
                )
                _create_user_notification(
                    current_user.id,
                    title="CCC API sync completed",
                    message=f"{endpoint_name or 'Selected endpoint'} synced successfully with {imported} imported records.",
                    notification_type="success",
                    link=url_for("data_sources_ccc_api"),
                )
            context["run_history"] = _read_run_history("ccc_api_source")
            context["ccc_last_sync"] = _latest_run_history_entry("ccc_api_source", statuses={"success"}, types={"ccc_api_sync"})
        except Exception as exc:
            context = _ccc_ui_context()
            error_message = f"CCC API {'connection test' if action == 'test_connection' else 'sync'} failed: {exc}"
            context["run_error"] = error_message
            context["ccc_connection_status"] = _ccc_connection_status_payload(
                state="failed",
                message=str(exc),
                tested_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            )
            _append_run_history(
                "ccc_api_source",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "ccc_api_test" if action == "test_connection" else "ccc_api_sync",
                    "status": "failed",
                    "scenario": "Connection test" if action == "test_connection" else (form_state["endpoint_name"] or "CCC endpoint"),
                    "parameters_summary": (
                        f"Base URL: {form_state['base_url']}, "
                        f"Endpoint: {form_state['endpoint_name'] or 'n/a'}, "
                        f"Page size: {int(form_state['page_size'])}"
                    ),
                    "base_url": form_state["base_url"],
                    "endpoint": form_state["endpoint_name"] or "",
                    "page_size": int(form_state["page_size"]),
                    "records_imported": 0,
                    "output_filename": "",
                    "output_file": "",
                    "message": str(exc),
                },
            )
            context["run_history"] = _read_run_history("ccc_api_source")
        context["form_state"] = form_state
    return render_template("analytics_output.html", user=current_user, **context)


@app.route("/analytics/forecasting", methods=["GET", "POST"])
@login_required
def analytics_forecasting():
    target_year = _parse_int_form("target_year", 2030, minimum=2026, maximum=2050) if request.method == "POST" else 2030
    context = _analytics_output_context("forecasting")
    context["form_state"] = {"target_year": target_year}
    if request.method == "POST":
        try:
            out_path = _run_forecasting_from_ui(target_year=target_year)
            context = _analytics_output_context_for_path("forecasting", out_path)
            context["form_state"] = {"target_year": target_year}
            context["run_notice"] = f"Forecasting completed successfully. Loaded fresh output: {out_path.name}"
            _append_run_history(
                "forecasting",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "forecasting",
                    "scenario": "Forecasting",
                    "parameters_summary": f"Target year: {target_year}",
                    "target_year": target_year,
                    "output_file": out_path.name,
                },
            )
            _create_user_notification(
                current_user.id,
                title="Forecasting completed",
                message=f"Forecasting output {out_path.name} is ready.",
                notification_type="success",
                link=url_for("analytics_forecasting"),
            )
            context["run_history"] = _read_run_history("forecasting")
        except Exception as exc:
            context["run_error"] = f"Forecasting run failed: {exc}"
    return render_template("analytics_output.html", user=current_user, **context)


@app.route("/analytics/mapped-window-output", methods=["GET", "POST"])
@login_required
def analytics_mapped_window_output():
    default_year = max(2025, datetime.now().year)
    form_state = {
        "year": default_year,
        "start_month": 1,
        "end_month": 12,
    }
    context = _analytics_output_context("mapped_window_output")
    if request.method == "POST":
        form_state = {
            "year": _parse_int_form("year", default_year, minimum=2020, maximum=2100),
            "start_month": _parse_int_form("start_month", 1, minimum=1, maximum=12),
            "end_month": _parse_int_form("end_month", 12, minimum=1, maximum=12),
        }
        form_state["end_month"] = max(int(form_state["start_month"]), int(form_state["end_month"]))
        try:
            out_path = _run_mapped_window_from_ui(
                year=int(form_state["year"]),
                start_month=int(form_state["start_month"]),
                end_month=int(form_state["end_month"]),
            )
            context = _analytics_output_context_for_path("mapped_window_output", out_path)
            context["run_notice"] = f"Mapped window output generated successfully: {out_path.name}"
            _append_run_history(
                "mapped_window_output",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "mapped_window",
                    "scenario": "Mapped window output",
                    "parameters_summary": (
                        f"Year: {form_state['year']}, "
                        f"Period: {int(form_state['start_month']):02d}-{int(form_state['end_month']):02d}"
                    ),
                    "year": int(form_state["year"]),
                    "start_month": int(form_state["start_month"]),
                    "end_month": int(form_state["end_month"]),
                    "output_file": out_path.name,
                },
            )
            context["run_history"] = _read_run_history("mapped_window_output")
        except Exception as exc:
            context["run_error"] = f"Mapped window generation failed: {exc}"
    context["form_state"] = form_state
    return render_template("analytics_output.html", user=current_user, **context)


@app.route("/analytics/decarbonization", methods=["GET", "POST"])
@login_required
def analytics_decarbonization():
    context = _analytics_output_context("decarbonization")
    form_state = {
        "preset": "custom",
        "target_year": 2030,
        "reduction_pct": 20,
        "scope": "scope3",
        "scenario_type": "decarbonization",
    }
    if request.method == "POST":
        form_state = {
            "preset": str(request.form.get("scenario_preset", "custom") or "custom"),
            "target_year": _parse_int_form("target_year", 2030, minimum=2026, maximum=2050),
            "reduction_pct": _parse_float_form("reduction_pct", 20.0, minimum=0.0, maximum=100.0),
            "scope": str(request.form.get("scope", "scope3") or "scope3"),
            "scenario_type": str(request.form.get("scenario_type", "decarbonization") or "decarbonization"),
        }
        try:
            scenario_output, companion_output = _run_decarbonization_from_ui(
                target_year=int(form_state["target_year"]),
                reduction_pct=float(form_state["reduction_pct"]) / 100.0,
                scope_key=str(form_state["scope"]),
                scenario_type=str(form_state["scenario_type"]),
            )
            context = _analytics_output_context_for_path("decarbonization", companion_output)
            context["run_notice"] = (
                "Decarbonization run completed successfully. "
                f"Loaded fresh scenario output: {companion_output.name}"
            )
            context["companion_outputs"] = [
                _build_companion_output_payload(
                    "Decarbonization lever workbook",
                    scenario_output,
                    ("Scenarios_Yearly", "Scenarios_Monthly", "S4_Delta", "Meta"),
                )
            ]
            _append_run_history(
                "decarbonization",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "decarbonization",
                    "scenario": str(form_state["preset"] or "custom"),
                    "parameters_summary": (
                        f"Target year: {form_state['target_year']}, "
                        f"Reduction: {form_state['reduction_pct']}%, "
                        f"Scope: {_scope_label(str(form_state['scope']))}, "
                        f"Type: {form_state['scenario_type']}"
                    ),
                    "target_year": int(form_state["target_year"]),
                    "reduction_pct": float(form_state["reduction_pct"]),
                    "scope": _scope_label(str(form_state["scope"])),
                    "scenario_type": str(form_state["scenario_type"]),
                    "output_file": companion_output.name,
                    "companion_output_file": scenario_output.name,
                },
            )
            _create_user_notification(
                current_user.id,
                title="Decarbonization completed",
                message=f"Scenario output {companion_output.name} is ready for review.",
                notification_type="success",
                link=url_for("analytics_decarbonization"),
            )
            context["run_history"] = _read_run_history("decarbonization")
        except Exception as exc:
            context["run_error"] = f"Decarbonization run failed: {exc}"
    context["form_state"] = form_state
    return render_template("analytics_output.html", user=current_user, **context)


@app.route("/analytics/emissions-totals", methods=["GET", "POST"])
@login_required
def analytics_emissions_totals():
    default_year = max(2025, datetime.now().year)
    form_state = {
        "year": default_year,
        "start_month": 1,
        "end_month": 12,
    }
    context = _analytics_output_context("emissions_totals")
    if request.method == "POST":
        form_state = {
            "year": _parse_int_form("year", default_year, minimum=2020, maximum=2100),
            "start_month": _parse_int_form("start_month", 1, minimum=1, maximum=12),
            "end_month": _parse_int_form("end_month", 12, minimum=1, maximum=12),
        }
        form_state["end_month"] = max(int(form_state["start_month"]), int(form_state["end_month"]))
        try:
            out_path = _run_totals_tables_from_ui(
                year=int(form_state["year"]),
                start_month=int(form_state["start_month"]),
                end_month=int(form_state["end_month"]),
            )
            context = _analytics_output_context_for_path("emissions_totals", out_path)
            context["run_notice"] = f"Totals tables generated successfully: {out_path.name}"
            _append_run_history(
                "emissions_totals",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "totals_tables",
                    "scenario": "Totals tables",
                    "parameters_summary": (
                        f"Year: {form_state['year']}, "
                        f"Period: {int(form_state['start_month']):02d}-{int(form_state['end_month']):02d}"
                    ),
                    "year": int(form_state["year"]),
                    "start_month": int(form_state["start_month"]),
                    "end_month": int(form_state["end_month"]),
                    "output_file": out_path.name,
                },
            )
            context["run_history"] = _read_run_history("emissions_totals")
        except Exception as exc:
            context["run_error"] = f"Totals generation failed: {exc}"
    context["form_state"] = form_state
    return render_template("analytics_output.html", user=current_user, **context)


@app.route("/analytics/share-analysis", methods=["GET", "POST"])
@login_required
def analytics_share_analysis():
    default_year = max(2025, datetime.now().year)
    form_state = {
        "year": default_year,
        "start_month": 1,
        "end_month": 12,
    }
    context = _analytics_output_context("share_analysis")
    if request.method == "POST":
        form_state = {
            "year": _parse_int_form("year", default_year, minimum=2020, maximum=2100),
            "start_month": _parse_int_form("start_month", 1, minimum=1, maximum=12),
            "end_month": _parse_int_form("end_month", 12, minimum=1, maximum=12),
        }
        form_state["end_month"] = max(int(form_state["start_month"]), int(form_state["end_month"]))
        try:
            out_path = _run_totals_tables_from_ui(
                year=int(form_state["year"]),
                start_month=int(form_state["start_month"]),
                end_month=int(form_state["end_month"]),
            )
            context = _analytics_output_context_for_path("share_analysis", out_path)
            context["run_notice"] = f"Share analysis generated successfully: {out_path.name}"
            _append_run_history(
                "share_analysis",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "share_analysis",
                    "scenario": "Share analysis",
                    "parameters_summary": (
                        f"Year: {form_state['year']}, "
                        f"Period: {int(form_state['start_month']):02d}-{int(form_state['end_month']):02d}"
                    ),
                    "year": int(form_state["year"]),
                    "start_month": int(form_state["start_month"]),
                    "end_month": int(form_state["end_month"]),
                    "output_file": out_path.name,
                },
            )
            context["run_history"] = _read_run_history("share_analysis")
        except Exception as exc:
            context["run_error"] = f"Share analysis generation failed: {exc}"
    context["form_state"] = form_state
    return render_template("analytics_output.html", user=current_user, **context)


@app.route("/analytics/emissions-map", methods=["GET"])
@login_required
def analytics_emissions_map():
    return render_template("emissions_map.html", user=current_user, **_load_emissions_map_points())


@app.route("/governance/double-counting-check", methods=["GET", "POST"])
@login_required
def governance_double_counting_check():
    context = _analytics_output_context("double_counting_check")
    if request.method == "POST":
        try:
            out_path = _run_double_counting_from_ui()
            context = _analytics_output_context_for_path("double_counting_check", out_path)
            context["run_notice"] = f"Double counting report generated successfully: {out_path.name}"
            _append_run_history(
                "double_counting_check",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "double_counting",
                    "scenario": "Double counting report",
                    "parameters_summary": "Generated from the latest merged mapping workbook.",
                    "output_file": out_path.name,
                },
            )
            context["run_history"] = _read_run_history("double_counting_check")
        except Exception as exc:
            context["run_error"] = f"Double counting generation failed: {exc}"
    return render_template("analytics_output.html", user=current_user, **context)


@app.route("/governance/audit-ready-output", methods=["GET", "POST"])
@login_required
def governance_audit_ready_output():
    default_year = max(2025, datetime.now().year)
    form_state = {
        "year": default_year,
        "start_month": 1,
        "end_month": 12,
    }
    context = _analytics_output_context("audit_ready_output")
    if request.method == "POST":
        form_state = {
            "year": _parse_int_form("year", default_year, minimum=2020, maximum=2100),
            "start_month": _parse_int_form("start_month", 1, minimum=1, maximum=12),
            "end_month": _parse_int_form("end_month", 12, minimum=1, maximum=12),
        }
        form_state["end_month"] = max(int(form_state["start_month"]), int(form_state["end_month"]))
        try:
            out_path = _run_mapped_window_from_ui(
                year=int(form_state["year"]),
                start_month=int(form_state["start_month"]),
                end_month=int(form_state["end_month"]),
            )
            context = _analytics_output_context_for_path("audit_ready_output", out_path)
            context["run_notice"] = f"Audit-ready dataset generated successfully: {out_path.name}"
            _append_run_history(
                "audit_ready_output",
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "type": "audit_output",
                    "scenario": "Audit ready dataset",
                    "parameters_summary": (
                        f"Year: {form_state['year']}, "
                        f"Period: {int(form_state['start_month']):02d}-{int(form_state['end_month']):02d}"
                    ),
                    "year": int(form_state["year"]),
                    "start_month": int(form_state["start_month"]),
                    "end_month": int(form_state["end_month"]),
                    "output_file": out_path.name,
                },
            )
            _create_user_notification(
                current_user.id,
                title="Audit dataset ready",
                message=f"Audit-ready dataset {out_path.name} has been generated.",
                notification_type="success",
                link=url_for("governance_audit_ready_output"),
            )
            context["run_history"] = _read_run_history("audit_ready_output")
        except Exception as exc:
            context["run_error"] = f"Audit dataset generation failed: {exc}"
    context["form_state"] = form_state
    return render_template("analytics_output.html", user=current_user, **context)


@app.route('/admin', methods=['GET', 'POST'])
@login_required
def admin():
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('dashboard'))

    users = User.query.all()
    mapping_runs = MappingRun.query.order_by(MappingRun.created_at.desc()).all()

    # Find all months with mapping runs
    all_months = set()
    for run in mapping_runs:
        if run.created_at:
            all_months.add(run.created_at.strftime('%B %Y'))
    if not all_months:
        all_months.add(datetime.now().strftime('%B %Y'))
    available_months = sorted(list(all_months), key=lambda x: datetime.strptime(x, '%B %Y'), reverse=True)
    selected_month = request.args.get('month') or available_months[0]

    # Calculate progress per company based on mapped categories vs available schema sheets
    companies = list(TEMPLATES.keys())
    submission_stats = []
    chart_data = []
    for company in companies:
        expected = _count_company_schema_sheets(company)
        submitted_sheets = set()
        for run in mapping_runs:
            if (run.company_name == company and run.created_at and run.created_at.strftime('%B %Y') == selected_month and run.status == 'succeeded'):
                submitted_sheets.add(run.sheet_name)
        submitted = len(submitted_sheets)
        rate = int(round((submitted / expected) * 100)) if expected > 0 else 0
        submission_stats.append({
            'company': company,
            'month': selected_month,
            'submitted': submitted,
            'expected': expected,
            'rate': rate
        })
        chart_data.append({
            'company': company,
            'submitted': submitted,
            'expected': expected,
            'rate': rate
        })

    # Last 7 days mapping count
    seven_days_ago = datetime.now() - timedelta(days=7)
    recent_admin_submissions_count = MappingRun.query.filter(
        MappingRun.created_at >= seven_days_ago
    ).count()

    users_for_admin = [_user_public_dict_for_admin(u) for u in users]
    is_owner = normalize_user_role(getattr(current_user, "role", None)) == "owner"
    return render_template(
        'admin.html',
        users=users,
        users_for_admin=users_for_admin,
        is_owner=is_owner,
        user_roles=list(USER_ROLES),
        submissions=[],
        mapping_runs=mapping_runs,
        recent_admin_submissions_count=recent_admin_submissions_count,
        available_months=available_months,
        selected_month=selected_month,
        submission_stats=submission_stats,
        chart_data=chart_data,
        logos=[],
    )


@app.route("/admin/user/<int:user_id>/profile", methods=["POST"])
@login_required
def admin_update_user_profile(user_id):
    """Owner-only: update another user's profile fields (existing columns only)."""
    if not current_user.is_admin:
        flash("Access denied")
        return redirect(url_for("dashboard"))
    if normalize_user_role(getattr(current_user, "role", None)) != "owner":
        flash("Only Owner can modify user details.")
        return redirect(url_for("admin"))
    _ensure_db_tables()
    target = db.session.get(User, user_id)
    if not target:
        flash("User not found.")
        return redirect(url_for("admin"))

    target.first_name = (request.form.get("first_name") or "").strip() or None
    target.last_name = (request.form.get("last_name") or "").strip() or None
    target.phone = (request.form.get("phone") or "").strip() or None
    cn = (request.form.get("company_name") or "").strip()
    if not cn:
        flash("Company name is required.")
        return redirect(url_for("admin"))
    target.company_name = cn
    target.company_country = (request.form.get("company_country") or "").strip() or None

    new_role = request.form.get("role")
    if new_role is not None and str(new_role).strip():
        new_role = normalize_user_role(new_role)
        prev = normalize_user_role(getattr(target, "role", None))
        if prev == "owner" and new_role != "owner":
            owners = User.query.filter(User.role == "owner").count()
            if owners <= 1:
                flash("Cannot remove the only owner account.")
                return redirect(url_for("admin"))
        target.role = new_role
        sync_user_admin_flag(target)

    pfile = request.files.get("profile_photo")
    if pfile and getattr(pfile, "filename", None):
        rel = _save_profile_photo_file(pfile, user_id)
        if rel:
            target.profile_photo_path = rel

    db.session.commit()
    flash(f"User {target.email} updated.")
    return redirect(url_for("admin"))


@app.route("/admin/users", methods=["GET", "POST"])
@login_required
def admin_users():
    """Owner-only: list users and change roles."""
    _ensure_db_tables()
    if normalize_user_role(getattr(current_user, "role", None)) != "owner":
        flash("Access denied")
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        uid = request.form.get("user_id", type=int)
        new_role = normalize_user_role(request.form.get("role"))
        if not uid:
            flash("Invalid request.")
            return redirect(url_for("admin_users"))
        target = db.session.get(User, uid)
        if not target:
            flash("User not found.")
            return redirect(url_for("admin_users"))

        prev = normalize_user_role(getattr(target, "role", None))
        if prev == "owner" and new_role != "owner":
            owners = User.query.filter(User.role == "owner").count()
            if owners <= 1:
                flash("Cannot remove the only owner account.")
                return redirect(url_for("admin_users"))

        target.role = new_role
        sync_user_admin_flag(target)
        db.session.commit()
        flash(f"Role updated for {target.email}.")
        return redirect(url_for("admin_users"))

    users = User.query.order_by(User.email.asc()).all()
    return render_template("admin_users.html", users=users, user_roles=USER_ROLES)


@app.route("/owner-analytics", methods=["GET"])
@login_required
def owner_analytics():
    if not bool(getattr(current_user, "is_admin", False)):
        abort(403)
    return render_template("owner_analytics.html", user=current_user, **_owner_analytics_context())


@app.route("/admin/preprocess/travel/upload", methods=["POST"])
@login_required
def upload_travel_preprocess():
    if not current_user.is_admin:
        flash("Access denied")
        return redirect(url_for("dashboard"))

    upload = request.files.get("travel_file")
    if not upload or not getattr(upload, "filename", None):
        flash("Please choose a Travel .xlsb file.")
        return redirect(url_for("admin"))

    ext = Path(secure_filename(upload.filename or "")).suffix.lower()
    if ext not in TRAVEL_ALLOWED_EXT:
        flash("Only .xlsb files are allowed for Travel uploads.")
        return redirect(url_for("admin"))

    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:10]}"
    run_dir = FRONTEND_UPLOAD_DIR / "preprocess" / "travel" / run_id
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    safe_name = secure_filename(upload.filename or "travel_source.xlsb") or "travel_source.xlsb"
    raw_path = raw_dir / safe_name
    upload.save(str(raw_path))

    validation_errors = validate_travel_upload(raw_path)
    validation_path = run_dir / "validation.json"
    if validation_errors:
        validation_path.write_text(
            json.dumps({"status": "failed", "errors": validation_errors}, indent=2),
            encoding="utf-8",
        )
        flash(validation_errors[0])
        return redirect(url_for("admin"))

    validation_path.write_text(
        json.dumps({"status": "passed", "file": raw_path.name}, indent=2),
        encoding="utf-8",
    )

    threading.Thread(
        target=run_travel_preprocess,
        args=(run_dir, raw_path),
        daemon=True,
    ).start()
    flash("Travel preprocessing started.")
    return redirect(url_for("admin"))


@app.route('/logout')
@login_required
def logout():
    request.environ["skip_activity_log"] = True
    _write_activity_log_for_user(current_user, action="logout")
    session.pop("activity_session_id", None)
    logout_user()
    return redirect(url_for('index'))

@app.route('/report', methods=['GET'])
@login_required
def report():
    if current_user.is_admin:
        return redirect(url_for('admin_report', **request.args.to_dict(flat=True)))
    template_filter = request.args.get('template', '').strip().lower()
    month_filter = request.args.get('date', '').strip()
    company_keys = _company_candidate_keys(current_user.company_name)
    summaries = (
        MappingRunSummary.query.filter(MappingRunSummary.company_name.in_(company_keys))
        .order_by(MappingRunSummary.created_at.desc())
        .all()
    )
    filtered_summaries = []
    seen = set()
    run_cache: dict[str, MappingRun | None] = {}
    for sub in summaries:
        key = ((sub.company_name or "").strip().lower(), (sub.sheet_name or "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        match = True
        if template_filter and template_filter not in (sub.sheet_name or "").lower():
            match = False
        profile = _period_profile_for_summary(sub, run_cache=run_cache)
        points = list(profile.get("points") or [])
        if month_filter:
            points = [p for p in points if str(p.get("key") or "").startswith(month_filter)]
            if not points:
                match = False
        if match:
            filtered_summaries.append((sub, points if month_filter else points))
    emission_results = []
    chart_data = []
    for idx, (sub, points) in enumerate(filtered_summaries):
        filtered_points = list(points or [])
        if not filtered_points:
            profile = _period_profile_for_summary(sub, run_cache=run_cache)
            filtered_points = list(profile.get("points") or [])
        total_t = _safe_float(sum(float(p.get("value") or 0.0) for p in filtered_points) if filtered_points else (sub.tco2e_total or 0.0))
        label = str(sub.sheet_name or "Category")
        emission_results.append({
            'template_name': label,
            'period_label': _format_period_label(filtered_points, getattr(sub, "created_at", None)),
            'total_emission': round(total_t, 6),
            'by_category': {label: total_t}
        })
        chart_data.append({
            'bar_id': f'barChart{idx+1}',
            'line_id': f'lineChart{idx+1}',
            'pie_id': f'pieChart{idx+1}',
            'labels': [str(p.get("label") or label) for p in filtered_points] if filtered_points else [label],
            'values': [_safe_float(p.get("value") or 0.0) for p in filtered_points] if filtered_points else [total_t]
        })
    return render_template('report.html', emission_results=emission_results, user=current_user, chart_data=chart_data)

@app.route('/admin_report', methods=['GET'])
@login_required
def admin_report():
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('dashboard'))
    company_filter = request.args.get('company', '').strip().lower()
    template_filter = request.args.get('template', '').strip().lower()
    month_filter = request.args.get('date', '').strip()
    summaries = MappingRunSummary.query.order_by(MappingRunSummary.created_at.desc()).all()
    filtered_submissions = []
    seen = set()
    run_cache: dict[str, MappingRun | None] = {}
    for sub in summaries:
        key = ((sub.company_name or "").strip().lower(), (sub.sheet_name or "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        match = True
        if company_filter and company_filter not in (sub.company_name or "").lower():
            match = False
        if template_filter and template_filter not in (sub.sheet_name or "").lower():
            match = False
        profile = _period_profile_for_summary(sub, run_cache=run_cache)
        points = list(profile.get("points") or [])
        if month_filter:
            points = [p for p in points if str(p.get("key") or "").startswith(month_filter)]
            if not points:
                match = False
        if match:
            filtered_submissions.append((sub, points if month_filter else points))
    emission_results = []
    chart_data = []
    for idx, (sub, points) in enumerate(filtered_submissions):
        filtered_points = list(points or [])
        if not filtered_points:
            profile = _period_profile_for_summary(sub, run_cache=run_cache)
            filtered_points = list(profile.get("points") or [])
        total_t = _safe_float(sum(float(p.get("value") or 0.0) for p in filtered_points) if filtered_points else (sub.tco2e_total or 0.0))
        label = str(sub.sheet_name or "Category")
        emission_results.append({
            'company_name': sub.company_name,
            'template_name': label,
            'period_label': _format_period_label(filtered_points, getattr(sub, "created_at", None)),
            'total_emission': round(total_t, 6),
            'by_category': {label: total_t}
        })
        chart_data.append({
            'bar_id': f'barChart{idx+1}',
            'line_id': f'lineChart{idx+1}',
            'pie_id': f'pieChart{idx+1}',
            'labels': [str(p.get("label") or label) for p in filtered_points] if filtered_points else [label],
            'values': [_safe_float(p.get("value") or 0.0) for p in filtered_points] if filtered_points else [total_t]
        })
    return render_template('admin_report.html', emission_results=emission_results, user=current_user, chart_data=chart_data)

@app.route('/admin/emission_factors', methods=['GET', 'POST'])
@login_required
def manage_emission_factors():
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        flash('Emission factors are now sourced from the mapping workbook and are read-only in the UI.')
        return redirect(url_for('manage_emission_factors'))

    data = _load_stage2_emission_factors()
    all_rows: list[dict[str, object]] = list(data.get("rows") or [])
    sheets: list[str] = list(data.get("sheets") or [])
    scopes: list[str] = list(data.get("scopes") or [])
    sources: list[str] = list(data.get("sources") or [])

    # Filters
    search = request.args.get('search', '').strip()
    sheet_filter = request.args.get('sheet', '').strip()
    scope_filter = request.args.get('scope', '').strip()
    source_filter = request.args.get('ef_source', '').strip()

    def _matches(row: dict[str, object]) -> bool:
        if sheet_filter and str(row.get("sheet") or "") != sheet_filter:
            return False
        if scope_filter and str(row.get("scope") or "") != scope_filter:
            return False
        if source_filter and str(row.get("ef_source") or "") != source_filter:
            return False

        if search:
            hay = " ".join(
                str(row.get(k) or "")
                for k in (
                    "sheet",
                    "ef_name",
                    "ef_description",
                    "scope",
                    "ef_category",
                    "ef_id",
                    "ef_value",
                    "ef_unit",
                    "ef_source",
                    "Emission Factor Category",
                )
            ).lower()
            if search.lower() not in hay:
                return False
        return True

    filtered = [r for r in all_rows if _matches(r)]
    filtered.sort(key=lambda r: (str(r.get("sheet") or ""), str(r.get("ef_name") or ""), str(r.get("ef_id") or "")))

    # Pagination (lightweight object compatible with template)
    page = int(request.args.get('page', 1) or 1)
    per_page = 60
    total = len(filtered)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    end = start + per_page
    items = filtered[start:end]

    factors = SimpleNamespace(
        items=items,
        page=page,
        pages=pages,
        has_prev=(page > 1),
        has_next=(page < pages),
        prev_num=(page - 1),
        next_num=(page + 1),
        total=total,
    )

    if request.method == 'POST':
        # handled above (read-only)
        pass

    return render_template(
        'emission_factors.html',
        factors=factors,
        search=search,
        sheet=sheet_filter,
        scope=scope_filter,
        ef_source=source_filter,
        available_sheets=sheets,
        available_scopes=scopes,
        available_sources=sources,
        mapping_path=str(STAGE2_EF_XLSX),
    )

@app.route('/admin/emission_factors/delete/<int:factor_id>', methods=['POST'])
@login_required
def delete_emission_factor(factor_id):
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('dashboard'))
    flash('Emission factors are sourced from the mapping workbook and cannot be deleted here.')
    return redirect(url_for('manage_emission_factors'))

@app.route('/admin/emission_factors/update/<int:factor_id>', methods=['POST'])
@login_required
def update_emission_factor(factor_id):
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('dashboard'))
    flash('Emission factors are sourced from the mapping workbook and cannot be updated here.')
    return redirect(url_for('manage_emission_factors'))


def _ef_expected_headers() -> list[str]:
    return [
        "ef_name",
        "ef_description",
        "scope",
        "ef_category",
        "ef_id",
        "ef_value",
        "ef_unit",
        "ef_source",
        "Emission Factor Category",
    ]


def _clear_ef_cache() -> None:
    global _EF_CACHE
    _EF_CACHE = {"mtime_ns": None, "rows": None, "sheets": None, "scopes": None, "sources": None}


@app.route("/admin/emission_factors/mapping/reload", methods=["GET"])
@login_required
def reload_emission_factors_mapping():
    if not current_user.is_admin:
        flash("Access denied")
        return redirect(url_for("dashboard"))
    _clear_ef_cache()
    flash("Emission factors reloaded from mapping workbook.")
    # Preserve current filters if present
    return redirect(
        url_for(
            "manage_emission_factors",
            search=request.args.get("search", ""),
            sheet=request.args.get("sheet", ""),
            scope=request.args.get("scope", ""),
            ef_source=request.args.get("ef_source", ""),
            page=request.args.get("page", 1),
        )
    )


def _cleanup_mapping_runs(max_age_minutes: int = 180) -> None:
    now = datetime.now()
    to_del = []
    for run_id, meta in list(_MAPPING_RUNS.items()):
        created = meta.get("created_at")
        if isinstance(created, datetime) and (now - created).total_seconds() > max_age_minutes * 60:
            to_del.append(run_id)
    for rid in to_del:
        try:
            p = _MAPPING_RUNS.get(rid, {}).get("path")
            if p and os.path.exists(str(p)):
                os.remove(str(p))
        except Exception:
            pass
        _MAPPING_RUNS.pop(rid, None)


def _to_numeric_spend(x: object) -> float | None:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):  # type: ignore[arg-type]
            return None
        if isinstance(x, (int, float)) and not (isinstance(x, float) and pd.isna(x)):
            return float(x)

        s = str(x).strip().replace(" ", "")
        if s == "":
            return None
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]
        # handle comma/period formats
        if s.count(",") == 1 and s.count(".") > 0 and s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        elif s.count(".") == 1 and s.count(",") > 0 and s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        elif s.count(",") == 1 and s.count(".") == 0:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
        return float(s)
    except Exception:
        return None


_FX_2025_TO_EUR: dict[str, float] = {
    "USD": 0.884969,
    "JPY": 0.005916,
    "BGN": 0.511300,
    "CZK": 0.040506,
    "DKK": 0.133987,
    "GBP": 1.167144,
    "HUF": 0.002514,
    "PLN": 0.235868,
    "RON": 0.198319,
    "SEK": 0.090364,
    "CHF": 1.067201,
    "ISK": 0.006913,
    "NOK": 0.085344,
    "TRY": 0.022313,
    "AUD": 0.570854,
    "BRL": 0.158550,
    "CAD": 0.633422,
    "CNY": 0.123175,
    "HKD": 0.113502,
    "IDR": 0.000054,
    "ILS": 0.256889,
    "INR": 0.010150,
    "KRW": 0.000623,
    "MXN": 0.046146,
    "MYR": 0.206874,
    "NZD": 0.514888,
    "PHP": 0.015390,
    "SGD": 0.677705,
    "THB": 0.026943,
    "ZAR": 0.049557,
    "EUR": 1.0,
    "EURO": 1.0,
    "EUROS": 1.0,
    "€": 1.0,
}


def _normalize_currency(raw: object) -> str | None:
    if raw is None:
        return None
    try:
        if isinstance(raw, float) and pd.isna(raw):
            return None
    except Exception:
        pass
    s = str(raw).upper()
    if "€" in s:
        return "EUR"
    if "£" in s:
        return "GBP"
    m = re.search(r"\b(USD|JPY|BGN|CZK|DKK|GBP|HUF|PLN|RON|SEK|CHF|ISK|NOK|TRY|AUD|BRL|CAD|CNY|HKD|IDR|ILS|INR|KRW|MXN|MYR|NZD|PHP|SGD|THB|ZAR|EUR|EURO|EUROS|€)\b", s)
    return m.group(1) if m else None


def preprocess_for_mapping(company_name: str, sheet_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Web-friendly 'mini Stage1': keep inputs stable for Stage2 mapping without multi-company merge.
    Does NOT change mapping rules; only prepares a single-sheet dataframe.
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Drop fully empty rows
    df = df.dropna(how="all")

    # Minimal whitespace normalization
    for c in df.columns:
        try:
            if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]):
                df[c] = df[c].astype("string").str.strip()
        except Exception:
            pass

    canon_company, canon_country = _canonical_company_name_and_country(company_name)
    company_display = canon_company or company_name

    # Compatibility columns expected by some downstream steps
    if "Source_File" not in df.columns:
        df["Source_File"] = f"{company_display}.xlsx"
    if "subsidiary_name" not in df.columns:
        df["subsidiary_name"] = company_display

    # Country is required by several mapping rules (previously derived from "Company Information")
    # Ensure both "Country" and "country" exist for robust header matching.
    if "Country" not in df.columns:
        df["Country"] = ""
    if "country" not in df.columns:
        df["country"] = ""

    if canon_country:
        # Fill only where blank
        try:
            mask = df["Country"].isna() | (df["Country"].astype(str).str.strip() == "")
            df.loc[mask, "Country"] = canon_country
        except Exception:
            df["Country"] = canon_country
        try:
            mask2 = df["country"].isna() | (df["country"].astype(str).str.strip() == "")
            df.loc[mask2, "country"] = canon_country
        except Exception:
            df["country"] = canon_country

    # Add Spend_Euro if possible (mirrors Currency_converter_17Dec behavior at sheet-level)
    if "Spend_Euro" not in df.columns:
        sheet_key = str(sheet_name).strip().lower()

        def find_col_exact(name: str) -> str | None:
            for col in df.columns:
                if str(col).strip().lower() == name.strip().lower():
                    return str(col)
            return None

        # Spend/currency columns with special cases
        if sheet_key == "scope 3 cat 15 pensions":
            spend_col = next((c for c in df.columns if "employer payment to pension provider" in str(c).lower()), None)
            currency_col = find_col_exact("currency")
        elif sheet_key == "scope 3 cat 6 business travel":
            spend_col = find_col_exact("spend")
            currency_col = find_col_exact("spend currency")
        else:
            spend_col = next((c for c in df.columns if "spend" in str(c).lower() and "euro" not in str(c).lower()), None)
            currency_col = next((c for c in df.columns if "currency" in str(c).lower()), None)

        spend_euro_vals = []
        if spend_col and currency_col:
            for _, row in df.iterrows():
                spend = _to_numeric_spend(row.get(spend_col))
                curr_code = _normalize_currency(row.get(currency_col))
                if spend is None or curr_code is None:
                    spend_euro_vals.append(pd.NA)
                    continue
                rate = _FX_2025_TO_EUR.get(curr_code)
                spend_euro_vals.append(spend * rate if rate else pd.NA)
        else:
            spend_euro_vals = [pd.NA] * len(df)
        df["Spend_Euro"] = spend_euro_vals

    return df


def _import_stage2_main_mapping():
    # Lazy import to keep Flask startup fast
    if str(STAGE2_MAPPING_DIR) not in sys.path:
        sys.path.insert(0, str(STAGE2_MAPPING_DIR))
    import importlib
    return importlib.import_module("main_mapping")


def _import_stage2_append_sources():
    if str(STAGE2_MAPPING_DIR) not in sys.path:
        sys.path.insert(0, str(STAGE2_MAPPING_DIR))
    import importlib
    return importlib.import_module("append_sources_to_mapped")


def _publish_klarakarbon_data_entry_output(company_name: str) -> Path:
    headers, _rules = _get_data_entry_template_schema(company_name, KLARAKARBON_SHEET_NAME)
    if not headers:
        raise RuntimeError("Klarakarbon headers are not configured for this company.")
    df = _load_data_entries_dataframe(company_name, KLARAKARBON_SHEET_NAME, headers)
    if df.empty:
        raise RuntimeError("No saved Klarakarbon rows found for this company.")
    publish_dir = STAGE1_KLARAKARBON_OUTPUT_DIR / company_slug(company_name)
    publish_dir.mkdir(parents=True, exist_ok=True)
    publish_path = publish_dir / "klarakarbon_categories_mapped_FINAL.xlsx"
    with pd.ExcelWriter(publish_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=KLARAKARBON_SHEET_NAME[:31], index=False)
    return publish_path


def _run_append_and_pipeline(company_name: str, sheet_name: str) -> dict[str, object]:
    sheet_key = str(sheet_name or "").strip()
    if _batch_action_type_for_sheet(sheet_key) != "append_run":
        raise RuntimeError("This sheet does not support append-and-run orchestration.")

    published_path: Path | None = None
    if sheet_key == KLARAKARBON_SHEET_NAME:
        published_path = _publish_klarakarbon_data_entry_output(company_name)
    elif sheet_key == TRAVEL_SHEET_NAME:
        published_path = STAGE2_TRAVEL_DIR / "analysis_summary.xlsx"
        if not published_path.exists():
            raise RuntimeError("Travel analysis_summary.xlsx was not found. Upload Travel data first.")

    append_src = _import_stage2_append_sources()
    with _STAGE2_MAP_LOCK:
        append_src.main(company_name=company_name)
        proc = subprocess.run(
            [sys.executable, str(STAGE2_MAPPING_DIR / "Run_Everything.py")],
            cwd=str(STAGE2_MAPPING_DIR),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    if proc.returncode != 0:
        detail = (proc.stdout or proc.stderr or "").strip()
        if detail:
            detail = detail[-1200:]
        raise RuntimeError(detail or f"Run_Everything.py failed with exit code {proc.returncode}")

    return {
        "ok": True,
        "company": company_name,
        "sheet": sheet_key,
        "action_type": "append_run",
        "published_path": str(published_path) if published_path else "",
        "processed_at": datetime.utcnow().isoformat() + "Z",
    }


def run_mapping(company_name: str, sheet_name: str, df: pd.DataFrame) -> tuple[pd.DataFrame, Path, Path]:
    """
    Run existing Stage2 mapping logic for a single company + single sheet dataframe.
    Mapping logic is NOT modified; we call main_mapping.process_all_sheets() on a temporary workbook.
    Returns (mapped_df, output_workbook_path).
    """
    _cleanup_mapping_runs()

    df_pre = preprocess_for_mapping(company_name, sheet_name, df)

    run_dir = INSTANCE_DIR / "mapping_runs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:10]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    input_xlsx = run_dir / f"{secure_filename(company_name)}__{secure_filename(sheet_name)}.xlsx"

    with pd.ExcelWriter(input_xlsx, engine="openpyxl") as writer:
        df_pre.to_excel(writer, sheet_name=str(sheet_name)[:31], index=False)

    mm = _import_stage2_main_mapping()
    append_src = _import_stage2_append_sources()

    # Run Stage2 mapping under a lock (Stage2 writes to a shared output/ directory)
    start_ts = time.time() if "time" in globals() else None
    import time as _time
    start_ts = _time.time()
    with _STAGE2_MAP_LOCK:
        orig = getattr(mm, "INPUT_WORKBOOK_NAME", None)
        try:
            setattr(mm, "INPUT_WORKBOOK_NAME", str(input_xlsx))
            mm.process_all_sheets()
            append_src.main(company_name=company_name)
        finally:
            try:
                setattr(mm, "INPUT_WORKBOOK_NAME", orig)
            except Exception:
                pass

    # Pick newest mapped_results output created after we started
    candidates = sorted(
        STAGE2_MAPPING_OUTPUT_DIR.glob("mapped_results*.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    out_path = None
    for p in candidates:
        try:
            if p.stat().st_mtime >= start_ts - 2:
                out_path = p
                break
        except Exception:
            continue
    if out_path is None and candidates:
        out_path = candidates[0]
    if out_path is None:
        raise RuntimeError("Stage2 mapping did not produce an output workbook.")

    # Copy output to a stable location for this run (Stage2 output dir is shared)
    stable_out = run_dir / "mapped_results.xlsx"
    try:
        shutil.copy2(out_path, stable_out)
        out_path = stable_out
    except Exception:
        pass

    sheets = pd.read_excel(out_path, sheet_name=None, engine="openpyxl")
    # Find the mapped sheet (safe-name truncation may apply)
    mapped_df = None
    for k, v in sheets.items():
        if str(k).strip().lower() == str(sheet_name).strip().lower():
            mapped_df = v
            break
    if mapped_df is None:
        # fallback: return first sheet if only one
        if len(sheets) == 1:
            mapped_df = next(iter(sheets.values()))
        else:
            # best-effort: match by prefix
            want = str(sheet_name).strip().lower()[:20]
            for k, v in sheets.items():
                if str(k).strip().lower().startswith(want):
                    mapped_df = v
                    break
    if mapped_df is None:
        raise RuntimeError("Could not locate mapped sheet in Stage2 output workbook.")

    return mapped_df, out_path, input_xlsx


def _find_ef_row_in_sheet(ws, ef_id: str) -> int | None:
    ef_id = (ef_id or "").strip()
    if not ef_id:
        return None
    headers = [("" if v is None else str(v).strip()) for v in (next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None) or [])]
    while headers and headers[-1] == "":
        headers.pop()
    if "ef_id" not in headers:
        return None
    ef_col = headers.index("ef_id") + 1
    for idx, row_values in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        v = row_values[ef_col - 1] if row_values and len(row_values) >= ef_col else None
        if v is None:
            continue
        if str(v).strip() == ef_id:
            return idx
    return None


@app.route("/admin/emission_factors/mapping/update", methods=["POST"])
@login_required
def update_emission_factor_mapping():
    if not current_user.is_admin:
        flash("Access denied")
        return redirect(url_for("dashboard"))

    sheet = (request.form.get("sheet") or "").strip()
    original_ef_id = (request.form.get("original_ef_id") or "").strip()
    ef_id = (request.form.get("ef_id") or "").strip()

    if not sheet or not original_ef_id:
        flash("Missing sheet or ef_id")
        return redirect(url_for("manage_emission_factors"))

    payload = {k: (request.form.get(k) or "").strip() for k in _ef_expected_headers()}
    if ef_id:
        payload["ef_id"] = ef_id

    try:
        wb = load_workbook(STAGE2_EF_XLSX, keep_links=False)
        if sheet not in wb.sheetnames:
            flash("Sheet not found in mapping workbook")
            return redirect(url_for("manage_emission_factors"))
        ws = wb[sheet]

        headers = [("" if v is None else str(v).strip()) for v in (next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None) or [])]
        while headers and headers[-1] == "":
            headers.pop()

        # Validate required headers exist
        missing = [h for h in _ef_expected_headers() if h not in headers]
        if missing:
            flash(f"Mapping sheet headers are missing: {', '.join(missing)}")
            return redirect(url_for("manage_emission_factors"))

        row_idx = _find_ef_row_in_sheet(ws, original_ef_id)
        if row_idx is None:
            flash("Could not locate the emission factor row by ef_id")
            return redirect(url_for("manage_emission_factors"))

        for h in _ef_expected_headers():
            col = headers.index(h) + 1
            v = payload.get(h)
            if h == "ef_value":
                # keep numeric if possible
                try:
                    ws.cell(row=row_idx, column=col).value = float(v) if v != "" else None
                except Exception:
                    ws.cell(row=row_idx, column=col).value = v if v != "" else None
            else:
                ws.cell(row=row_idx, column=col).value = v if v != "" else None

        # Backup then save (protect against accidental edits)
        backup_dir = STAGE2_EF_XLSX.parent / "_ef_backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"{STAGE2_EF_XLSX.stem}_before_update_{ts}{STAGE2_EF_XLSX.suffix}"
        try:
            shutil.copy2(STAGE2_EF_XLSX, backup_path)
        except Exception:
            pass

        wb.save(STAGE2_EF_XLSX)
        _clear_ef_cache()
        flash("Emission factor updated in mapping workbook.")
    except PermissionError:
        flash("Mapping workbook is open/locked. Close Excel and try again.")
    except Exception as e:
        flash(f"Update failed: {e}")

    return redirect(url_for("manage_emission_factors", search=request.args.get("search",""), sheet=request.args.get("sheet",""), scope=request.args.get("scope",""), ef_source=request.args.get("ef_source","")))


@app.route("/admin/emission_factors/mapping/delete", methods=["POST"])
@login_required
def delete_emission_factor_mapping():
    if not current_user.is_admin:
        flash("Access denied")
        return redirect(url_for("dashboard"))

    sheet = (request.form.get("sheet") or "").strip()
    ef_id = (request.form.get("ef_id") or "").strip()
    if not sheet or not ef_id:
        flash("Missing sheet or ef_id")
        return redirect(url_for("manage_emission_factors"))

    try:
        wb = load_workbook(STAGE2_EF_XLSX, keep_links=False)
        if sheet not in wb.sheetnames:
            flash("Sheet not found in mapping workbook")
            return redirect(url_for("manage_emission_factors"))
        ws = wb[sheet]
        row_idx = _find_ef_row_in_sheet(ws, ef_id)
        if row_idx is None:
            flash("Could not locate the emission factor row by ef_id")
            return redirect(url_for("manage_emission_factors"))

        backup_dir = STAGE2_EF_XLSX.parent / "_ef_backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"{STAGE2_EF_XLSX.stem}_before_delete_{ts}{STAGE2_EF_XLSX.suffix}"
        try:
            shutil.copy2(STAGE2_EF_XLSX, backup_path)
        except Exception:
            pass

        ws.delete_rows(row_idx, 1)
        wb.save(STAGE2_EF_XLSX)
        _clear_ef_cache()
        flash("Emission factor deleted from mapping workbook.")
    except PermissionError:
        flash("Mapping workbook is open/locked. Close Excel and try again.")
    except Exception as e:
        flash(f"Delete failed: {e}")

    return redirect(url_for("manage_emission_factors"))


@app.route("/admin/emission_factors/mapping/import", methods=["POST"])
@login_required
def import_emission_factors_mapping():
    if not current_user.is_admin:
        flash("Access denied")
        return redirect(url_for("dashboard"))

    f = request.files.get("excel_file")
    if not f or not f.filename:
        flash("Please choose an Excel file to import.")
        return redirect(url_for("manage_emission_factors"))

    fn = secure_filename(f.filename)
    if not fn.lower().endswith((".xlsx", ".xlsm")):
        flash("Please upload a .xlsx or .xlsm file.")
        return redirect(url_for("manage_emission_factors"))

    tmp_dir = FRONTEND_UPLOAD_DIR
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"ef_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{fn}"
    f.save(tmp_path)

    # Validate structure: sheets with expected headers
    try:
        wb = load_workbook(tmp_path, read_only=True, data_only=True, keep_links=False)
        expected = _ef_expected_headers()
        bad = []
        for name in wb.sheetnames:
            ws = wb[name]
            first = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
            headers = [("" if v is None else str(v).strip()) for v in (first or [])]
            while headers and headers[-1] == "":
                headers.pop()
            missing = [h for h in expected if h not in headers]
            if missing:
                bad.append(f"{name}: missing {', '.join(missing)}")
        if bad:
            flash("Import failed. Workbook structure is not compatible: " + " | ".join(bad[:4]) + (" ..." if len(bad) > 4 else ""))
            try:
                tmp_path.unlink()
            except Exception:
                pass
            return redirect(url_for("manage_emission_factors"))
    except Exception as e:
        flash(f"Import failed: {e}")
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return redirect(url_for("manage_emission_factors"))
    
    except Exception as e:
        flash(f"Import failed {e}")
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return redirect(url_for("manage_emission_factors"))
    
    try:
        backup_dir = STAGE2_EF_XLSX.parent / "_ef_backups"
        backup_dir.mkdir(parents=True, exists_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if STAGE2_EF_XLSX.exists():
            shutil.copy(STAGE2_EF_XLSX, backup_dir / f"{STAGE2_EF_XLSX.stem}_before_import_{ts}{STAGE2_EF_XLSX.suffix}" )
        os.replace(str(tmp_path), str(STAGE2_EF_XLSX))
        _clear_ef_cache()
        flash("Emission factors imported successfully (sheet preserved).")
    except PermissionError:
        flash("Mapping workbook is open. Please close Excel file and try it again")
        try:
            tmp_path.unlink()
        except Exception:
            pass
        except Exception as e:
            flash(f"Import failed: {e}")
            try:
                tmp_path.unlink()
            except Exception:
                pass

    # Backup current and replace
    try:
        backup_dir = STAGE2_EF_XLSX.parent / "_ef_backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if STAGE2_EF_XLSX.exists():
            shutil.copy2(STAGE2_EF_XLSX, backup_dir / f"{STAGE2_EF_XLSX.stem}_before_import_{ts}{STAGE2_EF_XLSX.suffix}")
        os.replace(str(tmp_path), str(STAGE2_EF_XLSX))
        _clear_ef_cache()
        flash("Emission factors imported successfully (sheets preserved).")
    except PermissionError:
        flash("Mapping workbook is open/locked. Close Excel and try again.")
        try:
            tmp_path.unlink()
        except Exception:
            pass
    except Exception as e:
        flash(f"Import failed: {e}")
        try:
            tmp_path.unlink()
        except Exception:
            pass

    return redirect(url_for("manage_emission_factors"))

@app.route('/admin/emission_factors/export', methods=['GET'])
def export_emission_factors():
    if not current_user.is_authenticated or not getattr(current_user, "is_admin", False):
        return redirect(url_for("dashboard"))

    data = _load_stage2_emission_factors()
    all_rows: list[dict[str, object]] = list(data.get("rows") or [])

    search = request.args.get('search', '').strip()
    sheet_filter = request.args.get('sheet', '').strip()
    scope_filter = request.args.get('scope', '').strip()
    source_filter = request.args.get('ef_source', '').strip()

    def _matches(row: dict[str, object]) -> bool:
        if sheet_filter and str(row.get("sheet") or "") != sheet_filter:
            return False
        if scope_filter and str(row.get("scope") or "") != scope_filter:
            return False
        if source_filter and str(row.get("ef_source") or "") != source_filter:
            return False
        if search:
            hay = " ".join(
                str(row.get(k) or "")
                for k in (
                    "sheet",
                    "ef_name",
                    "ef_description",
                    "scope",
                    "ef_category",
                    "ef_id",
                    "ef_value",
                    "ef_unit",
                    "ef_source",
                    "Emission Factor Category",
                )
            ).lower()
            if search.lower() not in hay:
                return False
        return True

    rows = [r for r in all_rows if _matches(r)]
    rows.sort(key=lambda r: (str(r.get("sheet") or ""), str(r.get("ef_name") or ""), str(r.get("ef_id") or "")))

    
    def _safe_xlsx_sheet_name(name: str) -> str:
        invalid = set(':/\\?*[]')
        cleaned = "".join(ch for ch in (name or "Sheet") if ch not in invalid)
        if not cleaned:
            cleaned = "Sheet"
        return cleaned[:31]

    def _valid_sheet_name(name: str) -> bool:
        if not name:
            return False
        if len(name) > 31:
            return False
        if any(ch in name for ch in ':/\\?*[]'):
            return False
        return True
    # Group by source sheet (category)
    groups: dict[str, list[dict[str, object]]] = {}
    for r in rows:
        groups.setdefault(str(r.get("sheet") or "Sheet"), []).append(r)

    expected_headers = _ef_expected_headers()

    wb = Workbook()
    try:
        wb.remove(wb.active)
    except Exception:
        pass

    used_names: dict[str, int] = {}
    for raw_name in sorted(groups.keys(), key=lambda x: x.lower()):
        base = raw_name if _valid_sheet_name(raw_name) else _safe_xlsx_sheet_name(raw_name)
        n = used_names.get(base, 0) + 1
        used_names[base] = n
        sheet_name = base if n == 1 else _safe_xlsx_sheet_name(f"{base}_{n}")

        ws = wb.create_sheet(title=sheet_name)
        # Header row
        for j, h in enumerate(expected_headers, start=1):
            ws.cell(row=1, column=j).value = h

        # Rows
        for i, r in enumerate(groups[raw_name], start=2):
            for j, h in enumerate(expected_headers, start=1):
                v = r.get(h)
                # Keep ef_value numeric when possible
                if h == "ef_value":
                    try:
                        if v is None or str(v).strip() == "":
                            ws.cell(row=i, column=j).value = None
                        else:
                            ws.cell(row=i, column=j).value = float(v)
                    except Exception:
                        ws.cell(row=i, column=j).value = "" if v is None else str(v)
                else:
                    ws.cell(row=i, column=j).value = None if v is None or str(v).strip() == "" else str(v)

        # Light column sizing
        for col in range(1, len(expected_headers) + 1):
            ws.column_dimensions[chr(64 + col)].width = 22 if col in (1, 4, 5, 6, 7, 8) else 34

        ws.freeze_panes = "A2"

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    filename = f"CTS_Emission_factors_short_list_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

def _home_overview_context():
    """Shared context for Overview (home) and per-scope detail pages."""
    _ensure_db_tables()
    year = datetime.utcnow().year

    if bool(getattr(current_user, "is_admin", False)):
        all_rows = MappingRunSummary.query.order_by(MappingRunSummary.created_at.desc()).all()
        latest_by_company_sheet: dict[tuple[str, str], MappingRunSummary] = {}
        for r in all_rows:
            key = ((r.company_name or "").strip(), (r.sheet_name or "").strip().lower())
            if key in latest_by_company_sheet:
                continue
            if not key[0] or not key[1]:
                continue
            latest_by_company_sheet[key] = r

        company_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"scope1": 0.0, "scope2": 0.0, "scope3": 0.0, "total": 0.0})
        for (_c, _s), r in latest_by_company_sheet.items():
            c = (r.company_name or "").strip()
            v = float(getattr(r, "tco2e_total", 0.0) or 0.0)
            sc = int(getattr(r, "scope", 0) or 0)
            if sc == 1:
                company_totals[c]["scope1"] += v
            elif sc == 2:
                company_totals[c]["scope2"] += v
            elif sc == 3:
                company_totals[c]["scope3"] += v
            company_totals[c]["total"] += v

        rows = []
        g = {"scope1": 0.0, "scope2": 0.0, "scope3": 0.0, "total": 0.0}
        for c in sorted(company_totals.keys(), key=lambda x: x.lower()):
            t = company_totals[c]
            rows.append({"company": c, **{k: round(t[k], 3) for k in ("scope1", "scope2", "scope3", "total")}})
            for k in g:
                g[k] += float(t[k] or 0.0)

        return {
            "year": year,
            "is_admin": True,
            "totals": {k: round(g[k], 3) for k in g},
            "company_rows": rows,
            "breakdown": [],
        }

    keys = _company_candidate_keys(getattr(current_user, "company_name", "") or "")
    latest = _latest_sheet_totals_for_company(keys)
    if latest and getattr(latest[0], "created_at", None):
        year = latest[0].created_at.year
    scope1 = sum(float(r.tco2e_total or 0.0) for r in latest if int(r.scope or 0) == 1)
    scope2 = sum(float(r.tco2e_total or 0.0) for r in latest if int(r.scope or 0) == 2)
    scope3 = sum(float(r.tco2e_total or 0.0) for r in latest if int(r.scope or 0) == 3)
    total_emission = scope1 + scope2 + scope3
    breakdown = [
        {
            "sheet": r.sheet_name,
            "scope": r.scope,
            "tco2e": round(float(r.tco2e_total or 0.0), 3),
            "updated_at": r.created_at.strftime("%Y-%m-%d %H:%M") if r.created_at else "",
        }
        for r in latest
    ]

    return {
        "year": year,
        "is_admin": False,
        "totals": {
            "total": round(total_emission, 3),
            "scope1": round(scope1, 3),
            "scope2": round(scope2, 3),
            "scope3": round(scope3, 3),
        },
        "breakdown": breakdown,
        "company_rows": [],
    }


@app.route('/home')
@login_required
def home():
    ctx = _home_overview_context()
    return render_template("home.html", **ctx)


@app.route("/scope1")
@login_required
def scope1_detail():
    ctx = _home_overview_context()
    return render_template(
        "scope_detail.html",
        scope_num=1,
        scope_title="Scope 1 — Direct Emissions",
        scope_subtitle="Fuel Usage (Diesel on site, fuels from owned and leased vehicles)",
        **ctx,
    )


@app.route("/scope2")
@login_required
def scope2_detail():
    ctx = _home_overview_context()
    return render_template(
        "scope_detail.html",
        scope_num=2,
        scope_title="Scope 2 - Indirect Energy Emissions",
        scope_subtitle="Electricity and purchased energy with time and category views. Electricity for facilities. District heating for some offices.",
        **ctx,
    )


@app.route("/scope3")
@login_required
def scope3_detail():
    ctx = _home_overview_context()
    return render_template(
        "scope_detail.html",
        scope_num=3,
        scope_title="Scope 3 - Value Chain Emissions",
        scope_subtitle="Purchased goods, transport, travel, waste, and other upstream/downstream categories.",
        **ctx,
    )

    
@app.route('/Emission-factors')
@login_required
def emission_facor():
    return redirect(url_for('manage_emission_factors'))


@app.route('/carbon-accounting')
@login_required
def carbon_accounting():
    _ensure_db_tables()
    year = datetime.utcnow().year

    keys = _company_candidate_keys(getattr(current_user, "company_name", "") or "")
    latest = _latest_sheet_totals_for_company(keys)
    if latest and getattr(latest[0], "created_at", None):
        year = latest[0].created_at.year

    scope1 = sum(float(r.tco2e_total or 0.0) for r in latest if int(r.scope or 0) == 1)
    scope2 = sum(float(r.tco2e_total or 0.0) for r in latest if int(r.scope or 0) == 2)
    scope3 = sum(float(r.tco2e_total or 0.0) for r in latest if int(r.scope or 0) == 3)
    total_emission = scope1 + scope2 + scope3

    by_sheet = [
        {
            "sheet": r.sheet_name,
            "scope": r.scope,
            "tco2e": round(float(r.tco2e_total or 0.0), 3),
            "updated_at": r.created_at.strftime("%Y-%m-%d %H:%M") if r.created_at else "",
        }
        for r in latest
    ]

    return render_template(
        "carbon_accounting.html",
        year=year,
        totals={
            "total": round(total_emission, 3),
            "scope1": round(scope1, 3),
            "scope2": round(scope2, 3),
            "scope3": round(scope3, 3),
        },
        by_sheet=by_sheet,
    )


@app.route("/search", methods=["GET"])
@login_required
def search_page():
    query = str(request.args.get("q", "") or "").strip()
    groups = search_service.search_all(
        query,
        CompanyModel=Company,
        UserModel=User,
        EmissionFactorModel=EmissionFactor,
        stage1_input_dir=STAGE1_INPUT_DIR,
        stage2_output_dir=STAGE2_OUTPUT_DIR,
        run_logs_dir=APP_DIR / "run_logs",
    )
    total_results = sum(len(rows) for rows in groups.values())
    return render_template(
        "search_results.html",
        user=current_user,
        query=query,
        results=groups,
        total_results=total_results,
    )


@app.route("/api/notifications/recent", methods=["GET"])
@login_required
def api_notifications_recent():
    _ensure_db_tables()
    rows = notification_service.recent_notifications(
        Notification,
        user_id=current_user.id,
        limit=_parse_int_arg("limit", 8, minimum=1, maximum=30),
    )
    unread = notification_service.unread_count(Notification, user_id=current_user.id)
    return jsonify({"notifications": [_notification_payload(row) for row in rows], "unread_count": unread})


@app.route("/api/notifications/unread_count", methods=["GET"])
@login_required
def api_notifications_unread_count():
    _ensure_db_tables()
    return jsonify({"unread_count": notification_service.unread_count(Notification, user_id=current_user.id)})


@app.route("/api/notifications/mark-read", methods=["POST"])
@login_required
def api_notifications_mark_read():
    _ensure_db_tables()
    payload = request.get_json(silent=True) or {}
    notification_id = payload.get("notification_id")
    if notification_id in (None, "", "all"):
        updated = notification_service.mark_all_read(db.session, Notification, user_id=current_user.id)
    else:
        updated = 1 if notification_service.mark_one_read(
            db.session,
            Notification,
            user_id=current_user.id,
            notification_id=int(notification_id),
        ) else 0
    db.session.commit()
    return jsonify(
        {
            "ok": True,
            "updated": int(updated),
            "unread_count": notification_service.unread_count(Notification, user_id=current_user.id),
        }
    )


@app.route("/api/messages/contacts", methods=["GET"])
@login_required
def api_message_contacts():
    _ensure_db_tables()
    contacts = messaging_service.available_contacts(User, current_user, limit=50)
    return jsonify({"contacts": [_contact_payload(row) for row in contacts]})


@app.route("/api/messages/conversations", methods=["GET"])
@login_required
def api_message_conversations():
    _ensure_db_tables()
    conversations = messaging_service.list_conversations(Message, User, current_user, limit=30)
    payload = []
    for item in conversations:
        payload.append(
            {
                "thread_id": item["thread_id"],
                "other_user": _contact_payload(item["other_user"]),
                "last_message": _message_payload(item["last_message"], viewer_id=current_user.id),
                "unread_count": int(item["unread_count"]),
            }
        )
    return jsonify(
        {
            "conversations": payload,
            "unread_count": messaging_service.unread_count(Message, current_user),
        }
    )


@app.route("/api/messages/thread", methods=["GET"])
@login_required
def api_message_thread():
    _ensure_db_tables()
    other_user_id = _parse_int_arg("user_id", None, minimum=1)
    if not other_user_id:
        return jsonify({"error": "user_id is required"}), 400
    try:
        other_user, rows = messaging_service.fetch_thread_messages(
            Message,
            User,
            current_user,
            other_user_id=other_user_id,
            limit=250,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 403
    return jsonify(
        {
            "contact": _contact_payload(other_user),
            "messages": [_message_payload(row, viewer_id=current_user.id) for row in rows],
        }
    )


@app.route("/api/messages/send", methods=["POST"])
@login_required
def api_message_send():
    _ensure_db_tables()
    payload = request.get_json(silent=True) or {}
    receiver_id = payload.get("receiver_id")
    if receiver_id in (None, ""):
        return jsonify({"error": "receiver_id is required"}), 400
    try:
        row = messaging_service.send_message(
            db.session,
            Message,
            User,
            current_user,
            receiver_id=int(receiver_id),
            body=str(payload.get("message", "") or ""),
        )
        db.session.commit()
        _create_user_notification(
            row.receiver_id,
            title=f"New message from {_user_display_name(current_user)}",
            message=row.message[:180],
            notification_type="message",
            link=None,
        )
    except ValueError as exc:
        db.session.rollback()
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Message send failed: {exc}"}), 500
    return jsonify(
        {
            "ok": True,
            "message": _message_payload(row, viewer_id=current_user.id),
            "unread_count": messaging_service.unread_count(Message, current_user),
        }
    )


@app.route("/api/messages/mark-read", methods=["POST"])
@login_required
def api_message_mark_read():
    _ensure_db_tables()
    payload = request.get_json(silent=True) or {}
    other_user_id = payload.get("user_id")
    if other_user_id in (None, ""):
        return jsonify({"error": "user_id is required"}), 400
    updated = messaging_service.mark_thread_read(
        db.session,
        Message,
        User,
        current_user,
        other_user_id=int(other_user_id),
    )
    db.session.commit()
    return jsonify(
        {
            "ok": True,
            "updated": int(updated),
            "unread_count": messaging_service.unread_count(Message, current_user),
        }
    )


@app.route("/api/messages/unread_count", methods=["GET"])
@login_required
def api_message_unread_count():
    _ensure_db_tables()
    return jsonify({"unread_count": messaging_service.unread_count(Message, current_user)})


@app.route('/admin_data', methods=['GET'])
@login_required
def admin_data():
    rows = MappingRunSummary.query.order_by(MappingRunSummary.created_at.desc()).all()
    data = [
        {
            "company_name": row.company_name,
            "created_at": row.created_at.strftime("%Y-%m") if row.created_at else "",
            "mapped_categories_count": int(getattr(row, "mapped_categories_count", 0) or 0),
            "total_categories": int(getattr(row, "total_categories", 0) or 0),
            "coverage_pct": float(getattr(row, "coverage_pct", 0) or 0),
        }
        for row in rows
    ]
    return jsonify({"data": data})


with app.app_context():
    _ensure_db_tables()


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5000) 
