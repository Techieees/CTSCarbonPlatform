from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


# Load environment variables once from the repository root so every script
# resolves the same data location both locally and on the server.
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")
CONFIG_DIR = PROJECT_ROOT / "config"
API_CREDENTIALS_ENV_PATH = CONFIG_DIR / "api_credentials.env"
load_dotenv(API_CREDENTIALS_ENV_PATH)


def _resolve_path(raw_value: str) -> Path:
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _resolve_env_path(env_name: str, default_value: str) -> Path:
    return _resolve_path(os.getenv(env_name, default_value))


DEFAULT_DATA_DIR = "./data"
DATA_DIR = _resolve_env_path("DATA_DIR", DEFAULT_DATA_DIR)


def data_path(*parts: str) -> Path:
    """Build data paths from the centralized DATA_DIR setting."""
    return Path(os.path.join(str(DATA_DIR), *parts))


def pick_first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production")

# Outbound email (password reset). Leave MAIL_SERVER empty to disable sending (link logged only in debug).
MAIL_SERVER = os.getenv("MAIL_SERVER", "").strip()
MAIL_PORT = int(os.getenv("MAIL_PORT", "587"))
MAIL_USERNAME = os.getenv("MAIL_USERNAME", "").strip()
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "").strip()
MAIL_DEFAULT_SENDER = os.getenv("MAIL_DEFAULT_SENDER", "noreply@cts-nordics.com").strip()
# Public site URL for reset links (no trailing slash), e.g. https://ctscarbonplatform.com
PUBLIC_APP_BASE_URL = os.getenv("PUBLIC_APP_BASE_URL", "http://127.0.0.1:5000").rstrip("/")
CCC_API_BASE_URL = os.getenv("CCC_API_BASE_URL", "").strip()
CCC_USERNAME = os.getenv("CCC_USERNAME", os.getenv("CCC_API_EMAIL", "")).strip()
CCC_PASSWORD = os.getenv("CCC_PASSWORD", os.getenv("CCC_API_PASSWORD", "")).strip()
CCC_API_PAGE_SIZE = int(os.getenv("CCC_API_PAGE_SIZE", "100") or "100")
CCC_SHEET_MAPPING_PATH = CONFIG_DIR / "ccc_sheet_mapping.json"
CCC_GET_ENDPOINTS_PATH = CONFIG_DIR / "ccc_get_endpoints.json"

# Frontend runtime paths
FRONTEND_DIR = PROJECT_ROOT / "frontend"
FRONTEND_INSTANCE_DIR = FRONTEND_DIR / "instance"
FRONTEND_DB_PATH = FRONTEND_INSTANCE_DIR / "ghg_data.db"
FRONTEND_UPLOAD_DIR = FRONTEND_INSTANCE_DIR / "uploads"

# Shared storage paths
STORAGE_ROOT = PROJECT_ROOT / "storage"
PIPELINE_TEMPLATE_DIR = STORAGE_ROOT / "pipeline_templates"
PIPELINE_RUNS_DIR = STORAGE_ROOT / "pipeline_runs_web"

# Stage 1 data paths
STAGE1_INPUT_DIR = data_path("stage1_preprocess", "input")
STAGE1_OUTPUT_DIR = data_path("stage1_preprocess", "output")
STAGE1_INPUT_BACKUP_DIR = STAGE1_INPUT_DIR / "_schema_backup"
ENGINE_STAGE1_PREPROCESS_DIR = PROJECT_ROOT / "engine" / "stage1_preprocess"
ENGINE_STAGE1_DATAS_DIR = ENGINE_STAGE1_PREPROCESS_DIR / "Datas"
ENGINE_STAGE1_KLARAKARBON_ALL_TOGETHER_DIR = ENGINE_STAGE1_DATAS_DIR / "All Together"
ENGINE_STAGE1_KLARAKARBON_OUTPUT_WORK_DIR = ENGINE_STAGE1_DATAS_DIR / "Output_Klarakarbon"
STAGE1_EXCHANGE_RATE_WORKBOOK = data_path(
    "stage1_preprocess",
    "exchange_rates",
    "Exchange_Rates_European_Central_Bank.xlsx",
)
STAGE1_KLARAKARBON_INPUT_DIR = data_path("stage1_preprocess", "klarakarbon", "input")
STAGE1_KLARAKARBON_OUTPUT_DIR = data_path("stage1_preprocess", "klarakarbon", "output")
STAGE1_BUSINESS_TRAVEL_DIR = data_path("stage1_preprocess", "business_travel_mgmt")

# Stage 2 data paths
STAGE2_MAPPING_DIR = PROJECT_ROOT / "engine" / "stage2_mapping"
STAGE2_INPUT_DIR = data_path("stage2_mapping", "input")
STAGE2_OUTPUT_DIR = data_path("stage2_mapping", "output")
STAGE2_MANUAL_MAPPING_DIR = data_path("stage2_mapping", "manual_mappings")
STAGE2_TRAVEL_DIR = data_path("stage2_mapping", "travel")
STAGE2_HEADCOUNT_CSV = data_path("stage2_mapping", "input", "headcount_info.csv")
STAGE2_EF_XLSX = STAGE2_MAPPING_DIR / "CTS_Emission_factors_short_list.xlsx"
STAGE2_IMPORT_EMISSION_FACTORS_XLSX = pick_first_existing(
    data_path("frontend", "Emission Factor sPosition Green 2025.xlsx"),
    STAGE2_EF_XLSX,
)


for directory in [
    DATA_DIR,
    CONFIG_DIR,
    FRONTEND_INSTANCE_DIR,
    FRONTEND_UPLOAD_DIR,
    PIPELINE_TEMPLATE_DIR,
    PIPELINE_RUNS_DIR,
    STAGE1_INPUT_DIR,
    STAGE1_OUTPUT_DIR,
    STAGE1_INPUT_BACKUP_DIR,
    ENGINE_STAGE1_DATAS_DIR,
    ENGINE_STAGE1_KLARAKARBON_ALL_TOGETHER_DIR,
    ENGINE_STAGE1_KLARAKARBON_OUTPUT_WORK_DIR,
    STAGE1_KLARAKARBON_INPUT_DIR,
    STAGE1_KLARAKARBON_OUTPUT_DIR,
    STAGE1_BUSINESS_TRAVEL_DIR,
    STAGE2_INPUT_DIR,
    STAGE2_OUTPUT_DIR,
    STAGE2_MANUAL_MAPPING_DIR,
    STAGE2_TRAVEL_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)
