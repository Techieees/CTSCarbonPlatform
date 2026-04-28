import os
from pathlib import Path
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PROJECT_ROOT = BASE_DIR.parent


load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BASE_DIR / "api_credentials.env")


# -----------------
# FRONTEND
# -----------------

FRONTEND_DIR = PROJECT_ROOT / "frontend"
FRONTEND_INSTANCE_DIR = FRONTEND_DIR / "instance"
FRONTEND_UPLOAD_DIR = FRONTEND_DIR / "uploads"
FRONTEND_DB_PATH = FRONTEND_INSTANCE_DIR / "ghg_data.db"


# -----------------
# API
# -----------------

CCC_API_BASE_URL = os.getenv("CCC_API_BASE_URL")
CCC_USERNAME = os.getenv("CCC_USERNAME")
CCC_PASSWORD = os.getenv("CCC_PASSWORD")
CCC_API_PAGE_SIZE = int(os.getenv("CCC_API_PAGE_SIZE", 200))

CCC_GET_ENDPOINTS_PATH = BASE_DIR / "ccc_get_endpoints.json"
CCC_SHEET_MAPPING_PATH = BASE_DIR / "ccc_sheet_mapping.json"


# -----------------
# APP
# -----------------

SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")

PUBLIC_APP_BASE_URL = os.getenv(
    "PUBLIC_APP_BASE_URL",
    "https://ctscarbonplatform.com"
)

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()


# -----------------
# PIPELINE
# -----------------

PIPELINE_TEMPLATE_DIR = PROJECT_ROOT / "engine"
PIPELINE_RUNS_DIR = PROJECT_ROOT / "engine"


# -----------------
# STAGE PATHS
# -----------------

STAGE1_INPUT_DIR = PROJECT_ROOT / "data"
STAGE1_INPUT_BACKUP_DIR = PROJECT_ROOT / "data"
STAGE1_KLARAKARBON_OUTPUT_DIR = PROJECT_ROOT / "data"

STAGE2_MAPPING_DIR = PROJECT_ROOT / "engine"
STAGE2_OUTPUT_DIR = PROJECT_ROOT / "data"
STAGE2_TRAVEL_DIR = PROJECT_ROOT / "data"

STAGE2_EF_XLSX = (
    PROJECT_ROOT
    / "engine"
    / "stage2_mapping"
    / "CTS_Emission_factors_short_list.xlsx"
)


# -----------------
# ENGINE PATHS (required by preprocess_jobs)
# -----------------

ENGINE_STAGE1_INPUT_DIR = PROJECT_ROOT / "data"
ENGINE_STAGE1_OUTPUT_DIR = PROJECT_ROOT / "data"

ENGINE_STAGE1_KLARAKARBON_ALL_TOGETHER_DIR = PROJECT_ROOT / "data"
ENGINE_STAGE1_KLARAKARBON_OUTPUT_WORK_DIR = PROJECT_ROOT / "data"

ENGINE_STAGE2_INPUT_DIR = PROJECT_ROOT / "data"
ENGINE_STAGE2_OUTPUT_DIR = PROJECT_ROOT / "data"
# -----------------
# MAIL SETTINGS
# -----------------

MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.office365.com")
MAIL_PORT = int(os.getenv("MAIL_PORT", 587))
MAIL_USERNAME = os.getenv("MAIL_USERNAME", "")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "")
MAIL_DEFAULT_SENDER = MAIL_USERNAME
