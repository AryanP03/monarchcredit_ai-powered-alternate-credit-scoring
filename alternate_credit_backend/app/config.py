from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------
# BASE PATHS
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
APP_DIR = BASE_DIR / "app"
MODELS_DIR = BASE_DIR / "models"
ARTIFACTS_DIR = BASE_DIR / "artifacts"
REPORTS_DIR = BASE_DIR / "reports"
GENERATED_PDFS_DIR = BASE_DIR / "generated_pdfs"

GENERATED_PDFS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# APP / ENV
# ---------------------------------------------------------
APP_NAME = os.getenv("APP_NAME", "Alternate Credit Scoring API")
APP_ENV = os.getenv("APP_ENV", "development")
DEBUG = os.getenv("DEBUG", "true").lower() == "true"


# ---------------------------------------------------------
# DATABASE
# ---------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "auditdb")

# Separate DB name for raw SHAP storage
RAW_SHAP_DB_NAME = os.getenv("RAW_SHAP_DB_NAME", "RAW_SHAP_VALUES")


def get_mysql_config() -> dict:
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_NAME,
    }


def get_mysql_config_for_database(database_name: str) -> dict:
    cfg = get_mysql_config().copy()
    cfg["database"] = database_name
    return cfg


# ---------------------------------------------------------
# KAFKA
# ---------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_BOOTSTRAP_SERVERS = KAFKA_BROKER


# ---------------------------------------------------------
# COMMON TOPICS
# ---------------------------------------------------------
CUST_INFO = "customer_info"
RAW_DATA_INGESTED = "raw_data_ingested"
PROCESSED_DATA = "processed_data"
FEATURES = "features"
MODEL_OUTPUT = "model_output"
EXPLAINABILITY_OUTPUT = "explainability_output"
FINAL_DECISION = "final_decision"
MODEL_MONITORING = "model_monitoring"


# ---------------------------------------------------------
# MSME TOPICS
# ---------------------------------------------------------
RAW_MSME_DATA = "raw_msme_data_injested"
MSME_INFO = "msme_info"
FEATURES_MSME = "features_msme"
MODEL_OUTPUT_MSME = "model_output_msme"
EXPLAINABILITY_OUTPUT_MSME = "explainability_output_msme"
MANAGER_PDF_MSME = "manager_pdf_msme"


# ---------------------------------------------------------
# MODEL PATHS
# ---------------------------------------------------------
MSME_MODEL_PATH = os.getenv(
    "MSME_MODEL_PATH",
    str(MODELS_DIR / "MSME" / "msme_RF_model_B.pkl"),
)

INDIVIDUAL_MODEL_PATH = os.getenv(
    "INDIVIDUAL_MODEL_PATH",
    str(MODELS_DIR / "individual" / "individual_model.pkl"),
)


# ---------------------------------------------------------
# INDIVIDUAL ARTIFACT PATHS
# ---------------------------------------------------------
INDIVIDUAL_ARTIFACT_DIR = Path(
    os.getenv("INDIVIDUAL_ARTIFACT_DIR", str(ARTIFACTS_DIR / "individuals"))
)


# ---------------------------------------------------------
# SERVICE DEFAULTS
# ---------------------------------------------------------
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Asia/Kolkata")
DEFAULT_RISK_THRESHOLD_MSME = float(os.getenv("DEFAULT_RISK_THRESHOLD_MSME", "0.14"))


# ---------------------------------------------------------
# CORS
# ---------------------------------------------------------
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173")


# ---------------------------------------------------------
# OPTIONAL HELPERS
# ---------------------------------------------------------
def as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}