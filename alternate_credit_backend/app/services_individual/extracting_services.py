from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.messaging.consumers import JSONKafkaConsumer
from app.messaging.producer import JSONKafkaProducer

from app.config import INDIVIDUAL_ARTIFACT_DIR

# ---------------------------------------------------------
# Kafka topics
# ---------------------------------------------------------
RAW_DATA_INGESTED = "raw_data_ingested"
FEATURES = "features"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------
# Artifact paths
# ---------------------------------------------------------
DEFAULT_ARTIFACT_DIRS = [INDIVIDUAL_ARTIFACT_DIR]

# ---------------------------------------------------------
# Education mapping
# The CLI/testing.py uses numeric education internally.
# Your ingestion payload sends a string like "higher_education".
# ---------------------------------------------------------
EDUCATION_STRING_TO_CODE = {
    "secondary / secondary special": 0,
    "secondary_special": 0,
    "secondary": 0,
    "higher education": 1,
    "higher_education": 1,
    "incomplete higher": 2,
    "incomplete_higher": 2,
    "lower secondary": 3,
    "lower_secondary": 3,
    "academic degree": 4,
    "academic_degree": 4,
}

# ---------------------------------------------------------
# Columns whose engineered values should remain median-filled
# ---------------------------------------------------------
MEDIAN_OVERRIDE = {
    "Social_Default_Density",
    "Social_Contagion_Velocity",
    "Regional_Purchasing_Power",
    "MinTrap_Score_Tension",
    "Debt_Spiral_Index",
    "Bureau_Recent_Stress",
    "Active_Debt_Accumulation",
    "Cash_Crunch_Signal",
    "POS_Install_Dual_Stress",
    "Loan_to_Payment_History",
    "AMT_CREDIT_vs_history",
}


class IndividualExtractionService:
    def __init__(self, artifact_dir: Optional[str] = None):
        self.producer = JSONKafkaProducer()
        self.consumer = JSONKafkaConsumer(
    RAW_DATA_INGESTED,
    group_id="individual_extraction_service_group"
)
        self.artifact_dir = self._find_artifacts(artifact_dir)
        self.artifacts = self._load_artifacts(self.artifact_dir)

    # =====================================================
    # Artifact loading
    # =====================================================
    def _find_artifacts(self, override: Optional[str] = None) -> Path:
        candidates = ([Path(override)] if override else []) + DEFAULT_ARTIFACT_DIRS
        for p in candidates:
            if p.exists() and (p / "feature_columns.csv").exists():
                return p
        raise FileNotFoundError(
            "Artifacts not found. Expected feature_columns.csv in:\n"
            + "\n".join(f"  {p}" for p in candidates)
        )

    def _load_artifacts(self, artifact_dir: Path) -> Dict[str, Any]:
        logger.info("Loading extraction artifacts from %s", artifact_dir)

        feat_df = pd.read_csv(artifact_dir / "feature_columns.csv")
        feature_cols = feat_df["feature"].tolist()
        feat_idx = {col: i for i, col in enumerate(feature_cols)}

        mp = artifact_dir / "imputer_medians.npy"
        medians = np.load(str(mp)) if mp.exists() else None

        stats_path = artifact_dir / "training_stats.json"
        if stats_path.exists():
            with open(stats_path, "r", encoding="utf-8") as f:
                tstats = json.load(f)
        else:
            tstats = {
                "income_p30": 135000.0,
                "income_q25": 112500.0,
                "ext1_median": 0.502,
                "ext2_median": 0.515,
                "ext3_median": 0.511,
            }

        logger.info("Loaded %d model feature columns", len(feature_cols))

        return {
            "feature_cols": feature_cols,
            "feat_idx": feat_idx,
            "medians": medians,
            "tstats": tstats,
        }

    # =====================================================
    # Helpers
    # =====================================================
    @staticmethod
    def safe_div(a: float, b: float, fill: float = 0.0) -> float:
        b = b if b != 0 else np.nan
        result = a / b if not (np.isnan(b) if isinstance(b, float) else False) else fill
        return fill if (np.isnan(result) or np.isinf(result)) else float(result)

    @staticmethod
    def _coerce_float(value: Any, default: float = 0.0) -> float:
        if value is None or value == "":
            return default
        try:
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _coerce_int(value: Any, default: int = 0) -> int:
        if value is None or value == "":
            return default
        try:
            return int(value)
        except Exception:
            return default

    def _normalize_education(self, value: Any) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if value is None:
            return 0
        key = str(value).strip().lower()
        return EDUCATION_STRING_TO_CODE.get(key, 0)

    def _preprocess_raw_payload(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalizes the incoming Kafka raw payload to match the assumptions
        used inside testing.py engineer(...).
        """
        r = dict(raw)

        # Education arrives as string in your Kafka payload; engineer() expects numeric
        r["NAME_EDUCATION_TYPE"] = self._normalize_education(r.get("NAME_EDUCATION_TYPE"))

        # If unemployed sentinel was used upstream, preserve fabrication flag
        days_employed = self._coerce_int(r.get("DAYS_EMPLOYED"))
        r["EMP_FABRICATION"] = 1 if days_employed == 365243 else 0

        # testing.py engineer(...) expects REGION_RATING_CLIENT_W_CITY
        # Your raw topic does not currently include it.
        # Best-effort reconstruction:
        if "REGION_RATING_CLIENT_W_CITY" not in r:
            rr = self._coerce_int(r.get("REGION_RATING_CLIENT"), 2)
            city_diff = self._coerce_int(r.get("CITY_RATING_DIFFERS_FROM_REGION"), 0)
            r["REGION_RATING_CLIENT_W_CITY"] = rr + 1 if city_diff else rr

        return r

    def _validate_raw_payload(self, raw: Dict[str, Any]) -> None:
        required = [
            "application_id",
            "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE",
            "AMT_INCOME_TOTAL", "AGE_YEARS", "NAME_EDUCATION_TYPE",
            "FLAG_OWN_CAR", "FLAG_OWN_REALTY", "CNT_CHILDREN", "CNT_FAM_MEMBERS",
            "ID_DOCUMENT_AGE_YEARS", "YEARS_AT_CURRENT_ADDRESS",
            "REGION_RATING_CLIENT", "REG_CITY_NOT_LIVE_CITY",
            "REG_CITY_NOT_WORK_CITY", "LIVE_CITY_NOT_WORK_CITY",
            "FLAG_EMP_PHONE", "FLAG_PHONE", "FLAG_MOBIL", "FLAG_CONT_MOBILE",
            "FLAG_EMAIL", "MONTHS_SINCE_LAST_PHONE_CHANGE", "TOTAL_DOCS_SUBMITTED",
            "AMT_REQ_CREDIT_BUREAU_YEAR", "AMT_REQ_CREDIT_BUREAU_MON",
            "PREV_APPROVED_COUNT", "PREV_REFUSED_COUNT", "INST_LATE_COUNT",
            "INST_RECENT_LATE", "INST_RATIO_MIN", "INST_RATIO_MEAN",
            "BUREAU_CREDIT_COUNT", "BUREAU_WORST_STATUS", "BUREAU_CLOSED_COUNT",
            "BUREAU_OVERDUE_SUM", "BUREAU_TOTAL_DEBT", "CC_DPD_MAX",
            "DAYS_BIRTH", "DAYS_EMPLOYED", "DAYS_ID_PUBLISH",
            "DAYS_REGISTRATION", "DAYS_LAST_PHONE_CHANGE",
        ]
        missing = [k for k in required if k not in raw]
        if missing:
            raise ValueError(f"Missing required raw fields: {missing}")

    # =====================================================
    # Feature engineering copied from testing.py logic
    # =====================================================
    def engineer(self, r: Dict[str, Any], tstats: Dict[str, Any]) -> Dict[str, Any]:
        f: Dict[str, Any] = {}

        for col in [
            "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE", "AMT_INCOME_TOTAL",
            "DAYS_BIRTH", "DAYS_EMPLOYED", "DAYS_ID_PUBLISH", "DAYS_REGISTRATION",
            "DAYS_LAST_PHONE_CHANGE", "CNT_CHILDREN", "CNT_FAM_MEMBERS",
            "NAME_EDUCATION_TYPE", "FLAG_OWN_CAR", "FLAG_OWN_REALTY",
            "FLAG_EMP_PHONE", "FLAG_PHONE", "FLAG_MOBIL", "FLAG_CONT_MOBILE",
            "FLAG_EMAIL", "REGION_RATING_CLIENT", "REGION_RATING_CLIENT_W_CITY",
            "REG_CITY_NOT_LIVE_CITY", "REG_CITY_NOT_WORK_CITY",
            "LIVE_CITY_NOT_WORK_CITY", "AMT_REQ_CREDIT_BUREAU_YEAR",
            "AMT_REQ_CREDIT_BUREAU_MON"
        ]:
            f[col] = r.get(col, 0)

        ext1_raw = r.get("EXT_SOURCE_1", np.nan)
        ext2_raw = r.get("EXT_SOURCE_2", np.nan)
        ext3_raw = r.get("EXT_SOURCE_3", np.nan)

        ext1 = ext1_raw if not pd.isna(ext1_raw) else tstats["ext1_median"]
        ext2 = ext2_raw if not pd.isna(ext2_raw) else tstats["ext2_median"]
        ext3 = ext3_raw if not pd.isna(ext3_raw) else tstats["ext3_median"]

        f["EXT_SOURCE_1_was_missing"] = int(pd.isna(ext1_raw))
        f["EXT_SOURCE_2_was_missing"] = int(pd.isna(ext2_raw))
        f["EXT_SOURCE_3_was_missing"] = int(pd.isna(ext3_raw))
        f["EXT_SOURCE_1"] = ext1
        f["EXT_SOURCE_2"] = ext2
        f["EXT_SOURCE_3"] = ext3

        f["EXT_SOURCE_MEAN"] = (ext1 + ext2 + ext3) / 3.0
        f["EXT_SOURCE_WEIGHTED"] = 0.5 * ext2 + 0.3 * ext3 + 0.2 * ext1
        f["EXT_SOURCE_MIN"] = min(ext1, ext2, ext3)
        f["EXT_SOURCE_MISSING_COUNT"] = (
            f["EXT_SOURCE_1_was_missing"]
            + f["EXT_SOURCE_2_was_missing"]
            + f["EXT_SOURCE_3_was_missing"]
        )
        ext_w = f["EXT_SOURCE_WEIGHTED"]

        f["Weighted_Asset_Score"] = r["FLAG_OWN_CAR"] + r["FLAG_OWN_REALTY"]
        income_p30 = tstats["income_p30"]
        f["Ghost_Wealth_Paradox"] = int(
            (f["Weighted_Asset_Score"] > 0) and (r["AMT_INCOME_TOTAL"] < income_p30)
        )
        f["Asset_Collateral_Coverage"] = self.safe_div(r["AMT_GOODS_PRICE"], r["AMT_CREDIT"], 1.0)

        days_emp = r["DAYS_EMPLOYED"]
        days_birth = r["DAYS_BIRTH"]
        emp_valid = (days_emp < 0) and not r.get("EMP_FABRICATION", 0)

        f["Years_Employed"] = abs(days_emp) / 365.0 if emp_valid else 0.0
        f["Age_at_Application"] = abs(days_birth) / 365.0
        f["Career_Age_Synergy"] = self.safe_div(f["Years_Employed"], f["Age_at_Application"], 0.0)
        f["Employment_Fabrication_Catch"] = int(r.get("EMP_FABRICATION", 0))
        f["Professional_Traceability_Gap"] = int(
            (r["FLAG_EMP_PHONE"] == 0)
            and (not r.get("EMP_FABRICATION", 0))
            and emp_valid
        )
        f["Daily_Revenue_Efficiency"] = self.safe_div(
            r["AMT_INCOME_TOTAL"], abs(days_emp) + 1, 0.0
        )

        f["Identity_Maturity_Score"] = abs(r["DAYS_ID_PUBLISH"]) / 365.0
        f["Residency_Stability"] = abs(r["DAYS_REGISTRATION"]) / 365.0
        f["Synthetic_Identity_Proxy"] = int(
            abs(r["DAYS_ID_PUBLISH"] - r["DAYS_REGISTRATION"]) < 30
        )

        amt_cr = r["AMT_CREDIT"]
        amt_inc = r["AMT_INCOME_TOTAL"]
        amt_ann = r["AMT_ANNUITY"]
        cnt_fam = max(r["CNT_FAM_MEMBERS"], 1)
        cnt_ch = r["CNT_CHILDREN"]

        f["Total_Debt_to_Income"] = self.safe_div(amt_cr, amt_inc, 0.0)
        f["Household_Survival_Margin"] = amt_inc - amt_ann
        f["Survival_Per_Capita"] = self.safe_div(f["Household_Survival_Margin"], cnt_fam, 0.0)
        f["Social_Debt_Load"] = cnt_ch * self.safe_div(amt_ann, amt_inc, 0.0)
        f["Income_Plausibility_Score"] = self.safe_div(amt_inc, amt_cr, 0.0)
        f["Productive_Debt_Ratio"] = self.safe_div(r["AMT_GOODS_PRICE"], amt_cr, 1.0)
        f["Desperation_Leap"] = self.safe_div(amt_cr, f["Years_Employed"] + 1, amt_cr)

        f["Borderline_Address_Proxy"] = abs(
            r["REGION_RATING_CLIENT"] - r["REGION_RATING_CLIENT_W_CITY"]
        )
        f["Address_Integrity_Flag"] = (
            r["REG_CITY_NOT_LIVE_CITY"]
            + r["REG_CITY_NOT_WORK_CITY"]
            + r["LIVE_CITY_NOT_WORK_CITY"]
        )

        f["Digital_Trust_Score"] = r["FLAG_EMAIL"] + r["FLAG_PHONE"] + r["FLAG_EMP_PHONE"]
        f["Total_Documents_Submitted"] = r["TOTAL_DOCS_SUBMITTED"]
        f["Contact_Change_Proximity"] = int(r["DAYS_LAST_PHONE_CHANGE"] > -30)
        f["Burner_Phone_Match"] = int(
            (r["FLAG_MOBIL"] == 1) and (r["FLAG_CONT_MOBILE"] == 0)
        )
        f["Transient_Lifestyle_Index"] = self.safe_div(
            abs(r["DAYS_LAST_PHONE_CHANGE"]), abs(days_birth) + 1, 0.0
        )
        yrs_employed = f["Years_Employed"]
        f["Panic_Mismatch"] = self.safe_div(
            r["AMT_REQ_CREDIT_BUREAU_YEAR"], yrs_employed + 1, 0.0
        )

        f["True_Unbanked_Indicator"] = int(r["AMT_REQ_CREDIT_BUREAU_YEAR"] == 0)
        f["Immediate_Credit_Hunger"] = 0
        f["Inquiry_Clustering"] = r["AMT_REQ_CREDIT_BUREAU_MON"]

        f["Social_Default_Density"] = 0.0
        f["Social_Contagion_Velocity"] = 0.0

        now = datetime.now()
        f["HOUR_APPR_PROCESS_START"] = now.hour
        f["WEEKDAY_APPR_PROCESS_START"] = now.weekday()
        f["Impulse_Control_Flag"] = int(now.hour >= 22 or now.hour <= 5)
        f["Weekend_Emergency_Flag"] = int(now.weekday() >= 5)

        f["Effective_Monthly_Rate"] = min(self.safe_div(amt_ann, amt_cr + 1, 0.0), 0.25)
        f["Predatory_Loan_Signal"] = int(f["Effective_Monthly_Rate"] > 0.04)
        prev_app = r["PREV_APPROVED_COUNT"]
        f["Desperation_Borrowing_Flag"] = int(f["Predatory_Loan_Signal"] and (prev_app == 0))

        edu_score = r["NAME_EDUCATION_TYPE"] / 4.0
        fam_score = (min(r["CNT_FAM_MEMBERS"], 5) - 1) / 4.0
        emp_score = float(emp_valid and (days_emp < -1095))
        realty_score = float(r["FLAG_OWN_REALTY"])
        f["Social_Embedding_Score"] = round(
            0.30 * realty_score + 0.25 * emp_score + 0.25 * edu_score + 0.20 * fam_score,
            4,
        )

        dti = f["Total_Debt_to_Income"]
        inst_min = r["INST_RATIO_MIN"]
        employed_f = float(emp_valid)

        inc_consist = 1.0 - float((dti < 0.3) and (inst_min < 0.7))
        emp_consist = min(
            1.0,
            (employed_f * float(inst_min >= 0.8))
            + ((1 - employed_f) * float(inst_min < 0.8)),
        )
        bur_worst = r["BUREAU_WORST_STATUS"]
        has_bur = float(r["BUREAU_CREDIT_COUNT"] > 0)
        bur_consist = max(0.0, 1.0 - has_bur * (bur_worst / 5.0))
        f["Behavioral_Consistency_Score"] = round(
            0.35 * inc_consist + 0.35 * emp_consist + 0.30 * bur_consist, 4
        )

        late = float(r["INST_LATE_COUNT"])
        r_late = float(r["INST_RECENT_LATE"])
        ratio_min = r["INST_RATIO_MIN"]
        ratio_mean = r["INST_RATIO_MEAN"]
        refused = float(r["PREV_REFUSED_COUNT"])
        approved = float(r["PREV_APPROVED_COUNT"])

        f["Credit_Behavior_Gap"] = round(ext_w * np.log1p(late), 4)
        f["Score_Payment_Contradiction"] = int((ext_w > 0.6) and (ratio_min < 0.75))
        f["Recent_Deterioration_vs_Score"] = round(ext_w * np.log1p(r_late), 4)
        f["MinTrap_Score_Tension"] = 0.0

        bur_debt = r["BUREAU_TOTAL_DEBT"]
        f["Debt_Spiral_Index"] = 0.0
        f["Total_Known_Debt_Burden"] = min(
            self.safe_div(bur_debt + amt_cr, amt_inc + 1, 0.0), 50.0
        )
        f["Bureau_Recent_Stress"] = 0.0
        f["Active_Debt_Accumulation"] = 0.0

        f["Refusal_Persistence_Ratio"] = min(self.safe_div(refused, approved + 1, 0.0), 20.0)
        f["All_Refused_Flag"] = int((refused > 0) and (approved == 0))
        f["Refusal_With_Payment_Stress"] = round(
            f["Refusal_Persistence_Ratio"] * np.log1p(r_late), 4
        )

        cc_dpd = float(r["CC_DPD_MAX"])
        f["Cash_Crunch_Signal"] = 0.0
        f["Triple_Stress_Flag"] = int(late > 0) + int(cc_dpd > 0)
        f["POS_Install_Dual_Stress"] = 0.0

        unbanked = f["True_Unbanked_Indicator"]
        f["Unbanked_High_Risk"] = int((unbanked == 1) and (ext_w < 0.4))
        f["Unbanked_Good_Risk"] = int((unbanked == 1) and (ext_w >= 0.6))
        f["Unbanked_Previously_Refused"] = int((unbanked == 1) and (refused > 0))

        f["Loan_to_Payment_History"] = 0.0
        f["Annuity_Stretch"] = min(self.safe_div(amt_ann, amt_inc / 12 + 1, 0.0), 5.0)
        f["Monthly_Headroom"] = round(amt_inc / 12 - amt_ann, 2)
        f["Negative_Headroom_Flag"] = int(f["Monthly_Headroom"] < 0)

        f["Perfect_Installment_Payer"] = int((late == 0) and (ratio_min >= 0.95))
        f["Consistent_Payer_Score"] = round(
            (1 - min(late / (late + 10), 1)) * min(ratio_mean, 1), 4
        )
        f["Long_Clean_POS_Record"] = int((cc_dpd == 0) and (late == 0))

        bur_count = float(r["BUREAU_CREDIT_COUNT"])
        bur_worst = float(r["BUREAU_WORST_STATUS"])
        bur_closed = float(r["BUREAU_CLOSED_COUNT"])
        bur_overdue = r["BUREAU_OVERDUE_SUM"]
        has_bur = float(bur_count > 0)
        clean_bur = float((bur_worst == 0) and (bur_overdue == 0))
        closure_rt = min(self.safe_div(bur_closed, bur_count + 1, 0.0), 1.0)

        f["Clean_Bureau_Record"] = int(
            (bur_count > 0) and (bur_worst == 0) and (bur_overdue == 0)
        )
        f["Multiple_Clean_Closures"] = int(
            (bur_closed >= 2) and (bur_worst == 0) and (bur_overdue == 0)
        )
        f["Bureau_Safety_Score"] = round(
            has_bur * clean_bur * (0.5 + 0.5 * closure_rt), 4
        )

        age_yrs_abs = abs(days_birth) / 365.0
        f["Verified_Long_Employment"] = int(
            (r["FLAG_EMP_PHONE"] == 1) and (days_emp < -730) and (days_emp > -36500)
        )
        f["Career_Stability_Score"] = round(
            min(self.safe_div(f["Years_Employed"], max(age_yrs_abs, 18), 0.0), 1.0), 4
        )
        income_q25 = tstats["income_q25"]
        f["Income_Employment_Match"] = int(
            (r["FLAG_EMP_PHONE"] == 1)
            and (days_emp < -1825)
            and (r["AMT_INCOME_TOTAL"] > income_q25)
        )

        f["Consistently_Approved"] = int((approved >= 2) and (refused == 0) and (late == 0))
        f["Approval_Quality_Score"] = round(
            self.safe_div(approved, approved + refused + 1, 0.0) * min(ratio_mean, 1), 4
        )
        has_cc = float(cc_dpd > 0 or r["BUREAU_CREDIT_COUNT"] > 0)
        f["Responsible_CC_Usage"] = int((has_cc == 1) and (cc_dpd == 0))

        f["Multi_Source_Safety_Score"] = round(
            0.30 * f["Consistent_Payer_Score"]
            + 0.25 * f["Bureau_Safety_Score"]
            + 0.20 * f["Career_Stability_Score"]
            + 0.15 * f["Approval_Quality_Score"]
            + 0.10 * float(f["Responsible_CC_Usage"]),
            4,
        )
        f["Safety_Certainty_Flag"] = int(
            f["Perfect_Installment_Payer"] == 1
            and f["Bureau_Safety_Score"] > 0
            and f["Verified_Long_Employment"] == 1
        )
        risk_norm = min(f["Credit_Behavior_Gap"], 3) / 3.0
        f["Safety_vs_Risk_Net"] = round(
            min(max(f["Multi_Source_Safety_Score"] - risk_norm, -1), 1),
            4,
        )

        f["cc_data_missing"] = int(cc_dpd == 0 and bur_count == 0)
        f["bureau_data_missing"] = int(bur_count == 0)
        f["AMT_CREDIT_vs_history"] = 1.0

        ext_min = f["EXT_SOURCE_MIN"]
        no_bur = f["bureau_data_missing"]

        f["EXT_Low_No_Bureau"] = int((ext_w < 0.35) and (no_bur == 1))
        f["EXT_Worst_vs_Mean_Gap"] = max(0, min(1, ext_w - ext_min))
        f["EXT_Low_Recent_Late"] = int((ext_w < 0.40) and (r_late > 0))
        debt_clip = min(f["Total_Debt_to_Income"], 10) / 10.0
        f["EXT_Debt_Stress"] = min(1.0, (1 - ext_w) * debt_clip)

        if ext_w < 0.30:
            tier = 0.0
        elif ext_w < 0.45:
            tier = 1.0
        elif ext_w < 0.60:
            tier = 2.0
        else:
            tier = 3.0
        f["EXT_Score_Tier"] = tier

        return f

    # =====================================================
    # Build final model-ready row
    # =====================================================
    def build_row(
        self,
        feat_dict: Dict[str, Any],
        feature_cols: List[str],
        medians: Optional[np.ndarray],
    ) -> pd.DataFrame:
        row = np.zeros(len(feature_cols))
        for i, col in enumerate(feature_cols):
            if medians is not None:
                row[i] = medians[i]
            if col in feat_dict and col not in MEDIAN_OVERRIDE:
                v = feat_dict[col]
                if not (isinstance(v, float) and np.isnan(v)):
                    row[i] = float(v)
        return pd.DataFrame([row], columns=feature_cols)

    # =====================================================
    # Processing pipeline
    # =====================================================
    def process_message(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        self._validate_raw_payload(raw_payload)
        normalized_raw = self._preprocess_raw_payload(raw_payload)

        application_id = normalized_raw["application_id"]
        feat_dict = self.engineer(normalized_raw, self.artifacts["tstats"])
        X = self.build_row(
            feat_dict,
            self.artifacts["feature_cols"],
            self.artifacts["medians"],
        )

        final_payload = {
            "application_id": application_id,
            "feature_count": len(self.artifacts["feature_cols"]),
            "feature_names": self.artifacts["feature_cols"],
            "feature_vector": X.iloc[0].tolist(),
            "model_input_feature_names": [f"Column_{i}" for i in range(len(self.artifacts["feature_cols"]))],
            "engineered_features": feat_dict,
            "created_at": datetime.utcnow().isoformat(),
            "source_topic": RAW_DATA_INGESTED,
        }
        return final_payload

    def publish_features(self, payload: Dict[str, Any]) -> None:
        self.producer.send(FEATURES, payload)
        logger.info(
            "Published final features for application_id=%s to topic=%s",
            payload.get("application_id"),
            FEATURES,
        )

    def run(self) -> None:
        logger.info("Individual Extraction Service listening on topic=%s", RAW_DATA_INGESTED)
        for msg in self.consumer:
            try:
                raw_payload = msg.value if hasattr(msg, "value") else msg
                final_payload = self.process_message(raw_payload)
                self.publish_features(final_payload)
            except Exception as exc:
                logger.exception("Extraction failed: %s", exc)


# Singleton-style runnable service
extraction_service = IndividualExtractionService()


if __name__ == "__main__":
    extraction_service.run()


"""
This is the data that would be stored in FEATURES topic:
{
  "application_id": "<uuid>",
  "feature_count": 234,
  "feature_names": ["...", "..."],
  "feature_vector": [0.0, 1.0, 0.542, ...],
  "engineered_features": {
    "EXT_SOURCE_WEIGHTED": 0.685,
    "Total_Debt_to_Income": 0.3889,
    "Monthly_Headroom": 63500.0,
    "...": "..."
  },
  "created_at": "2026-03-27T12:34:56.000000",
  "source_topic": "raw_data_ingested"
}
"""