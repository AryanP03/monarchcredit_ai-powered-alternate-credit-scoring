from __future__ import annotations

import json
import logging
import pickle
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.messaging.consumers import JSONKafkaConsumer
from app.messaging.producer import JSONKafkaProducer
from app.services_individual.audit_service_individual import audit_service

from app.config import KAFKA_BROKER, INDIVIDUAL_ARTIFACT_DIR


# ==========================================================
# Constants copied from testing.py logic
# ==========================================================
THRESHOLD = 0.133
LGD = 0.45
INTEREST_RATE = 0.15

FEATURES = "features"
MODEL_OUTPUT = "model_output"

DEFAULT_ARTIFACT_DIRS = [INDIVIDUAL_ARTIFACT_DIR]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IndividualScoringService:
    def __init__(self, artifact_dir: Optional[str] = None):
        self.kafka_broker = KAFKA_BROKER

        self.producer = JSONKafkaProducer()
        self.consumer = JSONKafkaConsumer(
    FEATURES,
    group_id="individual_scoring_service_group"
)

        self.artifact_dir = self._find_artifacts(artifact_dir)
        self.artifacts = self._load_artifacts(self.artifact_dir)

        self.feature_order = self.artifacts["feature_cols"]
        self.model_name = "MonarchCredit Ensemble (LightGBM + XGBoost)"
        self.model_version = "v2-full-coverage"
        self.threshold = THRESHOLD

    # ==========================================================
    # Artifact loading
    # ==========================================================
    def _find_artifacts(self, override: Optional[str] = None) -> Path:
        candidates = ([Path(override)] if override else []) + DEFAULT_ARTIFACT_DIRS
        for p in candidates:
            if p.exists() and (p / "lgbm_final.pkl").exists():
                return p

        raise FileNotFoundError(
            "Artifacts not found. Expected lgbm_final.pkl in:\n"
            + "\n".join(f"  {p}" for p in candidates)
        )

    def _load_artifacts(self, artifact_dir: Path) -> Dict[str, Any]:
        logger.info("Loading scoring artifacts from %s", artifact_dir)

        with open(artifact_dir / "lgbm_final.pkl", "rb") as f:
            lgbm = pickle.load(f)

        with open(artifact_dir / "xgb_final.pkl", "rb") as f:
            xgb = pickle.load(f)

        with open(artifact_dir / "ensemble_weights.pkl", "rb") as f:
            weights = pickle.load(f)

        feat_df = pd.read_csv(artifact_dir / "feature_columns.csv")
        feature_cols = feat_df["feature"].tolist()

        logger.info("Loaded models and %d feature columns", len(feature_cols))

        return {
            "lgbm": lgbm,
            "xgb": xgb,
            "lgbm_w": weights.get("lgbm", 0.50),
            "xgb_w": weights.get("xgb", 0.50),
            "feature_cols": feature_cols,
        }

    # ==========================================================
    # Small helpers
    # ==========================================================
    def _get_model_feature_names(self, model, expected_len: int) -> List[str]:
        """
        Returns the exact feature names expected by the model.
        If model was trained on numpy arrays, LightGBM often stores Column_0...Column_n.
        """
        if hasattr(model, "feature_names_in_"):
            names = list(model.feature_names_in_)
            if len(names) == expected_len:
                return [str(x) for x in names]

        return [f"Column_{i}" for i in range(expected_len)]

    @staticmethod
    def _looks_like_generic_columns(names: List[str]) -> bool:
        if not names:
            return False
        return all(str(name).startswith("Column_") for name in names)

    def _build_model_input_df(self, payload: Dict[str, Any]) -> pd.DataFrame:
        """
        Build dataframe using pipeline feature order, then rename columns to whatever
        the stored model expects (often Column_0...Column_n).
        """
        X_actual = self._build_model_input(payload)

        model_feature_names = self._get_model_feature_names(
            self.artifacts["lgbm"],
            len(X_actual.columns)
        )

        # rename by position, preserving values/order
        X_model = X_actual.copy()
        X_model.columns = model_feature_names
        return X_model

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            num = float(value)
            if np.isnan(num) or np.isinf(num):
                return default
            return num
        except Exception:
            return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _clamp(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _fmt_currency(value: float) -> str:
        try:
            v = float(value)
            if abs(v) >= 1e7:
                return f"₹{v / 1e7:.2f} Cr"
            if abs(v) >= 1e5:
                return f"₹{v / 1e5:.2f} L"
            if abs(v) >= 1e3:
                return f"₹{v / 1e3:.1f} K"
            return f"₹{v:.0f}"
        except Exception:
            return "₹0"

    # ==========================================================
    # Payload validation
    # ==========================================================
    def _validate_features_payload(self, payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise ValueError("Features payload must be a dictionary")

        if not payload.get("application_id"):
            raise ValueError("Missing application_id in features payload")

        if "feature_vector" not in payload and "engineered_features" not in payload:
            raise ValueError(
                "Features payload must contain either feature_vector or engineered_features"
            )

    # ==========================================================
    # Build model input
    # ==========================================================
    def _build_model_input(self, payload: Dict[str, Any]) -> pd.DataFrame:
        """
        Supports two safe input forms:
        1. feature_names + feature_vector
        2. engineered_features dict

        If feature_names exactly match training order, uses them directly.
        Otherwise rebuilds ordered row from dict.
        """
        expected = self.feature_order

        feature_names = payload.get("feature_names")
        feature_vector = payload.get("feature_vector")
        engineered = payload.get("engineered_features", {})

        # Fast path: exact ordered vector already present
        if (
            isinstance(feature_names, list)
            and isinstance(feature_vector, list)
            and feature_names == expected
            and len(feature_vector) == len(expected)
        ):
            return pd.DataFrame([feature_vector], columns=expected)

        # Fallback: rebuild from provided names/vector
        if isinstance(feature_names, list) and isinstance(feature_vector, list):
            current_map = {
                str(name): self._safe_float(val, 0.0)
                for name, val in zip(feature_names, feature_vector)
            }
            row = [current_map.get(col, 0.0) for col in expected]
            return pd.DataFrame([row], columns=expected)

        # Fallback: rebuild from engineered feature dict
        if isinstance(engineered, dict):
            row = [self._safe_float(engineered.get(col), 0.0) for col in expected]
            return pd.DataFrame([row], columns=expected)

        raise ValueError("Unable to build model input from features payload")

    # ==========================================================
    # Core scoring: exactly as testing.py
    # ==========================================================
    def _predict_scores(self, X: pd.DataFrame) -> Dict[str, float]:
        lgbm_prob = float(self.artifacts["lgbm"].predict_proba(X.values)[0, 1])
        xgb_prob = float(self.artifacts["xgb"].predict_proba(X.values)[0, 1])

        lgbm_w = float(self.artifacts["lgbm_w"])
        xgb_w = float(self.artifacts["xgb_w"])

        pd_score = lgbm_w * lgbm_prob + xgb_w * xgb_prob

        return {
            "lgbm_prob": lgbm_prob,
            "xgb_prob": xgb_prob,
            "lgbm_w": lgbm_w,
            "xgb_w": xgb_w,
            "pd_score": float(pd_score),
        }

    def _get_risk_band(self, pd_score: float) -> str:
        # Exact thresholds from testing.py
        if pd_score >= 0.40:
            return "VERY HIGH"
        elif pd_score >= 0.25:
            return "HIGH"
        elif pd_score >= 0.133:
            return "MODERATE"
        elif pd_score >= 0.08:
            return "LOW-MODERATE"
        else:
            return "LOW"

    # ==========================================================
    # Production policy layer
    # This is added on top of testing.py because the CLI does not
    # directly compute suggested principal / suggested rate.
    # ==========================================================
    def _compute_credit_policy(
        self,
        payload: Dict[str, Any],
        pd_score: float,
        risk_band: str,
    ) -> Dict[str, Any]:
        engineered = payload.get("engineered_features", {}) or {}

        loan_amt = self._safe_float(
            engineered.get("AMT_CREDIT", payload.get("loan_amt", 0.0)),
            0.0,
        )
        monthly_headroom = self._safe_float(engineered.get("Monthly_Headroom"), 0.0)
        dti = self._safe_float(engineered.get("Total_Debt_to_Income"), 0.0)
        safety_score = self._safe_float(engineered.get("Multi_Source_Safety_Score"), 0.0)
        payer_score = self._safe_float(engineered.get("Consistent_Payer_Score"), 0.0)
        bureau_safety = self._safe_float(engineered.get("Bureau_Safety_Score"), 0.0)
        predatory_signal = self._safe_int(engineered.get("Predatory_Loan_Signal"), 0)
        annuity_stretch = self._safe_float(engineered.get("Annuity_Stretch"), 0.0)
        requested_tenure = self._safe_int(payload.get("loan_tenure_months"), 0)

        # Base annual pricing starts from testing.py fixed 15%
        suggested_interest_rate = 15.0

        if pd_score < 0.08:
            suggested_interest_rate = 11.5
        elif pd_score < 0.133:
            suggested_interest_rate = 13.0
        elif pd_score < 0.25:
            suggested_interest_rate = 15.0
        elif pd_score < 0.40:
            suggested_interest_rate = 17.5
        else:
            suggested_interest_rate = 20.0

        # Small affordability adjustments
        if monthly_headroom < 0:
            suggested_interest_rate += 1.0
        if dti > 1.0:
            suggested_interest_rate += 1.0
        if predatory_signal == 1:
            suggested_interest_rate += 1.0

        suggested_interest_rate = round(self._clamp(suggested_interest_rate, 8.0, 24.0), 2)

        # Principal haircut policy
        principal_factor = 1.0
        if pd_score < 0.08:
            principal_factor = 1.00
        elif pd_score < 0.133:
            principal_factor = 0.95
        elif pd_score < 0.25:
            principal_factor = 0.85
        elif pd_score < 0.40:
            principal_factor = 0.65
        else:
            principal_factor = 0.0

        if monthly_headroom < 0:
            principal_factor = min(principal_factor, 0.50)
        if dti > 1.5:
            principal_factor = min(principal_factor, 0.50)
        elif dti > 1.0:
            principal_factor = min(principal_factor, 0.70)
        if annuity_stretch > 2.5:
            principal_factor = min(principal_factor, 0.60)

        suggested_principal = round(max(0.0, loan_amt * principal_factor), 2)

        # Tenure suggestion
        if requested_tenure > 0:
            if pd_score < 0.133:
                approved_tenure = requested_tenure
            elif pd_score < 0.25:
                approved_tenure = min(requested_tenure, 36)
            elif pd_score < 0.40:
                approved_tenure = min(requested_tenure, 24)
            else:
                approved_tenure = 0
        else:
            approved_tenure = 36 if pd_score < 0.25 else (24 if pd_score < 0.40 else 0)

        flagged = pd_score >= THRESHOLD

        # Decision aligned to testing.py:
        # flagged => reject, else approve
        # with a small manual-review state for borderline production use
        if pd_score >= 0.25 or suggested_principal <= 0:
            decision = "REJECTED"
            decision_reason = "High default risk or weak affordability profile"
        elif flagged:
            decision = "REVIEW"
            decision_reason = "Borderline risk above operating threshold; manual review recommended"
        else:
            decision = "APPROVED"
            decision_reason = "Risk is below threshold and affordability appears acceptable"

        expected_loss = round(pd_score * LGD * loan_amt, 2)
        interest_revenue = round(loan_amt * (suggested_interest_rate / 100.0), 2)
        net_expected_profit = round(interest_revenue - expected_loss, 2)
        break_even_pd = round((suggested_interest_rate / 100.0) / LGD, 6)

        return {
            "decision": decision,
            "decision_reason": decision_reason,
            "suggested_principal": suggested_principal,
            "approved_principal": suggested_principal,
            "approved_tenure": int(approved_tenure),
            "suggested_interest_rate": suggested_interest_rate if decision != "REJECTED" else None,
            "expected_loss": expected_loss,
            "interest_revenue": interest_revenue,
            "net_expected_profit": net_expected_profit,
            "break_even_pd": break_even_pd,
            "monthly_headroom": round(monthly_headroom, 2),
            "debt_to_income": round(dti, 4),
            "safety_score": round(safety_score, 4),
            "payer_score": round(payer_score, 4),
            "bureau_safety_score": round(bureau_safety, 4),
            "predatory_loan_signal": bool(predatory_signal),
        }

    # ==========================================================
    # Main processing
    # ==========================================================
    def process_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._validate_features_payload(payload)

        application_id = payload["application_id"]
        engineered = payload.get("engineered_features", {}) or {}

        X_actual = self._build_model_input(payload)
        X_model = self._build_model_input_df(payload)
        score_block = self._predict_scores(X_model)     

        pd_score = score_block["pd_score"]
        risk_band = self._get_risk_band(pd_score)

        decision_block = self._compute_credit_policy(payload, pd_score, risk_band)

        loan_amt = self._safe_float(engineered.get("AMT_CREDIT", payload.get("loan_amt", 0.0)), 0.0)

        output = {
            "application_id": application_id,

            # Core score outputs
            "risk_score": round(pd_score, 6),
            "risk_band": risk_band,
            "decision": decision_block["decision"],
            "decision_reason": decision_block["decision_reason"],

            # Recommendation outputs
            "suggested_principal": decision_block["suggested_principal"],
            "approved_principal": decision_block["approved_principal"],
            "approved_tenure": decision_block["approved_tenure"],
            "suggested_interest_rate": decision_block["suggested_interest_rate"],

            # Ensemble transparency
            "lgbm_probability": round(score_block["lgbm_prob"], 6),
            "xgb_probability": round(score_block["xgb_prob"], 6),
            "lgbm_weight": round(score_block["lgbm_w"], 4),
            "xgb_weight": round(score_block["xgb_w"], 4),

            # Financial impact, mirroring testing.py spirit
            "loan_amount": round(loan_amt, 2),
            "expected_loss": decision_block["expected_loss"],
            "interest_revenue": decision_block["interest_revenue"],
            "net_expected_profit": decision_block["net_expected_profit"],
            "break_even_pd": decision_block["break_even_pd"],

            # Key signals surfaced by testing.py
            "monthly_headroom": decision_block["monthly_headroom"],
            "debt_to_income": decision_block["debt_to_income"],
            "safety_score": decision_block["safety_score"],
            "payer_score": decision_block["payer_score"],
            "bureau_safety_score": decision_block["bureau_safety_score"],
            "predatory_loan_signal": decision_block["predatory_loan_signal"],
            "ext_source_weighted": round(self._safe_float(engineered.get("EXT_SOURCE_WEIGHTED"), 0.0), 6),

            # Metadata
            "model_name": self.model_name,
            "model_version": self.model_version,
            "feature_count": len(self.feature_order),
            "model_input_feature_names": list(X_model.columns),
            "threshold_reference": self.threshold,
            "lgd_reference": LGD,
            "base_interest_rate_reference": INTEREST_RATE,
            "score_status": "SUCCESS",
            "scored_at": self._utc_now_iso(),
        }

        return output

    # ==========================================================
    # Publishing
    # ==========================================================
    def publish_output(self, output_payload: Dict[str, Any]) -> None:
        self.producer.send(MODEL_OUTPUT, output_payload)
        audit_service.process_scoring_payload(output_payload)
        

    # ==========================================================
    # Runner
    # ==========================================================
    def run(self) -> None:
        print("=" * 80)
        print("Individual Scoring Service Started")
        print(f"Consuming topic : {FEATURES}")
        print(f"Publishing topic: {MODEL_OUTPUT}")
        print(f"Kafka broker    : {self.kafka_broker}")
        print(f"Model path      : {self.artifact_dir}")
        print("=" * 80)

        for message in self.consumer:
            try:
                payload = message.value
                application_id = payload.get("application_id")

                print(f"\n[INFO] Received features for application_id={application_id}")

                output_payload = self.process_message(payload)
                self.publish_output(output_payload)

                print(
                    f"[SUCCESS] Published model output for application_id={application_id} "
                    f"to topic={MODEL_OUTPUT}"
                )

            except Exception as exc:
                print("[ERROR] Failed to score individual application")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_scoring_service() -> None:
    service = IndividualScoringService()
    service.run()


if __name__ == "__main__":
    run_scoring_service()