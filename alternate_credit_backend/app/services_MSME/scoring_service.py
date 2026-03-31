from __future__ import annotations

import json
import math
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
import pickle

import joblib
import numpy as np
from kafka import KafkaConsumer, KafkaProducer

try:
    from app.services_MSME.audit_service_MSME import audit_service
except ImportError:
    from audit_service_MSME import audit_service

from app.config import KAFKA_BROKER, MSME_MODEL_PATH, DEFAULT_RISK_THRESHOLD_MSME


class MSMEScoringService:
    FEATURES_MSME = "features_msme"
    MODEL_OUTPUT_MSME = "model_output_msme"

    MODEL_PATH = Path(MSME_MODEL_PATH)
    

    FINAL_FEATURE_ORDER = [
        "SH_num",
        "MS_num",
        "Branch_num",
        "Ratepaying_Credit_Grade_A_num",
        "msme_size_category",
        "company_age_years",
        "total_corporate_changes",
        "recent_corporate_changes",
        "total_court_executions",
        "recent_court_executions",
        "total_overdue_tax",
        "total_admin_penalties",
        "recent_admin_penalties",
        "total_legal_proceedings",
        "recent_legal_proceedings",
        "total_case_filings",
        "total_trademarks",
        "total_patents",
        "total_copyrights",
        "total_certifications",
        "total_ip_score",
        "avg_insured_employees",
        "insurance_trend",
        "log_registered_capital",
        "log_paid_in_capital",
        "region_risk_score",
        "sector_Construction",
        "sector_Services",
        "sector_Tech",
        "sector_Trade",
        "salary_disbursement_regularity_score",
        "account_balance_volatility_score",
        "loan_repayment_history_score",
        "vendor_payment_delay_trend",
        "utility_bill_payment_score",
        "tax_filing_compliance_score",
        "digital_payment_ratio",
    ]

    def __init__(
        self,
        kafka_broker: str = KAFKA_BROKER,
        model_name: str = "msme_credit_risk_model",
        model_version: str = "v1",
    ):
        self.kafka_broker = kafka_broker
        self.model_name = model_name
        self.model_version = model_version

        self.consumer = KafkaConsumer(
            self.FEATURES_MSME,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="msme_scoring_service_group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.model = self._load_model()
        self.threshold = DEFAULT_RISK_THRESHOLD_MSME

    def _load_model(self):
        if not self.MODEL_PATH.exists():
            raise FileNotFoundError(f"Model not found at: {self.MODEL_PATH}")

        with open(self.MODEL_PATH, "rb") as f:
            model = pickle.load(f)

        print(f"[INFO] MSME model loaded from {self.MODEL_PATH}")
        print(f"[INFO] Model expects {model.n_features_in_} features")

        # ✅ CRITICAL CHECK (add here)
        if list(model.feature_names_in_) != self.FINAL_FEATURE_ORDER:
            raise ValueError(
                "\n❌ FEATURE MISMATCH DETECTED!\n"
                f"Model expects:\n{list(model.feature_names_in_)}\n\n"
                f"But pipeline provides:\n{self.FINAL_FEATURE_ORDER}\n\n"
                "Fix your feature order before proceeding."
        )

        print("[SUCCESS] Feature order matches model expectations")

        return model

    

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _validate_features_payload(self, payload: Dict[str, Any]) -> None:
        required = ["application_id", "loan_amt", "tenure"] + self.FINAL_FEATURE_ORDER
        missing = [col for col in required if col not in payload]
        if missing:
            raise ValueError(f"Missing required feature fields: {missing}")

    def _build_model_input(self, payload: Dict[str, Any]) -> np.ndarray:
        row = [self._safe_float(payload[col]) for col in self.FINAL_FEATURE_ORDER]
        return np.array([row], dtype=float)

    def _predict_risk_score(self, X: np.ndarray) -> float:
        """
        Returns probability of default / positive class probability.
        """
        if hasattr(self.model, "predict_proba"):
            proba = self.model.predict_proba(X)
            if proba.ndim == 2 and proba.shape[1] >= 2:
                return float(proba[0, 1])
            return float(proba[0])

        if hasattr(self.model, "decision_function"):
            score = float(self.model.decision_function(X)[0])
            return 1.0 / (1.0 + math.exp(-score))

        pred = self.model.predict(X)
        return float(pred[0])

    @staticmethod
    def _get_risk_band(risk_score: float) -> str:
        if risk_score < 0.14:
            return "LOW"
        if risk_score < 0.40:
            return "MODERATE"
        if risk_score < 0.70:
            return "HIGH"
        return "VERY_HIGH"
    
    @staticmethod
    def _clamp_01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def _compute_credit_decision(self, payload: Dict[str, Any], risk_score: float) -> Dict[str, Any]:
        loan_amt = self._safe_float(payload.get("loan_amt"), 0.0)
        tenure = self._safe_float(payload.get("tenure"), 0.0)

        log_paid_in_capital = self._safe_float(payload.get("log_paid_in_capital"), 0.0)
        log_registered_capital = self._safe_float(payload.get("log_registered_capital"), 0.0)

        loan_repayment_history_score = self._safe_float(payload.get("loan_repayment_history_score"), 0.0)
        account_balance_volatility_score = self._safe_float(payload.get("account_balance_volatility_score"), 0.0)
        salary_disbursement_regularity_score = self._safe_float(payload.get("salary_disbursement_regularity_score"), 0.0)
        vendor_payment_delay_trend = self._safe_float(payload.get("vendor_payment_delay_trend"), 0.0)
        utility_bill_payment_score = self._safe_float(payload.get("utility_bill_payment_score"), 0.0)
        tax_filing_compliance_score = self._safe_float(payload.get("tax_filing_compliance_score"), 0.0)

        paid_capital = max(math.expm1(log_paid_in_capital), 1.0)
        registered_capital = max(math.expm1(log_registered_capital), 1.0)

        loan_to_paid_capital = loan_amt / paid_capital if paid_capital > 0 else 999.0
        loan_to_registered_capital = loan_amt / registered_capital if registered_capital > 0 else 999.0
        tenure_factor = min(tenure / 10.0, 1.0)

        # Hard rules first
        if loan_amt <= 0:
            return {
                "decision": "REJECTED",
                "decision_reason": "Invalid loan amount",
                "policy_score": 0.0,
                "approved_principal": 0.0,
                "approved_tenure": 0,
                "suggested_interest_rate": None,
                "loan_to_paid_capital": loan_to_paid_capital,
                "loan_to_registered_capital": loan_to_registered_capital,
            }

        if tenure <= 0:
            return {
                "decision": "REJECTED",
                "decision_reason": "Invalid tenure",
                "policy_score": 0.0,
                "approved_principal": 0.0,
                "approved_tenure": 0,
                "suggested_interest_rate": None,
                "loan_to_paid_capital": loan_to_paid_capital,
                "loan_to_registered_capital": loan_to_registered_capital,
            }

        if risk_score >= 0.70:
            return {
                "decision": "REJECTED",
                "decision_reason": "Very high default risk",
                "policy_score": 0.0,
                "approved_principal": 0.0,
                "approved_tenure": 0,
                "suggested_interest_rate": 20.0,
                "loan_to_paid_capital": loan_to_paid_capital,
                "loan_to_registered_capital": loan_to_registered_capital,
            }

        if loan_to_paid_capital > 1.50:
            return {
                "decision": "REJECTED",
                "decision_reason": "Loan ask too high relative to paid capital",
                "policy_score": 0.0,
                "approved_principal": 0.0,
                "approved_tenure": 0,
                "suggested_interest_rate": None,
                "loan_to_paid_capital": loan_to_paid_capital,
                "loan_to_registered_capital": loan_to_registered_capital,
            }

        if tenure > 7 and risk_score > 0.40:
            return {
                "decision": "REJECTED",
                "decision_reason": "Long tenure not acceptable for this risk level",
                "policy_score": 0.0,
                "approved_principal": 0.0,
                "approved_tenure": 0,
                "suggested_interest_rate": None,
                "loan_to_paid_capital": loan_to_paid_capital,
                "loan_to_registered_capital": loan_to_registered_capital,
            }

        risk_component = self._clamp_01(1.0 - risk_score)
        capital_component = self._clamp_01(1.0 - min(loan_to_paid_capital, 2.0) / 2.0)
        tenure_component = self._clamp_01(1.0 - tenure_factor)
        repayment_component = self._clamp_01(loan_repayment_history_score)
        stability_component = self._clamp_01(salary_disbursement_regularity_score)
        liquidity_component = self._clamp_01(1.0 - account_balance_volatility_score)
        compliance_component = self._clamp_01(tax_filing_compliance_score)
        utility_component = self._clamp_01(utility_bill_payment_score)
        vendor_component = self._clamp_01(1.0 - vendor_payment_delay_trend)

        policy_score = (
            0.35 * risk_component +
            0.20 * capital_component +
            0.10 * tenure_component +
            0.12 * repayment_component +
            0.08 * stability_component +
            0.05 * liquidity_component +
            0.04 * compliance_component +
            0.03 * utility_component +
            0.03 * vendor_component
        )

        if risk_score < 0.14:
            suggested_interest_rate = 10.5
        elif risk_score < 0.40:
            suggested_interest_rate = 13.0
        elif risk_score < 0.70:
            suggested_interest_rate = 16.5
        else:
            suggested_interest_rate = 20.0

        if tenure > 5:
            suggested_interest_rate += 1.0
        if loan_to_paid_capital > 0.75:
            suggested_interest_rate += 1.0

        suggested_interest_rate = round(suggested_interest_rate, 2)

        if policy_score >= 0.68:
            decision = "APPROVED"
            decision_reason = "Risk and affordability are within acceptable limits"
            approved_principal = min(loan_amt, paid_capital * 0.75, registered_capital * 0.40)
            approved_tenure = int(min(tenure, 5))
        elif policy_score >= 0.50:
            decision = "REVIEW"
            decision_reason = "Borderline case; manual manager review recommended"
            approved_principal = min(loan_amt, paid_capital * 0.50, registered_capital * 0.30)
            approved_tenure = int(min(tenure, 4))
        else:
            decision = "REJECTED"
            decision_reason = "Risk-adjusted affordability is insufficient"
            approved_principal = 0.0
            approved_tenure = 0

        return {
            "decision": decision,
            "decision_reason": decision_reason,
            "policy_score": round(policy_score, 4),
            "approved_principal": round(float(max(0.0, approved_principal)), 2),
            "approved_tenure": approved_tenure,
            "suggested_interest_rate": suggested_interest_rate if decision != "REJECTED" else None,
            "loan_to_paid_capital": round(float(loan_to_paid_capital), 4),
            "loan_to_registered_capital": round(float(loan_to_registered_capital), 4),
        }

    def process_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._validate_features_payload(payload)

        application_id = payload["application_id"]
        loan_amt = self._safe_float(payload.get("loan_amt"), 0.0)
        tenure = self._safe_float(payload.get("tenure"), 0.0)

        X = self._build_model_input(payload)
        risk_score = self._predict_risk_score(X)
        risk_band = self._get_risk_band(risk_score)

        decision_block = self._compute_credit_decision(payload, risk_score)

        output = {
            "application_id": application_id,
            "loan_amt": loan_amt,
            "tenure": tenure,
            "risk_score": round(risk_score, 6),
            "risk_band": risk_band,
            "decision": decision_block["decision"],
            "decision_reason": decision_block["decision_reason"],
            "policy_score": decision_block["policy_score"],
            "approved_principal": decision_block["approved_principal"],
            "approved_tenure": decision_block["approved_tenure"],
            "suggested_interest_rate": decision_block["suggested_interest_rate"],
            "loan_to_paid_capital": decision_block["loan_to_paid_capital"],
            "loan_to_registered_capital": decision_block["loan_to_registered_capital"],
            "model_name": self.model_name,
            "model_version": self.model_version,
            "feature_count": len(self.FINAL_FEATURE_ORDER),
            "threshold_reference": self.threshold,
            "score_status": "SUCCESS",
            "scored_at": self._utc_now_iso(),
        }

        return output

    def publish_output(self, output_payload: Dict[str, Any]) -> None:
        self.producer.send(self.MODEL_OUTPUT_MSME, value=output_payload)
        
        audit_service.process_scoring_payload(output_payload)

        self.producer.flush()

    def run(self) -> None:
        
        print("=" * 80)
        print("MSME Scoring Service Started")
        print(f"Consuming topic : {self.FEATURES_MSME}")
        print(f"Publishing topic: {self.MODEL_OUTPUT_MSME}")
        print(f"Kafka broker    : {self.kafka_broker}")
        print(f"Model path      : {self.MODEL_PATH}")
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
                    f"to topic={self.MODEL_OUTPUT_MSME}"
                )

            except Exception as exc:
                print("[ERROR] Failed to score MSME application")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_scoring_service() -> None:
    service = MSMEScoringService()
    service.run()


if __name__ == "__main__":
    run_scoring_service()

""" 
WHAT SAVED INTO THE MODEL_OUTPUT_MSME
{
  "application_id": "abc123",
  "loan_amt": 200000,
  "tenure": 4,
  "risk_score": 0.1182,
  "risk_band": "LOW",
  "decision": "APPROVED",
  "decision_reason": "Risk and affordability are within acceptable limits",
  "policy_score": 0.7342,
  "approved_principal": 200000.0,
  "approved_tenure": 4,
  "suggested_interest_rate": 10.5,
  "loan_to_paid_capital": 0.04,
  "loan_to_registered_capital": 0.02,
  "model_name": "...",
  "model_version": "...",
  "score_status": "SUCCESS"
}
"""