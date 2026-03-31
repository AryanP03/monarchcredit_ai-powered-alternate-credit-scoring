# app/services_MSME/extraction_service.py

from __future__ import annotations

import json
import math
import traceback
from typing import Any, Dict, List

from kafka import KafkaConsumer, KafkaProducer

from app.config import KAFKA_BROKER


class MSMEExtractionService:
    """
    Consumes raw MSME model fields from:
        RAW_MSME_DATA = "raw_msme_data_injested"

    Applies model preprocessing exactly as required:
      - log1p transform for log_paid_in_capital and log_registered_capital
      - log1p transform for fields in DEFAULT_LOG_COLS when those fields
        are present in the final model feature list
      - preserves exact final feature order

    Produces to:
        FEATURES_MSME = "features_msme"
    """

    RAW_MSME_DATA = "raw_msme_data_injested"
    FEATURES_MSME = "features_msme"

    # This list is taken from the logic you shared.
    # In the prediction code, these are log1p-transformed IF the names are in model_features.
    DEFAULT_LOG_COLS = [
        "total_court_executions",
        "recent_court_executions",
        "total_overdue_tax",
        "total_admin_penalties",
        "recent_admin_penalties",
        "total_legal_proceedings",
        "recent_legal_proceedings",
        "total_case_filings",
        "total_corporate_changes",
        "recent_corporate_changes",
        "total_trademarks",
        "total_patents",
        "total_copyrights",
        "total_certifications",
        "total_ip_score",
    ]

    # Exact final order required by your model
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

    def __init__(self, kafka_broker: str = KAFKA_BROKER):
        self.kafka_broker = kafka_broker

        self.consumer = KafkaConsumer(
            self.RAW_MSME_DATA,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="msme_extraction_service_group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    @staticmethod
    def _safe_non_negative(value: Any, default: float = 0.0) -> float:
        """
        Convert value to float and clamp to minimum 0.
        Used before log1p transforms.
        """
        try:
            v = float(value)
            return max(0.0, v)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """
        Convert value to float without clamping.
        Used for non-log features that may legitimately be negative,
        like insurance_trend (-1 to 1).
        """
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _validate_raw_payload(self, payload: Dict[str, Any]) -> None:
        """
        Basic validation for required fields coming from raw_msme_data_injested.
        Since schema validation already happened in main.py and ingestion_service,
        this is mostly a safety check for Kafka payload integrity.
        """
        required_raw_fields = [
            "application_id",
            "loan_amt",
            "tenure",
            "msme_size_category",
            "company_age_years",
            "paid_in_capital",
            "registered_capital",
            "region_risk_score",
            "sector_Construction",
            "sector_Services",
            "sector_Tech",
            "sector_Trade",
            "SH_num",
            "MS_num",
            "avg_insured_employees",
            "Branch_num",
            "total_court_executions",
            "recent_court_executions",
            "total_overdue_tax",
            "total_admin_penalties",
            "recent_admin_penalties",
            "total_legal_proceedings",
            "recent_legal_proceedings",
            "total_case_filings",
            "total_corporate_changes",
            "recent_corporate_changes",
            "total_trademarks",
            "total_patents",
            "total_copyrights",
            "total_certifications",
            "total_ip_score",
            "Ratepaying_Credit_Grade_A_num",
            "insurance_trend",
            "salary_disbursement_regularity_score",
            "account_balance_volatility_score",
            "loan_repayment_history_score",
            "vendor_payment_delay_trend",
            "utility_bill_payment_score",
            "tax_filing_compliance_score",
            "digital_payment_ratio",
        ]

        missing = [field for field in required_raw_fields if field not in payload]
        if missing:
            raise ValueError(f"Missing required raw MSME fields: {missing}")

    def _build_features(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build final model-ready feature payload from raw MSME payload.

        Preprocessing logic mirrors the prediction code:
          - log_paid_in_capital       = log1p(max(0, paid_in_capital))
          - log_registered_capital   = log1p(max(0, registered_capital))
          - any field whose name is in DEFAULT_LOG_COLS and also appears in
            FINAL_FEATURE_ORDER is stored as log1p(max(0, raw[field]))
          - all remaining features are passed through as numeric values
        """
        features: Dict[str, Any] = {}
        features["application_id"] = raw["application_id"]
        features["loan_amt"] = self._safe_float(raw.get("loan_amt", 0.0))
        features["tenure"] = self._safe_int(raw.get("tenure", 0))

        for feat in self.FINAL_FEATURE_ORDER:
            if feat == "log_paid_in_capital":
                raw_value = self._safe_non_negative(raw.get("paid_in_capital", 0.0))
                features[feat] = math.log1p(raw_value)

            elif feat == "log_registered_capital":
                raw_value = self._safe_non_negative(raw.get("registered_capital", 0.0))
                features[feat] = math.log1p(raw_value)

            elif feat in self.DEFAULT_LOG_COLS:
                raw_value = self._safe_non_negative(raw.get(feat, 0.0))
                features[feat] = math.log1p(raw_value)

            elif feat == "insurance_trend":
                # can be negative (-1, 0, 1 etc.), so do NOT clamp/log this
                features[feat] = self._safe_float(raw.get(feat, 0.0))

            else:
                features[feat] = self._safe_float(raw.get(feat, 0.0))

        return features

    def _ordered_feature_vector(self, features_payload: Dict[str, Any]) -> List[float]:
        """
        Returns the 37 model features in exact order.
        Useful for debugging or downstream services if needed.
        """
        return [features_payload[feat] for feat in self.FINAL_FEATURE_ORDER]

    def process_message(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        End-to-end processing of one Kafka message from raw_msme_data_injested.
        """
        self._validate_raw_payload(raw_payload)
        features_payload = self._build_features(raw_payload)

        # Optional debug metadata
        features_payload["feature_order"] = self.FINAL_FEATURE_ORDER
        features_payload["feature_vector"] = self._ordered_feature_vector(features_payload)

        return features_payload

    def publish_features(self, features_payload: Dict[str, Any]) -> None:
        self.producer.send(self.FEATURES_MSME, value=features_payload)
        self.producer.flush()

    def run(self) -> None:
        """
        Infinite Kafka consumer loop.
        """
        print("=" * 80)
        print("MSME Extraction Service Started")
        print(f"Consuming topic : {self.RAW_MSME_DATA}")
        print(f"Publishing topic: {self.FEATURES_MSME}")
        print(f"Kafka broker    : {self.kafka_broker}")
        print("=" * 80)

        for message in self.consumer:
            try:
                raw_payload = message.value
                print("\n[INFO] Received raw MSME payload from Kafka")
                print(f"[INFO] application_id: {raw_payload.get('application_id')}")

                features_payload = self.process_message(raw_payload)
                self.publish_features(features_payload)

                print(f"[SUCCESS] Published features for application_id={features_payload['application_id']} "
                      f"to topic={self.FEATURES_MSME}")

            except Exception as exc:
                print("[ERROR] Failed to process raw MSME payload")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_extraction_service() -> None:
    service = MSMEExtractionService()
    service.run()


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_extraction_service()