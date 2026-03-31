from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from decimal import Decimal

import mysql.connector
from kafka import KafkaProducer

from app.config import KAFKA_BROKER, get_mysql_config



class FinalizationError(Exception):
    """Raised when manager final-decision payload is invalid."""


class MSMEFinalizationService:
    """
    Handles MSME manager final decision.

    Flow:
    1. Validate manager payload
    2. Fetch model output from msme_manager_review_data
    3. Compute override flag
    4. Store final decision in msme_final_decision
    5. Update msme_manager_review_data.status -> reviewed
    6. Publish event for customer PDF generator
    """
    

    CUSTOMER_PDF_MSME_TOPIC = "customer_pdf_msme"

    ALLOWED_DECISIONS = {"APPROVED", "REJECTED"}

    def __init__(self, kafka_broker: str = KAFKA_BROKER):
        self.config = get_mysql_config()
        self.kafka_broker = kafka_broker
        self.producer = KafkaProducer(
    bootstrap_servers=self.kafka_broker,
    value_serializer=lambda v: json.dumps(v, default=self._json_safe).encode("utf-8"),
)
    @staticmethod
    def _json_safe(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _normalize_string(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    @staticmethod
    def _normalize_float(value: Any) -> Optional[float]:
        if value is None:
            return None

        if isinstance(value, str) and not value.strip():
            return None

        try:
            num = float(value)
        except (TypeError, ValueError):
            return None

        if math.isnan(num) or math.isinf(num):
            return None

        return round(num, 2)

    @staticmethod
    def _normalize_int(value: Any) -> Optional[int]:
        if value is None:
            return None

        if isinstance(value, str) and not value.strip():
            return None

        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _get_connection(self):
        return mysql.connector.connect(**get_mysql_config())

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def validate_manager_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise FinalizationError("Payload must be a dictionary.")

        application_id = self._normalize_string(payload.get("application_id"))
        manager_id = self._normalize_string(payload.get("manager_id"))
        notes = self._normalize_string(payload.get("notes"))

        decision_raw = self._normalize_string(payload.get("decision"))
        if not decision_raw:
            raise FinalizationError("decision is required.")

        decision = decision_raw.upper()
        if decision == "APPROVE":
            decision = "APPROVED"
        elif decision == "REJECT":
            decision = "REJECTED"

        if decision not in self.ALLOWED_DECISIONS:
            raise FinalizationError(
                f"decision must be one of {sorted(self.ALLOWED_DECISIONS)}."
            )

        if not application_id:
            raise FinalizationError("application_id is required.")

        principal = self._normalize_float(payload.get("principal"))
        interest_rate = self._normalize_float(payload.get("interest_rate"))
        tenure = self._normalize_int(payload.get("tenure"))

        if decision == "APPROVED":
            if principal is None or principal <= 0:
                raise FinalizationError(
                    "For APPROVED decision, principal must be present and > 0."
                )
            if interest_rate is None or interest_rate <= 0:
                raise FinalizationError(
                    "For APPROVED decision, interest_rate must be present and > 0."
                )
            # tenure optional; if not supplied, we will fall back to requested tenure/model tenure
        else:
            principal = None
            interest_rate = None
            tenure = None

        return {
            "application_id": application_id,
            "manager_id": manager_id,
            "manager_decision": decision,
            "final_principal": principal,
            "final_interest_rate": interest_rate,
            "final_tenure": tenure,
            "manager_notes": notes,
        }

    # ------------------------------------------------------------------
    # Read review/model row
    # ------------------------------------------------------------------
    def fetch_review_row(self, application_id: str) -> Dict[str, Any]:
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute(
                """
                SELECT
                    application_id,
                    company_name,
                    applicant_name,
                    entity_type,
                    registration_number,
                    loan_ask,
                    decision,
                    risk_score,
                    risk_band,
                    suggested_principal,
                    suggested_interest_rate,
                    
                    status,
                    created_at,
                    updated_at
                FROM msme_manager_review_data
                WHERE application_id = %s
                """,
                (application_id,),
            )
            row = cursor.fetchone()

            if not row:
                raise FinalizationError(
                    f"Application not found for application_id={application_id}"
                )

            return row

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()

    # ------------------------------------------------------------------
    # Core business logic
    # ------------------------------------------------------------------
    def build_final_payload(
        self,
        validated_payload: Dict[str, Any],
        review_row: Dict[str, Any],
    ) -> Dict[str, Any]:
        manager_decision = validated_payload["manager_decision"]

        model_principal = review_row.get("suggested_principal")
        model_rate = review_row.get("suggested_interest_rate")
        model_tenure = None

        final_principal = validated_payload["final_principal"]
        final_interest_rate = validated_payload["final_interest_rate"]
        final_tenure = validated_payload["final_tenure"]

        # fallback tenure if not explicitly provided on approval
        if manager_decision == "APPROVED" and final_tenure is None:
            final_tenure = self._normalize_int(model_tenure)

        override_flag = False
        if manager_decision == "APPROVED":
            override_flag = (
                (
                    final_principal is not None
                    and model_principal is not None
                    and float(final_principal) != float(model_principal)
                )
                or
                (
                    final_interest_rate is not None
                    and model_rate is not None
                    and float(final_interest_rate) != float(model_rate)
                )
                or
                (
                    final_tenure is not None
                    and model_tenure is not None
                    and int(final_tenure) != int(model_tenure)
                )
            )

        payload = {
            "application_id": validated_payload["application_id"],
            "manager_id": validated_payload["manager_id"],
            "manager_decision": manager_decision,
            "manager_notes": validated_payload["manager_notes"],
            "final_principal": final_principal,
            "final_interest_rate": final_interest_rate,
            "final_tenure": final_tenure,
            "override_flag": override_flag,

            # model/reference values
            "model_decision": review_row.get("decision"),
            "model_risk_score": review_row.get("risk_score"),
            "model_risk_band": review_row.get("risk_band"),
            "model_suggested_principal": model_principal,
            "model_suggested_interest_rate": model_rate,
            "model_approved_tenure": model_tenure,

            # review row info useful for downstream PDF
            "company_name": review_row.get("company_name"),
            "applicant_name": review_row.get("applicant_name"),
            "entity_type": review_row.get("entity_type"),
            "registration_number": review_row.get("registration_number"),
            "loan_ask": review_row.get("loan_ask"),

            "finalized_at": self._utc_now_iso(),
            "application_type": "msme",
        }

        return payload

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def upsert_final_decision(self, final_payload: Dict[str, Any]) -> None:
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO msme_final_decision (
                    application_id,
                    model_decision,
                    model_risk_score,
                    model_suggested_principal,
                    model_suggested_interest_rate,
                    model_approved_tenure,
                    manager_decision,
                    final_principal,
                    final_interest_rate,
                    final_tenure,
                    manager_notes,
                    override_flag,
                    manager_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    model_decision = VALUES(model_decision),
                    model_risk_score = VALUES(model_risk_score),
                    model_suggested_principal = VALUES(model_suggested_principal),
                    model_suggested_interest_rate = VALUES(model_suggested_interest_rate),
                    model_approved_tenure = VALUES(model_approved_tenure),
                    manager_decision = VALUES(manager_decision),
                    final_principal = VALUES(final_principal),
                    final_interest_rate = VALUES(final_interest_rate),
                    final_tenure = VALUES(final_tenure),
                    manager_notes = VALUES(manager_notes),
                    override_flag = VALUES(override_flag),
                    manager_id = VALUES(manager_id),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    final_payload["application_id"],
                    final_payload["model_decision"],
                    final_payload["model_risk_score"],
                    final_payload["model_suggested_principal"],
                    final_payload["model_suggested_interest_rate"],
                    final_payload["model_approved_tenure"],
                    final_payload["manager_decision"],
                    final_payload["final_principal"],
                    final_payload["final_interest_rate"],
                    final_payload["final_tenure"],
                    final_payload["manager_notes"],
                    final_payload["override_flag"],
                    final_payload["manager_id"],
                ),
            )

            cursor.execute(
                """
                UPDATE msme_manager_review_data
                SET status = 'reviewed',
                    updated_at = CURRENT_TIMESTAMP
                WHERE application_id = %s
                """,
                (final_payload["application_id"],),
            )

            conn.commit()

        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()

    # ------------------------------------------------------------------
    # Kafka publishing
    # ------------------------------------------------------------------
    def publish_customer_pdf_request(self, final_payload: Dict[str, Any]) -> None:
        """
        Publishes the finalized decision so a customer PDF generator service
        can build the customer-facing PDF.
        """
        event = {
            "event_type": "CUSTOMER_PDF_REQUESTED",
            "application_type": "msme",
            "application_id": final_payload["application_id"],
            "company_name": final_payload.get("company_name"),
            "applicant_name": final_payload.get("applicant_name"),
            "entity_type": final_payload.get("entity_type"),
            "registration_number": final_payload.get("registration_number"),
            "loan_ask": final_payload.get("loan_ask"),

            "model_decision": final_payload.get("model_decision"),
            "model_risk_score": final_payload.get("model_risk_score"),
            "model_risk_band": final_payload.get("model_risk_band"),
            "model_suggested_principal": final_payload.get("model_suggested_principal"),
            "model_suggested_interest_rate": final_payload.get("model_suggested_interest_rate"),
            "model_approved_tenure": final_payload.get("model_approved_tenure"),

            "manager_decision": final_payload.get("manager_decision"),
            "final_principal": final_payload.get("final_principal"),
            "final_interest_rate": final_payload.get("final_interest_rate"),
            "final_tenure": final_payload.get("final_tenure"),
            "manager_notes": final_payload.get("manager_notes"),
            "override_flag": final_payload.get("override_flag"),
            "manager_id": final_payload.get("manager_id"),

            "created_at": self._utc_now_iso(),
        }

        future = self.producer.send(self.CUSTOMER_PDF_MSME_TOPIC, event)
        record_metadata = future.get(timeout=15)

        print(
            f"[✔] Sent to {self.CUSTOMER_PDF_MSME_TOPIC} | "
            f"Partition: {record_metadata.partition} | Offset: {record_metadata.offset}"
        )

    # ------------------------------------------------------------------
    # Public orchestration method
    # ------------------------------------------------------------------
    def finalize(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        End-to-end finalization entrypoint.
        """
        validated = self.validate_manager_payload(payload)
        review_row = self.fetch_review_row(validated["application_id"])
        final_payload = self.build_final_payload(validated, review_row)

        self.upsert_final_decision(final_payload)
        self.publish_customer_pdf_request(final_payload)

        return {
            "application_id": final_payload["application_id"],
            "status": "reviewed",
            "manager_decision": final_payload["manager_decision"],
            "override_flag": final_payload["override_flag"],
            "final_principal": final_payload["final_principal"],
            "final_interest_rate": final_payload["final_interest_rate"],
            "final_tenure": final_payload["final_tenure"],
            "message": "MSME final decision stored and customer PDF generation triggered.",
        }


# ----------------------------------------------------------------------
# Singleton
# ----------------------------------------------------------------------
finalization_service = MSMEFinalizationService()