from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import mysql.connector
from mysql.connector import Error

from app.config import get_mysql_config, get_mysql_config_for_database, RAW_SHAP_DB_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuditServiceIndividual:
    """
    Direct DB persistence helper for the INDIVIDUAL pipeline.

    Responsibilities:
    1. Insert/update initial row in individual_manager_review_data
    2. Update scoring output on the same manager-review row
    3. Update explainability output on the same manager-review row
    4. Store manager PDF on the same manager-review row
    5. Insert/update final manager decision into shared msme_final_decision
       with application_type='individual'
    6. Optionally attach final customer PDF to shared final-decision table
    """

    MANAGER_REVIEW_TABLE = "individual_manager_review_data"
    FINAL_DECISION_TABLE = "msme_final_decision"

    def __init__(self):
        self.config = get_mysql_config()

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------
    def _get_connection(self):
        return mysql.connector.connect(**self.config)

    def _execute_query(
        self,
        query: str,
        params: tuple = (),
        fetchone: bool = False,
        fetchall: bool = False,
    ):
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True if (fetchone or fetchall) else False)
            cursor.execute(query, params)

            result = None
            if fetchone:
                result = cursor.fetchone()
            elif fetchall:
                result = cursor.fetchall()

            conn.commit()
            return result

        except Error as e:
            logger.error("Database error: %s", e)
            raise
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()

    @staticmethod
    def _as_bool_int(value: Any) -> int:
        return 1 if bool(value) else 0

    @staticmethod
    def _safe_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    def process_raw_shap_values(self, application_id: str, shap_rows: list[dict]) -> None:
            """
            Stores raw per-feature SHAP values into RAW_SHAP_VALUES.individual_raw_shap_values

            Each row:
            {
                "feature_name": "...",
                "impact": 0.1234
            }
            """
            if not application_id:
                raise ValueError("Missing application_id for raw SHAP storage")

            if not shap_rows:
                logger.warning("No SHAP rows to store for %s", application_id)
                return

            conn = None
            cursor = None
            try:
                cfg = get_mysql_config_for_database(RAW_SHAP_DB_NAME)
                conn = mysql.connector.connect(**cfg)
                cursor = conn.cursor()

                # Remove old SHAP rows for same application so re-processing doesn't duplicate
                delete_query = """
                    DELETE FROM individual_raw_shap_values
                    WHERE application_id = %s
                """
                cursor.execute(delete_query, (application_id,))

                insert_query = """
                    INSERT INTO individual_raw_shap_values
                    (application_id, feature_name, impact)
                    VALUES (%s, %s, %s)
                """

                rows = [
                    (
                        application_id,
                        str(item["feature_name"]),
                        float(item["impact"]),
                    )
                    for item in shap_rows
                ]
                print(f"[RAW SHAP] First 3 rows: {rows[:3]}")
                cursor.executemany(insert_query, rows)
                conn.commit()

                logger.info(
                    "Individual audit: stored %d raw SHAP rows for %s",
                    len(rows),
                    application_id
                )

            except Error as e:
                logger.error("Raw SHAP storage error: %s", e)
                raise
            finally:
                if cursor is not None:
                    cursor.close()
                if conn is not None and conn.is_connected():
                    conn.close()
    # ------------------------------------------------------------------
    # 1) INGESTION -> individual_manager_review_data
    # ------------------------------------------------------------------
    def process_ingestion_payload(self, payload: Dict[str, Any]) -> None:
        """
        Called directly by Individual Ingestion Service.

        Expected payload shape: flat customer_info payload, e.g.
        application_id, full_name, email, primary_phone, pan_number,
        employment_status, loan_amount, loan_tenure_months, ...
        """
        application_id = payload.get("application_id")
        if not application_id:
            raise ValueError("Missing application_id in ingestion payload")

        query = f"""
    INSERT INTO {self.MANAGER_REVIEW_TABLE}
    (
        application_id,
        full_name,
        first_name,
        middle_name,
        last_name,
        gender,
        date_of_birth,
        age_years,
        marital_status,
        education_qualification,
        nationality,
        pan_number,
        aadhaar_last4,
        government_id_type,
        government_id_number,
        id_document_age_years,

        family_members_count,
        children_count,
        dependents_count,
        spouse_name,

        email,
        phone,
        alternate_phone,

        employment_status,
        occupation_type,
        employer_name,
        work_experience_years_total,
        employment_years_current_job,
        monthly_income,
        annual_income_total,
        has_employment_phone,

        owns_car,
        owns_property,

        has_personal_phone,
        has_mobile,
        mobile_contactable,
        has_email,
        months_since_last_phone_change,

        current_address_line_1,
        current_address_line_2,
        city,
        state,
        postal_code,
        country,
        years_at_current_address,
        region_rating_client,
        city_rating_differs_from_region,
        registered_city_not_live_city,
        registered_city_not_work_city,
        live_city_not_work_city,

        loan_purpose,
        loan_ask,
        goods_price,
        loan_tenure_months,

        total_documents_submitted,
        status
    )
    VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,
    %s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,
    %s,'pending'
)
    ON DUPLICATE KEY UPDATE
        full_name = VALUES(full_name),
        first_name = VALUES(first_name),
        middle_name = VALUES(middle_name),
        last_name = VALUES(last_name),
        gender = VALUES(gender),
        date_of_birth = VALUES(date_of_birth),
        age_years = VALUES(age_years),
        marital_status = VALUES(marital_status),
        education_qualification = VALUES(education_qualification),
        nationality = VALUES(nationality),
        pan_number = VALUES(pan_number),
        aadhaar_last4 = VALUES(aadhaar_last4),
        government_id_type = VALUES(government_id_type),
        government_id_number = VALUES(government_id_number),
        id_document_age_years = VALUES(id_document_age_years),
        family_members_count = VALUES(family_members_count),
        children_count = VALUES(children_count),
        dependents_count = VALUES(dependents_count),
        spouse_name = VALUES(spouse_name),
        email = VALUES(email),
        phone = VALUES(phone),
        alternate_phone = VALUES(alternate_phone),
        employment_status = VALUES(employment_status),
        occupation_type = VALUES(occupation_type),
        employer_name = VALUES(employer_name),
        work_experience_years_total = VALUES(work_experience_years_total),
        employment_years_current_job = VALUES(employment_years_current_job),
        monthly_income = VALUES(monthly_income),
        annual_income_total = VALUES(annual_income_total),
        has_employment_phone = VALUES(has_employment_phone),
        owns_car = VALUES(owns_car),
        owns_property = VALUES(owns_property),
        has_personal_phone = VALUES(has_personal_phone),
        has_mobile = VALUES(has_mobile),
        mobile_contactable = VALUES(mobile_contactable),
        has_email = VALUES(has_email),
        months_since_last_phone_change = VALUES(months_since_last_phone_change),
        current_address_line_1 = VALUES(current_address_line_1),
        current_address_line_2 = VALUES(current_address_line_2),
        city = VALUES(city),
        state = VALUES(state),
        postal_code = VALUES(postal_code),
        country = VALUES(country),
        years_at_current_address = VALUES(years_at_current_address),
        region_rating_client = VALUES(region_rating_client),
        city_rating_differs_from_region = VALUES(city_rating_differs_from_region),
        registered_city_not_live_city = VALUES(registered_city_not_live_city),
        registered_city_not_work_city = VALUES(registered_city_not_work_city),
        live_city_not_work_city = VALUES(live_city_not_work_city),
        loan_purpose = VALUES(loan_purpose),
        loan_ask = VALUES(loan_ask),
        goods_price = VALUES(goods_price),
        loan_tenure_months = VALUES(loan_tenure_months),
        total_documents_submitted = VALUES(total_documents_submitted),
        updated_at = CURRENT_TIMESTAMP
"""

        params = (
            self._safe_str(payload.get("application_id")),
            self._safe_str(payload.get("full_name")),
            self._safe_str(payload.get("first_name")),
            self._safe_str(payload.get("middle_name")),
            self._safe_str(payload.get("last_name")),
            self._safe_str(payload.get("gender")),
            self._safe_str(payload.get("date_of_birth")),
            self._safe_int(payload.get("age_years")),
            self._safe_str(payload.get("marital_status")),
            self._safe_str(payload.get("education_qualification")),
            self._safe_str(payload.get("nationality")),
            self._safe_str(payload.get("pan_number")),
            self._safe_str(payload.get("aadhaar_last4")),
            self._safe_str(payload.get("government_id_type")),
            self._safe_str(payload.get("government_id_number")),
            self._safe_float(payload.get("id_document_age_years")),

            self._safe_int(payload.get("family_members_count")),
            self._safe_int(payload.get("children_count")),
            self._safe_int(payload.get("dependents_count")),
            self._safe_str(payload.get("spouse_name")),

            self._safe_str(payload.get("email")),
            self._safe_str(payload.get("primary_phone") or payload.get("phone")),
            self._safe_str(payload.get("alternate_phone")),

            self._safe_str(payload.get("employment_status")),
            self._safe_str(payload.get("occupation_type")),
            self._safe_str(payload.get("employer_name")),
            self._safe_float(payload.get("work_experience_years_total")),
            self._safe_float(payload.get("employment_years_current_job")),
            self._safe_float(payload.get("monthly_income")),
            self._safe_float(payload.get("annual_income_total")),
            self._as_bool_int(payload.get("has_employment_phone")),

            self._as_bool_int(payload.get("owns_car")),
            self._as_bool_int(payload.get("owns_property")),

            self._as_bool_int(payload.get("has_personal_phone")),
            self._as_bool_int(payload.get("has_mobile")),
            self._as_bool_int(payload.get("mobile_contactable")),
            self._as_bool_int(payload.get("has_email")),
            self._safe_float(payload.get("months_since_last_phone_change")),

            self._safe_str(payload.get("current_address_line_1")),
            self._safe_str(payload.get("current_address_line_2")),
            self._safe_str(payload.get("city")),
            self._safe_str(payload.get("state")),
            self._safe_str(payload.get("postal_code")),
            self._safe_str(payload.get("country")),
            self._safe_float(payload.get("years_at_current_address")),
            self._safe_int(payload.get("region_rating_client")),
            self._as_bool_int(payload.get("city_rating_differs_from_region")),
            self._as_bool_int(payload.get("registered_city_not_live_city")),
            self._as_bool_int(payload.get("registered_city_not_work_city")),
            self._as_bool_int(payload.get("live_city_not_work_city")),

            self._safe_str(payload.get("loan_purpose")),
            self._safe_float(payload.get("loan_amount") or payload.get("loan_ask")),
            self._safe_float(payload.get("goods_price")),
            self._safe_int(payload.get("loan_tenure_months")),

            self._safe_int(payload.get("total_documents_submitted")),
        )

        self._execute_query(query, params)
        logger.info("Individual audit: ingestion row inserted/updated for %s", application_id)

    # ------------------------------------------------------------------
    # 2) SCORING -> individual_manager_review_data
    # ------------------------------------------------------------------
    def process_scoring_payload(self, payload: Dict[str, Any]) -> None:
        """
        Called directly by Individual Scoring Service.

        Expected fields:
        - application_id
        - risk_score
        - risk_band
        - decision
        - approved_principal OR suggested_principal
        - suggested_interest_rate
        """
        application_id = payload.get("application_id")
        if not application_id:
            raise ValueError("Missing application_id in scoring payload")

        suggested_principal = payload.get("approved_principal", payload.get("suggested_principal"))
        suggested_interest_rate = payload.get("suggested_interest_rate")

        query = f"""
            UPDATE {self.MANAGER_REVIEW_TABLE}
            SET risk_score = %s,
                risk_band = %s,
                decision = %s,
                suggested_principal = %s,
                suggested_interest_rate = %s,
                status = 'scored',
                updated_at = CURRENT_TIMESTAMP
            WHERE application_id = %s
        """

        params = (
            self._safe_float(payload.get("risk_score")),
            self._safe_str(payload.get("risk_band")),
            self._safe_str(payload.get("decision")),
            self._safe_float(suggested_principal),
            self._safe_float(suggested_interest_rate),
            self._safe_str(application_id),
        )

        self._execute_query(query, params)
        logger.info("Individual audit: scoring data updated for %s", application_id)

    # ------------------------------------------------------------------
    # 3) EXPLAINABILITY -> individual_manager_review_data
    # ------------------------------------------------------------------
    def process_explainability_payload(self, payload: Dict[str, Any]) -> None:
        """
        Called directly by Individual Explainability Service.

        Expected fields:
        - application_id
        - shap_waterfall_base64 OR shap_plot_base64
        - optionally recommended_principal / recommended_interest_rate_annual
        """
        application_id = payload.get("application_id")
        if not application_id:
            raise ValueError("Missing application_id in explainability payload")

        shap_plot = payload.get("shap_waterfall_base64", payload.get("shap_plot_base64"))
        recommended_principal = payload.get("recommended_principal")
        recommended_interest_rate = payload.get(
            "recommended_interest_rate_annual",
            payload.get("suggested_interest_rate"),
        )

        query = f"""
            UPDATE {self.MANAGER_REVIEW_TABLE}
            SET shap_plot_base64 = %s,
                suggested_principal = COALESCE(%s, suggested_principal),
                suggested_interest_rate = COALESCE(%s, suggested_interest_rate),
                status = 'explained',
                updated_at = CURRENT_TIMESTAMP
            WHERE application_id = %s
        """

        params = (
            self._safe_str(shap_plot),
            self._safe_float(recommended_principal),
            self._safe_float(recommended_interest_rate),
            self._safe_str(application_id),
        )

        self._execute_query(query, params)
        logger.info("Individual audit: explainability data updated for %s", application_id)

    # ------------------------------------------------------------------
    # 4) MANAGER PDF -> individual_manager_review_data
    # ------------------------------------------------------------------
    def process_pdf_payload(self, payload: Dict[str, Any], pdf_bytes: bytes) -> None:
        """
        Called directly by Individual Manager PDF Service.

        Stores the manager PDF and marks application ready_for_manager.
        """
        application_id = payload.get("application_id")
        if not application_id:
            raise ValueError("Missing application_id in manager PDF payload")

        filename = payload.get("manager_pdf_filename") or f"Manager_Report_{application_id}.pdf"

        query = f"""
            UPDATE {self.MANAGER_REVIEW_TABLE}
            SET manager_pdf_blob = %s,
                manager_pdf_filename = %s,
                status = 'ready_for_manager',
                updated_at = CURRENT_TIMESTAMP
            WHERE application_id = %s
        """

        params = (
            pdf_bytes,
            self._safe_str(filename),
            self._safe_str(application_id),
        )

        self._execute_query(query, params)
        logger.info("Individual audit: manager PDF stored for %s", application_id)

    # ------------------------------------------------------------------
    # 5) FINALIZATION -> shared msme_final_decision
    # ------------------------------------------------------------------
    def process_final_decision_payload(self, payload: Dict[str, Any]) -> None:
        """
        Called directly by Individual Finalization Service.

        Writes to shared final decision table:
        msme_final_decision with application_type='individual'

        Expected fields:
        - application_id
        - model_decision
        - model_risk_score
        - model_suggested_principal
        - model_suggested_interest_rate
        - model_approved_tenure
        - manager_decision
        - final_principal
        - final_interest_rate
        - final_tenure
        - override_flag
        - manager_notes
        - manager_id
        """
        application_id = payload.get("application_id")
        manager_decision = payload.get("manager_decision")

        if not application_id:
            raise ValueError("Missing application_id in final decision payload")
        if not manager_decision:
            raise ValueError("Missing manager_decision in final decision payload")

        query = f"""
            INSERT INTO {self.FINAL_DECISION_TABLE}
            (
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
                override_flag,
                manager_notes,
                manager_id,
                application_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'individual')
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
                override_flag = VALUES(override_flag),
                manager_notes = VALUES(manager_notes),
                manager_id = VALUES(manager_id),
                application_type = 'individual',
                updated_at = CURRENT_TIMESTAMP
        """

        params = (
            self._safe_str(application_id),
            self._safe_str(payload.get("model_decision")),
            self._safe_float(payload.get("model_risk_score")),
            self._safe_float(payload.get("model_suggested_principal")),
            self._safe_float(payload.get("model_suggested_interest_rate")),
            self._safe_int(payload.get("model_approved_tenure")),
            self._safe_str(manager_decision),
            self._safe_float(payload.get("final_principal")),
            self._safe_float(payload.get("final_interest_rate")),
            self._safe_int(payload.get("final_tenure")),
            self._as_bool_int(payload.get("override_flag", 0)),
            self._safe_str(payload.get("manager_notes")),
            self._safe_str(payload.get("manager_id")),
        )

        self._execute_query(query, params)
        logger.info("Individual audit: final decision upserted for %s", application_id)

        # Also mark manager review row as reviewed
        mark_query = f"""
            UPDATE {self.MANAGER_REVIEW_TABLE}
            SET status = 'reviewed',
                updated_at = CURRENT_TIMESTAMP
            WHERE application_id = %s
        """
        self._execute_query(mark_query, (self._safe_str(application_id),))
        logger.info("Individual audit: manager review row marked reviewed for %s", application_id)

    # ------------------------------------------------------------------
    # 6) CUSTOMER PDF -> shared msme_final_decision
    # ------------------------------------------------------------------
    def process_customer_pdf_payload(
        self,
        application_id: str,
        pdf_bytes: bytes,
        filename: Optional[str] = None,
    ) -> None:
        """
        Called directly by Individual Customer PDF Generator Service.

        Attaches the final customer PDF to shared final decision table.
        """
        if not application_id:
            raise ValueError("Missing application_id for customer PDF update")

        if not filename:
            filename = f"Customer_Report_{application_id}.pdf"

        query = f"""
            UPDATE {self.FINAL_DECISION_TABLE}
            SET customer_pdf_blob = %s,
                customer_pdf_filename = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE application_id = %s
              AND application_type = 'individual'
        """

        params = (
            pdf_bytes,
            self._safe_str(filename),
            self._safe_str(application_id),
        )

        self._execute_query(query, params)
        logger.info("Individual audit: customer PDF stored for %s", application_id)

    # ------------------------------------------------------------------
    # 7) Optional fetch helpers for services
    # ------------------------------------------------------------------
    def get_manager_review_row(self, application_id: str) -> Optional[Dict[str, Any]]:
        query = f"""
            SELECT *
            FROM {self.MANAGER_REVIEW_TABLE}
            WHERE application_id = %s
            LIMIT 1
        """
        return self._execute_query(query, (application_id,), fetchone=True)

    def get_final_decision_row(self, application_id: str) -> Optional[Dict[str, Any]]:
        query = f"""
            SELECT *
            FROM {self.FINAL_DECISION_TABLE}
            WHERE application_id = %s
              AND application_type = 'individual'
            ORDER BY id DESC
            LIMIT 1
        """
        return self._execute_query(query, (application_id,), fetchone=True)


# Singleton instance
audit_service = AuditServiceIndividual()