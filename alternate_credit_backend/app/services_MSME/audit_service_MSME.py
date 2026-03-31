import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging
from app.config import get_mysql_config



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuditService:
    def __init__(self):
       self.config = get_mysql_config()

    def _get_connection(self):
        return mysql.connector.connect(**self.config)

    def process_ingestion_payload(self, payload: dict):
        """
        Called by MSMEIngestionService.
        payload is the 'msme_info_payload' dict.
        Inserts the initial manager-review row.
        """
        application_id = payload.get("application_id")
        biz_id = payload.get("business_identity", {})
        loan_req = payload.get("loan_requirements", {})

        query = """
            INSERT INTO msme_manager_review_data
            (
                application_id,
                company_name,
                applicant_name,
                entity_type,
                registration_number,
                sector_id,
                loan_ask,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
            ON DUPLICATE KEY UPDATE
                company_name = VALUES(company_name),
                applicant_name = VALUES(applicant_name),
                entity_type = VALUES(entity_type),
                registration_number = VALUES(registration_number),
                sector_id = VALUES(sector_id),
                loan_ask = VALUES(loan_ask),
                updated_at = CURRENT_TIMESTAMP
        """

        params = (
            application_id,
            biz_id.get("company_name"),
            biz_id.get("contact_person"),  # or None if not present
            biz_id.get("entity_type"),
            biz_id.get("registration_number"),
            biz_id.get("sector_id"),
            loan_req.get("loan_amt", 0.0),
        )

        self._execute_query(query, params)
        logger.info(f"Audit: ingestion row inserted/updated for {application_id}")

    def process_explainability_payload(self, payload: dict):
        application_id = payload.get("application_id")

        query = """
        UPDATE msme_manager_review_data
        SET shap_plot_base64 = %s,
            updated_at = CURRENT_TIMESTAMP,
            status = 'explained'
        WHERE application_id = %s
    """

        params = (
        payload.get("shap_waterfall_base64"),
        application_id,
    )

        self._execute_query(query, params)
        logger.info(f"Audit: explainability data updated for {application_id}")

    def process_scoring_payload(self, payload: dict):
        application_id = payload.get("application_id")

        query = """
        UPDATE msme_manager_review_data
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
        payload.get("risk_score"),
        payload.get("risk_band"),
        payload.get("decision"),
        payload.get("approved_principal"),
        payload.get("suggested_interest_rate"),
        application_id,
    )

        self._execute_query(query, params)

    def process_pdf_payload(self, payload: dict, pdf_bytes: bytes):
        """
        Called by MSMEManagerPDFService.
        Stores the PDF and marks the application as ready_for_manager.
        """
        application_id = payload.get("application_id")

        query = """
            UPDATE msme_manager_review_data
            SET manager_pdf_blob = %s,
                manager_pdf_filename = %s,
                status = 'ready_for_manager',
                updated_at = CURRENT_TIMESTAMP
            WHERE application_id = %s
        """

        params = (
            pdf_bytes,
            f"Manager_Report_{application_id}.pdf",
            application_id,
        )

        self._execute_query(query, params)
        logger.info(f"Audit: PDF stored and status set to READY for {application_id}")

    def _execute_query(self, query, params):
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
        except Error as e:
            logger.error(f"Database Error: {e}")
            raise
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()


# Singleton instance
audit_service = AuditService()