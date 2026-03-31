from __future__ import annotations

import mysql.connector
from app.config import get_mysql_config

import base64
import json
import os
import tempfile
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from fpdf import FPDF
from kafka import KafkaConsumer, KafkaProducer

try:
    from app.services_individual.audit_service_individual import audit_service
except ImportError:
    from audit_service_individual import audit_service

from app.config import KAFKA_BROKER


# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
CUST_INFO = "customer_info"
MODEL_OUTPUT = "model_output"
EXPLAINABILITY_OUTPUT = "explainability_output"
MANAGER_PDF = "manager_pdf"


# ---------------------------------------------------------------------
# FEATURE EXPLANATIONS
# Keep this concise and manager-friendly
# ---------------------------------------------------------------------
FEATURE_EXPLANATIONS = {
    "EXT_SOURCE_WEIGHTED": "Combined external bureau score signal. Higher quality bureau signals generally reduce default risk.",
    "Total_Debt_to_Income": "Ratio of requested credit burden to applicant income. Higher values indicate affordability stress.",
    "Monthly_Headroom": "Estimated monthly surplus after EMI burden. Negative headroom is a strong affordability warning.",
    "Multi_Source_Safety_Score": "Composite score from payment behavior, bureau strength, employment stability, and approvals.",
    "Consistent_Payer_Score": "Measures how consistently the applicant has paid prior dues or installments.",
    "Bureau_Safety_Score": "Reflects cleanliness and closure quality of bureau-recorded credit lines.",
    "Career_Stability_Score": "Represents employment continuity relative to age and work history.",
    "Predatory_Loan_Signal": "Flags whether the implied EMI burden appears unusually aggressive for the requested loan.",
    "Annuity_Stretch": "Measures how stretched the EMI is relative to monthly income.",
    "Digital_Trust_Score": "Proxy based on contact availability and traceability signals.",
    "Identity_Maturity_Score": "Approximate maturity of identity document history.",
    "Residency_Stability": "Approximate stability of current address duration.",
    "Address_Integrity_Flag": "Counts mismatches between registered, living, and work city indicators.",
    "Refusal_Persistence_Ratio": "Measures how often prior loan applications were refused relative to approvals.",
    "Behavioral_Consistency_Score": "Compares repayment behavior with income and bureau context.",
}


# ---------------------------------------------------------------------
# PDF CLASS
# ---------------------------------------------------------------------
class MonarchCreditPDF(FPDF):
    def header(self):
        self.set_fill_color(245, 249, 255)
        self.rect(0, 0, 210, 22, style="F")

        self.set_xy(10, 8)
        self.set_text_color(30, 102, 245)
        self.set_font("Helvetica", "B", 20)
        self.cell(0, 8, "MonarchCredit", ln=1)

        self.set_x(10)
        self.set_text_color(90, 90, 90)
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, "Individual Manager Risk Report", ln=1)

        self.ln(4)

    def footer(self):
        self.set_y(-12)
        self.set_text_color(120, 120, 120)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 8, f"Page {self.page_no()}", align="C")

    def section_title(self, title: str):
        self.ln(2)
        self.set_fill_color(232, 240, 254)
        self.set_text_color(25, 25, 25)
        self.set_font("Helvetica", "B", 12)
        self.cell(0, 8, title, ln=1, fill=True)
        self.ln(1)

    def key_value(self, key: str, value: str):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(35, 35, 35)
        self.cell(58, 6, key)
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, value)

    def bullet_line(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 6, f"- {text}")


# ---------------------------------------------------------------------
# SERVICE
# ---------------------------------------------------------------------
class IndividualManagerPDFService:
    def __init__(self, kafka_broker: str = KAFKA_BROKER):
        self.kafka_broker = kafka_broker

        self.consumer = KafkaConsumer(
            CUST_INFO,
            MODEL_OUTPUT,
            EXPLAINABILITY_OUTPUT,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="individual_manager_pdf_service_group_v2",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.cust_info_cache: Dict[str, Dict[str, Any]] = {}
        self.model_output_cache: Dict[str, Dict[str, Any]] = {}
        self.explainability_cache: Dict[str, Dict[str, Any]] = {}

    # -----------------------------------------------------------------
    # UTILS
    # -----------------------------------------------------------------

    def _get_connection(self):
        return mysql.connector.connect(**get_mysql_config())

    def _fetch_customer_info_from_db(self, application_id: str) -> Optional[Dict[str, Any]]:
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute(
                """
                SELECT
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
                    phone AS primary_phone,
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
                    loan_ask AS loan_amount,
                    goods_price,
                    loan_tenure_months,
                    total_documents_submitted,
                    suggested_principal,
                    suggested_interest_rate,
                    status
                FROM individual_manager_review_data
                WHERE application_id = %s
                LIMIT 1
                """,
                (application_id,),
            )

            row = cursor.fetchone()
            return row

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()
    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _safe_get(d: Dict[str, Any], *keys, default="N/A"):
        cur = d
        try:
            for k in keys:
                cur = cur[k]
            if cur is None:
                return default
            return cur
        except Exception:
            return default

    @staticmethod
    def _format_bool(value: Any) -> str:
        if isinstance(value, bool):
            return "Yes" if value else "No"
        return str(value)

    @staticmethod
    def _fmt_currency(value: Any) -> str:
        if value is None or value == "":
            return "N/A"
        try:
            v = float(value)
            return f"Rs {v:,.0f}"
        except Exception:
            return "N/A"

    @staticmethod
    def _fmt_float(value: Any, ndigits: int = 4) -> str:
        try:
            return f"{float(value):.{ndigits}f}"
        except Exception:
            return str(value)

    
    @staticmethod
    def _decode_base64_to_temp_png(image_b64: str, prefix: str) -> str:
        if not image_b64:
            raise ValueError("Empty base64 image data")

        if "," in image_b64 and image_b64.strip().startswith("data:image"):
            image_b64 = image_b64.split(",", 1)[1]

        image_bytes = base64.b64decode(image_b64)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png", prefix=prefix)
        tmp.write(image_bytes)
        tmp.flush()
        tmp.close()
        return tmp.name

    @staticmethod
    def _cleanup_temp_files(paths):
        for p in paths:
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

    def _merge_ready_payloads(
        self,
        application_id: str
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
        a = self.cust_info_cache.get(application_id)
        b = self.model_output_cache.get(application_id)
        c = self.explainability_cache.get(application_id)

        # DB fallback for customer_info
        if a is None:
            a = self._fetch_customer_info_from_db(application_id)
            if a is not None:
                self.cust_info_cache[application_id] = a
                print(f"[DB FALLBACK] Loaded customer_info from DB for {application_id}")

        if a is None or b is None or c is None:
            return None

        return a, b, c

    # -----------------------------------------------------------------
    # CONTENT BUILDERS
    # -----------------------------------------------------------------
    def _selected_customer_details(self, cust_info: Dict[str, Any]) -> Dict[str, str]:
        return {
            "Full Name": str(cust_info.get("full_name", "N/A")),
            "Gender": str(cust_info.get("gender", "N/A")),
            "Date of Birth": str(cust_info.get("date_of_birth", "N/A")),
            "Age": str(cust_info.get("age_years", "N/A")),
            "Marital Status": str(cust_info.get("marital_status", "N/A")),
            "Education": str(cust_info.get("education_qualification", "N/A")),
            "Nationality": str(cust_info.get("nationality", "N/A")),
            "PAN Number": str(cust_info.get("pan_number", "N/A")),
            "Govt ID Type": str(cust_info.get("government_id_type", "N/A")),
            "Govt ID Number": str(cust_info.get("government_id_number", "N/A")),
            "ID Document Age": str(cust_info.get("id_document_age_years", "N/A")),
            "Phone": str(cust_info.get("primary_phone", "N/A")),
            "Email": str(cust_info.get("email", "N/A")),
            "Employment Status": str(cust_info.get("employment_status", "N/A")),
            "Occupation": str(cust_info.get("occupation_type", "N/A")),
            "Employer Name": str(cust_info.get("employer_name", "N/A")),
            "Current Job Tenure": f"{cust_info.get('employment_years_current_job', 'N/A')} years",
            "Total Work Experience": f"{cust_info.get('work_experience_years_total', 'N/A')} years",
            "Monthly Income": self._fmt_currency(cust_info.get("monthly_income", 0)),
            "Annual Income": self._fmt_currency(cust_info.get("annual_income_total", 0)),
            "Owns Car": self._format_bool(cust_info.get("owns_car", "N/A")),
            "Owns Property": self._format_bool(cust_info.get("owns_property", "N/A")),
            "Family Members": str(cust_info.get("family_members_count", "N/A")),
            "Children": str(cust_info.get("children_count", "N/A")),
            "Dependents": str(cust_info.get("dependents_count", "N/A")),
            "Spouse Name": str(cust_info.get("spouse_name", "N/A")),
            "Address": (
                f"{cust_info.get('current_address_line_1', '')}, "
                f"{cust_info.get('current_address_line_2', '')}, "
                f"{cust_info.get('city', '')}, "
                f"{cust_info.get('state', '')}, "
                f"{cust_info.get('postal_code', '')}, "
                f"{cust_info.get('country', '')}"
            ).strip(", "),
            "Years At Current Address": str(cust_info.get("years_at_current_address", "N/A")),
            "Loan Purpose": str(cust_info.get("loan_purpose", "N/A")),
            "Requested Loan Amount": self._fmt_currency(cust_info.get("loan_amount", 0)),
            "Requested Tenure": f"{cust_info.get('loan_tenure_months', 'N/A')} months",
            "Goods Price": self._fmt_currency(cust_info.get("goods_price")),
        }

    def _driver_lines(self, explainability_output: Dict[str, Any], key: str):
        items = explainability_output.get(key, [])
        lines = []
        if isinstance(items, list):
            for x in items:
                feat = x.get("feature", "Unknown")
                shap_impact = x.get("shap_impact", 0.0)
                fval = x.get("feature_value", "N/A")
                if isinstance(fval, (int, float)):
                    fval = self._fmt_float(fval, 4)
                lines.append(f"{feat}: impact={self._fmt_float(shap_impact, 4)}, value={fval}")
        return lines

    # -----------------------------------------------------------------
    # PDF GENERATION
    # -----------------------------------------------------------------
    def _generate_pdf_bytes(
        self,
        application_id: str,
        cust_info: Dict[str, Any],
        model_output: Dict[str, Any],
        explainability_output: Dict[str, Any],
    ) -> bytes:
        pdf = MonarchCreditPDF()
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_page()

        # -------------------------------------------------------------
        # CUSTOMER DETAILS
        # -------------------------------------------------------------
        pdf.section_title("Applicant Details")
        details = self._selected_customer_details(cust_info)
        for k, v in details.items():
            pdf.key_value(k, v)

        # -------------------------------------------------------------
        # RISK SUMMARY
        # -------------------------------------------------------------
        pdf.section_title("Risk Summary")
        pdf.key_value("Application ID", application_id)
        pdf.key_value("Risk Score", self._fmt_float(model_output.get("risk_score"), 4))
        pdf.key_value("Risk Band", str(model_output.get("risk_band", "N/A")))
        pdf.key_value("Decision", str(model_output.get("decision", "N/A")))
        pdf.key_value("Decision Reason", str(model_output.get("decision_reason", "N/A")))
        pdf.key_value("Suggested Principal", self._fmt_currency(model_output.get("suggested_principal", 0)))
        pdf.key_value(
            "Suggested Interest Rate",
            f"{self._fmt_float(model_output.get('suggested_interest_rate'), 2)}%"
            if model_output.get("suggested_interest_rate") is not None else "N/A"
        )
        pdf.key_value("Approved Tenure", str(model_output.get("approved_tenure", "N/A")))
        pdf.key_value("Expected Loss", self._fmt_currency(model_output.get("expected_loss", 0)))
        pdf.key_value("Interest Revenue", self._fmt_currency(model_output.get("interest_revenue", 0)))
        pdf.key_value("Net Expected Profit", self._fmt_currency(model_output.get("net_expected_profit", 0)))
        pdf.key_value("Model Name", str(model_output.get("model_name", "N/A")))
        pdf.key_value("Model Version", str(model_output.get("model_version", "N/A")))
        pdf.key_value("Generated At", str(explainability_output.get("generated_at", self._utc_now_iso())))
        pdf.key_value("Summary", str(explainability_output.get("explanation_summary", "No summary available")))

        # -------------------------------------------------------------
        # KEY SIGNALS
        # -------------------------------------------------------------
        pdf.section_title("Key Signals")
        pdf.key_value("EXT_SOURCE_WEIGHTED", self._fmt_float(explainability_output.get("ext_source_weighted"), 4))
        pdf.key_value("Monthly Headroom", self._fmt_currency(explainability_output.get("monthly_headroom", 0)))
        pdf.key_value("Debt To Income", self._fmt_float(explainability_output.get("debt_to_income"), 4))
        pdf.key_value("Safety Score", self._fmt_float(explainability_output.get("safety_score"), 4))
        pdf.key_value("Payer Score", self._fmt_float(explainability_output.get("payer_score"), 4))
        pdf.key_value("Bureau Safety Score", self._fmt_float(explainability_output.get("bureau_safety_score"), 4))
        pdf.key_value(
            "Predatory Loan Signal",
            "Yes" if explainability_output.get("predatory_loan_signal") else "No"
        )

        # -------------------------------------------------------------
        # PLOTS
        # -------------------------------------------------------------
        temp_files = []
        try:
            gauge_b64 = explainability_output.get("gauge_chart_base64")
            shap_b64 = explainability_output.get("shap_waterfall_base64")

            gauge_path = None
            shap_path = None

            if gauge_b64:
                gauge_path = self._decode_base64_to_temp_png(gauge_b64, "gauge_")
                temp_files.append(gauge_path)

            if shap_b64:
                shap_path = self._decode_base64_to_temp_png(shap_b64, "shap_")
                temp_files.append(shap_path)

            pdf.section_title("Visual Explainability")

            if gauge_path:
                pdf.set_font("Helvetica", "B", 10)
                pdf.cell(0, 6, "Risk Gauge", ln=1)
                pdf.image(gauge_path, x=25, w=160)
                pdf.ln(2)

            if shap_path:
                if pdf.get_y() > 180:
                    pdf.add_page()
                pdf.set_font("Helvetica", "B", 10)
                pdf.cell(0, 6, "SHAP Waterfall Plot", ln=1)
                pdf.image(shap_path, x=12, w=186)
                pdf.ln(2)

            # ---------------------------------------------------------
            # TOP DRIVERS
            # ---------------------------------------------------------
            pdf.section_title("Top Positive Drivers")
            pos_lines = self._driver_lines(explainability_output, "top_positive_drivers")
            if pos_lines:
                for line in pos_lines:
                    pdf.bullet_line(line)
            else:
                pdf.bullet_line("No positive drivers available.")

            pdf.section_title("Top Negative Drivers")
            neg_lines = self._driver_lines(explainability_output, "top_negative_drivers")
            if neg_lines:
                for line in neg_lines:
                    pdf.bullet_line(line)
            else:
                pdf.bullet_line("No negative drivers available.")

            # ---------------------------------------------------------
            # FEATURE GUIDE
            # ---------------------------------------------------------
            pdf.add_page()
            pdf.section_title("Feature Guide for Manager Review")
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(
                0, 6,
                "Below are short explanations of key individual-credit features shown in the SHAP analysis. "
                "These help interpret what each variable broadly represents in the personal loan risk assessment."
            )
            pdf.ln(2)

            for feat, text in FEATURE_EXPLANATIONS.items():
                if pdf.get_y() > 270:
                    pdf.add_page()
                pdf.set_font("Helvetica", "B", 10)
                pdf.multi_cell(0, 6, feat)
                pdf.set_font("Helvetica", "", 10)
                pdf.multi_cell(0, 6, text)
                pdf.ln(1)

            pdf_bytes = pdf.output(dest="S").encode("latin1")
            return pdf_bytes

        finally:
            self._cleanup_temp_files(temp_files)

    # -----------------------------------------------------------------
    # KAFKA PUBLISH
    # -----------------------------------------------------------------
    def _build_output_payload(
        self,
        application_id: str,
        pdf_bytes: bytes,
        model_output: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "application_id": application_id,
            "document_type": "manager_pdf_individual",
            "risk_score": model_output.get("risk_score"),
            "risk_band": model_output.get("risk_band"),
            "pdf_base64": base64.b64encode(pdf_bytes).decode("utf-8"),
            "manager_pdf_filename": f"Individual_Manager_Report_{application_id}.pdf",
            "generated_at": self._utc_now_iso(),
            "status": "SUCCESS",
        }

    def publish_output(self, payload: Dict[str, Any]) -> None:
        self.producer.send(MANAGER_PDF, value=payload)
        self.producer.flush()

    # -----------------------------------------------------------------
    # RUNNER
    # -----------------------------------------------------------------
    def run(self):
        print("=" * 80)
        print("Individual Manager PDF Service Started")
        print(f"Consuming topics : {CUST_INFO}, {MODEL_OUTPUT}, {EXPLAINABILITY_OUTPUT}")
        print(f"Publishing topic : {MANAGER_PDF}")
        print(f"Kafka broker     : {self.kafka_broker}")
        print("=" * 80)

        for message in self.consumer:
            try:
                topic = message.topic
                payload = message.value
                application_id = payload.get("application_id")

                if not application_id:
                    print("[WARN] Skipping message without application_id")
                    continue

                if topic == CUST_INFO:
                    self.cust_info_cache[application_id] = payload
                    print(f"[INFO] Cached customer_info for application_id={application_id}")

                elif topic == MODEL_OUTPUT:
                    self.model_output_cache[application_id] = payload
                    print(f"[INFO] Cached model_output for application_id={application_id}")

                elif topic == EXPLAINABILITY_OUTPUT:
                    self.explainability_cache[application_id] = payload
                    print(f"[INFO] Cached explainability_output for application_id={application_id}")

                merged = self._merge_ready_payloads(application_id)
                if merged is None:
                    print(
                        f"[INFO] Waiting for all 3 payloads for {application_id} | "
                        f"customer_info={'yes' if application_id in self.cust_info_cache else 'no'}, "
                        f"model_output={'yes' if application_id in self.model_output_cache else 'no'}, "
                        f"explainability={'yes' if application_id in self.explainability_cache else 'no'}"
                    )
                    continue

                cust_info, model_output, explainability_output = merged

                pdf_bytes = self._generate_pdf_bytes(
                    application_id=application_id,
                    cust_info=cust_info,
                    model_output=model_output,
                    explainability_output=explainability_output,
                )

                output_payload = self._build_output_payload(
                    application_id=application_id,
                    pdf_bytes=pdf_bytes,
                    model_output=model_output,
                )

                # 1. Publish PDF metadata/base64 to Kafka
                self.publish_output(output_payload)

                # 2. Store raw bytes in MySQL through audit service
                try:
                    print(f"[DEBUG] PDF byte size for {application_id}: {len(pdf_bytes)}")
                    audit_service.process_pdf_payload(output_payload, pdf_bytes)
                    print(f"[DB SUCCESS] PDF stored in MySQL for {application_id}")
                except Exception as db_exc:
                    print(f"[DB ERROR] Failed to store PDF in MySQL: {db_exc}")
                    traceback.print_exc()

                print(f"[SUCCESS] Published manager PDF for application_id={application_id}")

                # Cleanup caches after success
                self.cust_info_cache.pop(application_id, None)
                self.model_output_cache.pop(application_id, None)
                self.explainability_cache.pop(application_id, None)

            except Exception as exc:
                print("[ERROR] Failed to generate individual manager PDF")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_manager_pdf_service():
    service = IndividualManagerPDFService()
    service.run()


if __name__ == "__main__":
    run_manager_pdf_service()