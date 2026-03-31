from __future__ import annotations

import base64
import json
import os
import tempfile
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import mysql.connector
from fpdf import FPDF
from kafka import KafkaConsumer

from app.config import get_mysql_config, KAFKA_BROKER


# ---------------------------------------------------------------------
# TOPICS
# ---------------------------------------------------------------------
CUST_INFO = "customer_info"
EXPLAINABILITY_OUTPUT = "explainability_output"
CUSTOMER_PDF = "customer_pdf"


# ---------------------------------------------------------------------
# FEATURE EXPLANATIONS
# ---------------------------------------------------------------------
FEATURE_EXPLANATIONS = {
    "EXT_SOURCE_WEIGHTED": "This is a combined external credit-bureau score signal. Stronger external score signals generally support lower credit risk.",
    "Total_Debt_to_Income": "This measures the debt burden relative to income. Higher values indicate stronger affordability stress.",
    "Monthly_Headroom": "This shows estimated surplus income after considering repayment burden. Negative headroom is a warning sign.",
    "Multi_Source_Safety_Score": "This is a combined safety score using repayment history, bureau behavior, employment stability, and related signals.",
    "Consistent_Payer_Score": "This reflects how consistently the applicant has paid prior obligations.",
    "Bureau_Safety_Score": "This captures the quality and cleanliness of bureau-recorded credit history.",
    "Career_Stability_Score": "This indicates how stable the applicant’s work profile appears relative to age and employment duration.",
    "Predatory_Loan_Signal": "This flags whether the loan structure appears unusually stressful relative to repayment capacity.",
    "Annuity_Stretch": "This measures how stretched the EMI burden is relative to monthly income.",
    "Digital_Trust_Score": "This is a proxy based on contact availability and digital traceability signals.",
    "Identity_Maturity_Score": "This reflects the maturity of identity document history.",
    "Residency_Stability": "This reflects how stable the current residence history appears.",
    "Address_Integrity_Flag": "This counts mismatch signals across registered, living, and work locations.",
    "Behavioral_Consistency_Score": "This compares repayment behavior with income and credit history context.",
    "Refusal_Persistence_Ratio": "This measures how often prior applications were refused relative to approvals.",
    "Income_Plausibility_Score": "This compares reported income against requested credit burden.",
    "Household_Survival_Margin": "This estimates remaining annual income after the repayment burden.",
    "Survival_Per_Capita": "This adjusts household margin by family size to approximate per-person resilience.",
    "Bureau_Recent_Stress": "This captures recent bureau-linked credit stress signals.",
    "Negative_Headroom_Flag": "This flags whether the repayment burden appears to exceed monthly income comfort.",
}


def pdf_safe_text(value: Any) -> str:
    """
    Convert text to a Latin-1 safe string for pyfpdf.
    Replaces smart quotes, dashes, bullets, rupee symbol, etc.
    """
    if value is None:
        return "N/A"

    text = str(value)

    replacements = {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u2022": "-",
        "\u2026": "...",
        "\u00a0": " ",
        "\u20b9": "Rs ",
    }

    for bad, good in replacements.items():
        text = text.replace(bad, good)

    return text.encode("latin-1", "replace").decode("latin-1")


# ---------------------------------------------------------------------
# PDF CLASS
# ---------------------------------------------------------------------
class MonarchCustomerPDF(FPDF):
    def header(self):
        self.set_fill_color(244, 248, 255)
        self.rect(0, 0, 210, 26, style="F")

        self.set_xy(12, 8)
        self.set_text_color(28, 102, 245)
        self.set_font("Helvetica", "B", 21)
        self.cell(0, 8, "MonarchCredit", ln=1)

        self.set_x(12)
        self.set_text_color(95, 95, 95)
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, "Individual Customer Loan Decision Report", ln=1)

        self.ln(6)

    def footer(self):
        self.set_y(-16)
        self.set_text_color(120, 120, 120)
        self.set_font("Helvetica", "I", 8)
        self.cell(
            0,
            5,
            "This is a system-generated report. Please preserve it for your records.",
            ln=1,
            align="C",
        )
        self.cell(0, 5, f"Page {self.page_no()}", align="C")

    def section_title(self, title: str):
        self.ln(2)
        self.set_fill_color(232, 240, 254)
        self.set_text_color(20, 20, 20)
        self.set_font("Helvetica", "B", 12)
        self.cell(0, 8, pdf_safe_text(title), ln=1, fill=True)
        self.ln(1)

    def key_value(self, key: str, value: str):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(35, 35, 35)
        self.cell(58, 6, pdf_safe_text(key))
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, pdf_safe_text(value))

    def body_text(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 6, pdf_safe_text(text))

    def bullet_line(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, pdf_safe_text(f"- {text}"))

    def status_chip(self, decision: str):
        decision = pdf_safe_text(str(decision or "").upper())
        self.set_font("Helvetica", "B", 12)
        if decision == "APPROVED":
            self.set_fill_color(225, 245, 234)
            self.set_text_color(10, 120, 60)
            self.cell(48, 9, "APPROVED", ln=1, fill=True, align="C")
        else:
            self.set_fill_color(255, 235, 235)
            self.set_text_color(180, 30, 30)
            self.cell(48, 9, "NOT APPROVED", ln=1, fill=True, align="C")
        self.set_text_color(35, 35, 35)


# ---------------------------------------------------------------------
# SERVICE
# ---------------------------------------------------------------------
class IndividualCustomerPDFGeneratorService:
    def __init__(self, kafka_broker: str = KAFKA_BROKER):
        self.kafka_broker = kafka_broker
        self.db_config = get_mysql_config()

        self.consumer = KafkaConsumer(
            CUST_INFO,
            EXPLAINABILITY_OUTPUT,
            CUSTOMER_PDF,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="individual_customer_pdf_generator_service_group_v1",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.customer_info_cache: Dict[str, Dict[str, Any]] = {}
        self.explainability_cache: Dict[str, Dict[str, Any]] = {}
        self.finalization_cache: Dict[str, Dict[str, Any]] = {}

    # -----------------------------------------------------------------
    # DB
    # -----------------------------------------------------------------
    def _get_connection(self):
        return mysql.connector.connect(**get_mysql_config())

    def _fetch_customer_info_from_db(self, application_id: str):
        """
        Full DB fallback for customer_info.
        Mirrors manager_pdf.py so customer PDF can still render complete data
        even if the service missed the original CUST_INFO Kafka message.
        """
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

            return cursor.fetchone()

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()

    def _fetch_final_payload_from_db(self, application_id: str):
        """
        DB fallback for final customer decision data.
        Pulls both manager-final values and model-suggested values.
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute(
                """
                SELECT
                    application_id,
                    application_type,
                    model_decision,
                    model_risk_score,
                    model_risk_band,
                    model_suggested_principal,
                    model_suggested_interest_rate,
                    model_approved_tenure,
                    manager_decision,
                    final_principal,
                    final_interest_rate,
                    final_tenure,
                    manager_notes,
                    override_flag,
                    manager_id,
                    created_at,
                    updated_at
                FROM msme_final_decision
                WHERE application_id = %s
                  AND application_type = 'individual'
                LIMIT 1
                """,
                (application_id,),
            )

            return cursor.fetchone()

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()

    def _store_customer_pdf(self, application_id: str, pdf_bytes: bytes) -> None:
        """
        Stores generated customer PDF in shared final decision table.
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE msme_final_decision
                SET customer_pdf_blob = %s,
                    customer_pdf_filename = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE application_id = %s
                  AND application_type = 'individual'
                """,
                (
                    pdf_bytes,
                    f"Customer_Report_{application_id}.pdf",
                    application_id,
                ),
            )
            conn.commit()
            print(f"[DB SUCCESS] Customer PDF stored for application_id={application_id}")

        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()

    # -----------------------------------------------------------------
    # UTILS
    # -----------------------------------------------------------------
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
    def _display_text(value: Any, default: str = "N/A") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    @staticmethod
    def _pick_first(*values):
        for v in values:
            if v is not None and v != "":
                return v
        return None

    @staticmethod
    def _fmt_currency(value: Any) -> str:
        try:
            return f"Rs {float(value):,.0f}"
        except Exception:
            return "N/A"

    @staticmethod
    def _fmt_float(value: Any, ndigits: int = 2) -> str:
        try:
            return f"{float(value):.{ndigits}f}"
        except Exception:
            return "N/A"

    @staticmethod
    def _decode_base64_to_temp_png(image_b64: str, prefix: str) -> str:
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

    @staticmethod
    def _convert_risk_to_cibil(risk_score: Any) -> int:
        """
        Converts model risk score (0 to 1, higher = riskier) into an approximate
        customer-friendly bureau-like score between 300 and 900.
        Lower risk => higher score.
        """
        try:
            rs = float(risk_score)
        except Exception:
            rs = 0.5

        rs = max(0.0, min(1.0, rs))
        cibil = 900 - int(rs * 600)
        return max(300, min(900, cibil))

    @staticmethod
    def _compute_emi(principal: Any, annual_rate_pct: Any, tenure_months: Any) -> Optional[float]:
        try:
            p = float(principal)
            r_annual = float(annual_rate_pct)
            n = int(tenure_months)

            if p <= 0 or r_annual <= 0 or n <= 0:
                return None

            r = r_annual / 12 / 100
            emi = p * r * ((1 + r) ** n) / (((1 + r) ** n) - 1)
            return round(emi, 2)
        except Exception:
            return None

    def _merge_ready_payloads(
        self,
        application_id: str,
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
        a = self.customer_info_cache.get(application_id)
        b = self.explainability_cache.get(application_id)
        c = self.finalization_cache.get(application_id)

        if a is None:
            a = self._fetch_customer_info_from_db(application_id)
            if a is not None:
                self.customer_info_cache[application_id] = a
                print(f"[DB FALLBACK] Loaded customer_info from DB for {application_id}")

        if c is None:
            c = self._fetch_final_payload_from_db(application_id)
            if c is not None:
                self.finalization_cache[application_id] = c
                print(f"[DB FALLBACK] Loaded finalization payload from DB for {application_id}")

        if a is None or b is None or c is None:
            return None

        return a, b, c

    # -----------------------------------------------------------------
    # CONTENT HELPERS
    # -----------------------------------------------------------------
    def _selected_customer_details(
        self,
        customer_info: Dict[str, Any],
        final_payload: Dict[str, Any],
    ) -> Dict[str, str]:
        display_principal = self._pick_first(
            final_payload.get("final_principal"),
            final_payload.get("model_suggested_principal"),
            customer_info.get("suggested_principal"),
        )
        display_rate = self._pick_first(
            final_payload.get("final_interest_rate"),
            final_payload.get("model_suggested_interest_rate"),
            customer_info.get("suggested_interest_rate"),
        )
        display_tenure = self._pick_first(
            final_payload.get("final_tenure"),
            final_payload.get("model_approved_tenure"),
            customer_info.get("loan_tenure_months"),
        )
        risk_score = self._pick_first(
            final_payload.get("model_risk_score"),
            final_payload.get("risk_score"),
        )

        return {
            "Application ID": self._display_text(final_payload.get("application_id")),
            "Applicant Name": self._display_text(customer_info.get("full_name")),
            "Gender": self._display_text(customer_info.get("gender")),
            "Date of Birth": self._display_text(customer_info.get("date_of_birth")),
            "Age": self._display_text(customer_info.get("age_years")),
            "Marital Status": self._display_text(customer_info.get("marital_status")),
            "Education": self._display_text(customer_info.get("education_qualification")),
            "PAN Number": self._display_text(customer_info.get("pan_number")),
            "Phone": self._display_text(customer_info.get("primary_phone")),
            "Email": self._display_text(customer_info.get("email")),
            "Employment Status": self._display_text(customer_info.get("employment_status")),
            "Occupation": self._display_text(customer_info.get("occupation_type")),
            "Employer": self._display_text(customer_info.get("employer_name")),
            "Annual Income": self._fmt_currency(customer_info.get("annual_income_total")),
            "Monthly Income": self._fmt_currency(customer_info.get("monthly_income")),
            "Loan Purpose": self._display_text(customer_info.get("loan_purpose")),
            "Requested Loan Amount": self._fmt_currency(customer_info.get("loan_amount")),
            "Requested Tenure": (
                f"{customer_info.get('loan_tenure_months')} months"
                if customer_info.get("loan_tenure_months") is not None
                else "N/A"
            ),
            "Approved Principal": self._fmt_currency(display_principal),
            "Final Interest Rate": (
                f"{self._fmt_float(display_rate, 2)}% p.a."
                if display_rate is not None
                else "N/A"
            ),
            "Final Tenure": (
                f"{display_tenure} months"
                if display_tenure is not None
                else "N/A"
            ),
            "Approx. CIBIL-style Score": str(self._convert_risk_to_cibil(risk_score)),
        }

    def _driver_lines(self, explainability_output: Dict[str, Any], key: str):
        items = explainability_output.get(key, [])
        lines = []
        if isinstance(items, list):
            for x in items:
                feat = x.get("feature", "Unknown")
                shap_impact = x.get("shap_impact", 0.0)
                feature_value = x.get("feature_value", "N/A")
                expl = FEATURE_EXPLANATIONS.get(
                    feat,
                    "This feature contributes to the model's assessment of personal credit risk and repayment capacity.",
                )
                lines.append(
                    {
                        "feature": feat,
                        "impact": shap_impact,
                        "value": feature_value,
                        "explanation": expl,
                    }
                )
        return lines

    # -----------------------------------------------------------------
    # PDF GENERATION
    # -----------------------------------------------------------------
    def _generate_pdf_bytes(
        self,
        application_id: str,
        customer_info: Dict[str, Any],
        explainability_output: Dict[str, Any],
        final_payload: Dict[str, Any],
    ) -> bytes:
        pdf = MonarchCustomerPDF()
        pdf.set_auto_page_break(auto=True, margin=18)
        pdf.add_page()

        risk_score = self._pick_first(
            final_payload.get("model_risk_score"),
            explainability_output.get("risk_score"),
        )
        risk_band = self._pick_first(
            final_payload.get("model_risk_band"),
            explainability_output.get("risk_band"),
        )
        final_principal = self._pick_first(
            final_payload.get("final_principal"),
            final_payload.get("model_suggested_principal"),
            customer_info.get("suggested_principal"),
        )
        final_interest_rate = self._pick_first(
            final_payload.get("final_interest_rate"),
            final_payload.get("model_suggested_interest_rate"),
            customer_info.get("suggested_interest_rate"),
        )
        final_tenure = self._pick_first(
            final_payload.get("final_tenure"),
            final_payload.get("model_approved_tenure"),
            customer_info.get("loan_tenure_months"),
        )

        emi = self._compute_emi(final_principal, final_interest_rate, final_tenure)
        cibil_score = self._convert_risk_to_cibil(risk_score)

        full_name = customer_info.get("full_name", "Customer")
        manager_decision = str(final_payload.get("manager_decision", "N/A"))
        manager_notes = final_payload.get("manager_notes") or "No additional notes were provided."

        # -------------------------------------------------------------
        # HERO / DECISION
        # -------------------------------------------------------------
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(25, 25, 25)
        pdf.cell(0, 8, pdf_safe_text(f"Loan Decision Summary for {full_name}"), ln=1)

        pdf.ln(1)
        pdf.status_chip(manager_decision)
        pdf.ln(2)

        intro = (
            "Dear Customer,\n\n"
            "This report summarises the outcome of your personal loan application. "
            "It includes the approved loan terms, your profile summary, a customer-friendly "
            "credit score estimate, and an AI explanation of the key factors that influenced "
            "the risk assessment."
        )
        pdf.body_text(intro)

        # -------------------------------------------------------------
        # KEY APPROVAL TERMS
        # -------------------------------------------------------------
        pdf.section_title("Final Loan Terms")
        pdf.key_value("Approved Principal", self._fmt_currency(final_principal))
        pdf.key_value(
            "Final Interest Rate",
            f"{self._fmt_float(final_interest_rate, 2)}% per annum"
            if final_interest_rate is not None else "N/A",
        )
        pdf.key_value(
            "Final Tenure",
            f"{final_tenure} months" if final_tenure is not None else "N/A",
        )
        pdf.key_value("Estimated EMI", self._fmt_currency(emi) if emi is not None else "N/A")
        pdf.key_value("Approx. CIBIL-style Score", str(cibil_score))
        pdf.key_value("Risk Score", self._fmt_float(risk_score, 4))
        pdf.key_value("Risk Band", self._display_text(risk_band))
        pdf.key_value("Manager Notes", self._display_text(manager_notes))

        # -------------------------------------------------------------
        # CUSTOMER DETAILS
        # -------------------------------------------------------------
        pdf.section_title("Applicant Information")
        details = self._selected_customer_details(customer_info, final_payload)
        for k, v in details.items():
            pdf.key_value(k, v)

        # -------------------------------------------------------------
        # SHAP PLOT
        # -------------------------------------------------------------
        temp_files = []
        try:
            shap_b64 = explainability_output.get("shap_waterfall_base64")
            shap_path = None

            if shap_b64:
                shap_path = self._decode_base64_to_temp_png(shap_b64, "customer_shap_")
                temp_files.append(shap_path)

            pdf.section_title("AI Explainability Overview")
            pdf.body_text(
                "The chart below is a SHAP explainability plot. It shows how individual applicant features "
                "pushed the final model assessment toward lower or higher risk. This helps improve transparency "
                "by highlighting which factors contributed most strongly to the decision."
            )

            if shap_path:
                if pdf.get_y() > 180:
                    pdf.add_page()
                pdf.image(shap_path, x=12, w=186)
                pdf.ln(4)
            else:
                pdf.body_text("No SHAP image was available for this application.")

            # ---------------------------------------------------------
            # TOP POSITIVE / NEGATIVE DRIVERS
            # ---------------------------------------------------------
            pdf.section_title("Key Positive Drivers")
            pos_lines = self._driver_lines(explainability_output, "top_positive_drivers")
            if pos_lines:
                for item in pos_lines:
                    pdf.set_font("Helvetica", "B", 10)
                    pdf.multi_cell(
                        0,
                        6,
                        pdf_safe_text(
                            f"{item['feature']} | impact={self._fmt_float(item['impact'], 4)} | value={item['value']}"
                        ),
                    )
                    pdf.set_font("Helvetica", "", 10)
                    pdf.multi_cell(0, 6, pdf_safe_text(item["explanation"]))
                    pdf.ln(1)
            else:
                pdf.bullet_line("No positive driver details were available.")

            pdf.section_title("Key Negative Drivers")
            neg_lines = self._driver_lines(explainability_output, "top_negative_drivers")
            if neg_lines:
                for item in neg_lines:
                    pdf.set_font("Helvetica", "B", 10)
                    pdf.multi_cell(
                        0,
                        6,
                        pdf_safe_text(
                            f"{item['feature']} | impact={self._fmt_float(item['impact'], 4)} | value={item['value']}"
                        ),
                    )
                    pdf.set_font("Helvetica", "", 10)
                    pdf.multi_cell(0, 6, pdf_safe_text(item["explanation"]))
                    pdf.ln(1)
            else:
                pdf.bullet_line("No negative driver details were available.")

            # ---------------------------------------------------------
            # FULL FEATURE GUIDE
            # ---------------------------------------------------------
            pdf.add_page()
            pdf.section_title("Complete Feature Explanation Guide")
            pdf.body_text(
                "Below is a plain-language explanation of the main model features used in your personal credit assessment. "
                "These notes are intended to improve transparency and help you understand how different applicant signals "
                "can influence the final evaluation."
            )
            pdf.ln(2)

            for feat, text in FEATURE_EXPLANATIONS.items():
                if pdf.get_y() > 268:
                    pdf.add_page()
                pdf.set_font("Helvetica", "B", 10)
                pdf.multi_cell(0, 6, pdf_safe_text(feat))
                pdf.set_font("Helvetica", "", 10)
                pdf.multi_cell(0, 6, pdf_safe_text(text))
                pdf.ln(1)

            # ---------------------------------------------------------
            # SAFETY / DISCLAIMER
            # ---------------------------------------------------------
            pdf.section_title("Important Notes")
            pdf.body_text(
                "This is a system-generated PDF prepared by MonarchCredit. "
                "The decision shown here is based on the submitted applicant information, model outputs, "
                "and final loan terms approved during the review process.\n\n"
                "Your information is handled using secure internal systems and is intended only for the "
                "processing, evaluation, and documentation of your credit application. "
                "Please keep this report for your records."
            )

            pdf_bytes = pdf.output(dest="S").encode("latin1")
            return pdf_bytes

        finally:
            self._cleanup_temp_files(temp_files)

    # -----------------------------------------------------------------
    # RUNNER
    # -----------------------------------------------------------------
    def run(self):
        print("=" * 80)
        print("Individual Customer PDF Generator Service Started")
        print(f"Consuming topics : {CUST_INFO}, {EXPLAINABILITY_OUTPUT}, {CUSTOMER_PDF}")
        print(f"Kafka broker     : {self.kafka_broker}")
        print("=" * 80)

        for message in self.consumer:
            try:
                topic = message.topic
                payload = message.value
                application_id = payload.get("application_id")
                print(f"[DEBUG] Received topic={topic} application_id={application_id}")

                if not application_id:
                    print("[WARN] Skipping message without application_id")
                    continue

                if topic == CUST_INFO:
                    self.customer_info_cache[application_id] = payload
                    print(f"[INFO] Cached customer_info for application_id={application_id}")

                elif topic == EXPLAINABILITY_OUTPUT:
                    self.explainability_cache[application_id] = payload
                    print(f"[INFO] Cached explainability for application_id={application_id}")

                elif topic == CUSTOMER_PDF:
                    self.finalization_cache[application_id] = payload
                    print(f"[INFO] Cached finalization payload for application_id={application_id}")

                merged = self._merge_ready_payloads(application_id)
                if merged is None:
                    print(
                        f"[INFO] Waiting for all 3 payloads for {application_id} | "
                        f"customer_info={'yes' if application_id in self.customer_info_cache else 'no'}, "
                        f"explainability={'yes' if application_id in self.explainability_cache else 'no'}, "
                        f"finalization={'yes' if application_id in self.finalization_cache else 'no'}"
                    )
                    continue

                customer_info, explainability_output, final_payload = merged

                pdf_bytes = self._generate_pdf_bytes(
                    application_id=application_id,
                    customer_info=customer_info,
                    explainability_output=explainability_output,
                    final_payload=final_payload,
                )

                self._store_customer_pdf(application_id, pdf_bytes)
                print(f"[SUCCESS] Customer PDF generated for application_id={application_id}")

                self.customer_info_cache.pop(application_id, None)
                self.explainability_cache.pop(application_id, None)
                self.finalization_cache.pop(application_id, None)

            except Exception as exc:
                print("[ERROR] Failed to generate customer PDF")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_customer_pdf_service():
    service = IndividualCustomerPDFGeneratorService()
    service.run()


if __name__ == "__main__":
    run_customer_pdf_service()