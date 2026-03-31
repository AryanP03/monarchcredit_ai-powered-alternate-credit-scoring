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
MSME_INFO = "msme_info"
EXPLAINABILITY_OUTPUT_MSME = "explainability_output_msme"
CUSTOMER_PDF_MSME = "customer_pdf_msme"


# ---------------------------------------------------------------------
# FEATURE EXPLANATIONS
# ---------------------------------------------------------------------
FEATURE_EXPLANATIONS = {
    "SH_num": "This reflects the number of shareholders in your business. A balanced ownership structure can support governance quality and continuity.",
    "MS_num": "This is the number of management staff. Strong management depth usually indicates better operational capacity.",
    "Branch_num": "This shows how many branches or operating locations your business has. It can reflect business reach and scale.",
    "Ratepaying_Credit_Grade_A_num": "This represents the count of strong external credit ratings. Higher values usually support credit confidence.",
    "msme_size_category": "This indicates whether the business is Micro, Small, or Medium. Larger and more established businesses often show stronger resilience.",
    "company_age_years": "This is the number of years the business has been operating. Older firms typically show greater stability.",
    "total_corporate_changes": "This counts historical corporate changes. Too many structural changes may indicate instability.",
    "recent_corporate_changes": "This captures recent corporate changes. Recent changes may indicate transition or uncertainty.",
    "total_court_executions": "This reflects total court execution actions involving the business. Higher values are generally a negative legal signal.",
    "recent_court_executions": "This reflects recent legal enforcement activity. Recent issues carry stronger near-term risk impact.",
    "total_overdue_tax": "This is the overdue tax amount. Higher unpaid tax amounts can indicate liquidity or compliance stress.",
    "total_admin_penalties": "This counts total administrative penalties. Repeated penalties may indicate weaker compliance discipline.",
    "recent_admin_penalties": "This counts recent penalties. Recent compliance issues are more relevant for current risk.",
    "total_legal_proceedings": "This is the total count of legal proceedings involving the business. Higher values may indicate operational friction.",
    "recent_legal_proceedings": "This is the recent count of legal proceedings. Recent cases weigh more on present risk.",
    "total_case_filings": "This is the total number of filed cases involving the business. Larger counts may indicate ongoing disputes.",
    "total_trademarks": "This reflects the number of trademarks owned by the business. It can indicate brand strength.",
    "total_patents": "This reflects the number of patents held. It can indicate innovation and defensibility.",
    "total_copyrights": "This reflects copyrights owned by the business. It may support intellectual property strength.",
    "total_certifications": "This reflects formal certifications held by the business. It can indicate process maturity and quality standards.",
    "total_ip_score": "This is a composite intellectual property score. A stronger IP profile may improve long-term business strength.",
    "avg_insured_employees": "This reflects average insured employee count. It is a signal of formal workforce scale and stability.",
    "insurance_trend": "This reflects the trend in insurance-related activity. Improving trends can support stability.",
    "log_registered_capital": "This represents registered capital on a transformed scale for model stability. Higher capital generally supports financial strength.",
    "log_paid_in_capital": "This represents paid-in capital on a transformed scale. Higher capital infusion usually supports commitment and business backing.",
    "region_risk_score": "This is the regional risk score associated with the business location. Riskier regions may face higher structural stress.",
    "sector_Construction": "This indicates whether the business is in the construction sector.",
    "sector_Services": "This indicates whether the business is in the services sector.",
    "sector_Tech": "This indicates whether the business is in the technology sector.",
    "sector_Trade": "This indicates whether the business is in the trade sector.",
    "salary_disbursement_regularity_score": "This reflects payroll regularity. More regular salary disbursement supports operational discipline.",
    "account_balance_volatility_score": "This reflects account balance stability. More stable balances generally support liquidity confidence.",
    "loan_repayment_history_score": "This reflects historical repayment discipline. Better repayment behavior is a strong positive signal.",
    "vendor_payment_delay_trend": "This reflects the trend of delays in vendor payments. Rising delays can indicate working-capital stress.",
    "utility_bill_payment_score": "This reflects regularity of utility bill payments. Timely bill payments support operational consistency.",
    "tax_filing_compliance_score": "This reflects tax filing discipline. Better compliance improves reliability and governance confidence.",
    "digital_payment_ratio": "This reflects the share of transactions handled digitally. Higher digital usage supports traceability and transparency.",
}


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
        self.cell(0, 5, "MSME Customer Loan Decision Report", ln=1)

        self.ln(6)

    def footer(self):
        self.set_y(-16)
        self.set_text_color(120, 120, 120)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 5, "This is a system-generated report. Please preserve it for your records.", ln=1, align="C")
        self.cell(0, 5, f"Page {self.page_no()}", align="C")

    def section_title(self, title: str):
        self.ln(2)
        self.set_fill_color(232, 240, 254)
        self.set_text_color(20, 20, 20)
        self.set_font("Helvetica", "B", 12)
        self.cell(0, 8, title, ln=1, fill=True)
        self.ln(1)

    def key_value(self, key: str, value: str):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(35, 35, 35)
        self.cell(58, 6, key)
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, value)

    def body_text(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 6, text)

    def bullet_line(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, f"- {text}")

    def status_chip(self, decision: str):
        decision = str(decision or "").upper()
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
class MSMECustomerPDFGeneratorService:
    def __init__(self, kafka_broker: str = KAFKA_BROKER):
        self.kafka_broker = kafka_broker
        self.db_config = get_mysql_config()

        self.consumer = KafkaConsumer(
            MSME_INFO,
            EXPLAINABILITY_OUTPUT_MSME,
            CUSTOMER_PDF_MSME,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="msme_customer_pdf_generator_service_group_v2",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.msme_info_cache: Dict[str, Dict[str, Any]] = {}
        self.explainability_cache: Dict[str, Dict[str, Any]] = {}
        self.finalization_cache: Dict[str, Dict[str, Any]] = {}

    # -----------------------------------------------------------------
    # DB
    # -----------------------------------------------------------------
    def _get_connection(self):
        return mysql.connector.connect(**get_mysql_config())

    def _store_customer_pdf(self, application_id: str, pdf_bytes: bytes) -> None:
        """
        Stores generated customer PDF in final decision table.
        Requires columns:
            customer_pdf_blob LONGBLOB NULL
            customer_pdf_filename VARCHAR(255) NULL
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
    def _compute_emi(principal: Any, annual_rate_pct: Any, tenure_years: Any) -> Optional[float]:
        try:
            p = float(principal)
            r_annual = float(annual_rate_pct)
            t_years = float(tenure_years)
            if p <= 0 or r_annual <= 0 or t_years <= 0:
                return None

            n = int(round(t_years * 12))
            if n <= 0:
                return None

            r = r_annual / 12 / 100
            emi = p * r * ((1 + r) ** n) / (((1 + r) ** n) - 1)
            return round(emi, 2)
        except Exception:
            return None

    @staticmethod
    def _business_size_label(value: Any) -> str:
        mapping = {
            0: "Micro",
            1: "Small",
            2: "Medium",
        }
        try:
            return mapping.get(int(value), str(value))
        except Exception:
            return str(value)

    @staticmethod
    def _sector_label(sector_id: Any) -> str:
        mapping = {
            1: "Construction",
            2: "Services",
            3: "Technology",
            4: "Trade",
        }
        try:
            return mapping.get(int(sector_id), str(sector_id))
        except Exception:
            return str(sector_id)

    def _merge_ready_payloads(
        self,
        application_id: str,
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
        a = self.msme_info_cache.get(application_id)
        b = self.explainability_cache.get(application_id)
        c = self.finalization_cache.get(application_id)

        if a is None or b is None or c is None:
            return None

        return a, b, c

    # -----------------------------------------------------------------
    # CONTENT HELPERS
    # -----------------------------------------------------------------
    def _selected_customer_details(
        self,
        msme_info: Dict[str, Any],
        final_payload: Dict[str, Any],
    ) -> Dict[str, str]:
        return {
            "Application ID": str(final_payload.get("application_id", "N/A")),
            "Company Name": str(self._safe_get(msme_info, "business_identity", "company_name")),
            "Entity Type": str(self._safe_get(msme_info, "business_identity", "entity_type")),
            "Registration Number": str(self._safe_get(msme_info, "business_identity", "registration_number")),
            "Business Size": self._business_size_label(self._safe_get(msme_info, "business_identity", "msme_size_category", default="N/A")),
            "Sector": self._sector_label(self._safe_get(msme_info, "business_identity", "sector_id", default="N/A")),
            "Company Age": f"{self._safe_get(msme_info, 'business_identity', 'company_age_years', default='N/A')} years",
            "Requested Loan Amount": self._fmt_currency(self._safe_get(msme_info, "loan_requirements", "loan_amt", default=0)),
            "Requested Tenure": f"{self._safe_get(msme_info, 'loan_requirements', 'tenure', default='N/A')} years",
            "Approved Principal": self._fmt_currency(final_payload.get("final_principal")),
            "Final Interest Rate": f"{self._fmt_float(final_payload.get('final_interest_rate'), 2)}% p.a.",
            "Final Tenure": f"{final_payload.get('final_tenure', 'N/A')} years",
            "Approx. CIBIL-style Score": str(self._convert_risk_to_cibil(final_payload.get("model_risk_score"))),
        }

    def _driver_lines(self, explainability_output: Dict[str, Any], key: str):
        items = explainability_output.get(key, [])
        lines = []
        if isinstance(items, list):
            for x in items:
                feat = x.get("feature", "Unknown")
                shap_impact = x.get("shap_impact", 0.0)
                feature_value = x.get("feature_value", "N/A")
                expl = FEATURE_EXPLANATIONS.get(feat, "This feature contributes to the model's assessment of business risk and repayment capacity.")
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
        msme_info: Dict[str, Any],
        explainability_output: Dict[str, Any],
        final_payload: Dict[str, Any],
    ) -> bytes:
        pdf = MonarchCustomerPDF()
        pdf.set_auto_page_break(auto=True, margin=18)
        pdf.add_page()

        risk_score = final_payload.get("model_risk_score")
        risk_band = final_payload.get("model_risk_band")
        final_principal = final_payload.get("final_principal")
        final_interest_rate = final_payload.get("final_interest_rate")
        final_tenure = final_payload.get("final_tenure")
        emi = self._compute_emi(final_principal, final_interest_rate, final_tenure)
        cibil_score = self._convert_risk_to_cibil(risk_score)

        company_name = self._safe_get(msme_info, "business_identity", "company_name")
        manager_decision = str(final_payload.get("manager_decision", "N/A"))
        manager_notes = final_payload.get("manager_notes") or "No additional notes were provided."

        # -------------------------------------------------------------
        # HERO / DECISION
        # -------------------------------------------------------------
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(25, 25, 25)
        pdf.cell(0, 8, f"Loan Decision Summary for {company_name}", ln=1)

        pdf.ln(1)
        pdf.status_chip(manager_decision)
        pdf.ln(2)

        intro = (
            "Dear Customer,\n\n"
            "This report summarises the outcome of your MSME loan application. "
            "It includes the approved loan terms, business details, a customer-friendly "
            "credit score estimate, and an AI explanation of the key factors that influenced "
            "the risk assessment."
        )
        pdf.body_text(intro)

        # -------------------------------------------------------------
        # KEY APPROVAL TERMS
        # -------------------------------------------------------------
        pdf.section_title("Final Loan Terms")

        pdf.key_value("Approved Principal", self._fmt_currency(final_principal))
        pdf.key_value("Final Interest Rate", f"{self._fmt_float(final_interest_rate, 2)}% per annum")
        pdf.key_value("Final Tenure", f"{final_tenure} years" if final_tenure is not None else "N/A")
        pdf.key_value("Estimated EMI", self._fmt_currency(emi) if emi is not None else "N/A")
        pdf.key_value("Approx. CIBIL-style Score", str(cibil_score))
        pdf.key_value("Risk Score", self._fmt_float(risk_score, 4))
        pdf.key_value("Risk Band", str(risk_band or "N/A"))
        pdf.key_value("Manager Notes", str(manager_notes))

        # -------------------------------------------------------------
        # BUSINESS DETAILS
        # -------------------------------------------------------------
        pdf.section_title("Business Information")
        details = self._selected_customer_details(msme_info, final_payload)
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
                "The chart below is a SHAP explainability plot. It shows how individual business features "
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
                        f"{item['feature']} | impact={self._fmt_float(item['impact'], 4)} | value={item['value']}",
                    )
                    pdf.set_font("Helvetica", "", 10)
                    pdf.multi_cell(0, 6, item["explanation"])
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
                        f"{item['feature']} | impact={self._fmt_float(item['impact'], 4)} | value={item['value']}",
                    )
                    pdf.set_font("Helvetica", "", 10)
                    pdf.multi_cell(0, 6, item["explanation"])
                    pdf.ln(1)
            else:
                pdf.bullet_line("No negative driver details were available.")

            # ---------------------------------------------------------
            # FULL FEATURE GUIDE
            # ---------------------------------------------------------
            pdf.add_page()
            pdf.section_title("Complete Feature Explanation Guide")
            pdf.body_text(
                "Below is a plain-language explanation of the main model features used in your MSME credit assessment. "
                "These notes are intended to improve transparency and help you understand how different business signals "
                "can influence the final evaluation."
            )
            pdf.ln(2)

            for feat, text in FEATURE_EXPLANATIONS.items():
                if pdf.get_y() > 268:
                    pdf.add_page()
                pdf.set_font("Helvetica", "B", 10)
                pdf.multi_cell(0, 6, feat)
                pdf.set_font("Helvetica", "", 10)
                pdf.multi_cell(0, 6, text)
                pdf.ln(1)

            # ---------------------------------------------------------
            # SAFETY / DISCLAIMER
            # ---------------------------------------------------------
            pdf.section_title("Important Notes")
            pdf.body_text(
                "This is a system-generated PDF prepared by MonarchCredit. "
                "The decision shown here is based on the submitted business information, model outputs, "
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
        print("MSME Customer PDF Generator Service Started")
        print(f"Consuming topics : {MSME_INFO}, {EXPLAINABILITY_OUTPUT_MSME}, {CUSTOMER_PDF_MSME}")
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

                if topic == MSME_INFO:
                    self.msme_info_cache[application_id] = payload
                    print(f"[INFO] Cached msme_info for application_id={application_id}")

                elif topic == EXPLAINABILITY_OUTPUT_MSME:
                    self.explainability_cache[application_id] = payload
                    print(f"[INFO] Cached explainability for application_id={application_id}")

                elif topic == CUSTOMER_PDF_MSME:
                    self.finalization_cache[application_id] = payload
                    print(f"[INFO] Cached finalization payload for application_id={application_id}")

                merged = self._merge_ready_payloads(application_id)
                if merged is None:
                    print(
                        f"[INFO] Waiting for all 3 payloads for {application_id} | "
                        f"msme_info={'yes' if application_id in self.msme_info_cache else 'no'}, "
                        f"explainability={'yes' if application_id in self.explainability_cache else 'no'}, "
                        f"finalization={'yes' if application_id in self.finalization_cache else 'no'}"
                    )
                    continue

                msme_info, explainability_output, final_payload = merged

                pdf_bytes = self._generate_pdf_bytes(
                    application_id=application_id,
                    msme_info=msme_info,
                    explainability_output=explainability_output,
                    final_payload=final_payload,
                )

                self._store_customer_pdf(application_id, pdf_bytes)
                print(f"[SUCCESS] Customer PDF generated for application_id={application_id}")

                # cleanup caches
                self.msme_info_cache.pop(application_id, None)
                self.explainability_cache.pop(application_id, None)
                self.finalization_cache.pop(application_id, None)

            except Exception as exc:
                print("[ERROR] Failed to generate customer PDF")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_customer_pdf_service():
    service = MSMECustomerPDFGeneratorService()
    service.run()


if __name__ == "__main__":
    run_customer_pdf_service()