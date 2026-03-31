from __future__ import annotations

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
    from app.services_MSME.audit_service_MSME import audit_service
except ImportError:
    from audit_service_MSME import audit_service


# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
from app.config import KAFKA_BROKER

MSME_INFO = "msme_info"
MODEL_OUTPUT_MSME = "model_output_msme"
EXPLAINABILITY_OUTPUT_MSME = "explainability_output_msme"
MANAGER_PDF_MSME = "manager_pdf_msme"


# ---------------------------------------------------------------------
# FEATURE EXPLANATIONS
# ---------------------------------------------------------------------

FEATURE_EXPLANATIONS = {
    "SH_num": "Number of shareholders in the business. A broader ownership structure can indicate stronger governance, though excessive complexity may also raise review needs.",
    "MS_num": "Count of management staff. Stronger management depth can indicate operational maturity and better execution capacity.",
    "Branch_num": "Number of branches or operating locations. More branches may suggest business reach, but rapid expansion can also increase operational risk.",
    "Ratepaying_Credit_Grade_A_num": "Number of Grade-A credit ratings received. Higher values generally support stronger external creditworthiness.",
    "msme_size_category": "MSME size bucket such as Micro, Small, or Medium. Larger and more established business size may indicate stronger resilience.",
    "company_age_years": "How long the company has been operating. Older firms often show more stability and survival strength.",
    "total_corporate_changes": "Total corporate structure changes ever recorded. Too many changes may indicate instability or frequent restructuring.",
    "recent_corporate_changes": "Corporate changes in the recent period. Higher recent changes may signal transition or governance uncertainty.",
    "total_court_executions": "Total court execution actions faced by the firm. Higher values are generally a negative legal signal.",
    "recent_court_executions": "Recent court execution activity. Recent legal enforcement is a stronger near-term risk signal than old activity.",
    "total_overdue_tax": "Outstanding overdue tax burden. Larger unpaid tax obligations can reflect compliance stress or liquidity pressure.",
    "total_admin_penalties": "Total administrative penalties ever faced. Repeated penalties may indicate weaker compliance discipline.",
    "recent_admin_penalties": "Administrative penalties in the recent period. Recent penalties can be especially important for current risk assessment.",
    "total_legal_proceedings": "Lifetime count of legal proceedings involving the firm. Higher legal burden usually increases perceived operational risk.",
    "recent_legal_proceedings": "Recent legal proceedings count. Recent disputes often weigh more heavily than historical ones.",
    "total_case_filings": "Total number of filed cases involving the business. Larger counts may indicate ongoing legal or commercial friction.",
    "total_trademarks": "Number of trademarks owned. Trademarks can indicate brand development and intangible business strength.",
    "total_patents": "Number of patents held. Patents can reflect innovation strength and competitive advantage.",
    "total_copyrights": "Count of copyrights owned. This may support business uniqueness, especially in technology or creative sectors.",
    "total_certifications": "Number of formal certifications. Certifications may indicate standardization, quality processes, and operational maturity.",
    "total_ip_score": "Composite intellectual property score. A stronger IP profile can improve long-term defensibility and business quality.",
    "avg_insured_employees": "Average insured employee count. This can act as a signal of workforce formalization and operating scale.",
    "insurance_trend": "Trend in insurance-related activity. Improving trend can support stability, while declining trend may indicate weakening coverage or business contraction.",
    "log_registered_capital": "Log-transformed registered capital. Higher capital base usually supports financial strength, but the model uses the transformed scale for stability.",
    "log_paid_in_capital": "Log-transformed paid-in capital. Higher actual capital infused may support business commitment and financial base.",
    "region_risk_score": "Regional default risk score. Businesses located in riskier regions may face structurally higher credit stress.",
    "sector_Construction": "Indicator for construction sector. Sector membership matters because some industries naturally carry different risk profiles.",
    "sector_Services": "Indicator for services sector. Service businesses may behave differently from asset-heavy sectors in credit performance.",
    "sector_Tech": "Indicator for technology sector. Tech firms may show growth potential, but sometimes also greater volatility depending on stage.",
    "sector_Trade": "Indicator for trade sector. Trade businesses may be influenced by inventory cycles, margins, and payment behavior.",
    "salary_disbursement_regularity_score": "Score for salary payment regularity. More regular payroll behavior usually supports operational discipline.",
    "account_balance_volatility_score": "Score representing account stability. More stable balances generally suggest better liquidity management.",
    "loan_repayment_history_score": "Score for historical repayment discipline. Better repayment behavior is typically a strong positive signal.",
    "vendor_payment_delay_trend": "Trend of payment delays to vendors. Rising delays can indicate working-capital stress or weakening discipline.",
    "utility_bill_payment_score": "Score for utility bill payment regularity. Timely bill payments often support operational consistency.",
    "tax_filing_compliance_score": "Tax filing compliance score. Stronger compliance is generally a positive governance and reliability indicator.",
    "digital_payment_ratio": "Share of payments handled digitally. Higher digital usage can support traceability and transaction transparency.",
}


# ---------------------------------------------------------------------
# PDF CLASS
# ---------------------------------------------------------------------

class MonarchCreditPDF(FPDF):
    def header(self):
        self.set_fill_color(245, 249, 255)
        self.rect(0, 0, 210, 22, style="F")

        self.set_xy(10, 8)
        self.set_text_color(30, 102, 245)  # blue
        self.set_font("Helvetica", "B", 20)
        self.cell(0, 8, "MonarchCredit", ln=1)

        self.set_x(10)
        self.set_text_color(90, 90, 90)
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, "MSME Manager Risk Report", ln=1)

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
        self.cell(55, 6, key)
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, value)

    def bullet_line(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 6, f"- {text}")


# ---------------------------------------------------------------------
# SERVICE
# ---------------------------------------------------------------------

class MSMEManagerPDFService:
    def __init__(self, kafka_broker: str = KAFKA_BROKER):
        self.kafka_broker = kafka_broker

        self.consumer = KafkaConsumer(
            MSME_INFO,
            MODEL_OUTPUT_MSME,
            EXPLAINABILITY_OUTPUT_MSME,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="msme_manager_pdf_service_group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.msme_info_cache: Dict[str, Dict[str, Any]] = {}
        self.model_output_cache: Dict[str, Dict[str, Any]] = {}
        self.explainability_cache: Dict[str, Dict[str, Any]] = {}

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
    def _format_bool(value: Any) -> str:
        if isinstance(value, bool):
            return "Yes" if value else "No"
        return str(value)

    @staticmethod
    def _fmt_currency(value: Any) -> str:
        try:
            v = float(value)
            return f"Rs {v:,.0f}"
        except Exception:
            return str(value)

    @staticmethod
    def _fmt_float(value: Any, ndigits: int = 4) -> str:
        try:
            return f"{float(value):.{ndigits}f}"
        except Exception:
            return str(value)

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

    def _merge_ready_payloads(
        self,
        application_id: str
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
        a = self.msme_info_cache.get(application_id)
        b = self.model_output_cache.get(application_id)
        c = self.explainability_cache.get(application_id)

        if a is None or b is None or c is None:
            return None

        return a, b, c

    # -----------------------------------------------------------------
    # CONTENT BUILDERS
    # -----------------------------------------------------------------

    def _selected_msme_details(self, msme_info: Dict[str, Any]) -> Dict[str, str]:
        return {
    "Company Name": str(self._safe_get(msme_info, "business_identity", "company_name")),
    "Entity Type": str(self._safe_get(msme_info, "business_identity", "entity_type")),
    "Registration Number": str(self._safe_get(msme_info, "business_identity", "registration_number")),

    # ADD THESE 2 LINES HERE
    "Requested Loan Amount": self._fmt_currency(
        self._safe_get(msme_info, "loan_requirements", "loan_amt", default=0)
    ),
    "Requested Tenure": f"{self._safe_get(msme_info, 'loan_requirements', 'tenure', default='N/A')} years",

    "Founded Year": str(self._safe_get(msme_info, "business_identity", "founded_year")),
    "Sector ID": str(self._safe_get(msme_info, "business_identity", "sector_id")),
    "Business Description": str(self._safe_get(msme_info, "business_identity", "business_description")),
    "Active Status": self._format_bool(self._safe_get(msme_info, "business_identity", "is_active")),

    "Paid-in Capital": self._fmt_currency(
    self._safe_get(msme_info, "financial_foundation", "capital_structure", "paid_in_capital", default=0)
),
"Registered Capital": self._fmt_currency(
    self._safe_get(msme_info, "financial_foundation", "capital_structure", "registered_capital", default=0)
),
    "Debt to Equity Ratio": str(self._safe_get(msme_info, "financial_foundation", "risk_metrics", "debt_to_equity_ratio")),
    "Current Ratio": str(self._safe_get(msme_info, "financial_foundation", "risk_metrics", "current_ratio")),
    "GST Status": str(self._safe_get(msme_info, "financial_foundation", "taxation", "gst_status")),
    "PAN Verified": self._format_bool(self._safe_get(msme_info, "financial_foundation", "taxation", "pan_verified")),

    "Pledged Shares": self._format_bool(self._safe_get(msme_info, "ownership_and_governance", "cap_table", "is_pledged_shares")),
    "Board Members": str(self._safe_get(msme_info, "ownership_and_governance", "management", "board_members")),
    "HQ Address": str(self._safe_get(msme_info, "ownership_and_governance", "infrastructure", "headquarters_address")),

    "ISO Certified": self._format_bool(self._safe_get(msme_info, "intellectual_property_and_ratings", "external_ratings", "iso_certified")),
}

    def _equity_split_lines(self, msme_info: Dict[str, Any]):
        entries = self._safe_get(msme_info, "ownership_and_governance", "cap_table", "equity_split", default=[])
        if not isinstance(entries, list):
            return []

        lines = []
        for e in entries:
            name = e.get("name", "Unknown")
            role = e.get("role", "Unknown")
            eq = e.get("equity_percent", "N/A")
            promoter = "Promoter" if e.get("is_promoter") else "Non-promoter"
            lines.append(f"{name} | {role} | {eq}% | {promoter}")
        return lines

    def _driver_lines(self, explainability_output: Dict[str, Any], key: str):
        items = explainability_output.get(key, [])
        lines = []
        if isinstance(items, list):
            for x in items:
                feat = x.get("feature", "Unknown")
                shap_impact = x.get("shap_impact", 0.0)
                fval = x.get("feature_value", "N/A")
                lines.append(
                    f"{feat}: impact={self._fmt_float(shap_impact, 4)}, value={self._fmt_float(fval, 4) if isinstance(fval, (int, float)) else fval}"
                )
        return lines

    # -----------------------------------------------------------------
    # PDF GENERATION
    # -----------------------------------------------------------------

    def _generate_pdf_bytes(
        self,
        application_id: str,
        msme_info: Dict[str, Any],
        model_output: Dict[str, Any],
        explainability_output: Dict[str, Any],
    ) -> bytes:
        pdf = MonarchCreditPDF()
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_page()

        # -------------------------------------------------------------
        # MSME DETAILS
        # -------------------------------------------------------------
        pdf.section_title("MSME Details")

        details = self._selected_msme_details(msme_info)
        for k, v in details.items():
            pdf.key_value(k, v)

        eq_lines = self._equity_split_lines(msme_info)
        if eq_lines:
            pdf.ln(1)
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 6, "Equity Split", ln=1)
            for line in eq_lines:
                pdf.bullet_line(line)

        # -------------------------------------------------------------
        # RISK SUMMARY
        # -------------------------------------------------------------
        pdf.section_title("Risk Summary")
        pdf.key_value("Application ID", application_id)
        pdf.key_value("Risk Score", self._fmt_float(model_output.get("risk_score"), 4))
        pdf.key_value("Risk Band", str(model_output.get("risk_band", "N/A")))
        pdf.key_value("Model Name", str(model_output.get("model_name", "N/A")))
        pdf.key_value("Model Version", str(model_output.get("model_version", "N/A")))
        pdf.key_value("Generated At", str(explainability_output.get("generated_at", self._utc_now_iso())))
        pdf.key_value("Summary", str(explainability_output.get("explanation_summary", "No summary available")))

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
            # FEATURE EXPLANATIONS
            # ---------------------------------------------------------
            pdf.add_page()
            pdf.section_title("Feature Guide for Manager Review")
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(
                0, 6,
                "Below are short explanations of the model features shown in the SHAP analysis. "
                "These help interpret what each variable broadly represents in the MSME risk assessment."
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
            "document_type": "manager_pdf_msme",
            "risk_score": model_output.get("risk_score"),
            "risk_band": model_output.get("risk_band"),
            "pdf_base64": base64.b64encode(pdf_bytes).decode("utf-8"),
            "generated_at": self._utc_now_iso(),
            "status": "SUCCESS",
        }

    def publish_output(self, payload: Dict[str, Any]) -> None:
        self.producer.send(MANAGER_PDF_MSME, value=payload)
        self.producer.flush()

    def _merge_ready_payloads(
        self,
        application_id: str
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
        a = self.msme_info_cache.get(application_id)
        b = self.model_output_cache.get(application_id)
        c = self.explainability_cache.get(application_id)
        if a is None or b is None or c is None:
            return None
        return a, b, c

    def run(self):
        print("=" * 80)
        print("MSME Manager PDF Service Started")
        print(f"Consuming topics : {MSME_INFO}, {MODEL_OUTPUT_MSME}, {EXPLAINABILITY_OUTPUT_MSME}")
        print(f"Publishing topic : {MANAGER_PDF_MSME}")
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

                if topic == MSME_INFO:
                    self.msme_info_cache[application_id] = payload
                    print(f"[INFO] Cached msme_info for application_id={application_id}")

                elif topic == MODEL_OUTPUT_MSME:
                    self.model_output_cache[application_id] = payload
                    print(f"[INFO] Cached model_output_msme for application_id={application_id}")

                elif topic == EXPLAINABILITY_OUTPUT_MSME:
                    self.explainability_cache[application_id] = payload
                    print(f"[INFO] Cached explainability_output_msme for application_id={application_id}")

                merged = self._merge_ready_payloads(application_id)
                if merged is None:
                    print(f"[INFO] Waiting for all 3 payloads for {application_id} | "
                          f"msme_info={'yes' if application_id in self.msme_info_cache else 'no'}, "
                          f"model_output={'yes' if application_id in self.model_output_cache else 'no'}, "
                          f"explainability={'yes' if application_id in self.explainability_cache else 'no'}")
                    continue

                msme_info, model_output, explainability_output = merged

                pdf_bytes = self._generate_pdf_bytes(
                    application_id=application_id,
                    msme_info=msme_info,
                    model_output=model_output,
                    explainability_output=explainability_output,
                )

                output_payload = self._build_output_payload(
                    application_id=application_id,
                    pdf_bytes=pdf_bytes,
                    model_output=model_output,
                )

                # 1. Publish to Kafka
                self.publish_output(output_payload)

                # 2. Write raw bytes to MySQL via audit_service
                try:
                    print(f"[DEBUG] PDF byte size for {application_id}: {len(pdf_bytes)}")
                    audit_service.process_pdf_payload(output_payload, pdf_bytes)
                    print(f"[DB SUCCESS] PDF stored in MySQL for {application_id}")
                except Exception as db_exc:
                    print(f"[DB ERROR] Failed to store PDF in MySQL: {db_exc}")
                    traceback.print_exc()

                print(f"[SUCCESS] Published manager PDF for application_id={application_id}")

                # Cleanup caches
                self.msme_info_cache.pop(application_id, None)
                self.model_output_cache.pop(application_id, None)
                self.explainability_cache.pop(application_id, None)

            except Exception as exc:
                print("[ERROR] Failed to generate manager PDF")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_manager_pdf_service():
    service = MSMEManagerPDFService()
    service.run()


if __name__ == "__main__":
    run_manager_pdf_service()

    