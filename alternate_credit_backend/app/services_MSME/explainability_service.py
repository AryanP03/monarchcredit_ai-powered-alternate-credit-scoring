from __future__ import annotations

import base64
import io
import json
import math
import pickle
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from kafka import KafkaConsumer, KafkaProducer
try:
    from .audit_service_MSME import audit_service
except ImportError:
    from audit_service_MSME import audit_service


# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

from app.config import KAFKA_BROKER

FEATURES_MSME = "features_msme"
MODEL_OUTPUT_MSME = "model_output_msme"
EXPLAINABILITY_OUTPUT_MSME = "explainability_output_msme"

MODEL_PATH = Path(r"D:\alternate_credit_backend\models\MSME\msme_RF_model_B.pkl")


# ---------------------------------------------------------------------
# SERVICE
# ---------------------------------------------------------------------

class MSMEExplainabilityService:
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
            FEATURES_MSME,
            MODEL_OUTPUT_MSME,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="msme_explainability_service_group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.model = self._load_model()
        self.explainer = self._build_explainer()

        # Temporary in-memory join store
        self.features_cache: Dict[str, Dict[str, Any]] = {}
        self.model_output_cache: Dict[str, Dict[str, Any]] = {}

    # -----------------------------------------------------------------
    # LOADERS
    # -----------------------------------------------------------------

    def _load_model(self):
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Model not found at: {MODEL_PATH}")

        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)

        if hasattr(model, "feature_names_in_"):
            if list(model.feature_names_in_) != self.FINAL_FEATURE_ORDER:
                raise ValueError(
                    "\nFEATURE MISMATCH DETECTED!\n"
                    f"Model expects:\n{list(model.feature_names_in_)}\n\n"
                    f"Pipeline provides:\n{self.FINAL_FEATURE_ORDER}\n"
                )

        print(f"[INFO] MSME model loaded from {MODEL_PATH}")
        return model

    def _build_explainer(self):
        """
        Prefer TreeExplainer for tree models.
        Fall back to generic Explainer if needed.
        """
        try:
            explainer = shap.TreeExplainer(self.model)
            print("[INFO] SHAP TreeExplainer initialized")
            return explainer
        except Exception:
            explainer = shap.Explainer(self.model)
            print("[INFO] SHAP generic Explainer initialized")
            return explainer

    # -----------------------------------------------------------------
    # UTILS
    # -----------------------------------------------------------------

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
        
    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _compute_recommended_principal_and_interest(
        self,
        features_payload: Dict[str, Any],
        risk_score: float
    ) -> Tuple[float, float]:
        """
        Returns:
            recommended_principal
            recommended_interest_rate_annual_percent
        """

        # --- Recover capital amounts from log features ---
        log_paid_in_capital = self._safe_float(features_payload.get("log_paid_in_capital"))
        log_registered_capital = self._safe_float(features_payload.get("log_registered_capital"))

        # assuming training used log1p(x)
        paid_in_capital = max(0.0, math.exp(log_paid_in_capital) - 1.0)
        registered_capital = max(0.0, math.exp(log_registered_capital) - 1.0)

        # --- Core behavioral scores ---
        repayment_score = self._clamp(
            self._safe_float(features_payload.get("loan_repayment_history_score")), 0.0, 1.0
        )
        salary_score = self._clamp(
            self._safe_float(features_payload.get("salary_disbursement_regularity_score")), 0.0, 1.0
        )
        utility_score = self._clamp(
            self._safe_float(features_payload.get("utility_bill_payment_score")), 0.0, 1.0
        )
        tax_score = self._clamp(
            self._safe_float(features_payload.get("tax_filing_compliance_score")), 0.0, 1.0
        )
        digital_score = self._clamp(
            self._safe_float(features_payload.get("digital_payment_ratio")), 0.0, 1.0
        )

        # --- Risk / instability signals ---
        region_risk = self._clamp(
            self._safe_float(features_payload.get("region_risk_score")), 0.0, 1.0
        )
        volatility = self._clamp(
            self._safe_float(features_payload.get("account_balance_volatility_score")), 0.0, 1.0
        )
        vendor_delay = self._clamp(
            self._safe_float(features_payload.get("vendor_payment_delay_trend")), 0.0, 1.0
        )

        overdue_tax = max(0.0, self._safe_float(features_payload.get("total_overdue_tax")))
        overdue_tax_norm = self._clamp(math.log1p(overdue_tax) / 15.0, 0.0, 1.0)

        avg_employees = max(0.0, self._safe_float(features_payload.get("avg_insured_employees")))
        employee_factor = self._clamp(avg_employees / 200.0, 0.0, 1.0)

        # --- Strength score ---
        business_strength = (
            0.30 * repayment_score +
            0.20 * salary_score +
            0.15 * utility_score +
            0.15 * tax_score +
            0.10 * digital_score +
            0.10 * employee_factor
        )
        business_strength = self._clamp(business_strength, 0.0, 1.0)

        # --- Stress score ---
        stress_score = (
            0.35 * risk_score +
            0.20 * volatility +
            0.15 * vendor_delay +
            0.15 * region_risk +
            0.15 * overdue_tax_norm
        )
        stress_score = self._clamp(stress_score, 0.0, 1.0)

        # --- Capital base ---
        capital_base = (0.65 * paid_in_capital) + (0.35 * registered_capital)

        # recommended sanction multiplier
        sanction_multiplier = (
            0.08 +
            0.22 * business_strength -
            0.14 * stress_score
        )
        sanction_multiplier = self._clamp(sanction_multiplier, 0.03, 0.25)

        recommended_principal = capital_base * sanction_multiplier

        # round for cleaner business output
        recommended_principal = round(recommended_principal / 1000.0) * 1000.0

        # --- Risk-based annual interest pricing ---
        base_rate = 11.0  # 11%
        annual_interest_rate = (
            base_rate +
            12.0 * risk_score +
            3.0 * volatility +
            2.5 * vendor_delay +
            2.0 * region_risk +
            1.5 * overdue_tax_norm -
            2.5 * repayment_score -
            1.5 * tax_score
        )
        annual_interest_rate = round(self._clamp(annual_interest_rate, 10.0, 30.0), 2)

        return recommended_principal, annual_interest_rate
    
    def _build_feature_df(self, payload: Dict[str, Any]) -> pd.DataFrame:
        missing = [f for f in self.FINAL_FEATURE_ORDER if f not in payload]
        if missing:
            raise ValueError(f"Missing required features: {missing}")

        row = {f: self._safe_float(payload[f]) for f in self.FINAL_FEATURE_ORDER}
        return pd.DataFrame([row], columns=self.FINAL_FEATURE_ORDER)

    @staticmethod
    def _fig_to_base64(fig) -> str:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=180)
        buf.seek(0)
        image_b64 = base64.b64encode(buf.read()).decode("utf-8")
        plt.close(fig)
        return image_b64

    # -----------------------------------------------------------------
    # SHAP HELPERS
    # -----------------------------------------------------------------

    def _compute_shap_for_row(
        self, X: pd.DataFrame
    ) -> Tuple[np.ndarray, float, float]:
        """
        Returns:
            shap_values_1d
            base_value
            predicted_value_from_shap
        """
        # Try TreeExplainer style first
        try:
            shap_values = self.explainer.shap_values(X)

            if isinstance(shap_values, list):
                # Binary classifier often returns [class0, class1]
                shap_1d = np.array(shap_values[1][0], dtype=float)
            else:
                shap_arr = np.array(shap_values)
                if shap_arr.ndim == 3:
                    shap_1d = shap_arr[0, :, 1]
                elif shap_arr.ndim == 2:
                    shap_1d = shap_arr[0]
                else:
                    raise ValueError("Unsupported SHAP output shape")

            expected = self.explainer.expected_value
            if isinstance(expected, (list, np.ndarray)):
                if len(np.array(expected).shape) > 0 and len(expected) > 1:
                    base_value = float(expected[1])
                else:
                    base_value = float(np.array(expected).reshape(-1)[0])
            else:
                base_value = float(expected)

            pred_from_shap = float(base_value + shap_1d.sum())
            return shap_1d, base_value, pred_from_shap

        except Exception:
            pass

        # Fallback to generic Explainer output
        explanation = self.explainer(X)
        vals = np.array(explanation.values)

        if vals.ndim == 3:
            shap_1d = vals[0, :, 1]
        elif vals.ndim == 2:
            shap_1d = vals[0]
        else:
            raise ValueError("Unsupported generic SHAP output shape")

        base_vals = np.array(explanation.base_values)
        if base_vals.ndim == 2:
            base_value = float(base_vals[0, 1])
        elif base_vals.ndim == 1:
            base_value = float(base_vals[0])
        else:
            base_value = float(base_vals.reshape(-1)[0])

        pred_from_shap = float(base_value + shap_1d.sum())
        return shap_1d, base_value, pred_from_shap

    def _top_drivers(
        self,
        shap_values_1d: np.ndarray,
        X: pd.DataFrame,
        top_n: int = 5
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        contributions = []
        row = X.iloc[0]

        for i, feat in enumerate(self.FINAL_FEATURE_ORDER):
            contributions.append({
                "feature": feat,
                "feature_value": float(row[feat]),
                "shap_impact": float(shap_values_1d[i]),
            })

        positive = sorted(
            [c for c in contributions if c["shap_impact"] > 0],
            key=lambda x: x["shap_impact"],
            reverse=True,
        )[:top_n]

        negative = sorted(
            [c for c in contributions if c["shap_impact"] < 0],
            key=lambda x: x["shap_impact"],
        )[:top_n]

        return positive, negative

    def _build_summary_text(
        self,
        risk_band: str,
        top_positive: List[Dict[str, Any]],
        top_negative: List[Dict[str, Any]]
    ) -> str:
        pos_feats = ", ".join([x["feature"] for x in top_positive[:3]]) or "none"
        neg_feats = ", ".join([x["feature"] for x in top_negative[:3]]) or "none"

        return (
            f"Risk band is {risk_band}. "
            f"Main factors increasing risk were: {pos_feats}. "
            f"Main factors reducing risk were: {neg_feats}."
        )

    # -----------------------------------------------------------------
    # PLOTS
    # -----------------------------------------------------------------

    def _build_waterfall_base64(
        self,
        X: pd.DataFrame,
        shap_values_1d: np.ndarray,
        base_value: float,
        max_display: int = 12
    ) -> str:
        """
        Uses SHAP waterfall plot and returns image as base64.
        """
        explanation = shap.Explanation(
            values=shap_values_1d,
            base_values=base_value,
            data=X.iloc[0].values,
            feature_names=self.FINAL_FEATURE_ORDER,
        )

        shap.plots.waterfall(explanation, max_display=max_display, show=False)
        fig = plt.gcf()
        return self._fig_to_base64(fig)

    def _build_gauge_chart_base64(
        self,
        risk_score: float,
        risk_band: str
    ) -> str:
        """
        Semicircular gauge chart.
        """
        fig, ax = plt.subplots(figsize=(8, 4), subplot_kw={"aspect": "equal"})
        ax.axis("off")

        # Gauge segments
        segments = [
            (0.00, 0.20, "#2ecc71", "LOW"),
            (0.20, 0.40, "#f1c40f", "MODERATE"),
            (0.40, 0.70, "#e67e22", "HIGH"),
            (0.70, 1.00, "#e74c3c", "VERY HIGH"),
        ]

        radius = 1.0
        width = 0.22

        for start, end, color, label in segments:
            theta1 = 180 - end * 180
            theta2 = 180 - start * 180
            wedge = plt.matplotlib.patches.Wedge(
                (0, 0),
                r=radius,
                theta1=theta1,
                theta2=theta2,
                width=width,
                facecolor=color,
                edgecolor="white",
            )
            ax.add_patch(wedge)

            mid = (start + end) / 2
            angle = math.radians(180 - mid * 180)
            ax.text(
                0.78 * math.cos(angle),
                0.78 * math.sin(angle),
                label,
                ha="center",
                va="center",
                fontsize=9,
                fontweight="bold",
            )

        # Needle
        score = min(max(risk_score, 0.0), 1.0)
        angle = math.radians(180 - score * 180)
        needle_x = 0.72 * math.cos(angle)
        needle_y = 0.72 * math.sin(angle)

        ax.plot([0, needle_x], [0, needle_y], linewidth=3)
        ax.add_patch(plt.Circle((0, 0), 0.05, fill=True))

        # Labels
        ax.text(0, -0.18, f"Risk Score: {risk_score:.4f}", ha="center", fontsize=12, fontweight="bold")
        ax.text(0, -0.32, f"Band: {risk_band}", ha="center", fontsize=11)

        ax.set_xlim(-1.1, 1.1)
        ax.set_ylim(-0.45, 1.1)

        return self._fig_to_base64(fig)

    # -----------------------------------------------------------------
    # CORE PROCESSING
    # -----------------------------------------------------------------

    def _merge_ready_payloads(
        self,
        application_id: str
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        features = self.features_cache.get(application_id)
        model_output = self.model_output_cache.get(application_id)

        if features is None or model_output is None:
            return None

        return features, model_output

    def _build_output_payload(
        self,
        features_payload: Dict[str, Any],
        model_output_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        application_id = features_payload["application_id"]
        risk_score = float(model_output_payload["risk_score"])
        risk_band = str(model_output_payload["risk_band"])

        X = self._build_feature_df(features_payload)
        shap_values_1d, base_value, pred_from_shap = self._compute_shap_for_row(X)

        top_positive, top_negative = self._top_drivers(shap_values_1d, X, top_n=5)
        summary_text = self._build_summary_text(risk_band, top_positive, top_negative)

        recommended_principal, recommended_interest_rate = (
            self._compute_recommended_principal_and_interest(
                features_payload=features_payload,
                risk_score=risk_score
            )
        )

        waterfall_b64 = self._build_waterfall_base64(X, shap_values_1d, base_value, max_display=12)
        gauge_b64 = self._build_gauge_chart_base64(risk_score, risk_band)

        output = {
            "application_id": application_id,
            "risk_score": risk_score,
            "risk_band": risk_band,
            "model_name": model_output_payload.get("model_name"),
            "model_version": model_output_payload.get("model_version"),
            "base_value": float(base_value),
            "predicted_value_from_shap": float(pred_from_shap),
            "top_positive_drivers": top_positive,
            "top_negative_drivers": top_negative,
            "explanation_summary": summary_text,
            "shap_waterfall_base64": waterfall_b64,
            "gauge_chart_base64": gauge_b64,
            "generated_at": self._utc_now_iso(),
            "recommended_principal": float(recommended_principal),
            "recommended_interest_rate_annual": float(recommended_interest_rate),
            "status": "SUCCESS",
        }
        return output

    def publish_output(self, output_payload: Dict[str, Any]) -> None:
        self.producer.send(EXPLAINABILITY_OUTPUT_MSME, value=output_payload)
        print("Published to explainability topic:", EXPLAINABILITY_OUTPUT_MSME)
        self.producer.flush()

    def run(self) -> None:
        print("=" * 80)
        print("MSME Explainability Service Started")
        print(f"Consuming topics : {FEATURES_MSME}, {MODEL_OUTPUT_MSME}")
        print(f"Publishing topic : {EXPLAINABILITY_OUTPUT_MSME}")
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

                if topic == FEATURES_MSME:
                    self.features_cache[application_id] = payload
                    print(f"[INFO] Cached features for application_id={application_id}")
                    print(f"[DEBUG] Features cache size: {len(self.features_cache)}")

                elif topic == MODEL_OUTPUT_MSME:
                    self.model_output_cache[application_id] = payload
                    print(f"[INFO] Cached model output for application_id={application_id}")
                    print(f"[DEBUG] Model output cache size: {len(self.model_output_cache)}")

                print(
                    f"[DEBUG] Ready check for {application_id} | "
                    f"has_features={application_id in self.features_cache}, "
                    f"has_model_output={application_id in self.model_output_cache}"
)


                merged = self._merge_ready_payloads(application_id)
                if merged is None:
                    continue

                features_payload, model_output_payload = merged

                explainability_payload = self._build_output_payload(
                    features_payload=features_payload,
                    model_output_payload=model_output_payload,
                )
                
                audit_service.process_explainability_payload(explainability_payload)
                print(f"[DEBUG] SHAP base64 length for {application_id}: {len(explainability_payload.get('shap_waterfall_base64', ''))}")

                print(f"[DEBUG] Explainability payload built for {application_id}")
                print(f"[DEBUG] Publishing explainability output for {application_id}")

                self.publish_output(explainability_payload)

                try:
                    print(f"[DEBUG] Saving SHAP for {application_id}")
                    audit_service.process_explainability_payload(explainability_payload)
                    print(f"[DB SUCCESS] Updated Explainability for {application_id}")
                except Exception as db_exc:
                    print(f"[DB ERROR] Audit service failed: {db_exc}")

                print(
                    f"[SUCCESS] Published explainability output for "
                    f"application_id={application_id}"
                )

                # cleanup after successful publish
                self.features_cache.pop(application_id, None)
                self.model_output_cache.pop(application_id, None)

            except Exception as exc:
                print("[ERROR] Failed in explainability service")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_explainability_service() -> None:
    service = MSMEExplainabilityService()
    service.run()


if __name__ == "__main__":
    run_explainability_service()