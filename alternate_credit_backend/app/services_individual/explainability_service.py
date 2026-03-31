from __future__ import annotations

import base64
import io
import json
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
import inspect
try:
    from .audit_service_individual import audit_service
except ImportError:
    from audit_service_individual import audit_service

import inspect

print("[DEBUG] explainability file =", __file__)
print("[DEBUG] audit service file =", inspect.getfile(audit_service.__class__))

from app.config import KAFKA_BROKER, INDIVIDUAL_ARTIFACT_DIR


# ==========================================================
# Kafka topics
# ==========================================================
FEATURES = "features"
MODEL_OUTPUT = "model_output"
EXPLAINABILITY_OUTPUT = "explainability_output"


# ==========================================================
# Artifact paths
# ==========================================================
DEFAULT_ARTIFACT_DIRS = [INDIVIDUAL_ARTIFACT_DIR]


# ==========================================================
# Service
# ==========================================================
class IndividualExplainabilityService:
    def __init__(self, kafka_broker: str = KAFKA_BROKER):
        self.kafka_broker = kafka_broker

        self.consumer = KafkaConsumer(
            FEATURES,
            MODEL_OUTPUT,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="individual_explainability_service_group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.artifact_dir = self._find_artifacts()
        self.feature_order = self._load_feature_order()
        self.lgbm_model = self._load_lgbm_model()
        self.explainer = self._build_explainer()

        # temporary in-memory join store
        self.features_cache: Dict[str, Dict[str, Any]] = {}
        self.model_output_cache: Dict[str, Dict[str, Any]] = {}

    def _build_raw_shap_rows(
        self,
        shap_values_1d: np.ndarray
    ) -> List[Dict[str, Any]]:
        rows = []
        for feat, impact in zip(self.feature_order, shap_values_1d):
            rows.append({
                "feature_name": str(feat),
                "impact": float(impact),
        })
        return rows
    # ------------------------------------------------------
    # Loaders
    # ------------------------------------------------------
    
    def _find_artifacts(self) -> Path:
        for p in DEFAULT_ARTIFACT_DIRS:
            if p.exists() and (p / "lgbm_final.pkl").exists():
                return p
        raise FileNotFoundError(
            "Artifacts not found. Expected lgbm_final.pkl in:\n"
            + "\n".join(str(p) for p in DEFAULT_ARTIFACT_DIRS)
        )

    def _load_feature_order(self) -> List[str]:
        feature_path = self.artifact_dir / "feature_columns.csv"
        if not feature_path.exists():
            raise FileNotFoundError(f"feature_columns.csv not found at {feature_path}")

        feat_df = pd.read_csv(feature_path)
        if "feature" not in feat_df.columns:
            raise ValueError("feature_columns.csv must contain a 'feature' column")

        feature_order = feat_df["feature"].tolist()
        print(f"[INFO] Loaded {len(feature_order)} individual feature columns")
        return feature_order

    def _load_lgbm_model(self):
        model_path = self.artifact_dir / "lgbm_final.pkl"
        if not model_path.exists():
            raise FileNotFoundError(f"LightGBM model not found at: {model_path}")

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        if hasattr(model, "feature_names_in_"):
            self.model_feature_names = [str(x) for x in list(model.feature_names_in_)]
        else:
            self.model_feature_names = [f"Column_{i}" for i in range(len(self.feature_order))]

        print(f"[INFO] Individual LightGBM model loaded from {model_path}")
        print(f"[INFO] Model expects {len(self.model_feature_names)} input columns")
        return model
    
    @staticmethod
    def _looks_like_generic_columns(names: List[str]) -> bool:
        if not names:
            return False
        return all(str(name).startswith("Column_") for name in names)

    def _build_model_input_df_from_actual(self, X_actual: pd.DataFrame) -> pd.DataFrame:
        """
        Converts actual pipeline dataframe into the column naming expected by the model.
        Values/order are preserved exactly.
        """
        X_model = X_actual.copy()

        if len(self.model_feature_names) != len(X_actual.columns):
            raise ValueError(
                f"Model expects {len(self.model_feature_names)} columns but pipeline produced {len(X_actual.columns)}"
            )

        X_model.columns = self.model_feature_names
        return X_model

    def _build_explainer(self):
        try:
            explainer = shap.TreeExplainer(self.lgbm_model)
            print("[INFO] SHAP TreeExplainer initialized for individual model")
            return explainer
        except Exception:
            explainer = shap.Explainer(self.lgbm_model)
            print("[INFO] SHAP generic Explainer initialized for individual model")
            return explainer

    # ------------------------------------------------------
    # Utils
    # ------------------------------------------------------
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

    @staticmethod
    def _fig_to_base64(fig) -> str:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=180)
        buf.seek(0)
        image_b64 = base64.b64encode(buf.read()).decode("utf-8")
        plt.close(fig)
        return image_b64

    # ------------------------------------------------------
    # Feature frame building
    # ------------------------------------------------------
    def _build_feature_df(self, payload: Dict[str, Any]) -> pd.DataFrame:
        """
        Supports payloads containing:
        1. exact feature_names + feature_vector
        2. engineered_features dictionary
        """
        feature_names = payload.get("feature_names")
        feature_vector = payload.get("feature_vector")
        engineered = payload.get("engineered_features", {})

        if (
            isinstance(feature_names, list)
            and isinstance(feature_vector, list)
            and feature_names == self.feature_order
            and len(feature_vector) == len(self.feature_order)
        ):
            return pd.DataFrame([feature_vector], columns=self.feature_order)

        if isinstance(feature_names, list) and isinstance(feature_vector, list):
            current_map = {
                str(name): self._safe_float(val)
                for name, val in zip(feature_names, feature_vector)
            }
            row = {f: current_map.get(f, 0.0) for f in self.feature_order}
            return pd.DataFrame([row], columns=self.feature_order)

        if isinstance(engineered, dict):
            row = {f: self._safe_float(engineered.get(f)) for f in self.feature_order}
            return pd.DataFrame([row], columns=self.feature_order)

        raise ValueError(
            "Unable to build feature DataFrame. "
            "Expected feature_names + feature_vector or engineered_features."
        )

    # ------------------------------------------------------
    # SHAP helpers
    # ------------------------------------------------------
    def _compute_shap_for_row(
        self, X_model: pd.DataFrame
    ) -> Tuple[np.ndarray, float, float]:
        """
        Returns:
            shap_values_1d
            base_value
            predicted_value_from_shap
        """
        try:
            shap_values = self.explainer.shap_values(X_model)

            if isinstance(shap_values, list):
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
                expected_arr = np.array(expected).reshape(-1)
                if len(expected_arr) > 1:
                    base_value = float(expected_arr[1])
                else:
                    base_value = float(expected_arr[0])
            else:
                base_value = float(expected)

            pred_from_shap = float(base_value + shap_1d.sum())
            return shap_1d, base_value, pred_from_shap

        except Exception:
            explanation = self.explainer(X_model)
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

        for i, feat in enumerate(self.feature_order):
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

    # ------------------------------------------------------
    # Plots
    # ------------------------------------------------------
    def _build_waterfall_base64(
        self,
        X: pd.DataFrame,
        shap_values_1d: np.ndarray,
        base_value: float,
        max_display: int = 12
    ) -> str:
        explanation = shap.Explanation(
            values=shap_values_1d,
            base_values=base_value,
            data=X.iloc[0].values,
            feature_names=self.feature_order,
        )

        shap.plots.waterfall(explanation, max_display=max_display, show=False)
        fig = plt.gcf()
        return self._fig_to_base64(fig)

    def _build_gauge_chart_base64(
        self,
        risk_score: float,
        risk_band: str
    ) -> str:
        fig, ax = plt.subplots(figsize=(8, 4), subplot_kw={"aspect": "equal"})
        ax.axis("off")

        segments = [
            (0.00, 0.08, "#2ecc71", "LOW"),
            (0.08, 0.133, "#a3d977", "LOW-MOD"),
            (0.133, 0.25, "#f1c40f", "MODERATE"),
            (0.25, 0.40, "#e67e22", "HIGH"),
            (0.40, 1.00, "#e74c3c", "VERY HIGH"),
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
            angle = np.radians(180 - mid * 180)
            ax.text(
                0.78 * np.cos(angle),
                0.78 * np.sin(angle),
                label,
                ha="center",
                va="center",
                fontsize=8,
                fontweight="bold",
            )

        score = min(max(risk_score, 0.0), 1.0)
        angle = np.radians(180 - score * 180)
        needle_x = 0.72 * np.cos(angle)
        needle_y = 0.72 * np.sin(angle)

        ax.plot([0, needle_x], [0, needle_y], linewidth=3)
        ax.add_patch(plt.Circle((0, 0), 0.05, fill=True))

        ax.text(0, -0.18, f"Risk Score: {risk_score:.4f}", ha="center", fontsize=12, fontweight="bold")
        ax.text(0, -0.32, f"Band: {risk_band}", ha="center", fontsize=11)

        ax.set_xlim(-1.1, 1.1)
        ax.set_ylim(-0.45, 1.1)

        return self._fig_to_base64(fig)

    # ------------------------------------------------------
    # Core merging and output
    # ------------------------------------------------------
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

        X_actual = self._build_feature_df(features_payload)
        X_model = self._build_model_input_df_from_actual(X_actual)

        shap_values_1d, base_value, pred_from_shap = self._compute_shap_for_row(X_model)
        raw_shap_rows = self._build_raw_shap_rows(shap_values_1d)

        # Use actual feature names for user-facing explanation
        top_positive, top_negative = self._top_drivers(shap_values_1d, X_actual, top_n=5)
        summary_text = self._build_summary_text(risk_band, top_positive, top_negative)

        waterfall_b64 = self._build_waterfall_base64(
            X_actual,
            shap_values_1d,
            base_value,
            max_display=12
        )
        gauge_b64 = self._build_gauge_chart_base64(risk_score, risk_band)

        output = {
            "application_id": application_id,
            "risk_score": risk_score,
            "risk_band": risk_band,
            "decision": model_output_payload.get("decision"),
            "decision_reason": model_output_payload.get("decision_reason"),
            "model_name": model_output_payload.get("model_name"),
            "model_version": model_output_payload.get("model_version"),

            "base_value": float(base_value),
            "predicted_value_from_shap": float(pred_from_shap),

            "raw_shap_values": raw_shap_rows,

            "top_positive_drivers": top_positive,
            "top_negative_drivers": top_negative,
            "explanation_summary": summary_text,

            "shap_waterfall_base64": waterfall_b64,
            "gauge_chart_base64": gauge_b64,

            "recommended_principal": model_output_payload.get("suggested_principal"),
            "recommended_interest_rate_annual": model_output_payload.get("suggested_interest_rate"),
            "approved_tenure": model_output_payload.get("approved_tenure"),

            "ext_source_weighted": model_output_payload.get("ext_source_weighted"),
            "monthly_headroom": model_output_payload.get("monthly_headroom"),
            "debt_to_income": model_output_payload.get("debt_to_income"),
            "safety_score": model_output_payload.get("safety_score"),
            "payer_score": model_output_payload.get("payer_score"),
            "bureau_safety_score": model_output_payload.get("bureau_safety_score"),
            "predatory_loan_signal": model_output_payload.get("predatory_loan_signal"),

            "generated_at": self._utc_now_iso(),
            "status": "SUCCESS",
        }
        return output

    def publish_output(self, output_payload: Dict[str, Any]) -> None:
        self.producer.send(EXPLAINABILITY_OUTPUT, value=output_payload)
        print("Published to explainability topic:", EXPLAINABILITY_OUTPUT)
        self.producer.flush()

    # ------------------------------------------------------
    # Runner
    # ------------------------------------------------------
    def run(self) -> None:
        print("=" * 80)
        print("Individual Explainability Service Started")
        print(f"Consuming topics : {FEATURES}, {MODEL_OUTPUT}")
        print(f"Publishing topic : {EXPLAINABILITY_OUTPUT}")
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

                if topic == FEATURES:
                    self.features_cache[application_id] = payload
                    print(f"[INFO] Cached features for application_id={application_id}")

                elif topic == MODEL_OUTPUT:
                    self.model_output_cache[application_id] = payload
                    print(f"[INFO] Cached model output for application_id={application_id}")

                merged = self._merge_ready_payloads(application_id)
                if merged is None:
                    continue

                features_payload, model_output_payload = merged

                explainability_payload = self._build_output_payload(
                    features_payload=features_payload,
                    model_output_payload=model_output_payload,
                )

                audit_service.process_explainability_payload(explainability_payload)
                print(f"[DEBUG] About to store raw SHAP for application_id={application_id}")
                print(f"[DEBUG] raw_shap_values length = {len(explainability_payload.get('raw_shap_values', []))}")
                audit_service.process_raw_shap_values(
                    application_id=application_id,
                    shap_rows=explainability_payload.get("raw_shap_values", [])
                    )
                print(f"[DEBUG] Raw SHAP storage finished for application_id={application_id}")
                self.publish_output(explainability_payload)

                print(
                    f"[SUCCESS] Published explainability output for "
                    f"application_id={application_id}"
                )

                # cleanup after successful publish
                self.features_cache.pop(application_id, None)
                self.model_output_cache.pop(application_id, None)

            except Exception as exc:
                print("[ERROR] Failed in individual explainability service")
                print(f"[ERROR] {str(exc)}")
                traceback.print_exc()


def run_explainability_service() -> None:
    service = IndividualExplainabilityService()
    service.run()


if __name__ == "__main__":
    run_explainability_service()