"""
model_bridge.py
─────────────────────────────────────────────────────────────────────────────
AI Credit Scoring Chatbot — Model Bridge (Individual + MSME)
─────────────────────────────────────────────────────────────────────────────
Imports both scoring services directly from the backend app folder.
Used for what-if simulation in the chatbot.

Folder structure assumed:
    ALTERNATE_CREDIT_BACKEND/
    ├── app/
    │   ├── services_individual/scoring_service.py
    │   └── services_MSME/scoring_service.py
    └── chatbot/
        └── model_bridge.py   ← this file
─────────────────────────────────────────────────────────────────────────────
"""

import sys
from pathlib import Path

# ── Add project root to sys.path so app.* imports work ───────────────────────
PROJECT_ROOT = Path(__file__).parent.parent   # ALTERNATE_CREDIT_BACKEND/
sys.path.insert(0, str(PROJECT_ROOT))

# ── Import Individual scoring service ─────────────────────────────────────────
individual_service = None   # just None at import


def _get_individual_service():
    global _individual_service
    if _individual_service is None:
        _individual_service = IndividualScoringService()
    return _individual_service
try:
    from app.services_individual.scoring_service import IndividualScoringService
    _individual_service = IndividualScoringService()
    print("[INFO] model_bridge: IndividualScoringService loaded successfully.")
except Exception as e:
    print(f"[WARNING] model_bridge: Could not load IndividualScoringService — {e}")
    print("[WARNING] What-if simulation for Individual pipeline will be unavailable.")

# ── Import MSME scoring service ────────────────────────────────────────────────
_msme_service = None
try:
    from app.services_MSME.scoring_service import MSMEScoringService
    _msme_service = MSMEScoringService()
    print("[INFO] model_bridge: MSMEScoringService loaded successfully.")
except Exception as e:
    print(f"[WARNING] model_bridge: Could not load MSMEScoringService — {e}")
    print("[WARNING] What-if simulation for MSME pipeline will be unavailable.")


# ── Availability checks ────────────────────────────────────────────────────────

def is_individual_available() -> bool:
    return _individual_service is not None


def is_msme_available() -> bool:
    return _msme_service is not None


# ── Individual: what-if simulation ────────────────────────────────────────────

def run_individual_prediction(payload: dict) -> dict | None:
    """
    Run Individual pipeline on a modified payload for what-if simulation.

    Args:
        payload: must contain:
            - application_id: str
            - engineered_features: dict  (all 61 individual features)
            - loan_amt: float (optional)
            - loan_tenure_months: int (optional)

    Returns:
        {
            "pd_score":                float,
            "risk_band":               str,
            "decision":                str,
            "decision_reason":         str,
            "lgbm_prob":               float,
            "xgb_prob":                float,
            "suggested_principal":     float,
            "suggested_interest_rate": float | None,
            "approved_tenure":         int,
        }
        or None if service unavailable.
    """
    if not is_individual_available():
        print("[WARNING] model_bridge: IndividualScoringService not loaded.")
        return None

    try:
        X_model        = _individual_service._build_model_input_df(payload)
        score_block    = _individual_service._predict_scores(X_model)
        pd_score       = score_block["pd_score"]
        risk_band      = _individual_service._get_risk_band(pd_score)
        decision_block = _individual_service._compute_credit_policy(
            payload, pd_score, risk_band
        )

        return {
            "pd_score":                round(pd_score, 6),
            "risk_band":               risk_band,
            "decision":                decision_block["decision"],
            "decision_reason":         decision_block["decision_reason"],
            "lgbm_prob":               round(score_block["lgbm_prob"], 6),
            "xgb_prob":                round(score_block["xgb_prob"], 6),
            "suggested_principal":     decision_block["suggested_principal"],
            "suggested_interest_rate": decision_block.get("suggested_interest_rate"),
            "approved_tenure":         decision_block["approved_tenure"],
        }

    except Exception as e:
        print(f"[ERROR] model_bridge.run_individual_prediction: {e}")
        return None


# ── MSME: what-if simulation ───────────────────────────────────────────────────

def run_msme_prediction(payload: dict) -> dict | None:
    """
    Run MSME pipeline on a modified payload for what-if simulation.

    Args:
        payload: flat dict containing:
            - application_id: str
            - loan_amt: float
            - tenure: float
            - all 37 MSME features (see MSMEScoringService.FINAL_FEATURE_ORDER)

    Returns:
        {
            "risk_score":              float,
            "risk_band":               str,
            "decision":                str,
            "decision_reason":         str,
            "policy_score":            float,
            "approved_principal":      float,
            "approved_tenure":         int,
            "suggested_interest_rate": float | None,
        }
        or None if service unavailable.
    """
    if not is_msme_available():
        print("[WARNING] model_bridge: MSMEScoringService not loaded.")
        return None

    try:
        X              = _msme_service._build_model_input(payload)
        risk_score     = _msme_service._predict_risk_score(X)
        risk_band      = _msme_service._get_risk_band(risk_score)
        decision_block = _msme_service._compute_credit_decision(payload, risk_score)

        return {
            "risk_score":              round(risk_score, 6),
            "risk_band":               risk_band,
            "decision":                decision_block["decision"],
            "decision_reason":         decision_block["decision_reason"],
            "policy_score":            decision_block["policy_score"],
            "approved_principal":      decision_block["approved_principal"],
            "approved_tenure":         decision_block["approved_tenure"],
            "suggested_interest_rate": decision_block.get("suggested_interest_rate"),
        }

    except Exception as e:
        print(f"[ERROR] model_bridge.run_msme_prediction: {e}")
        return None


# ── Shared: format before/after diff for LLM prompt ──────────────────────────

def format_whatif_for_prompt(
    old_result: dict,
    new_result: dict,
    changed_feature: str,
    pipeline: str,
) -> str:
    """
    Formats old vs new prediction into readable text for LLM context injection.

    Args:
        old_result:      original prediction dict
        new_result:      new prediction dict after feature change
        changed_feature: name of the feature that was modified
        pipeline:        "individual" or "msme"
    """
    score_key = "pd_score" if pipeline == "individual" else "risk_score"
    old_score = old_result.get(score_key, "?")
    new_score = new_result.get(score_key, "?")
    old_band  = old_result.get("risk_band", "?")
    new_band  = new_result.get("risk_band", "?")
    old_dec   = old_result.get("decision", "?")
    new_dec   = new_result.get("decision", "?")

    try:
        direction = "improved" if float(new_score) < float(old_score) else "worsened"
    except Exception:
        direction = "changed"

    lines = [
        f"What-if simulation result for changing '{changed_feature}':",
        f"  Before → Score: {old_score}, Risk Band: {old_band}, Decision: {old_dec}",
        f"  After  → Score: {new_score}, Risk Band: {new_band}, Decision: {new_dec}",
        f"  Outcome: Credit risk has {direction}.",
    ]

    if old_dec != new_dec:
        lines.append(
            f"  Note: Decision changed from {old_dec} → {new_dec}."
        )

    # Surface interest rate change if available
    old_rate = old_result.get("suggested_interest_rate")
    new_rate = new_result.get("suggested_interest_rate")
    if old_rate is not None and new_rate is not None and old_rate != new_rate:
        lines.append(
            f"  Interest rate: {old_rate}% → {new_rate}%"
        )

    return "\n".join(lines)
