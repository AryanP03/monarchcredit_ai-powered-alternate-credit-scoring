import os
import pickle
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

os.environ["TOKENIZERS_PARALLELISM"] = "false"

import mysql.connector
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from rank_bm25 import BM25Okapi

# ── Paths ──────────────────────────────────────────────────────────────────────
BM25_PATH   = Path(__file__).parent / "bm25_index.pkl"
CHROMA_DIR  = Path(__file__).parent / "chroma_db"
EMBED_MODEL = "all-MiniLM-L6-v2"

TOP_K        = 5
BM25_TOP_N   = 6
CHROMA_TOP_N = 6
RRF_K        = 60

_EXECUTOR = ThreadPoolExecutor(max_workers=2)

# ── MySQL config ───────────────────────────────────────────────────────────────
MYSQL_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "aryan123",
    "database": "raw_shap_values",
}

# ── MySQL table names ──────────────────────────────────────────────────────────
INDIVIDUAL_SHAP_TABLE = "individual_raw_shap_values"
MSME_SHAP_TABLE       = "msme_raw_shap_values"  # keep only if you actually have this


# ── Keyword lists ──────────────────────────────────────────────────────────────
APPLICANT_KEYWORDS = [
    "my score", "my risk", "my result", "my credit", "my application",
    "why rejected", "why denied", "why low", "why high",
    "my shap", "shap chart", "show shap", "show chart", "my chart",
    "explain my", "my features", "my band", "risk band",
    "what if", "if i improve", "if i increase", "if i decrease",
    "what happens if", "how can i improve", "how to improve my",
    "my loan", "my principal", "my interest", "my tenure",
    "my decision", "loan amount", "interest rate", "suggested",
    "my pd score", "probability of default", "my probability",
    "which feature", "top feature", "feature hurt", "feature helped",
    "score breakdown", "score explanation", "explain score",
    "my policy score", "policy score", "my business score",
]

PROJECT_KEYWORDS = [
    "credit scoring", "alternate credit", "alternative credit", "msme", "barclays challenge",
    "financial inclusion", "unbanked", "under-banked", "creditworthiness", "monarch",
    "monarchcredit",
    "pipeline", "train", "training", "precision", "recall", "f1", "auc",
    "ensemble", "random forest", "xgboost", "lightgbm", "lgbm",
    "smote", "oversampling", "imbalanced",
    "synthetic feature", "behavioral feature", "shap", "feature importance",
    "tax compliance", "digital payment", "utility payment", "vendor payment",
    "account balance", "loan repayment",
    "fairness", "disparate impact", "equalized odds", "proxy discrimination",
    "selection rate", "calibration",
    "duckdb", "mlflow", "dvc", "chroma", "bm25", "rag", "vector",
    "problem statement", "financial exclusion", "e-commerce data",
    "individual pipeline", "stacking", "optuna", "isotonic",
    "working capital", "screener",
]

FINANCE_KEYWORDS = [
    "loan", "interest rate", "emi", "credit score", "cibil", "credit report",
    "savings", "investment", "mutual fund", "stock", "equity", "debt",
    "mortgage", "home loan", "personal loan", "car loan", "education loan",
    "bank", "banking", "account", "deposit", "withdrawal", "transfer",
    "inflation", "gdp", "economy", "recession", "budget", "tax",
    "insurance", "premium", "policy", "pension", "retirement",
    "nbfc", "rbi", "sebi", "financial", "finance", "money", "payment",
    "wallet", "upi", "neft", "rtgs", "imps", "cheque", "kyc",
    "net worth", "asset", "liability", "balance sheet", "profit", "loss",
    "revenue", "expense", "cash flow", "working capital",
    "microfinance", "self help group", "shg", "priority sector",
    "financial literacy", "financial planning", "wealth management",
    "barclays", "barclay", "barclaycard", "barclays bank",
    "barclays india", "barclays uk", "barclays innovation",
    "barclays rise", "barclays accelerator",
]

OUT_OF_SCOPE_RESPONSE = (
    "I can help you with:\n"
    "• Questions about your credit score and SHAP explanation\n"
    "• Questions about the AI Credit Scoring project\n"
    "• General finance topics (loans, credit scores, banking, etc.)\n"
    "• Information about Barclays\n\n"
    "Your question seems to be outside these topics. "
    "Please ask something related to finance, banking, or your credit report!"
)


def is_applicant_question(query: str) -> bool:
    q = query.lower()
    return any(kw in q for kw in APPLICANT_KEYWORDS)


def is_project_question(query: str) -> bool:
    q = query.lower()
    return any(kw in q for kw in PROJECT_KEYWORDS)


def is_finance_or_barclays(query: str) -> bool:
    q = query.lower()
    return any(kw in q for kw in FINANCE_KEYWORDS)


def is_in_scope(query: str) -> bool:
    return (
        is_applicant_question(query)
        or is_project_question(query)
        or is_finance_or_barclays(query)
    )


# ── MySQL fetch: Individual from SHAP-only table ──────────────────────────────
def _fetch_individual_context(cursor, application_id: str) -> dict:
    """
    Fetch Individual applicant data using only individual_raw_shap_values.
    """

    cursor.execute(
        f"""
        SELECT feature_name, impact
        FROM {INDIVIDUAL_SHAP_TABLE}
        WHERE application_id = %s
        ORDER BY ABS(impact) DESC
        LIMIT 10
        """,
        (application_id,),
    )
    shap_rows = cursor.fetchall()

    if not shap_rows:
        return {}

    shap_lines = []
    positive_rows = []
    negative_rows = []

    for row in shap_rows:
        feature = row["feature_name"]
        impact = float(row["impact"])

        if impact > 0:
            direction = "↑ increases default risk"
            positive_rows.append((feature, impact))
        else:
            direction = "↓ reduces default risk"
            negative_rows.append((feature, impact))

        shap_lines.append(f"  • {feature}: {round(impact, 4)} ({direction})")

    top_risk_drivers = "\n".join(
        [f"  • {f}: {round(v, 4)}" for f, v in positive_rows[:5]]
    ) or "  • No major risk-increasing features found"

    top_supporting_factors = "\n".join(
        [f"  • {f}: {round(v, 4)}" for f, v in negative_rows[:5]]
    ) or "  • No major risk-reducing features found"

    shap_text = "\n".join(shap_lines)

    text_context = (
        f"Individual Applicant SHAP Report (application_id: {application_id}):\n\n"
        f"Top SHAP Feature Impacts:\n"
        f"{shap_text}\n\n"
        f"Main risk-increasing features:\n"
        f"{top_risk_drivers}\n\n"
        f"Main risk-reducing features:\n"
        f"{top_supporting_factors}\n\n"
        f"Interpretation rules:\n"
        f"  • Positive SHAP impact = feature increased default risk (bad)\n"
        f"  • Negative SHAP impact = feature reduced default risk (good)\n"
        f"  • Larger absolute value = stronger influence on the prediction\n"
        f"  • If the sum of major positive impacts dominates, rejection risk is higher"
    )

    return {"text": text_context, "image": "", "pipeline": "individual"}


# ── MySQL fetch: MSME from SHAP-only table ────────────────────────────────────
def _fetch_msme_context(cursor, application_id: str) -> dict:
    cursor.execute(
        f"""
        SELECT feature_name, impact
        FROM {MSME_SHAP_TABLE}
        WHERE application_id = %s
        ORDER BY ABS(impact) DESC
        LIMIT 10
        """,
        (application_id,),
    )
    shap_rows = cursor.fetchall()

    if not shap_rows:
        return {}

    shap_lines = []
    for row in shap_rows:
        direction = "↑ increases default risk" if row["impact"] > 0 else "↓ reduces default risk"
        shap_lines.append(
            f"  • {row['feature_name']}: {round(float(row['impact']), 4)} ({direction})"
        )

    shap_text = "\n".join(shap_lines)

    text_context = (
        f"MSME Business SHAP Report (application_id: {application_id}):\n\n"
        f"Top SHAP Feature Impacts:\n"
        f"{shap_text}\n\n"
        f"Interpretation rules:\n"
        f"  • Positive SHAP impact = feature increased default risk (bad)\n"
        f"  • Negative SHAP impact = feature reduced default risk (good)\n"
        f"  • Larger absolute value = stronger influence on the prediction"
    )

    return {"text": text_context, "image": "", "pipeline": "msme"}


def fetch_applicant_context(application_id: str, pipeline: str = "individual") -> dict:
    if not application_id:
        return {"text": "", "image": "", "pipeline": pipeline, "found": False}

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        print(f"[DEBUG] Connected to MySQL. app_id={application_id}, pipeline={pipeline}")

        if pipeline == "msme":
            result = _fetch_msme_context(cursor, application_id)
        else:
            result = _fetch_individual_context(cursor, application_id)

        cursor.close()
        conn.close()

        if not result:
            print(f"[DEBUG] No rows found for application_id={application_id}")
            return {"text": "", "image": "", "pipeline": pipeline, "found": False}

        result["found"] = True
        return result

    except mysql.connector.Error as e:
        print(f"[ERROR] fetch_applicant_context: MySQL error — {e}")
        return {"text": "", "image": "", "pipeline": pipeline, "found": False}
    except Exception as e:
        print(f"[ERROR] fetch_applicant_context: Unexpected error — {e}")
        return {"text": "", "image": "", "pipeline": pipeline, "found": False}


def rrf_score(rank: int, k: int = RRF_K) -> float:
    return 1.0 / (k + rank + 1)


def fuse_results(bm25_texts, chroma_texts, all_chunks):
    scores: dict[str, float] = {}

    for rank, text in enumerate(bm25_texts):
        key = text[:200]
        scores[key] = scores.get(key, 0.0) + rrf_score(rank)

    for rank, text in enumerate(chroma_texts):
        key = text[:200]
        scores[key] = scores.get(key, 0.0) + rrf_score(rank)

    chunk_map = {c.page_content[:200]: c for c in all_chunks}
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    result = []
    for key, _ in ranked[:TOP_K]:
        if key in chunk_map:
            result.append(chunk_map[key])
    return result


class HybridRetriever:
    def __init__(self):
        print("[INFO] Loading BM25 index...")
        with open(BM25_PATH, "rb") as f:
            data = pickle.load(f)
        self.bm25: BM25Okapi = data["bm25"]
        self.bm25_texts: list[str] = data["texts"]
        self.all_chunks = data["chunks"]

        print(f"[INFO] Loading ChromaDB with '{EMBED_MODEL}'...")
        self.embeddings = HuggingFaceEmbeddings(
            model_name=EMBED_MODEL,
            model_kwargs={"device": "cpu"},
            encode_kwargs={"normalize_embeddings": True, "batch_size": 32},
        )
        self.vectorstore = Chroma(
            persist_directory=str(CHROMA_DIR),
            embedding_function=self.embeddings,
        )
        print(f"[INFO] Hybrid retriever ready — {len(self.bm25_texts)} chunks indexed")

    def _bm25_search(self, query: str) -> list[str]:
        tokenized = query.lower().split()
        scores = self.bm25.get_scores(tokenized)
        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:BM25_TOP_N]
        return [self.bm25_texts[i] for i in top_idx]

    def _chroma_search(self, query: str) -> list[str]:
        results = self.vectorstore.similarity_search(query, k=CHROMA_TOP_N)
        return [doc.page_content for doc in results]

    def retrieve(
        self,
        query: str,
        application_id: str = None,
        pipeline: str = "individual",
    ) -> tuple[list, bool, dict]:
        if not is_in_scope(query):
            return [], False, {}

        if is_applicant_question(query):
            if application_id:
                applicant_data = fetch_applicant_context(application_id, pipeline)
                return [], True, applicant_data
            return [], True, {"text": "", "image": "", "pipeline": pipeline, "found": False}

        if is_project_question(query):
            future_bm25 = _EXECUTOR.submit(self._bm25_search, query)
            future_chroma = _EXECUTOR.submit(self._chroma_search, query)
            bm25_texts = future_bm25.result()
            chroma_texts = future_chroma.result()
            fused = fuse_results(bm25_texts, chroma_texts, self.all_chunks)
            return fused, True, {}

        return [], True, {}