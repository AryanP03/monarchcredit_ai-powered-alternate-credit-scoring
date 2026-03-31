"""
app.py
─────────────────────────────────────────────────────────────────────────────
AI Credit Scoring Chatbot — FastAPI Backend (Individual + MSME)
─────────────────────────────────────────────────────────────────────────────
Changes:
  • ChatRequest accepts application_id + session_id + pipeline
  • /chat correctly passes application_id to stream_response()
  • /chat correctly routes by pipeline ("individual" or "msme")
  • Fixed Optional import
  • Fixed mutable default for history using Field(default_factory=list)
  • Default language: English
  • Switches language ONLY when user writes in another language
  • Priority languages: Hindi, Hinglish, English, Marathi
  • Secondary languages: Bengali, Tamil, Telugu, Kannada, Gujarati,
    Punjabi, Malayalam, Odia, Urdu, French, Spanish, German, Arabic,
    Portuguese, Russian, Japanese, Chinese, Korean

Run:
    python app.py  →  http://localhost:8999
─────────────────────────────────────────────────────────────────────────────
"""

import json
from pathlib import Path
from typing import AsyncGenerator, Optional, Literal

from groq import Groq
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from hybrid_retriever import HybridRetriever, OUT_OF_SCOPE_RESPONSE
import model_bridge
import os


app = FastAPI(title="AI Credit Scoring Chatbot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Load retriever once on startup ────────────────────────────────────────────
print("[INFO] Loading hybrid retriever...")
retriever = HybridRetriever()
print("[INFO] Retriever ready.")

# ── Groq config ───────────────────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = "llama-3.1-8b-instant"

client = Groq(api_key=GROQ_API_KEY)

SYSTEM_PROMPT = """You are a friendly and helpful assistant for the AI-Powered Alternate Credit Scoring project — a Barclays innovation challenge submission.

Your job is to help people understand their credit score, SHAP explanation, finance topics, and Barclays in a simple and clear way.

The project has two pipelines:
1. MSME Pipeline — scores small businesses using RF+XGB+LightGBM ensemble, 37 features, fairness-aware with SHAP analysis
2. Individual Pipeline (MonarchCredit) — scores individuals using RF+XGB+LR stacking ensemble, 61 engineered features, Optuna hyperparameter tuning

APPLICANT CONTEXT RULES:
- When applicant credit report data is provided, use it to answer questions about their score.
- For Individual pipeline: PD Score = Probability of Default. Lower is better. Threshold = 0.133.
- For MSME pipeline: Risk Score = probability of default. Lower is better. Threshold = 0.14.
- Explain SHAP values clearly: positive impact = increased default risk (bad), negative impact = reduced default risk (good).
- Risk bands for Individual: LOW < LOW-MODERATE < MODERATE < HIGH < VERY HIGH
- Risk bands for MSME: LOW < MODERATE < HIGH < VERY_HIGH
- For what-if results, clearly explain the before/after difference.
- If no applicant data found, say the application may not have been processed yet.

LANGUAGE RULES (CRITICAL):
- DEFAULT LANGUAGE IS ENGLISH. Always reply in English unless the user's message is clearly written in another language.
- If the user writes in English or if the language is ambiguous, ALWAYS reply in English.
- ONLY switch to another language when the user's message is clearly and explicitly written in that language.
- Hindi: reply in Hindi ONLY if the user wrote in Devanagari Hindi script.
- Hinglish: reply in Hinglish ONLY if the user clearly wrote in Roman Hindi or Hindi-English mix.
- Marathi: reply in Marathi ONLY if the user clearly wrote in Marathi (Devanagari or Roman).
- Same rule applies to all other languages — only match the language the user actually wrote in.
- Never switch languages on your own. Follow the user's language exactly.
- Never mention language switching.

TONE RULES:
- Answer directly. No preamble.
- Never repeat the user's question back.
- Short sentences. Bullet points for lists. No long paragraphs.
- Off-topic: say "I can only help with finance, credit scoring, or Barclays topics!"

STRICT RULES:
- Never make up project metrics — only use what's in provided context.
- Never mention "context", "knowledge base", or "provided context".
- Keep answers concise."""


# ── What-if keywords ──────────────────────────────────────────────────────────
WHATIF_KEYWORDS = [
    "what if", "if i improve", "if i increase", "if i decrease",
    "what happens if", "how can i improve", "if my",
    "suppose i", "if i change", "what would happen",
]


def is_whatif_question(query: str) -> bool:
    return any(kw in query.lower() for kw in WHATIF_KEYWORDS)


# ── Request model ─────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str
    history: list[dict] = Field(default_factory=list)
    application_id: Optional[str] = None
    session_id: Optional[str] = None
    pipeline: Literal["individual", "msme"] = "individual"


# ── Language detection ────────────────────────────────────────────────────────

# Priority word lists — used in fallback heuristic if AI detection fails
_MARATHI_DEVANAGARI = [
    "काय", "कसं", "कसा", "कशी", "कुठे", "किती", "माझा", "माझी", "माझे",
    "मला", "तुमचा", "तुमची", "तुमचे", "सांगा", "सांग", "करू", "झाला",
    "झाली", "पाहिजे", "कारण", "आहे", "नाही", "होतं", "मराठी",
]
_HINDI_DEVANAGARI = [
    "क्या", "क्यों", "कैसे", "मेरा", "मेरी", "मुझे", "बताओ", "समझाओ",
    "है", "हूँ", "हो", "था", "थी", "करो", "करें", "हिंदी", "नहीं",
]
_MARATHI_ROMAN = [
    "kay", "kaay", "kasa", "kashi", "kiti", "kuthe", "aahe", "nahi",
    "majha", "majhi", "majhe", "mala", "tumcha", "tumchi", "tumche",
    "sanga", "sang", "karu", "zala", "jhala", "pahije", "karan",
    "ahe", "naahi", "hota", "hoti", "marathi",
]
_HINGLISH_ROMAN = [
    "kya", "kyu", "kyun", "kaise", "mera", "meri", "mere", "mujhe",
    "samjhao", "samjha", "batao", "hai", "hua", "ho gaya", "kya hua",
    "nahi", "haan", "theek", "acha", "accha", "yaar", "bhai", "hindi",
]


def detect_language_ai(message: str) -> str:
    msg = message.strip()
    msg_lower = msg.lower()

    # ── Step 1: AI-based detection ────────────────────────────────────────────
    try:
        detection_prompt = f"""Detect the language of this message and reply with ONLY one label.

DEFAULT: If the message is in English or is ambiguous, reply "english".

PRIORITY — detect these with highest accuracy:
- english    → plain English, or ambiguous/mixed where English dominates
- hindi      → Hindi clearly written in Devanagari script
- hinglish   → Hindi clearly written in Roman script, or clear Hindi-English Roman mix
- marathi    → Marathi clearly written in Devanagari or Roman script

SECONDARY — detect these ONLY if the message is clearly and exclusively in that language:
- bengali, tamil, telugu, kannada, gujarati, punjabi, malayalam, odia, urdu
- french, spanish, german, arabic, portuguese, russian, japanese, chinese, korean
- other:<lang>  → only if clearly another language not listed

KEY RULES:
- When in doubt, return "english"
- "hinglish" = Roman-script Hindi or clear Hindi-English mix (e.g. "mera score kya hai")
- "hindi" = clearly Devanagari Hindi (e.g. "मेरा स्कोर क्या है")
- "marathi" = clearly Marathi (e.g. "माझा स्कोर काय आहे" or "maza score kay aahe")
- A single Hindi/Indian word inside an English sentence = still "english"

Message: "{msg}"

Reply with ONLY one label, nothing else."""

        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": detection_prompt}],
            temperature=0,
            max_tokens=10,
        )

        result = response.choices[0].message.content.strip().lower()

        allowed = {
            "english", "hindi", "hinglish", "marathi",
            "bengali", "tamil", "telugu", "kannada", "gujarati",
            "punjabi", "malayalam", "odia", "urdu",
            "french", "spanish", "german", "arabic", "portuguese",
            "russian", "japanese", "chinese", "korean",
        }
        if result in allowed or result.startswith("other:"):
            return result

    except Exception:
        pass

    # ── Step 2: Unicode script detection (fallback) ───────────────────────────
    has_devanagari  = any('\u0900' <= ch <= '\u097F' for ch in msg)
    has_bengali     = any('\u0980' <= ch <= '\u09FF' for ch in msg)
    has_tamil       = any('\u0B80' <= ch <= '\u0BFF' for ch in msg)
    has_telugu      = any('\u0C00' <= ch <= '\u0C7F' for ch in msg)
    has_kannada     = any('\u0C80' <= ch <= '\u0CFF' for ch in msg)
    has_malayalam   = any('\u0D00' <= ch <= '\u0D7F' for ch in msg)
    has_gujarati    = any('\u0A80' <= ch <= '\u0AFF' for ch in msg)
    has_gurmukhi    = any('\u0A00' <= ch <= '\u0A7F' for ch in msg)
    has_odia        = any('\u0B00' <= ch <= '\u0B7F' for ch in msg)
    has_arabic      = any('\u0600' <= ch <= '\u06FF' for ch in msg)
    has_cyrillic    = any('\u0400' <= ch <= '\u04FF' for ch in msg)
    has_korean      = any('\uAC00' <= ch <= '\uD7A3' for ch in msg)
    has_japanese    = any('\u3040' <= ch <= '\u30FF' for ch in msg)
    has_chinese     = any('\u4E00' <= ch <= '\u9FFF' for ch in msg)

    # Devanagari: distinguish Marathi vs Hindi
    if has_devanagari:
        if any(word in msg for word in _MARATHI_DEVANAGARI):
            return "marathi"
        if any(word in msg for word in _HINDI_DEVANAGARI):
            return "hindi"
        return "hindi"

    if has_bengali:   return "bengali"
    if has_tamil:     return "tamil"
    if has_telugu:    return "telugu"
    if has_kannada:   return "kannada"
    if has_malayalam: return "malayalam"
    if has_gujarati:  return "gujarati"
    if has_gurmukhi:  return "punjabi"
    if has_odia:      return "odia"
    if has_arabic:    return "arabic"
    if has_cyrillic:  return "russian"
    if has_korean:    return "korean"
    if has_japanese:  return "japanese"
    if has_chinese:   return "chinese"

    # ── Step 3: Roman-script heuristics (fallback only) ───────────────────────
    # Marathi before Hinglish to avoid false matches
    if any(w in msg_lower for w in _MARATHI_ROMAN):
        return "marathi"
    if any(w in msg_lower for w in _HINGLISH_ROMAN):
        return "hinglish"

    # Secondary Indian languages in Roman
    _BENGALI_ROMAN   = ["ami", "tumi", "apni", "kemon", "achho", "achhen", "bolo", "kothay", "bhalo"]
    _TAMIL_ROMAN     = ["naan", "neengal", "enna", "eppo", "eppadi", "theriyuma", "irukku", "romba"]
    _TELUGU_ROMAN    = ["nenu", "meeru", "enti", "ela", "evaru", "cheppandi", "telusaa", "chala"]
    _KANNADA_ROMAN   = ["naanu", "neevu", "enu", "hege", "yaaru", "heli", "gotthu", "thumba"]
    _GUJARATI_ROMAN  = ["ame", "tame", "shu", "kem", "chho", "chhe", "kaho", "khaber", "maja"]
    _PUNJABI_ROMAN   = ["kiddan", "kiven", "dasso", "sohna", "tussi", "tusi"]
    _MALAYALAM_ROMAN = ["njan", "ningal", "enthu", "engane", "paranju", "ariyamo"]
    _URDU_ROMAN      = ["aap", "bataiye", "samjhiye", "shukriya", "meherbani", "janab"]

    if any(w in msg_lower for w in _BENGALI_ROMAN):   return "bengali"
    if any(w in msg_lower for w in _TAMIL_ROMAN):     return "tamil"
    if any(w in msg_lower for w in _TELUGU_ROMAN):    return "telugu"
    if any(w in msg_lower for w in _KANNADA_ROMAN):   return "kannada"
    if any(w in msg_lower for w in _GUJARATI_ROMAN):  return "gujarati"
    if any(w in msg_lower for w in _PUNJABI_ROMAN):   return "punjabi"
    if any(w in msg_lower for w in _MALAYALAM_ROMAN): return "malayalam"
    if any(w in msg_lower for w in _URDU_ROMAN):      return "urdu"

    # Default → English
    return "english"


def build_lang_instruction(lang: str) -> str:
    # ── Default: English ──────────────────────────────────────────────────────
    if lang == "english":
        return (
            "IMPORTANT LANGUAGE RULE: Reply in English. "
            "This is the default language. Always use English unless the user clearly writes in another language."
        )

    # ── Priority languages ────────────────────────────────────────────────────
    if lang == "hindi":
        return (
            "IMPORTANT LANGUAGE RULE: The user wrote in Hindi. Reply only in Hindi (Devanagari script). "
            "Keep it simple and natural."
        )

    if lang == "hinglish":
        return (
            "IMPORTANT LANGUAGE RULE: The user wrote in Hinglish. Reply only in Hinglish. "
            "Use Roman Hindi mixed naturally with English. Do not switch to full English or full Hindi."
        )

    if lang == "marathi":
        return (
            "IMPORTANT LANGUAGE RULE: The user wrote in Marathi. Reply only in Marathi. "
            "If the user wrote in Devanagari, reply in Devanagari Marathi. "
            "If the user wrote in Roman script, reply in Roman Marathi. "
            "Keep it simple and natural."
        )

    # ── Secondary languages ───────────────────────────────────────────────────
    secondary = {
        "bengali":    ("Bengali",              "If the user wrote in Bengali script, reply in Bengali script. If Roman, reply in Roman Bengali."),
        "tamil":      ("Tamil",                "If the user wrote in Tamil script, reply in Tamil script. If Roman, reply in Roman Tamil."),
        "telugu":     ("Telugu",               "If the user wrote in Telugu script, reply in Telugu script. If Roman, reply in Roman Telugu."),
        "kannada":    ("Kannada",              "If the user wrote in Kannada script, reply in Kannada script. If Roman, reply in Roman Kannada."),
        "gujarati":   ("Gujarati",             "If the user wrote in Gujarati script, reply in Gujarati script. If Roman, reply in Roman Gujarati."),
        "punjabi":    ("Punjabi",              "If the user wrote in Gurmukhi script, reply in Gurmukhi. If Roman, reply in Roman Punjabi."),
        "malayalam":  ("Malayalam",            "If the user wrote in Malayalam script, reply in Malayalam script. If Roman, reply in Roman Malayalam."),
        "odia":       ("Odia",                 "If the user wrote in Odia script, reply in Odia script. If Roman, reply in Roman Odia."),
        "urdu":       ("Urdu",                 "If the user wrote in Nastaliq script, reply in Nastaliq. If Roman, reply in Roman Urdu."),
        "french":     ("French",               ""),
        "spanish":    ("Spanish",              ""),
        "german":     ("German",               ""),
        "arabic":     ("Arabic",               ""),
        "portuguese": ("Portuguese",           ""),
        "russian":    ("Russian",              ""),
        "japanese":   ("Japanese",             ""),
        "chinese":    ("Chinese (Simplified)", ""),
        "korean":     ("Korean",               ""),
    }

    if lang in secondary:
        name, script_rule = secondary[lang]
        extra = f" {script_rule}" if script_rule else ""
        return (
            f"IMPORTANT LANGUAGE RULE: The user wrote in {name}. Reply only in {name}.{extra} "
            f"Keep it simple and natural."
        )

    if lang.startswith("other:"):
        detected = lang.replace("other:", "").strip().capitalize()
        return (
            f"IMPORTANT LANGUAGE RULE: The user wrote in {detected}. Reply only in {detected}."
        )

    # Fallback → English
    return (
        "IMPORTANT LANGUAGE RULE: Reply in English. "
        "This is the default language."
    )


# ── What-if feature extraction ────────────────────────────────────────────────
def extract_whatif_feature(message: str, context_snippet: str) -> dict | None:
    """Ask Groq to extract feature name + new value from a what-if question."""
    try:
        prompt = f"""From the user's what-if question, extract:
1. The exact feature name they want to change (match one of the SHAP features in context)
2. The new numeric value they suggest (if not mentioned, use a 20% improvement direction)

Current SHAP features context:
{context_snippet[:400]}

User question: "{message}"

Reply ONLY with valid JSON:
{{"feature": "feature_name_here", "new_value": 0.0}}
No explanation. Just JSON."""

        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=50,
        )
        raw = response.choices[0].message.content.strip()
        return json.loads(raw)
    except Exception as e:
        print(f"[WARNING] extract_whatif_feature failed: {e}")
        return None


# ── Main stream response ──────────────────────────────────────────────────────
async def stream_response(
    message: str,
    history: list[dict],
    application_id: str | None,
    pipeline: str,
) -> AsyncGenerator[str, None]:
    """
    Full pipeline:
      1. Route query → MySQL / RAG / finance / out-of-scope
      2. Build context block
      3. Handle what-if simulation if needed
      4. Yield image SSE event if base64 available
      5. Detect language
      6. Build prompt
      7. Stream Groq response
    """

    # ── Step 1: Retrieve ──────────────────────────────────────────────────────
    chunks, in_scope, applicant_data = retriever.retrieve(
        message, application_id, pipeline
    )

    # Do not crash multilingual/general questions just because retriever said out of scope
    if not in_scope:
        chunks = []
        applicant_data = {}

    # ── Step 2: Build context block ───────────────────────────────────────────
    context_block = ""
    image_b64 = ""

    if applicant_data.get("found"):
        context_block = (
            f"Applicant context ({pipeline.upper()} pipeline):\n\n"
            f"{applicant_data['text']}\n\n---\n\n"
        )
        image_b64 = applicant_data.get("image", "")

        if is_whatif_question(message):
            bridge_available = (
                model_bridge.is_individual_available()
                if pipeline == "individual"
                else model_bridge.is_msme_available()
            )

            if bridge_available:
                extracted = extract_whatif_feature(message, applicant_data["text"])
                if extracted:
                    context_block += (
                        f"What-if scenario requested:\n"
                        f"  Feature : {extracted.get('feature', 'unknown')}\n"
                        f"  New value: {extracted.get('new_value', 'N/A')}\n"
                        f"  Instruction: Based on the SHAP impacts above, explain "
                        f"how improving '{extracted.get('feature')}' would affect "
                        f"the {pipeline.upper()} credit score and decision.\n\n---\n\n"
                    )

    elif "found" in applicant_data and not applicant_data["found"]:
        context_block = (
            "Note: No credit report found for this application ID. "
            "Inform the user their application may not have been processed yet.\n\n---\n\n"
        )

    elif chunks:
        rag_context = "\n\n---\n\n".join(
            [chunk.page_content.strip() for chunk in chunks if getattr(chunk, "page_content", "").strip()]
        )
        context_block = f"Context from project knowledge base:\n\n{rag_context}\n\n---\n\n"

    # ── Step 3: Yield image SSE before text ───────────────────────────────────
    if image_b64:
        yield f"data: {json.dumps({'image': image_b64})}\n\n"

    # ── Step 4: Detect language ───────────────────────────────────────────────
    lang = detect_language_ai(message)
    lang_instruction = build_lang_instruction(lang)

    # ── Step 5: Build messages ────────────────────────────────────────────────
    messages = [{
        "role": "system",
        "content": SYSTEM_PROMPT + "\n\n" + lang_instruction
    }]

    for turn in history[-4:]:
        messages.append({"role": turn["role"], "content": turn["content"]})

    messages.append({
        "role": "user",
        "content": f"{context_block}User question: {message}",
    })

    # ── Step 6: Stream from Groq ──────────────────────────────────────────────
    try:
        stream = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            temperature=0.3,
            max_tokens=1024,
            stream=True,
        )

        for chunk in stream:
            token = chunk.choices[0].delta.content
            if token:
                yield f"data: {json.dumps({'token': token})}\n\n"

    except Exception as e:
        yield f"data: {json.dumps({'token': f'Error connecting to Groq API: {str(e)}'})}\n\n"

    yield "data: [DONE]\n\n"


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.post("/chat")
async def chat(req: ChatRequest):
    print("MESSAGE:", req.message)
    print("APPLICATION ID:", req.application_id)
    print("SESSION ID:", req.session_id)
    print("PIPELINE:", req.pipeline)

    return StreamingResponse(
        stream_response(
            req.message,
            req.history,
            req.application_id,
            req.pipeline,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "model": GROQ_MODEL,
        "retriever": "hybrid BM25+ChromaDB (parallel)",
        "mysql": "auditdb @ localhost:3306",
        "individual_pipeline": "available" if model_bridge.is_individual_available() else "unavailable",
        "msme_pipeline": "available" if model_bridge.is_msme_available() else "unavailable",
    }


@app.get("/models")
async def list_models():
    try:
        models = client.models.list()
        return {"models": [m.id for m in models.data]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/")
async def serve_ui():
    html_path = Path(__file__).parent / "chat.html"
    if html_path.exists():
        return FileResponse(str(html_path))
    return {"message": "React frontend is serving the UI"}


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("  AI Credit Scoring Chatbot  [Individual + MSME]")
    print("=" * 65)
    print(f"  Model              : {GROQ_MODEL}")
    print("  URL                : http://localhost:8999")
    print("  MySQL              : auditdb @ localhost:3306")
    print(f"  Individual Bridge  : {'ready' if model_bridge.is_individual_available() else 'unavailable'}")
    print(f"  MSME Bridge        : {'ready' if model_bridge.is_msme_available() else 'unavailable'}")
    print("=" * 65 + "\n")
    uvicorn.run(app, host="127.0.0.1", port=8999)