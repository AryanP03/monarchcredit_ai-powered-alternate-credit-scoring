"""
ingest.py
─────────────────────────────────────────────────────────────────────────────
AI Credit Scoring Chatbot — Document Ingestion (CPU-Optimised)
─────────────────────────────────────────────────────────────────────────────
Changes from original:
  • TOKENIZERS_PARALLELISM disabled to suppress HuggingFace warnings on CPU
  • encode_kwargs batch_size set for efficient CPU embedding
  • Everything else unchanged — run once when docs/ changes

Run once, or whenever you update any doc in docs/:
    python ingest.py
"""

import os
import pickle
import shutil
from pathlib import Path

# Suppress HuggingFace tokenizer warning on CPU
os.environ["TOKENIZERS_PARALLELISM"] = "false"

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from rank_bm25 import BM25Okapi

DOCS_DIR   = Path(__file__).parent / "docs"
CHROMA_DIR = Path(__file__).parent / "chroma_db"
BM25_PATH  = Path(__file__).parent / "bm25_index.pkl"

CHUNK_SIZE    = 500
CHUNK_OVERLAP = 50
EMBED_MODEL   = "all-MiniLM-L6-v2"


def load_and_chunk():
    if not DOCS_DIR.exists():
        raise FileNotFoundError(f"docs/ folder not found at {DOCS_DIR}")
    files = list(DOCS_DIR.glob("*.txt"))
    if not files:
        raise FileNotFoundError("No .txt files in docs/ — add your knowledge base files first.")

    print(f"[INFO] Loading {len(files)} document(s):")
    for f in files:
        print(f"       {f.name}")

    loader = DirectoryLoader(
        str(DOCS_DIR),
        glob="*.txt",
        loader_cls=TextLoader,
        loader_kwargs={"encoding": "utf-8"},
    )
    docs = loader.load()

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", " ", ""],
    )
    chunks = splitter.split_documents(docs)
    print(f"[INFO] Created {len(chunks)} chunks (size={CHUNK_SIZE}, overlap={CHUNK_OVERLAP})")
    return chunks


def build_bm25(chunks):
    print("\n[INFO] Building BM25 sparse index...")
    texts     = [c.page_content for c in chunks]
    tokenized = [t.lower().split() for t in texts]
    bm25      = BM25Okapi(tokenized)

    with open(BM25_PATH, "wb") as f:
        pickle.dump({"bm25": bm25, "texts": texts, "chunks": chunks}, f)
    print(f"[INFO] BM25 saved → {BM25_PATH}  ({len(texts)} entries)")


def build_chroma(chunks):
    print(f"\n[INFO] Building ChromaDB with '{EMBED_MODEL}' (CPU-optimised)...")
    print("[INFO] First run downloads ~90MB model — takes ~1 min on CPU...")

    embeddings = HuggingFaceEmbeddings(
        model_name    = EMBED_MODEL,
        model_kwargs  = {"device": "cpu"},
        encode_kwargs = {
            "normalize_embeddings": True,
            "batch_size": 32,   # efficient for CPU embedding during ingestion
        },
    )

    if CHROMA_DIR.exists():
        shutil.rmtree(CHROMA_DIR)

    vs = Chroma.from_documents(
        documents         = chunks,
        embedding         = embeddings,
        persist_directory = str(CHROMA_DIR),
    )
    vs.persist()
    print(f"[INFO] ChromaDB saved → {CHROMA_DIR}  ({vs._collection.count()} vectors)")


def main():
    print("=" * 55)
    print("  AI Credit Scoring RAG — Ingestion [CPU-Optimised]")
    print("=" * 55)
    chunks = load_and_chunk()
    build_bm25(chunks)
    build_chroma(chunks)
    print("\n" + "=" * 55)
    print("[DONE] Ingestion complete.")
    print("       Pull faster model : ollama pull phi3:mini")
    print("       Run chatbot       : python app.py")
    print("=" * 55)


if __name__ == "__main__":
    main()