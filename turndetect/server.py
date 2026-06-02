"""
Turn-end detection sidecar.

POST /detect  {"text": "..."}  →  {"pause_ms": 450}

Uses KoljaB/SentenceFinishedClassification (DistilBERT fine-tuned binary
classifier) to estimate whether the user's utterance is complete.  The
pause formula mirrors RealtimeVoiceChat/code/turndetect.py:

    weighted = 0.65 * punctuation_pause + 0.35 * (1.0 - prob_complete)
    pause    = max(weighted, MIN_PAUSE)
"""

from __future__ import annotations

import functools
import os

import torch
import torch.nn.functional as F
from fastapi import FastAPI
from pydantic import BaseModel
from transformers import DistilBertForSequenceClassification, DistilBertTokenizerFast

MODEL_ID = os.getenv("TURNDETECT_MODEL", "KoljaB/SentenceFinishedClassification")
MIN_PAUSE = float(os.getenv("TURNDETECT_MIN_PAUSE_MS", "300")) / 1000.0

app = FastAPI()

# ---------------------------------------------------------------------------
# Model loading (once at startup)
# ---------------------------------------------------------------------------

tokenizer: DistilBertTokenizerFast
model: DistilBertForSequenceClassification


@app.on_event("startup")
def load_model() -> None:
    global tokenizer, model
    tokenizer = DistilBertTokenizerFast.from_pretrained(MODEL_ID)
    model = DistilBertForSequenceClassification.from_pretrained(MODEL_ID)
    model.eval()


# ---------------------------------------------------------------------------
# Classifier (LRU-cached to avoid re-computing identical partial transcripts)
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=256)
def _prob_complete(text: str) -> float:
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=128)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = F.softmax(logits, dim=1).squeeze().tolist()
    return probs[1]  # index 1 = "complete"


# ---------------------------------------------------------------------------
# Pause formula
# ---------------------------------------------------------------------------

_PUNCT_PAUSE: dict[str, float] = {
    ".": 0.6,
    "!": 0.5,
    "?": 0.5,
    "…": 2.0,
    "，": 1.0,  # CJK comma
    "。": 0.6,
    "！": 0.5,
    "？": 0.5,
}


def _suggested_pause(text: str) -> float:
    prob = _prob_complete(text.strip())
    model_pause = 1.0 - prob

    last = text.rstrip()[-1] if text.strip() else ""
    punct_pause = _PUNCT_PAUSE.get(last, 1.4)

    weighted = 0.65 * punct_pause + 0.35 * model_pause
    return max(weighted, MIN_PAUSE)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

class DetectRequest(BaseModel):
    text: str


class DetectResponse(BaseModel):
    pause_ms: int
    prob_complete: float


@app.post("/detect", response_model=DetectResponse)
def detect(req: DetectRequest) -> DetectResponse:
    text = req.text.strip()
    if not text:
        return DetectResponse(pause_ms=int(MIN_PAUSE * 1000), prob_complete=0.0)
    prob = _prob_complete(text)
    pause = _suggested_pause(text)
    return DetectResponse(pause_ms=int(pause * 1000), prob_complete=prob)


@app.get("/health")
def health() -> dict:
    return {"ok": True}
