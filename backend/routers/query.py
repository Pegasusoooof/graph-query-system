# backend/routers/query.py
import json
import re
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import backend.config as cfg
from backend.services.guardrails import is_otc_query, get_rejection_response
from backend.services.context_builder import ContextBuilder

router = APIRouter(prefix="/query", tags=["query"])

# ── System prompt ─────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a data analysis assistant for an Order-to-Cash (O2C) business process dataset.

STRICT RULES — follow these without exception:
1. You ONLY answer questions about the data in the context provided below.
2. If a question is unrelated to O2C — orders, deliveries, billing, journal entries, customers, products — respond ONLY with:
   "This system is designed to answer questions related to the provided dataset only."
3. NEVER invent, estimate, or hallucinate data. Every claim must be traceable to the context.
4. When you mention a specific entity (order, delivery, billing doc, customer), always include its ID.
5. At the end of every response, output the IDs of nodes the UI should highlight in this exact format:
   <highlight_nodes>id1,id2,id3</highlight_nodes>
   If nothing to highlight, output: <highlight_nodes></highlight_nodes>
6. Be concise and analytical. Use bullet points for lists of entities.
7. When asked about broken or incomplete flows, be specific: name the exact IDs affected.

DATASET CONTEXT:
{context}"""


class QueryRequest(BaseModel):
    message: str
    conversation_history: list[dict] = []
    highlighted_nodes: list[str] = []


# ── Standard (non-streaming) ──────────────────────────────────────
@router.post("/")
async def natural_language_query(req: QueryRequest):
    # Guardrail check BEFORE any LLM call
    if not is_otc_query(req.message):
        return {
            "response": get_rejection_response(),
            "highlighted_nodes": [],
            "model_used": "guardrail",
            "rejected": True,
        }

    cb = ContextBuilder(cfg.G, cfg.analyzer)
    context = cb.build(req.message, req.highlighted_nodes)
    system = SYSTEM_PROMPT.format(context=context)

    messages = req.conversation_history + [
        {"role": "user", "content": req.message}
    ]

    response = cfg.groq_client.chat.completions.create(
        model=cfg.GROQ_MODEL,
        messages=[{"role": "system", "content": system}] + messages,
        max_tokens=1500,
        temperature=0.1,   # low temp = factual, not creative
    )

    raw_text = response.choices[0].message.content

    # Parse highlight tags
    highlighted = []
    match = re.search(r"<highlight_nodes>(.*?)</highlight_nodes>", raw_text, re.DOTALL)
    if match:
        ids = match.group(1).strip()
        highlighted = [i.strip() for i in ids.split(",") if i.strip()]
        raw_text = re.sub(r"<highlight_nodes>.*?</highlight_nodes>", "", raw_text, flags=re.DOTALL).strip()

    return {
        "response": raw_text,
        "highlighted_nodes": highlighted,
        "model_used": cfg.GROQ_MODEL,
        "rejected": False,
    }


# ── Streaming ─────────────────────────────────────────────────────
@router.post("/stream")
async def stream_query(req: QueryRequest):
    # Guardrail check
    if not is_otc_query(req.message):
        async def rejection_stream():
            rejection = get_rejection_response()
            yield f"data: {json.dumps({'token': rejection})}\n\n"
            yield f"data: {json.dumps({'done': True, 'highlighted_nodes': []})}\n\n"
        return StreamingResponse(rejection_stream(), media_type="text/event-stream")

    cb = ContextBuilder(cfg.G, cfg.analyzer)
    context = cb.build(req.message, req.highlighted_nodes)
    system = SYSTEM_PROMPT.format(context=context)

    messages = req.conversation_history + [
        {"role": "user", "content": req.message}
    ]

    def generate():
        full_text = ""
        with cfg.groq_client.chat.completions.create(
            model=cfg.GROQ_MODEL,
            messages=[{"role": "system", "content": system}] + messages,
            max_tokens=1500,
            temperature=0.1,
            stream=True,
        ) as stream:
            for chunk in stream:
                token = chunk.choices[0].delta.content or ""
                full_text += token
                yield f"data: {json.dumps({'token': token})}\n\n"

        # After full response — extract highlight tags and send final event
        highlighted = []
        match = re.search(r"<highlight_nodes>(.*?)</highlight_nodes>", full_text, re.DOTALL)
        if match:
            ids = match.group(1).strip()
            highlighted = [i.strip() for i in ids.split(",") if i.strip()]

        yield f"data: {json.dumps({'done': True, 'highlighted_nodes': highlighted})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")