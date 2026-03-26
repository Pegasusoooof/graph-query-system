# backend/routers/query.py
import json
import re
import traceback
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import backend.config as cfg
from backend.services.guardrails import is_otc_query, get_rejection_response
from backend.services.context_builder import ContextBuilder
from backend.logger import log, new_request_id, log_divider, Timer

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
    req_id = new_request_id()
    extra_req = {"stage": "REQUEST", "req_id": req_id}
    timer = Timer()

    log_divider(req_id)
    log.info(
        f"POST /query  |  history={len(req.conversation_history)} msgs  |  highlighted={len(req.highlighted_nodes)} nodes",
        extra=extra_req,
    )
    log.info(f"User message: {repr(req.message)}", extra=extra_req)

    # ── Guardrail ─────────────────────────────────────────────────
    if not is_otc_query(req.message, req_id=req_id):
        log.warning(
            f"Request REJECTED by guardrail  [{timer.elapsed_ms()}ms total]",
            extra={"stage": "GUARDRAIL", "req_id": req_id},
        )
        return {
            "response": get_rejection_response(),
            "highlighted_nodes": [],
            "model_used": "guardrail",
            "rejected": True,
        }

    # ── Context build ─────────────────────────────────────────────
    cb = ContextBuilder(cfg.G, cfg.analyzer)
    context = cb.build(req.message, req.highlighted_nodes, req_id=req_id)
    system = SYSTEM_PROMPT.format(context=context)

    messages = req.conversation_history + [
        {"role": "user", "content": req.message}
    ]

    log.info(
        f"System prompt: {len(system):,} chars  |  messages in thread: {len(messages)}",
        extra={"stage": "LLM", "req_id": req_id},
    )
    log.info(
        f"Calling Groq  model={cfg.GROQ_MODEL}  max_tokens=1500  temperature=0.1",
        extra={"stage": "LLM", "req_id": req_id},
    )

    llm_timer = Timer()
    try:
        response = await cfg.groq_client.chat.completions.create(
            model=cfg.GROQ_MODEL,
            messages=[{"role": "system", "content": system}] + messages,
            max_tokens=1500,
            temperature=0.1,
        )
    except Exception as exc:
        log.error(
            f"Groq API call FAILED: {type(exc).__name__}: {exc}",
            extra={"stage": "ERROR", "req_id": req_id},
            exc_info=True,
        )
        raise

    raw_text = response.choices[0].message.content
    finish_reason = response.choices[0].finish_reason
    usage = response.usage

    log.info(
        f"Groq response received  [{llm_timer.elapsed_ms()}ms]  "
        f"finish_reason={finish_reason}  "
        f"prompt_tokens={usage.prompt_tokens if usage else 'N/A'}  "
        f"completion_tokens={usage.completion_tokens if usage else 'N/A'}",
        extra={"stage": "LLM", "req_id": req_id},
    )

    if not raw_text:
        log.warning(
            "Groq returned EMPTY content (raw_text is None or empty)",
            extra={"stage": "LLM", "req_id": req_id},
        )

    # ── Parse highlight tags ──────────────────────────────────────
    highlighted = []
    match = re.search(r"<highlight_nodes>(.*?)</highlight_nodes>", raw_text or "", re.DOTALL)
    if match:
        ids = match.group(1).strip()
        highlighted = [i.strip() for i in ids.split(",") if i.strip()]
        raw_text = re.sub(r"<highlight_nodes>.*?</highlight_nodes>", "", raw_text, flags=re.DOTALL).strip()

    log.info(
        f"highlight_nodes extracted: {len(highlighted)}  |  final response: {len(raw_text or ''):,} chars",
        extra={"stage": "RESPONSE", "req_id": req_id},
    )
    log.debug(
        f"Response preview: {repr((raw_text or '')[:200])}{'...' if len(raw_text or '') > 200 else ''}",
        extra={"stage": "RESPONSE", "req_id": req_id},
    )
    log.info(
        f"Request complete  [{timer.elapsed_ms()}ms total]",
        extra={"stage": "RESPONSE", "req_id": req_id},
    )

    return {
        "response": raw_text,
        "highlighted_nodes": highlighted,
        "model_used": cfg.GROQ_MODEL,
        "rejected": False,
    }


# ── Streaming ─────────────────────────────────────────────────────
@router.post("/stream")
async def stream_query(req: QueryRequest):
    req_id = new_request_id()
    extra_req = {"stage": "REQUEST", "req_id": req_id}
    timer = Timer()

    log_divider(req_id)
    log.info(
        f"POST /query/stream  |  history={len(req.conversation_history)} msgs  |  highlighted={len(req.highlighted_nodes)} nodes",
        extra=extra_req,
    )
    log.info(f"User message: {repr(req.message)}", extra=extra_req)

    # ── Guardrail ─────────────────────────────────────────────────
    if not is_otc_query(req.message, req_id=req_id):
        log.warning(
            f"Request REJECTED by guardrail  [{timer.elapsed_ms()}ms total]",
            extra={"stage": "GUARDRAIL", "req_id": req_id},
        )

        async def rejection_stream():
            rejection = get_rejection_response()
            log.info(
                f"Streaming guardrail rejection: {repr(rejection)}",
                extra={"stage": "STREAM", "req_id": req_id},
            )
            yield f"data: {json.dumps({'token': rejection})}\n\n"
            yield f"data: {json.dumps({'done': True, 'highlighted_nodes': []})}\n\n"
            log.info("Rejection stream complete", extra={"stage": "STREAM", "req_id": req_id})

        return StreamingResponse(rejection_stream(), media_type="text/event-stream")

    # ── Context build ─────────────────────────────────────────────
    cb = ContextBuilder(cfg.G, cfg.analyzer)
    context = cb.build(req.message, req.highlighted_nodes, req_id=req_id)
    system = SYSTEM_PROMPT.format(context=context)

    messages = req.conversation_history + [
        {"role": "user", "content": req.message}
    ]

    log.info(
        f"System prompt: {len(system):,} chars  |  messages in thread: {len(messages)}",
        extra={"stage": "LLM", "req_id": req_id},
    )
    if req.conversation_history:
        for i, h in enumerate(req.conversation_history):
            log.debug(
                f"  History[{i}] role={h.get('role')}  content_len={len(str(h.get('content', '')))} chars",
                extra={"stage": "LLM", "req_id": req_id},
            )

    log.info(
        f"Calling Groq (stream)  model={cfg.GROQ_MODEL}  max_tokens=1500  temperature=0.1",
        extra={"stage": "LLM", "req_id": req_id},
    )

    async def generate():
        full_text = ""
        token_count = 0
        llm_timer = Timer()
        stream_extra = {"stage": "STREAM", "req_id": req_id}

        try:
            async with await cfg.groq_client.chat.completions.create(
                model=cfg.GROQ_MODEL,
                messages=[{"role": "system", "content": system}] + messages,
                max_tokens=1500,
                temperature=0.1,
                stream=True,
            ) as stream:
                log.info("Groq stream opened — receiving tokens...", extra=stream_extra)
                first_token_logged = False

                async for chunk in stream:
                    delta = chunk.choices[0].delta
                    finish_reason = chunk.choices[0].finish_reason
                    token = delta.content or ""

                    if token:
                        full_text += token
                        token_count += 1

                        if not first_token_logged:
                            log.info(
                                f"First token received  [TTFT: {llm_timer.elapsed_ms()}ms]",
                                extra=stream_extra,
                            )
                            first_token_logged = True

                        # Log every 25 tokens to show streaming progress
                        if token_count % 25 == 0:
                            log.debug(
                                f"Streaming... {token_count} tokens so far  ({len(full_text)} chars)",
                                extra=stream_extra,
                            )

                        yield f"data: {json.dumps({'token': token})}\n\n"

                    elif token == "" and finish_reason:
                        log.info(
                            f"Stream chunk: finish_reason={repr(finish_reason)}",
                            extra=stream_extra,
                        )

                    # Log empty-content chunks at DEBUG level (these are normal for final chunk)
                    if not token and not finish_reason:
                        log.debug(
                            f"Stream chunk: empty delta (content=None, no finish_reason yet)",
                            extra=stream_extra,
                        )

            log.info(
                f"Groq stream closed  [{llm_timer.elapsed_ms()}ms]  "
                f"total_tokens={token_count}  total_chars={len(full_text)}",
                extra=stream_extra,
            )

            if not full_text:
                log.warning(
                    "EMPTY RESPONSE — Groq returned zero content tokens. "
                    "Possible causes: API-level content filter, safety refusal, or rate limit.",
                    extra={"stage": "ERROR", "req_id": req_id},
                )

        except Exception as exc:
            log.error(
                f"Groq stream FAILED: {type(exc).__name__}: {exc}",
                extra={"stage": "ERROR", "req_id": req_id},
            )
            log.error(traceback.format_exc(), extra={"stage": "ERROR", "req_id": req_id})
            # Yield error token so frontend shows something instead of empty bubble
            yield f"data: {json.dumps({'token': '[Error: LLM request failed. See backend logs.]'})}\n\n"
            yield f"data: {json.dumps({'done': True, 'highlighted_nodes': []})}\n\n"
            log.info(
                f"Error stream sent to client  [{timer.elapsed_ms()}ms total]",
                extra={"stage": "STREAM", "req_id": req_id},
            )
            return

        # ── Extract highlight tags ────────────────────────────────
        highlighted = []
        match = re.search(r"<highlight_nodes>(.*?)</highlight_nodes>", full_text, re.DOTALL)
        if match:
            ids = match.group(1).strip()
            highlighted = [i.strip() for i in ids.split(",") if i.strip()]
            log.info(
                f"highlight_nodes extracted: {len(highlighted)} node(s)",
                extra={"stage": "RESPONSE", "req_id": req_id},
            )
        else:
            log.debug(
                "No <highlight_nodes> tag found in response",
                extra={"stage": "RESPONSE", "req_id": req_id},
            )

        clean_text = re.sub(r"<highlight_nodes>.*?</highlight_nodes>", "", full_text, flags=re.DOTALL).strip()
        log.info(
            f"Final response: {len(clean_text):,} chars after tag cleanup",
            extra={"stage": "RESPONSE", "req_id": req_id},
        )
        log.debug(
            f"Response preview: {repr(clean_text[:200])}{'...' if len(clean_text) > 200 else ''}",
            extra={"stage": "RESPONSE", "req_id": req_id},
        )
        log.info(
            f"Request complete  [{timer.elapsed_ms()}ms total]",
            extra={"stage": "RESPONSE", "req_id": req_id},
        )

        yield f"data: {json.dumps({'done': True, 'highlighted_nodes': highlighted})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")
