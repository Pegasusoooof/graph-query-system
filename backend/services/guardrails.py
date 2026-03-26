# backend/services/guardrails.py
from backend.logger import log

REJECTION_RESPONSE = (
    "This system is designed to answer questions related to the "
    "provided dataset only."
)

# Hard blocklist — obviously unrelated topics that should never pass
_BLOCKLIST = {
    "weather", "recipe", "cook", "movie", "film", "sport", "cricket",
    "football", "music", "song", "poem", "joke", "story", "write me",
    "capital of", "who is the president", "translate", "language",
    "history of", "explain quantum", "meaning of life", "chatgpt",
    "openai", "anthropic", "stock price", "bitcoin", "crypto",
}

# Any of these means it's definitely O2C-related — fast pass
_ALLOWLIST_STEMS = [
    "order", "deliver", "billing", "invoice", "journal", "customer",
    "product", "material", "payment", "shipment", "dispatch", "item",
    "quantity", "amount", "status", "document", "entry", "ship",
    "goods", "address", "posting", "o2c", "otc", "order-to-cash",
    "sales", "revenue", "fiscal", "account", "gl ", "billed",
    "unbilled", "trace", "flow", "broken", "incomplete", "fulfil",
    "fulfill", "net value", "how many", "which order", "which product",
    "which customer", "which deliver", "which billing", "which journal",
    "dataset", "data", "graph", "node", "entity",
]


def is_otc_query(message: str, req_id: str = None) -> bool:
    """
    Two-stage check:
    1. If message contains a hard-blocklist term → reject immediately.
    2. If message contains any allowlist stem → pass immediately.
    3. Otherwise → pass (let the LLM + system prompt handle it;
       the system prompt already instructs the model to refuse
       unrelated questions, so double-rejecting causes false negatives).
    """
    extra = {"stage": "GUARDRAIL", "req_id": req_id}
    lower = message.lower()

    log.debug(
        f"Evaluating message ({len(message)} chars): {repr(message[:120])}{'...' if len(message) > 120 else ''}",
        extra=extra,
    )

    # Stage 1 — hard blocklist
    for term in _BLOCKLIST:
        if term in lower:
            log.warning(
                f"REJECTED — Stage 1 blocklist match: term={repr(term)}",
                extra=extra,
            )
            return False

    # Stage 2 — fast-pass on domain stems
    for stem in _ALLOWLIST_STEMS:
        if stem in lower:
            log.info(
                f"PASSED — Stage 2 allowlist match: stem={repr(stem)}",
                extra=extra,
            )
            return True

    # Stage 3 — default PASS
    log.info(
        "PASSED — Stage 3 default (no blocklist/allowlist match; forwarding to LLM)",
        extra=extra,
    )
    return True


def get_rejection_response() -> str:
    return REJECTION_RESPONSE