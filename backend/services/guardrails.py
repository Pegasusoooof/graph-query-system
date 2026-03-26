# backend/services/guardrails.py

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


def is_otc_query(message: str) -> bool:
    """
    Two-stage check:
    1. If message contains a hard-blocklist term → reject immediately.
    2. If message contains any allowlist stem → pass immediately.
    3. Otherwise → pass (let the LLM + system prompt handle it;
       the system prompt already instructs the model to refuse
       unrelated questions, so double-rejecting causes false negatives).
    """
    lower = message.lower()

    # Stage 1 — hard blocklist
    for term in _BLOCKLIST:
        if term in lower:
            return False

    # Stage 2 — fast-pass on domain stems
    for stem in _ALLOWLIST_STEMS:
        if stem in lower:
            return True

    # Stage 3 — default PASS (short/ambiguous queries like "summarize",
    # "what's the status", "give me an overview" should reach the LLM
    # which will answer from context or politely refuse if truly off-topic)
    return True


def get_rejection_response() -> str:
    return REJECTION_RESPONSE