# backend/logger.py
"""
Centralized logger for the OTC Graph Query API.
Outputs colored, structured logs to the terminal.
"""

import logging
import sys
import time
import uuid

# ── ANSI color codes ───────────────────────────────────────────────
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
RED     = "\033[31m"
GREEN   = "\033[32m"
YELLOW  = "\033[33m"
BLUE    = "\033[34m"
MAGENTA = "\033[35m"
CYAN    = "\033[36m"
BRIGHT_RED     = "\033[91m"
BRIGHT_GREEN   = "\033[92m"
BRIGHT_YELLOW  = "\033[93m"
BRIGHT_BLUE    = "\033[94m"
BRIGHT_MAGENTA = "\033[95m"
BRIGHT_CYAN    = "\033[96m"
WHITE   = "\033[97m"

# ── Level → color mapping ──────────────────────────────────────────
LEVEL_COLORS = {
    "DEBUG":    DIM + WHITE,
    "INFO":     BRIGHT_CYAN,
    "WARNING":  BRIGHT_YELLOW,
    "ERROR":    BRIGHT_RED,
    "CRITICAL": BOLD + RED,
}

# ── Stage tag colors ───────────────────────────────────────────────
STAGE_COLORS = {
    "REQUEST":   BRIGHT_BLUE,
    "GUARDRAIL": BRIGHT_YELLOW,
    "CONTEXT":   BRIGHT_MAGENTA,
    "LLM":       BRIGHT_GREEN,
    "STREAM":    CYAN,
    "RESPONSE":  GREEN,
    "ERROR":     BRIGHT_RED,
    "STARTUP":   BRIGHT_CYAN,
}


class ColorFormatter(logging.Formatter):
    """Formats log records with ANSI colors and structured layout."""

    def format(self, record: logging.LogRecord) -> str:
        level_color = LEVEL_COLORS.get(record.levelname, WHITE)
        ts = self.formatTime(record, datefmt="%H:%M:%S")

        # Extract optional stage and request_id from extra fields
        stage    = getattr(record, "stage", None)
        req_id   = getattr(record, "req_id", None)

        stage_str  = f" {STAGE_COLORS.get(stage, WHITE)}[{stage}]{RESET}" if stage else ""
        req_str    = f" {DIM}#{req_id[:8]}{RESET}" if req_id else ""

        level_str = f"{level_color}{record.levelname:<8}{RESET}"
        msg       = record.getMessage()

        line = f"{DIM}{ts}{RESET} {level_str}{stage_str}{req_str}  {msg}"

        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)

        return line


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("otc")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(ColorFormatter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger


log = _build_logger()


# ── Convenience helpers ────────────────────────────────────────────

def new_request_id() -> str:
    """Generate a short unique ID to correlate all logs for one request."""
    return uuid.uuid4().hex


def log_divider(req_id: str = None):
    """Print a visual separator between requests."""
    line = f"{DIM}{'─' * 72}{RESET}"
    log.debug(line, extra={"req_id": req_id} if req_id else {})


class Timer:
    """Simple wall-clock timer for measuring stage durations."""

    def __init__(self):
        self._start = time.perf_counter()

    def elapsed_ms(self) -> int:
        return int((time.perf_counter() - self._start) * 1000)

    def reset(self):
        self._start = time.perf_counter()
