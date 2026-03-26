# backend/config.py
import os
import sys
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
from groq import Groq
import networkx as nx

# ── Env vars ──────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]

# ── Groq model ────────────────────────────────────────────────────
# llama-3.3-70b-versatile is the best option for this project:
# - 128k context window (handles large graph data dumps)
# - Strongest structured reasoning on Groq
# - Better than Mixtral/Gemma for JSON-grounded Q&A
GROQ_MODEL = "llama-3.3-70b-versatile"

groq_client = Groq(api_key=GROQ_API_KEY)

# ── Graph (loaded once at startup) ────────────────────────────────
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from graph.cache import load_graph, load_graph_json
from graph.analyzer import OTCGraphAnalyzer

G: nx.DiGraph = None
graph_json: dict = None
analyzer: OTCGraphAnalyzer = None


def load_all():
    """Called once at FastAPI startup."""
    global G, graph_json, analyzer
    G = load_graph()
    graph_json = load_graph_json()
    analyzer = OTCGraphAnalyzer(G)
    print(f"  ✓ Graph ready: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")