# graph/cache.py
import pickle
import json
import os
import networkx as nx
from graph.serializer import OTCGraphSerializer


CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "cache")


def save_graph(G: nx.DiGraph, name: str = "otc_graph"):
    """Save graph as both pickle (fast reload) and JSON (frontend)."""
    os.makedirs(CACHE_DIR, exist_ok=True)

    # Pickle — for fast Python reload
    pkl_path = os.path.join(CACHE_DIR, f"{name}.pkl")
    with open(pkl_path, "wb") as f:
        pickle.dump(G, f)
    print(f"  ✓ Pickle saved → {pkl_path}")

    # JSON — for frontend / API
    json_path = os.path.join(CACHE_DIR, f"{name}.json")
    serializer = OTCGraphSerializer(G)
    with open(json_path, "w") as f:
        f.write(serializer.to_json_string())
    print(f"  ✓ JSON saved   → {json_path}")


def load_graph(name: str = "otc_graph") -> nx.DiGraph:
    """Load from pickle cache — fast, no Supabase call needed."""
    pkl_path = os.path.join(CACHE_DIR, f"{name}.pkl")
    if not os.path.exists(pkl_path):
        raise FileNotFoundError(
            f"No cached graph at {pkl_path}. Run scripts/build_graph.py first."
        )
    with open(pkl_path, "rb") as f:
        G = pickle.load(f)
    print(f"  ✓ Graph loaded from cache ({G.number_of_nodes()} nodes, {G.number_of_edges()} edges)")
    return G


def load_graph_json(name: str = "otc_graph") -> dict:
    """Load the frontend JSON directly — used by FastAPI /graph endpoint."""
    json_path = os.path.join(CACHE_DIR, f"{name}.json")
    if not os.path.exists(json_path):
        raise FileNotFoundError(
            f"No cached JSON at {json_path}. Run scripts/build_graph.py first."
        )
    with open(json_path, "r") as f:
        return json.load(f)


def graph_exists(name: str = "otc_graph") -> bool:
    pkl_path = os.path.join(CACHE_DIR, f"{name}.pkl")
    return os.path.exists(pkl_path)