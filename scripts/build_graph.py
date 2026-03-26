# scripts/build_graph.py
import os
import sys
from dotenv import load_dotenv

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

load_dotenv()

from graph.builder import OTCGraphBuilder
from graph.analyzer import OTCGraphAnalyzer
from graph.cache import save_graph

def main():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

    # 1. Build
    builder = OTCGraphBuilder(url, key)
    G = builder.build()

    # 2. Analyze — print summary to confirm correctness
    print("\nRunning post-build analysis...")
    analyzer = OTCGraphAnalyzer(G)

    summary = analyzer.summary()
    print(f"\nGraph summary:")
    for etype, count in summary["by_type"].items():
        print(f"  {etype}: {count}")
    print(f"  Total nodes: {summary['total_nodes']}")
    print(f"  Total edges: {summary['total_edges']}")

    broken = analyzer.detect_broken_flows()
    print(f"\nBroken flow detection:")
    print(f"  Delivered not billed:     {len(broken['delivered_not_billed'])}")
    print(f"  Billed without delivery:  {len(broken['billed_without_delivery'])}")
    print(f"  Orders not delivered:     {len(broken['orders_not_delivered'])}")
    print(f"  Journals without billing: {len(broken['journals_without_billing'])}")

    # 3. Save
    print("\nSaving graph cache...")
    save_graph(G)

    print("\n✅ Phase 2 complete. Graph is ready for Phase 3 (FastAPI backend).")
    print("   Cache location: cache/otc_graph.pkl  (Python reload)")
    print("   Cache location: cache/otc_graph.json (frontend JSON)")


if __name__ == "__main__":
    main()