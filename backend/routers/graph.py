# backend/routers/graph.py
from fastapi import APIRouter, HTTPException
import backend.config as cfg

router = APIRouter(prefix="/graph", tags=["graph"])


@router.get("/")
async def get_full_graph():
    """
    Returns the full graph as {nodes, links} for react-force-graph.
    Called once on frontend load.
    """
    return cfg.graph_json


@router.get("/summary")
async def get_summary():
    """Entity counts and edge totals."""
    return cfg.analyzer.summary()


@router.get("/node/{node_id}")
async def get_node(node_id: str):
    """
    Returns a node's full attributes + all its direct connections.
    Called by the frontend NodeInspector panel on click.
    """
    if node_id not in cfg.G.nodes:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")

    attrs = dict(cfg.G.nodes[node_id])

    # Outgoing edges
    outgoing = [
        {
            "node_id": n,
            "relationship": cfg.G.edges[node_id, n].get("relationship"),
            "direction": "outgoing",
            **dict(cfg.G.nodes[n]),
        }
        for n in cfg.G.successors(node_id)
    ]

    # Incoming edges
    incoming = [
        {
            "node_id": n,
            "relationship": cfg.G.edges[n, node_id].get("relationship"),
            "direction": "incoming",
            **dict(cfg.G.nodes[n]),
        }
        for n in cfg.G.predecessors(node_id)
    ]

    return {
        "node": {"node_id": node_id, **attrs},
        "connections": outgoing + incoming,
    }


@router.get("/trace/order/{order_id}")
async def trace_order(order_id: str):
    """
    Full O2C chain for one sales order:
    Customer → SalesOrder → OrderItems → Delivery → BillingDoc → JournalEntries
    """
    result = cfg.analyzer.trace_order(order_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/trace/billing/{billing_id}")
async def trace_billing(billing_id: str):
    """Trace backwards from a billing document to its sales order."""
    billing_node = next(
        (n for n, d in cfg.G.nodes(data=True) if d.get("billing_id") == billing_id),
        None,
    )
    if not billing_node:
        raise HTTPException(status_code=404, detail=f"Billing doc {billing_id} not found")

    chain = {"billing": {"node_id": billing_node, **dict(cfg.G.nodes[billing_node])}}

    for pred in cfg.G.predecessors(billing_node):
        if cfg.G.nodes[pred].get("entity_type") == "Delivery":
            chain["delivery"] = {"node_id": pred, **dict(cfg.G.nodes[pred])}
            for order_pred in cfg.G.predecessors(pred):
                if cfg.G.nodes[order_pred].get("entity_type") == "SalesOrder":
                    chain["sales_order"] = {"node_id": order_pred, **dict(cfg.G.nodes[order_pred])}
                    for cust_pred in cfg.G.predecessors(order_pred):
                        if cfg.G.nodes[cust_pred].get("entity_type") == "Customer":
                            chain["customer"] = {"node_id": cust_pred, **dict(cfg.G.nodes[cust_pred])}

    chain["journal_entries"] = [
        {"node_id": s, **dict(cfg.G.nodes[s])}
        for s in cfg.G.successors(billing_node)
        if cfg.G.nodes[s].get("entity_type") == "JournalEntry"
    ]

    return chain


@router.get("/broken-flows")
async def get_broken_flows():
    """
    All incomplete O2C chains:
    delivered not billed, billed without delivery,
    orders not delivered, journals without billing.
    """
    return cfg.analyzer.detect_broken_flows()


@router.get("/customer/{customer_id}/orders")
async def get_customer_orders(customer_id: str):
    """All sales orders for a given customer_id value."""
    orders = cfg.analyzer.get_customer_orders(customer_id)
    if not orders:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found or has no orders")
    return {"customer_id": customer_id, "orders": orders}


@router.get("/products/top")
async def get_top_products(top_n: int = 10):
    """Products ranked by total order value."""
    return cfg.analyzer.top_products_by_order_value(top_n=top_n)


@router.get("/debug/orderitem-edges")
async def debug_orderitem_edges():
    """Show edges around first 3 OrderItems to understand graph direction."""
    results = []
    count = 0
    for node_id, data in cfg.G.nodes(data=True):
        if data.get("entity_type") == "OrderItem":
            successors = [
                {"node_id": s, "entity_type": cfg.G.nodes[s].get("entity_type")}
                for s in cfg.G.successors(node_id)
            ]
            predecessors = [
                {"node_id": p, "entity_type": cfg.G.nodes[p].get("entity_type")}
                for p in cfg.G.predecessors(node_id)
            ]
            results.append({
                "node_id": node_id,
                "data": data,
                "successors": successors,
                "predecessors": predecessors,
            })
            count += 1
            if count >= 3:
                break
    return results
@router.get("/debug/salesorder-edges")
async def debug_salesorder_edges():
    """Show successors of first 3 SalesOrders to check Delivery linkage."""
    results = []
    count = 0
    for node_id, data in cfg.G.nodes(data=True):
        if data.get("entity_type") == "SalesOrder":
            successors = [
                {"node_id": s, "entity_type": cfg.G.nodes[s].get("entity_type")}
                for s in cfg.G.successors(node_id)
            ]
            predecessors = [
                {"node_id": p, "entity_type": cfg.G.nodes[p].get("entity_type")}
                for p in cfg.G.predecessors(node_id)
            ]
            results.append({
                "node_id": node_id,
                "order_id": data.get("order_id"),
                "successors": successors,
                "predecessors": predecessors,
            })
            count += 1
            if count >= 3:
                break
    return results


@router.get("/debug/delivery-edges")
async def debug_delivery_edges():
    """Show successors/predecessors of first 3 Deliveries."""
    results = []
    count = 0
    for node_id, data in cfg.G.nodes(data=True):
        if data.get("entity_type") == "Delivery":
            successors = [
                {"node_id": s, "entity_type": cfg.G.nodes[s].get("entity_type")}
                for s in cfg.G.successors(node_id)
            ]
            predecessors = [
                {"node_id": p, "entity_type": cfg.G.nodes[p].get("entity_type")}
                for p in cfg.G.predecessors(node_id)
            ]
            results.append({
                "node_id": node_id,
                "data": data,
                "successors": successors,
                "predecessors": predecessors,
            })
            count += 1
            if count >= 3:
                break
    return results