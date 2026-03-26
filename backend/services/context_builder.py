# backend/services/context_builder.py
import json
import re
import networkx as nx
from graph.analyzer import OTCGraphAnalyzer
from backend.logger import log, Timer


class ContextBuilder:
    """
    Dynamically builds a focused context string for each query.
    Routes to the right analyzer methods based on detected intent,
    so the LLM always gets relevant, grounded data — not a generic dump.
    """

    def __init__(self, G: nx.DiGraph, analyzer: OTCGraphAnalyzer):
        self.G = G
        self.analyzer = analyzer

    # ── Public entry point ────────────────────────────────────────
    def build(self, query: str, highlighted_node_ids: list[str] = None, req_id: str = None) -> str:
        extra = {"stage": "CONTEXT", "req_id": req_id}
        timer = Timer()
        q = query.lower()
        parts = []

        # 1. Always include dataset summary (entity counts)
        summary = self.analyzer.summary()
        parts.append("DATASET SUMMARY:\n" + json.dumps(summary, indent=2))

        # 2. Detect intents and fetch only what's needed
        intents = self._detect_intents(q)
        log.info(
            f"Detected intents: {sorted(intents) if intents else ['(none — broad overview fallback)']}",
            extra=extra,
        )

        if "entity_lookup" in intents:
            log.debug("Running handler: entity_lookup", extra=extra)
            result = self._handle_entity_lookup(query)
            log.debug(f"  entity_lookup → {len(result)} context part(s)", extra=extra)
            parts += result

        if "product_billing" in intents:
            log.debug("Running handler: product_billing", extra=extra)
            result = self._handle_product_billing()
            log.debug(f"  product_billing → {len(result)} context part(s)", extra=extra)
            parts += result

        if "trace" in intents:
            log.debug("Running handler: trace", extra=extra)
            result = self._handle_trace(query)
            log.debug(f"  trace → {len(result)} context part(s)", extra=extra)
            parts += result

        if "broken_flows" in intents:
            log.debug("Running handler: broken_flows", extra=extra)
            result = self._handle_broken_flows()
            log.debug(f"  broken_flows → {len(result)} context part(s)", extra=extra)
            parts += result

        if "delivery_status" in intents:
            log.debug("Running handler: delivery_status", extra=extra)
            result = self._handle_delivery_status(q)
            log.debug(f"  delivery_status → {len(result)} context part(s)", extra=extra)
            parts += result

        if "customer" in intents:
            log.debug("Running handler: customer", extra=extra)
            result = self._handle_customers()
            log.debug(f"  customer → {len(result)} context part(s)", extra=extra)
            parts += result

        if "journal" in intents:
            log.debug("Running handler: journal", extra=extra)
            result = self._handle_journals()
            log.debug(f"  journal → {len(result)} context part(s)", extra=extra)
            parts += result

        if "billing_overview" in intents:
            log.debug("Running handler: billing_overview", extra=extra)
            result = self._handle_billing_overview()
            log.debug(f"  billing_overview → {len(result)} context part(s)", extra=extra)
            parts += result

        if "sales_order_overview" in intents:
            log.debug("Running handler: sales_order_overview", extra=extra)
            result = self._handle_sales_order_overview()
            log.debug(f"  sales_order_overview → {len(result)} context part(s)", extra=extra)
            parts += result

        # 3. If no specific intent matched beyond summary, add a broad overview
        #    so the LLM can still answer generic questions like "what's in the dataset"
        if len(intents) == 0:
            log.debug("Running handler: broad_overview (fallback)", extra=extra)
            parts += self._handle_broad_overview()

        # 4. Currently highlighted nodes (UI context)
        if highlighted_node_ids:
            log.debug(f"Injecting {len(highlighted_node_ids)} highlighted node(s) into context", extra=extra)
            parts += self._handle_highlighted(highlighted_node_ids)

        sep = "\n\n" + "─" * 40 + "\n"
        context = sep.join(parts)

        log.info(
            f"Context built: {len(parts)} section(s), {len(context):,} chars, {len(context.split()):,} words  [{timer.elapsed_ms()}ms]",
            extra=extra,
        )
        return context

    # ── Intent detection ──────────────────────────────────────────
    def _detect_intents(self, q: str) -> set:
        intents = set()

        # Specific entity/ID lookup
        if re.search(r'\b(\d{6,})\b', q) or re.search(
            r'\b(SO|DEL|BILL|ITEM|CUST|PROD|JE)[-_]?\d+\b', q, re.IGNORECASE
        ):
            intents.add("entity_lookup")

        # Product + billing count questions
        if any(kw in q for kw in [
            "product", "material", "most billing", "highest billing",
            "billing document", "most bill", "top product"
        ]):
            intents.add("product_billing")

        # Flow trace
        if any(kw in q for kw in [
            "trace", "flow", "track", "path", "follow", "full flow",
            "order flow", "end to end", "end-to-end", "chain"
        ]):
            intents.add("trace")

        # Broken / incomplete flows
        if any(kw in q for kw in [
            "broken", "incomplete", "missing", "not billed", "unbilled",
            "no delivery", "without delivery", "no billing", "gap",
            "issue", "problem", "error", "failed", "never billed",
            "never delivered", "identify orders"
        ]):
            intents.add("broken_flows")

        # Delivery status / counts
        if any(kw in q for kw in [
            "deliver", "dispatch", "shipped", "goods issue",
            "how many deliver", "pending deliver", "done deliver"
        ]):
            intents.add("delivery_status")

        # Customer queries
        if any(kw in q for kw in ["customer", "client", "buyer"]):
            intents.add("customer")

        # Journal / accounting
        if any(kw in q for kw in [
            "journal", "gl account", "posting", "accounting",
            "fiscal", "ledger", "gl entry"
        ]):
            intents.add("journal")

        # Billing overview (not product-specific)
        if any(kw in q for kw in [
            "billing", "invoice", "billed", "bill count",
            "total billed", "revenue"
        ]) and "product_billing" not in intents:
            intents.add("billing_overview")

        # Sales order overview
        if any(kw in q for kw in [
            "sales order", "order count", "how many order",
            "order status", "all order", "list order"
        ]):
            intents.add("sales_order_overview")

        return intents

    # ── Intent handlers ───────────────────────────────────────────

    def _handle_entity_lookup(self, query: str) -> list:
        results = []
        id_match = re.search(r'\b(\d{6,})\b', query)
        prefix_match = re.search(
            r'\b(SO|DEL|BILL|ITEM|CUST|PROD|JE)[-_]?(\d+)\b',
            query, re.IGNORECASE
        )
        found_nodes = []
        prefix_map = {
            "SO": "SalesOrder", "DEL": "Delivery",
            "BILL": "BillingDocument", "ITEM": "OrderItem",
            "CUST": "Customer", "PROD": "Product", "JE": "JournalEntry"
        }

        if prefix_match:
            entity_type = prefix_map.get(prefix_match.group(1).upper())
            number = prefix_match.group(2)
            for n, d in self.G.nodes(data=True):
                if number in str(n) or number in str(d.get("order_id", "")):
                    if not entity_type or d.get("entity_type") == entity_type:
                        found_nodes.append(self._node_with_connections(n, d))

        if not found_nodes and id_match:
            raw_id = id_match.group(1)
            for n, d in self.G.nodes(data=True):
                if (raw_id in str(n) or
                    raw_id == str(d.get("order_id", "")) or
                    raw_id == str(d.get("delivery_id", "")) or
                    raw_id == str(d.get("billing_id", "")) or
                    raw_id in str(d.get("label", ""))):
                    found_nodes.append(self._node_with_connections(n, d))

        if found_nodes:
            results.append(
                f"DIRECT NODE LOOKUP ({len(found_nodes)} found):\n"
                + json.dumps(found_nodes, indent=2, default=str)
            )
            # Auto-trace any SalesOrder found
            for node in found_nodes:
                if node.get("entity_type") == "SalesOrder":
                    order_id = str(node.get("order_id") or node.get("node_id", "")).replace("SO-", "")
                    trace = self.analyzer.trace_order(order_id)
                    results.append(
                        f"FULL O2C TRACE FOR ORDER {order_id}:\n"
                        + json.dumps(trace, indent=2, default=str)
                    )
        return results

    def _handle_product_billing(self) -> list:
        top_value = self.analyzer.top_products_by_order_value(top_n=15)
        billing_counts = self._product_billing_counts()
        return [
            "TOP PRODUCTS BY ORDER VALUE:\n" + json.dumps(top_value, indent=2),
            "PRODUCTS RANKED BY BILLING DOCUMENT COUNT:\n"
            + json.dumps(billing_counts[:15], indent=2),
        ]

    def _handle_trace(self, query: str) -> list:
        order_match = re.search(r'\b(\d{6,})\b', query)
        if order_match:
            order_id = order_match.group(1)
            trace = self.analyzer.trace_order(order_id)

            # Extract all node_ids in O2C flow order so LLM always highlights all of them
            highlight_ids = []
            if "customer" in trace:
                highlight_ids.append(trace["customer"]["node_id"])
            if "sales_order" in trace:
                highlight_ids.append(trace["sales_order"]["node_id"])
            for item in trace.get("order_items", []):
                highlight_ids.append(item["node_id"])
                if "product" in item:
                    highlight_ids.append(item["product"]["node_id"])
            if "delivery" in trace:
                highlight_ids.append(trace["delivery"]["node_id"])
            if "billing" in trace:
                highlight_ids.append(trace["billing"]["node_id"])
            for je in trace.get("journal_entries", []):
                highlight_ids.append(je["node_id"])

            return [
                f"FULL O2C TRACE FOR ORDER {order_id}:\n"
                + json.dumps(trace, indent=2, default=str),
                f"ALL NODE IDS TO HIGHLIGHT (in flow order, Customer → SalesOrder → OrderItems → Products → Delivery → BillingDocument → JournalEntries):\n"
                + json.dumps(highlight_ids, indent=2)
            ]
        # No specific order — show 3 sample traces
        sample_ids = [
            d.get("order_id") for _, d in self.G.nodes(data=True)
            if d.get("entity_type") == "SalesOrder" and d.get("order_id")
        ][:3]
        traces = [self.analyzer.trace_order(oid) for oid in sample_ids]
        return ["SAMPLE O2C TRACES (first 3 orders):\n"
                + json.dumps(traces, indent=2, default=str)]

    def _handle_broken_flows(self) -> list:
        broken = self.analyzer.detect_broken_flows()
        return ["BROKEN / INCOMPLETE FLOWS:\n"
                + json.dumps(broken, indent=2, default=str)]

    def _handle_delivery_status(self, q: str) -> list:
        deliveries = [
            {"node_id": n, **d}
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "Delivery"
        ]
        # Count by status
        status_counts = {}
        for d in deliveries:
            s = d.get("status", "Unknown")
            status_counts[s] = status_counts.get(s, 0) + 1

        result = {
            "total_deliveries": len(deliveries),
            "by_status": status_counts,
            "sample_deliveries": deliveries[:20],
        }
        return [f"DELIVERY DATA ({len(deliveries)} total):\n"
                + json.dumps(result, indent=2, default=str)]

    def _handle_customers(self) -> list:
        customers = [
            {"node_id": n, **d}
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "Customer"
        ]
        return [f"ALL CUSTOMERS ({len(customers)} total):\n"
                + json.dumps(customers, indent=2, default=str)]

    def _handle_journals(self) -> list:
        journals = [
            {"node_id": n, **d}
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "JournalEntry"
        ]
        return [f"JOURNAL ENTRIES ({len(journals)} total, first 30 shown):\n"
                + json.dumps(journals[:30], indent=2, default=str)]

    def _handle_billing_overview(self) -> list:
        bills = [
            {"node_id": n, **d}
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "BillingDocument"
        ]
        total_value = sum(b.get("net_value") or 0 for b in bills)
        return [f"BILLING DOCUMENTS ({len(bills)} total, net value sum={total_value}):\n"
                + json.dumps(bills[:30], indent=2, default=str)]

    def _handle_sales_order_overview(self) -> list:
        orders = [
            {"node_id": n, **d}
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "SalesOrder"
        ]
        status_counts = {}
        for o in orders:
            s = o.get("status", "Unknown")
            status_counts[s] = status_counts.get(s, 0) + 1
        result = {
            "total_orders": len(orders),
            "by_status": status_counts,
            "sample_orders": orders[:20],
        }
        return [f"SALES ORDERS ({len(orders)} total):\n"
                + json.dumps(result, indent=2, default=str)]

    def _handle_broad_overview(self) -> list:
        """Fallback for generic questions — give a richer summary."""
        orders = sum(1 for _, d in self.G.nodes(data=True) if d.get("entity_type") == "SalesOrder")
        deliveries = sum(1 for _, d in self.G.nodes(data=True) if d.get("entity_type") == "Delivery")
        billing = sum(1 for _, d in self.G.nodes(data=True) if d.get("entity_type") == "BillingDocument")
        journals = sum(1 for _, d in self.G.nodes(data=True) if d.get("entity_type") == "JournalEntry")
        customers = sum(1 for _, d in self.G.nodes(data=True) if d.get("entity_type") == "Customer")
        products = sum(1 for _, d in self.G.nodes(data=True) if d.get("entity_type") == "Product")
        overview = {
            "sales_orders": orders,
            "deliveries": deliveries,
            "billing_documents": billing,
            "journal_entries": journals,
            "customers": customers,
            "products": products,
        }
        return ["DATASET OVERVIEW:\n" + json.dumps(overview, indent=2)]

    def _handle_highlighted(self, highlighted_node_ids: list) -> list:
        highlighted_data = [
            {"node_id": nid, **dict(self.G.nodes[nid])}
            for nid in highlighted_node_ids
            if nid in self.G.nodes
        ]
        if highlighted_data:
            return ["CURRENTLY SELECTED NODES IN UI:\n"
                    + json.dumps(highlighted_data, indent=2, default=str)]
        return []

    # ── Helpers ───────────────────────────────────────────────────

    def _node_with_connections(self, n, d) -> dict:
        node_data = {"node_id": n, **d}
        neighbors = []
        for succ in self.G.successors(n):
            neighbors.append({
                "direction": "outgoing",
                "node_id": succ,
                "edge_type": self.G.edges[n, succ].get("edge_type", ""),
                **dict(self.G.nodes[succ]),
            })
        for pred in self.G.predecessors(n):
            neighbors.append({
                "direction": "incoming",
                "node_id": pred,
                "edge_type": self.G.edges[pred, n].get("edge_type", ""),
                **dict(self.G.nodes[pred]),
            })
        node_data["connections"] = neighbors
        return node_data

    def _product_billing_counts(self) -> list:
        """
        Traversal: OrderItem → Product (successor) to get material_id
                   OrderItem ← SalesOrder (predecessor) → Delivery → BillingDocument
        """
        product_billing = {}
        for node_id, data in self.G.nodes(data=True):
            if data.get("entity_type") != "OrderItem":
                continue
            # Get material_id from Product successor
            material_id = None
            for succ in self.G.successors(node_id):
                if self.G.nodes[succ].get("entity_type") == "Product":
                    material_id = (
                        self.G.nodes[succ].get("material_id")
                        or self.G.nodes[succ].get("label")
                        or succ
                    )
                    break
            if not material_id:
                material_id = data.get("material_id") or data.get("product_id")
            if not material_id:
                continue
            # Walk: OrderItem ← SalesOrder → Delivery → BillingDocument
            for so_node in self.G.predecessors(node_id):
                if self.G.nodes[so_node].get("entity_type") != "SalesOrder":
                    continue
                for del_node in self.G.successors(so_node):
                    if self.G.nodes[del_node].get("entity_type") != "Delivery":
                        continue
                    for bill_node in self.G.successors(del_node):
                        if self.G.nodes[bill_node].get("entity_type") != "BillingDocument":
                            continue
                        if material_id not in product_billing:
                            product_billing[material_id] = set()
                        product_billing[material_id].add(bill_node)

        result = [
            {"material_id": mid, "billing_document_count": len(bills)}
            for mid, bills in product_billing.items()
        ]
        return sorted(result, key=lambda x: x["billing_document_count"], reverse=True)