# graph/analyzer.py
import networkx as nx


class OTCGraphAnalyzer:
    """
    Runs analytical queries directly on the NetworkX graph.
    Used by the FastAPI query layer to build LLM context.
    """

    def __init__(self, G: nx.DiGraph):
        self.G = G

    def summary(self) -> dict:
        """Entity counts — always included in LLM context."""
        counts = {}
        for _, data in self.G.nodes(data=True):
            etype = data.get("entity_type", "Unknown")
            counts[etype] = counts.get(etype, 0) + 1
        return {
            "total_nodes": self.G.number_of_nodes(),
            "total_edges": self.G.number_of_edges(),
            "by_type": counts,
        }

    def detect_broken_flows(self) -> dict:
        """
        Finds incomplete O2C chains:
        - Deliveries never billed
        - Billing docs with no delivery
        - Orders never delivered
        - Journal entries with no billing doc
        """
        delivery_nodes = [
            (n, d) for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "Delivery"
        ]
        billing_nodes = [
            (n, d) for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "BillingDocument"
        ]
        order_nodes = [
            (n, d) for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "SalesOrder"
        ]
        journal_nodes = [
            (n, d) for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "JournalEntry"
        ]

        # Deliveries → no outgoing BillingDocument
        delivered_not_billed = []
        for node_id, data in delivery_nodes:
            successors = list(self.G.successors(node_id))
            has_billing = any(
                self.G.nodes[s].get("entity_type") == "BillingDocument"
                for s in successors
            )
            if not has_billing:
                delivered_not_billed.append({
                    "delivery_id": data.get("delivery_id"),
                    "node_id": node_id,
                    "status": data.get("status"),
                    "delivery_date": data.get("delivery_date"),
                })

        # Billing docs → no incoming Delivery
        billed_without_delivery = []
        for node_id, data in billing_nodes:
            predecessors = list(self.G.predecessors(node_id))
            has_delivery = any(
                self.G.nodes[p].get("entity_type") == "Delivery"
                for p in predecessors
            )
            if not has_delivery:
                billed_without_delivery.append({
                    "billing_id": data.get("billing_id"),
                    "node_id": node_id,
                    "billing_date": data.get("billing_date"),
                    "net_value": data.get("net_value"),
                })

        # Orders → no outgoing Delivery
        orders_not_delivered = []
        for node_id, data in order_nodes:
            successors = list(self.G.successors(node_id))
            has_delivery = any(
                self.G.nodes[s].get("entity_type") == "Delivery"
                for s in successors
            )
            if not has_delivery:
                orders_not_delivered.append({
                    "order_id": data.get("order_id"),
                    "node_id": node_id,
                    "status": data.get("status"),
                    "net_value": data.get("net_value"),
                })

        # Journal entries → no incoming BillingDocument
        journals_without_billing = []
        for node_id, data in journal_nodes:
            predecessors = list(self.G.predecessors(node_id))
            has_billing = any(
                self.G.nodes[p].get("entity_type") == "BillingDocument"
                for p in predecessors
            )
            if not has_billing:
                journals_without_billing.append({
                    "journal_id": data.get("journal_id"),
                    "node_id": node_id,
                    "amount": data.get("amount"),
                    "gl_account": data.get("gl_account"),
                })

        return {
            "delivered_not_billed": delivered_not_billed,
            "billed_without_delivery": billed_without_delivery,
            "orders_not_delivered": orders_not_delivered,
            "journals_without_billing": journals_without_billing,
        }

    def trace_order(self, order_id: str) -> dict:
        """
        Full O2C chain trace for one order:
        Customer → SalesOrder → [OrderItems] → Delivery → BillingDoc → JournalEntries
        """
        order_node = next(
            (n for n, d in self.G.nodes(data=True)
             if d.get("order_id") == order_id),
            None
        )
        if not order_node:
            return {"error": f"Order {order_id} not found"}

        result = {"sales_order": dict(self.G.nodes[order_node])}
        result["sales_order"]["node_id"] = order_node

        # Customer (predecessor)
        for pred in self.G.predecessors(order_node):
            if self.G.nodes[pred].get("entity_type") == "Customer":
                result["customer"] = {**dict(self.G.nodes[pred]), "node_id": pred}

        # Order items (successors)
        result["order_items"] = []
        for succ in self.G.successors(order_node):
            if self.G.nodes[succ].get("entity_type") == "OrderItem":
                item = {**dict(self.G.nodes[succ]), "node_id": succ}
                # Product for this item
                for item_succ in self.G.successors(succ):
                    if self.G.nodes[item_succ].get("entity_type") == "Product":
                        item["product"] = {**dict(self.G.nodes[item_succ]), "node_id": item_succ}
                result["order_items"].append(item)

        # Delivery (successor)
        for succ in self.G.successors(order_node):
            if self.G.nodes[succ].get("entity_type") == "Delivery":
                result["delivery"] = {**dict(self.G.nodes[succ]), "node_id": succ}
                # Billing (successor of delivery)
                for bill_succ in self.G.successors(succ):
                    if self.G.nodes[bill_succ].get("entity_type") == "BillingDocument":
                        result["billing"] = {**dict(self.G.nodes[bill_succ]), "node_id": bill_succ}
                        # Journal entries (successors of billing)
                        result["journal_entries"] = [
                            {**dict(self.G.nodes[je]), "node_id": je}
                            for je in self.G.successors(bill_succ)
                            if self.G.nodes[je].get("entity_type") == "JournalEntry"
                        ]

        return result

    def get_customer_orders(self, customer_id: str) -> list:
        """All orders for a given customer_id value."""
        cust_node = next(
            (n for n, d in self.G.nodes(data=True)
             if d.get("customer_id") == customer_id),
            None
        )
        if not cust_node:
            return []
        return [
            {**dict(self.G.nodes[s]), "node_id": s}
            for s in self.G.successors(cust_node)
            if self.G.nodes[s].get("entity_type") == "SalesOrder"
        ]

    def top_products_by_order_value(self, top_n: int = 10) -> list:
        """Products ranked by total net_value of their order items."""
        product_totals = {}
        for node_id, data in self.G.nodes(data=True):
            if data.get("entity_type") == "OrderItem":
                for succ in self.G.successors(node_id):
                    if self.G.nodes[succ].get("entity_type") == "Product":
                        mid = self.G.nodes[succ].get("material_id", succ)
                        product_totals[mid] = (
                            product_totals.get(mid, 0) + (data.get("net_value") or 0)
                        )
        ranked = sorted(product_totals.items(), key=lambda x: x[1], reverse=True)
        return [{"material_id": k, "total_net_value": v} for k, v in ranked[:top_n]]