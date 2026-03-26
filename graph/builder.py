# graph/builder.py
import networkx as nx
from supabase import create_client, Client


class OTCGraphBuilder:
    def __init__(self, supabase_url: str, supabase_key: str):
        self.db: Client = create_client(supabase_url, supabase_key)
        self.G = nx.DiGraph()

    def build(self) -> nx.DiGraph:
        print("Building OTC graph...")
        print("─" * 40)
        self._add_customers()
        self._add_products()
        self._add_addresses()
        self._add_sales_orders()
        self._add_sales_order_items()
        self._add_deliveries()
        self._add_billing_documents()
        self._add_journal_entries()
        print("─" * 40)
        print(f"Nodes total: {self.G.number_of_nodes()}")
        print("Adding edges...")
        self._add_edges()
        print(f"Edges total: {self.G.number_of_edges()}")
        print("Graph build complete ✓")
        return self.G

    def _add_customers(self):
        rows = self.db.table("customers").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="Customer",
                label=r.get("name") or r.get("customer_id", ""),
                customer_id=r.get("customer_id"),
                region=r.get("region"), country=r.get("country"))
        print(f"  ✓ Customers:        {len(rows)}")

    def _add_products(self):
        rows = self.db.table("products_clean").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="Product",
                label=r.get("description") or r.get("material_id", ""),
                material_id=r.get("material_id"),
                product_group=r.get("product_group"),
                unit_of_measure=r.get("unit_of_measure"))
        print(f"  ✓ Products:         {len(rows)}")

    def _add_addresses(self):
        rows = self.db.table("addresses").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="Address",
                label=r.get("city") or r.get("address_id", ""),
                address_id=r.get("address_id"), city=r.get("city"),
                postal_code=r.get("postal_code"), country=r.get("country"),
                plant_code=r.get("plant_code"))
        print(f"  ✓ Addresses:        {len(rows)}")

    def _add_sales_orders(self):
        rows = self.db.table("sales_orders").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="SalesOrder",
                label=f"SO-{r['order_id']}",
                order_id=r.get("order_id"),
                order_date=str(r.get("order_date") or ""),
                net_value=float(r.get("net_value") or 0),
                status=r.get("status"), currency=r.get("currency"))
        print(f"  ✓ Sales orders:     {len(rows)}")

    def _add_sales_order_items(self):
        rows = self.db.table("sales_order_items_clean").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="OrderItem",
                label=f"ITEM-{r.get('item_id', '')}",
                item_id=r.get("item_id"),
                quantity=float(r.get("quantity") or 0),
                unit_price=float(r.get("unit_price") or 0),
                net_value=float(r.get("net_value") or 0),
                item_number=r.get("item_number"))
        print(f"  ✓ Order items:      {len(rows)}")

    def _add_deliveries(self):
        rows = self.db.table("deliveries").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="Delivery",
                label=f"DEL-{r['delivery_id']}",
                delivery_id=r.get("delivery_id"),
                delivery_date=str(r.get("delivery_date") or ""),
                actual_goods_issue_date=str(r.get("actual_goods_issue_date") or ""),
                plant_code=r.get("plant_code"), status=r.get("status"))
        print(f"  ✓ Deliveries:       {len(rows)}")

    def _add_billing_documents(self):
        rows = self.db.table("billing_documents").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="BillingDocument",
                label=f"BILL-{r['billing_id']}",
                billing_id=r.get("billing_id"),
                billing_date=str(r.get("billing_date") or ""),
                net_value=float(r.get("net_value") or 0),
                document_type=r.get("document_type"),
                currency=r.get("currency"))
        print(f"  ✓ Billing docs:     {len(rows)}")

    def _add_journal_entries(self):
        rows = self.db.table("journal_entries").select("*").execute().data
        for r in rows:
            self.G.add_node(r["id"], entity_type="JournalEntry",
                label=f"JE-{r['journal_id']}",
                journal_id=r.get("journal_id"),
                posting_date=str(r.get("posting_date") or ""),
                amount=float(r.get("amount") or 0),
                currency=r.get("currency"),
                gl_account=r.get("gl_account"),
                document_type=r.get("document_type"))
        print(f"  ✓ Journal entries:  {len(rows)}")

    def _add_edges(self):
        counts = {}

        # ── UUID lookup maps ───────────────────────────────────────
        order_id_to_uuid = {
            d["order_id"]: n
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "SalesOrder" and d.get("order_id")
        }
        delivery_id_to_uuid = {
            d["delivery_id"]: n
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "Delivery" and d.get("delivery_id")
        }
        billing_id_to_uuid = {
            d["billing_id"]: n
            for n, d in self.G.nodes(data=True)
            if d.get("entity_type") == "BillingDocument" and d.get("billing_id")
        }

        print(f"  [lookup] order_id map:    {len(order_id_to_uuid)} entries")
        print(f"  [lookup] delivery_id map: {len(delivery_id_to_uuid)} entries")
        print(f"  [lookup] billing_id map:  {len(billing_id_to_uuid)} entries")

        # ── Customer → SalesOrder ──────────────────────────────────
        c = 0
        for r in self.db.table("sales_orders").select("id, customer_id").execute().data:
            if r.get("customer_id") and r["customer_id"] in self.G and r["id"] in self.G:
                self.G.add_edge(r["customer_id"], r["id"],
                    relationship="PLACES_ORDER", edge_type="primary")
                c += 1
        counts["Customer→SalesOrder"] = c

        # ── SalesOrder → OrderItem ─────────────────────────────────
        c = 0
        for r in self.db.table("sales_order_items_clean").select("id, order_id").execute().data:
            if r.get("order_id") and r["order_id"] in self.G and r["id"] in self.G:
                self.G.add_edge(r["order_id"], r["id"],
                    relationship="HAS_ITEM", edge_type="primary")
                c += 1
        counts["SalesOrder→OrderItem"] = c

        # ── SalesOrder → Delivery ──────────────────────────────────
        c = 0
        for r in self.db.table("deliveries").select("id, order_id").execute().data:
            if r.get("order_id") and r["order_id"] in self.G and r["id"] in self.G:
                self.G.add_edge(r["order_id"], r["id"],
                    relationship="FULFILLED_BY", edge_type="primary")
                c += 1
        counts["SalesOrder→Delivery"] = c

        # ── Delivery → BillingDocument ─────────────────────────────
        # billing_document_items.referenceSdDocument = delivery_id (string)
        # billing_document_items.billingDocument = billing_id (string)
        # Confirmed via SQL: referenceSdDocument matches deliveries.delivery_id exactly
        c = 0
        seen = set()
        rows = self.db.table("billing_document_items").select(
            "billingDocument, referenceSdDocument"
        ).execute().data
        print(f"  [billing_document_items] {len(rows)} rows fetched")
        no_delivery = 0
        no_billing = 0
        for r in rows:
            bid_str = str(r.get("billingDocument") or "").strip()
            del_str = str(r.get("referenceSdDocument") or "").strip()
            if not bid_str or not del_str:
                continue
            edge_key = (del_str, bid_str)
            if edge_key in seen:
                continue
            seen.add(edge_key)
            delivery_uuid = delivery_id_to_uuid.get(del_str)
            billing_uuid = billing_id_to_uuid.get(bid_str)
            if not delivery_uuid:
                no_delivery += 1
                continue
            if not billing_uuid:
                no_billing += 1
                continue
            self.G.add_edge(delivery_uuid, billing_uuid,
                relationship="BILLED_AS", edge_type="primary")
            c += 1
        print(f"  [billing] skipped: {no_delivery} missing delivery, {no_billing} missing billing")
        counts["Delivery→BillingDoc"] = c

        # ── BillingDocument → JournalEntry ────────────────────────
        c = 0
        for r in self.db.table("journal_entries").select("id, billing_id").execute().data:
            if r.get("billing_id") and r["billing_id"] in self.G and r["id"] in self.G:
                self.G.add_edge(r["billing_id"], r["id"],
                    relationship="POSTS_TO_JOURNAL", edge_type="primary")
                c += 1
        counts["BillingDoc→JournalEntry"] = c

        # ── OrderItem → Product ────────────────────────────────────
        c = 0
        for r in self.db.table("sales_order_items_clean").select("id, material_id").execute().data:
            if r.get("material_id") and r["material_id"] in self.G and r["id"] in self.G:
                self.G.add_edge(r["id"], r["material_id"],
                    relationship="IS_OF_PRODUCT", edge_type="contextual")
                c += 1
        counts["OrderItem→Product"] = c

        # ── Delivery → Address ─────────────────────────────────────
        c = 0
        for r in self.db.table("deliveries").select("id, ship_to_address_id").execute().data:
            if r.get("ship_to_address_id") and r["ship_to_address_id"] in self.G:
                self.G.add_edge(r["id"], r["ship_to_address_id"],
                    relationship="SHIPS_TO", edge_type="contextual")
                c += 1
        counts["Delivery→Address"] = c

        # ── BillingDocument → Customer ─────────────────────────────
        c = 0
        for r in self.db.table("billing_documents").select("id, customer_id").execute().data:
            if r.get("customer_id") and r["customer_id"] in self.G and r["id"] in self.G:
                self.G.add_edge(r["id"], r["customer_id"],
                    relationship="BILLED_TO", edge_type="contextual")
                c += 1
        counts["BillingDoc→Customer"] = c

        for label, count in counts.items():
            print(f"  ✓ {label}: {count} edges")