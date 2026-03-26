"""
OTC Data Audit Script
=====================
Connects to Supabase and runs a comprehensive audit of:
  1. Raw tables  — row counts, nulls in key columns
  2. Clean tables — row counts, null FKs, orphaned refs
  3. Cross-table linkage — does every FK actually resolve?
  4. Business logic checks — duplicates, amounts, date ordering

Run:
    python audit_tables.py

Requires: SUPABASE_URL and SUPABASE_KEY in environment or .env file
"""

import os
import json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ── ANSI colours ─────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg):  print(f"  {RED}✗ {msg}{RESET}")
def warn(msg):  print(f"  {YELLOW}⚠ {msg}{RESET}")
def info(msg):  print(f"  {BLUE}→{RESET} {msg}")
def section(title):
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")


class OTCAuditor:
    def __init__(self, url: str, key: str):
        self.db = create_client(url, key)
        self.results = {"passed": 0, "failed": 0, "warnings": 0}

    # ── helpers ──────────────────────────────────────────────────────────────

    def count(self, table: str) -> int:
        try:
            r = self.db.table(table).select("*", count="exact").execute()
            return r.count or 0
        except Exception as e:
            return -1

    def fetch_all(self, table: str, columns: str = "*") -> list:
        try:
            r = self.db.table(table).select(columns).execute()
            return r.data or []
        except Exception as e:
            warn(f"Could not fetch {table}: {e}")
            return []

    def count_nulls(self, table: str, col: str) -> int:
        try:
            r = self.db.table(table).select(col, count="exact").is_(col, "null").execute()
            return r.count or 0
        except Exception as e:
            warn(f"Could not check nulls in {table}.{col}: {e}")
            return -1

    def check_count(self, table: str, expected: int, label: str = None):
        actual = self.count(table)
        label = label or table
        if actual == -1:
            fail(f"{label}: table not found or error")
            self.results["failed"] += 1
        elif actual == expected:
            ok(f"{label}: {actual} rows ✓")
            self.results["passed"] += 1
        elif actual == 0:
            fail(f"{label}: 0 rows — table is EMPTY (expected {expected})")
            self.results["failed"] += 1
        elif actual < expected:
            fail(f"{label}: {actual} rows — MISSING {expected - actual} rows (expected {expected})")
            self.results["failed"] += 1
        else:
            warn(f"{label}: {actual} rows — MORE than expected {expected} (duplicates?)")
            self.results["warnings"] += 1

    def check_no_nulls(self, table: str, col: str, total: int):
        nulls = self.count_nulls(table, col)
        if nulls == -1:
            warn(f"{table}.{col}: could not check")
            self.results["warnings"] += 1
        elif nulls == 0:
            ok(f"{table}.{col}: no nulls ✓")
            self.results["passed"] += 1
        else:
            fail(f"{table}.{col}: {nulls}/{total} rows have NULL — broken FK linkage")
            self.results["failed"] += 1

    def check_null_ok(self, table: str, col: str, total: int, context: str = ""):
        """For columns that are EXPECTED to be null — just report count."""
        nulls = self.count_nulls(table, col)
        pct = round(100 * nulls / total, 1) if total > 0 else 0
        if nulls == 0:
            ok(f"{table}.{col}: fully populated (0 nulls)")
        elif nulls == total:
            warn(f"{table}.{col}: ALL {total} rows are NULL {context}")
            self.results["warnings"] += 1
        else:
            info(f"{table}.{col}: {nulls}/{total} rows NULL ({pct}%) {context}")

    # ── RAW TABLE AUDITS ─────────────────────────────────────────────────────

    def audit_raw_tables(self):
        section("RAW TABLE ROW COUNTS")
        print("  Checking every raw source table exists and has the expected rows\n")

        expected = {
            "business_partners":                       8,
            "business_partner_addresses":              8,
            "customer_company_assignments":            8,
            "customer_sales_area_assignments":         28,
            "products":                                69,
            "product_descriptions":                    69,
            "product_plants":                          3036,
            "product_storage_locations":               16723,
            "plants":                                  44,
            "sales_order_headers":                     100,
            "sales_order_items":                       167,
            "sales_order_schedule_lines":              179,
            "outbound_delivery_headers":               86,
            "outbound_delivery_items":                 137,
            "billing_document_headers":                163,
            "billing_document_cancellations":          80,
            "billing_document_items":                  245,
            "journal_entry_items_accounts_receivable": 123,
            "payments_accounts_receivable":            120,
        }

        for table, exp in expected.items():
            self.check_count(table, exp, f"[RAW] {table}")

    def audit_raw_key_nulls(self):
        section("RAW TABLE — KEY COLUMN NULL CHECKS")
        print("  Primary/join keys in raw tables must never be null\n")

        checks = [
            ("business_partners",                       "businessPartner"),
            ("business_partners",                       "customer"),
            ("business_partner_addresses",              "businessPartner"),
            ("business_partner_addresses",              "addressId"),
            ("products",                                "product"),
            ("product_descriptions",                    "product"),
            ("sales_order_headers",                     "salesOrder"),
            ("sales_order_headers",                     "soldToParty"),
            ("sales_order_items",                       "salesOrder"),
            ("sales_order_items",                       "salesOrderItem"),
            ("sales_order_items",                       "material"),
            ("outbound_delivery_headers",               "deliveryDocument"),
            ("outbound_delivery_items",                 "deliveryDocument"),
            ("outbound_delivery_items",                 "referenceSdDocument"),
            ("billing_document_headers",                "billingDocument"),
            ("billing_document_headers",                "soldToParty"),
            ("billing_document_items",                  "billingDocument"),
            ("billing_document_items",                  "material"),
            ("journal_entry_items_accounts_receivable", "accountingDocument"),
            ("journal_entry_items_accounts_receivable", "referenceDocument"),
        ]

        for table, col in checks:
            total = self.count(table)
            self.check_no_nulls(table, col, total)

    # ── CLEAN TABLE AUDITS ───────────────────────────────────────────────────

    def audit_clean_counts(self):
        section("CLEAN TABLE ROW COUNTS")
        print("  Checking pipeline output tables have the correct row counts\n")

        # Clean tables should match their source raw tables
        # (after dedup — some raw tables have dupes stripped)
        expected = {
            "customers":                  8,    # from business_partners (8 unique customers)
            "products_clean":             69,   # from products (69 unique)
            "addresses":                  8,    # from business_partner_addresses (8)
            "sales_orders":               100,  # from sales_order_headers (100)
            "sales_order_items_clean":    167,  # from sales_order_items (167)
            "deliveries":                 86,   # from outbound_delivery_headers (86)
            "delivery_items":             137,  # from outbound_delivery_items (137)
            "billing_documents":          163,  # from billing_document_headers (163)
            "journal_entries":            123,  # from journal_entry_items_accounts_receivable (123)
        }

        for table, exp in expected.items():
            self.check_count(table, exp, f"[CLEAN] {table}")

    def audit_clean_fk_nulls(self):
        section("CLEAN TABLE — FOREIGN KEY NULL CHECKS")
        print("  Every FK that CAN be resolved from raw data MUST be non-null\n")

        # (table, column, must_be_full, context_note)
        checks = [
            # MUST be fully populated
            ("customers",               "customer_id",   True,  ""),
            ("products_clean",          "material_id",   True,  ""),
            ("addresses",               "address_id",    True,  ""),
            ("sales_orders",            "order_id",      True,  ""),
            ("sales_orders",            "customer_id",   True,  "soldToParty always present in raw"),
            ("sales_order_items_clean", "item_id",       True,  ""),
            ("sales_order_items_clean", "order_id",      True,  "salesOrder always present"),
            ("sales_order_items_clean", "material_id",   True,  "material always present in raw"),
            ("deliveries",              "delivery_id",   True,  ""),
            ("deliveries",              "order_id",      True,  "resolvable via outbound_delivery_items"),
            ("delivery_items",          "item_id",       True,  ""),
            ("delivery_items",          "delivery_id",   True,  ""),
            ("delivery_items",          "order_item_id", True,  "resolvable via referenceSdDocument+Item"),
            ("billing_documents",       "billing_id",    True,  ""),
            ("billing_documents",       "customer_id",   True,  "soldToParty always present in raw"),
            ("journal_entries",         "journal_id",    True,  ""),
            ("journal_entries",         "billing_id",    True,  "referenceDocument → billingDocument"),

            # MAY be null (raw data doesn't have these columns)
            ("deliveries",        "customer_id",        False, "(no customer in delivery header — expected NULL)"),
            ("deliveries",        "ship_to_address_id", False, "(no ship-to in delivery header — expected NULL)"),
            ("delivery_items",    "material_id",        False, "(no material col in outbound_delivery_items — expected NULL)"),
            ("billing_documents", "delivery_id",        False, "(not in billing header — acceptable NULL)"),
            ("billing_documents", "order_id",           False, "(not in billing header — acceptable NULL)"),
        ]

        totals = {}
        for table, col, must_full, note in checks:
            if table not in totals:
                totals[table] = self.count(table)
            total = totals[table]
            if must_full:
                self.check_no_nulls(table, col, total)
            else:
                self.check_null_ok(table, col, total, note)

    # ── CROSS-TABLE LINKAGE ──────────────────────────────────────────────────

    def audit_linkage(self):
        section("CROSS-TABLE LINKAGE — FK RESOLUTION")
        print("  Verify every UUID FK in clean tables actually exists in the target\n")

        checks = [
            # (child_table, child_col, parent_table, parent_col, label)
            ("sales_orders",            "customer_id",   "customers",               "id",  "sales_orders → customers"),
            ("sales_order_items_clean", "order_id",      "sales_orders",            "id",  "order_items → orders"),
            ("sales_order_items_clean", "material_id",   "products_clean",          "id",  "order_items → products"),
            ("deliveries",              "order_id",      "sales_orders",            "id",  "deliveries → orders"),
            ("delivery_items",          "delivery_id",   "deliveries",              "id",  "delivery_items → deliveries"),
            ("delivery_items",          "order_item_id", "sales_order_items_clean", "id",  "delivery_items → order_items"),
            ("billing_documents",       "customer_id",   "customers",               "id",  "billing_docs → customers"),
            ("journal_entries",         "billing_id",    "billing_documents",       "id",  "journal_entries → billing_docs"),
        ]

        for child_table, child_col, parent_table, parent_col, label in checks:
            try:
                # Get all FK values from child (non-null only)
                child_rows = self.fetch_all(child_table, f"{child_col}")
                child_ids  = set(
                    r[child_col] for r in child_rows
                    if r.get(child_col) is not None
                )

                # Get all PK values from parent
                parent_rows = self.fetch_all(parent_table, parent_col)
                parent_ids  = set(r[parent_col] for r in parent_rows)

                orphans = child_ids - parent_ids
                if orphans:
                    fail(f"{label}: {len(orphans)} orphaned FK values (point to non-existent parent rows!)")
                    self.results["failed"] += 1
                    # Show a sample
                    sample = list(orphans)[:3]
                    info(f"  Sample orphaned IDs: {sample}")
                else:
                    ok(f"{label}: all {len(child_ids)} FKs resolve correctly ✓")
                    self.results["passed"] += 1
            except Exception as e:
                warn(f"{label}: could not check — {e}")
                self.results["warnings"] += 1

    # ── BUSINESS LOGIC CHECKS ────────────────────────────────────────────────

    def audit_business_logic(self):
        section("BUSINESS LOGIC CHECKS")
        print("  Verify data makes sense — no impossible values, amounts, dates\n")

        # 1. Duplicate order_ids in sales_orders
        try:
            rows = self.fetch_all("sales_orders", "order_id")
            ids = [r["order_id"] for r in rows]
            dupes = len(ids) - len(set(ids))
            if dupes == 0:
                ok("sales_orders: no duplicate order_ids ✓")
                self.results["passed"] += 1
            else:
                fail(f"sales_orders: {dupes} duplicate order_ids found!")
                self.results["failed"] += 1
        except Exception as e:
            warn(f"Could not check order_id duplicates: {e}")

        # 2. Negative quantities in order items
        try:
            rows = self.fetch_all("sales_order_items_clean", "quantity")
            neg = [r for r in rows if r.get("quantity") is not None and float(r["quantity"]) < 0]
            if not neg:
                ok("sales_order_items_clean: no negative quantities ✓")
                self.results["passed"] += 1
            else:
                warn(f"sales_order_items_clean: {len(neg)} items have negative quantity")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check order item quantities: {e}")

        # 3. Negative quantities in delivery items
        try:
            rows = self.fetch_all("delivery_items", "quantity_delivered")
            neg = [r for r in rows if r.get("quantity_delivered") is not None and float(r["quantity_delivered"]) < 0]
            if not neg:
                ok("delivery_items: no negative quantities_delivered ✓")
                self.results["passed"] += 1
            else:
                warn(f"delivery_items: {len(neg)} items have negative quantity_delivered")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check delivery quantities: {e}")

        # 4. Billing documents with zero or null net_value
        try:
            rows = self.fetch_all("billing_documents", "net_value,billing_id")
            zero_or_null = [r for r in rows if r.get("net_value") is None or float(r["net_value"]) == 0]
            if not zero_or_null:
                ok("billing_documents: no zero/null net_values ✓")
                self.results["passed"] += 1
            else:
                warn(f"billing_documents: {len(zero_or_null)} rows with zero or null net_value")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check billing net_values: {e}")

        # 5. Journal entries — check amounts are non-null
        try:
            rows = self.fetch_all("journal_entries", "amount,journal_id")
            null_amt = [r for r in rows if r.get("amount") is None]
            if not null_amt:
                ok("journal_entries: all amounts populated ✓")
                self.results["passed"] += 1
            else:
                warn(f"journal_entries: {len(null_amt)} rows have null amount")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check journal amounts: {e}")

        # 6. Every sales order has at least one order item
        try:
            orders    = self.fetch_all("sales_orders", "id,order_id")
            order_ids = set(r["id"] for r in orders)
            items     = self.fetch_all("sales_order_items_clean", "order_id")
            orders_with_items = set(r["order_id"] for r in items if r.get("order_id"))
            orphan_orders = order_ids - orders_with_items
            if not orphan_orders:
                ok(f"All {len(order_ids)} sales orders have at least one order item ✓")
                self.results["passed"] += 1
            else:
                warn(f"{len(orphan_orders)} sales orders have NO order items")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check orders vs items: {e}")

        # 7. Every delivery has at least one delivery item
        try:
            deliveries   = self.fetch_all("deliveries", "id,delivery_id")
            delivery_ids = set(r["id"] for r in deliveries)
            ditems       = self.fetch_all("delivery_items", "delivery_id")
            deliveries_with_items = set(r["delivery_id"] for r in ditems if r.get("delivery_id"))
            orphan_deliveries = delivery_ids - deliveries_with_items
            if not orphan_deliveries:
                ok(f"All {len(delivery_ids)} deliveries have at least one delivery item ✓")
                self.results["passed"] += 1
            else:
                warn(f"{len(orphan_deliveries)} deliveries have NO delivery items")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check deliveries vs items: {e}")

        # 8. Customer IDs in clean tables match what's in business_partners
        try:
            raw_custs   = self.fetch_all("business_partners", "customer")
            raw_ids     = set(str(r["customer"]) for r in raw_custs if r.get("customer"))
            clean_custs = self.fetch_all("customers", "customer_id")
            clean_ids   = set(r["customer_id"] for r in clean_custs if r.get("customer_id"))
            missing_in_clean = raw_ids - clean_ids
            extra_in_clean   = clean_ids - raw_ids
            if not missing_in_clean and not extra_in_clean:
                ok(f"customers table perfectly mirrors business_partners ({len(clean_ids)} rows) ✓")
                self.results["passed"] += 1
            else:
                if missing_in_clean:
                    fail(f"customers: {len(missing_in_clean)} customers from raw are MISSING: {missing_in_clean}")
                    self.results["failed"] += 1
                if extra_in_clean:
                    warn(f"customers: {len(extra_in_clean)} extra customer_ids not in raw: {extra_in_clean}")
                    self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not compare customers: {e}")

        # 9. Products in clean table match raw products
        try:
            raw_prods   = self.fetch_all("products", "product")
            raw_mids    = set(r["product"] for r in raw_prods if r.get("product"))
            clean_prods = self.fetch_all("products_clean", "material_id")
            clean_mids  = set(r["material_id"] for r in clean_prods if r.get("material_id"))
            missing = raw_mids - clean_mids
            if not missing:
                ok(f"products_clean mirrors products table ({len(clean_mids)} rows) ✓")
                self.results["passed"] += 1
            else:
                fail(f"products_clean: {len(missing)} products from raw are MISSING")
                self.results["failed"] += 1
        except Exception as e:
            warn(f"Could not compare products: {e}")

        # 10. Sales order IDs in items all exist as headers
        try:
            raw_headers  = self.fetch_all("sales_order_headers", "salesOrder")
            raw_order_ids = set(r["salesOrder"] for r in raw_headers if r.get("salesOrder"))
            raw_items    = self.fetch_all("sales_order_items", "salesOrder")
            item_order_ids = set(r["salesOrder"] for r in raw_items if r.get("salesOrder"))
            orphan = item_order_ids - raw_order_ids
            if not orphan:
                ok("All salesOrders in sales_order_items exist in sales_order_headers ✓")
                self.results["passed"] += 1
            else:
                fail(f"sales_order_items references {len(orphan)} salesOrders not in headers: {orphan}")
                self.results["failed"] += 1
        except Exception as e:
            warn(f"Could not check order item references: {e}")

        # 11. Delivery items reference documents that exist as sales orders
        try:
            raw_del_items = self.fetch_all("outbound_delivery_items", "referenceSdDocument")
            ref_orders    = set(r["referenceSdDocument"] for r in raw_del_items if r.get("referenceSdDocument"))
            raw_headers   = self.fetch_all("sales_order_headers", "salesOrder")
            so_ids        = set(r["salesOrder"] for r in raw_headers if r.get("salesOrder"))
            missing_refs  = ref_orders - so_ids
            if not missing_refs:
                ok(f"All delivery items reference valid sales orders ✓")
                self.results["passed"] += 1
            else:
                warn(f"delivery items reference {len(missing_refs)} salesOrders not in headers: {missing_refs}")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check delivery item references: {e}")

        # 12. Journal referenceDocuments resolve to billing docs
        try:
            journals  = self.fetch_all("journal_entry_items_accounts_receivable", "referenceDocument")
            j_refs    = set(r["referenceDocument"] for r in journals if r.get("referenceDocument"))
            billings  = self.fetch_all("billing_document_headers", "billingDocument")
            b_ids     = set(r["billingDocument"] for r in billings if r.get("billingDocument"))
            unmatched = j_refs - b_ids
            matched   = j_refs & b_ids
            if not unmatched:
                ok(f"All {len(j_refs)} journal referenceDocuments resolve to billing_document_headers ✓")
                self.results["passed"] += 1
            else:
                warn(f"journal_entries: {len(unmatched)} referenceDocuments not in billing_document_headers")
                info(f"  Matched: {len(matched)}, Unmatched: {len(unmatched)}")
                self.results["warnings"] += 1
        except Exception as e:
            warn(f"Could not check journal→billing references: {e}")

    # ── SUMMARY ──────────────────────────────────────────────────────────────

    def print_summary(self):
        section("AUDIT SUMMARY")
        total = sum(self.results.values())
        p = self.results["passed"]
        f = self.results["failed"]
        w = self.results["warnings"]
        print(f"\n  {GREEN}Passed  : {p}{RESET}")
        print(f"  {YELLOW}Warnings: {w}{RESET}")
        print(f"  {RED}Failed  : {f}{RESET}")
        print(f"  {'─'*30}")
        print(f"  Total checks: {total}\n")
        if f == 0 and w == 0:
            print(f"  {GREEN}{BOLD}🎉 All checks passed — data is clean and complete!{RESET}\n")
        elif f == 0:
            print(f"  {YELLOW}{BOLD}⚠  No failures but {w} warnings — review above.{RESET}\n")
        else:
            print(f"  {RED}{BOLD}❌ {f} checks FAILED — see above for details.{RESET}\n")

    def run(self):
        print(f"\n{BOLD}OTC DATA AUDIT — Supabase Validation{RESET}")
        print("Checks raw tables, clean tables, FK linkage & business logic\n")

        self.audit_raw_tables()
        self.audit_raw_key_nulls()
        self.audit_clean_counts()
        self.audit_clean_fk_nulls()
        self.audit_linkage()
        self.audit_business_logic()
        self.print_summary()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Set SUPABASE_URL and SUPABASE_KEY in your .env file")

    auditor = OTCAuditor(url, key)
    auditor.run()