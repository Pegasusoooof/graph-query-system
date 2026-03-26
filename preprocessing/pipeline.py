import pandas as pd
import uuid
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


class OTCPreprocessor:
    def __init__(self, supabase_url: str, supabase_key: str):
        self.db = create_client(supabase_url, supabase_key)
        self.id_map = {}

    def normalize_id(self, entity_type: str, source_id: str) -> str:
        """Deterministic UUID from source ID — idempotent on re-runs."""
        key = f"{entity_type}:{source_id}"
        if key not in self.id_map:
            self.id_map[key] = str(uuid.uuid5(uuid.NAMESPACE_DNS, key))
        return self.id_map[key]

    def fetch(self, table: str) -> pd.DataFrame:
        """Fetch all rows from a raw source table into a DataFrame."""
        result = self.db.table(table).select("*").execute()
        df = pd.DataFrame(result.data)
        if df.empty:
            print(f"  Warning: {table} returned 0 rows")
            return df
        print(f"  Fetched {len(df)} rows from {table}")
        print(f"  Columns: {list(df.columns)}")
        return df.drop_duplicates()

    def safe_date(self, val):
        """Return a date string or None — safely handles nulls and JSON objects."""
        if val is None or val == "null" or val == "nan" or val == "None":
            return None
        if isinstance(val, dict):
            return None  # JSON time objects like {"hours":6,...}
        try:
            ts = pd.to_datetime(val, errors="coerce")
            if pd.isnull(ts):
                return None
            return ts.date().isoformat()  # DATE only, not timestamp
        except Exception:
            return None

    # ------------------------------------------------------------------
    # CUSTOMERS  →  business_partners
    # Columns: businessPartner, customer, businessPartnerFullName,
    #          businessPartnerName, creationDate
    # ✅ All 8 customers match the 8 unique soldToParty values in sales_orders
    # ------------------------------------------------------------------
    def process_customers(self):
        print("\nProcessing customers...")
        df = self.fetch("business_partners")
        if df.empty:
            return
        records = []
        for _, row in df.iterrows():
            cid = str(row.get("customer") or row.get("businessPartner") or "")
            if not cid or cid == "nan":
                continue
            name = (
                row.get("businessPartnerFullName")
                or row.get("businessPartnerName")
                or row.get("organizationBpName1")
                or f"{row.get('firstName', '')} {row.get('lastName', '')}".strip()
            )
            records.append({
                "id": self.normalize_id("customer", cid),
                "customer_id": cid,
                "name": name,
                "country": None,   # not in business_partners; comes from addresses
                "region": None,
                "created_at": self.safe_date(row.get("creationDate")),
            })
        if records:
            self.db.table("customers").upsert(records).execute()
            print(f"  → {len(records)} customers upserted")
        else:
            print("  → No valid customer records found")

    # ------------------------------------------------------------------
    # PRODUCTS  →  product_descriptions
    # ⚠️  FIX: Raw table in your zip is 'product_descriptions', NOT 'products'
    #          Columns: product, language, productDescription
    #          All 69 materials in sales_order_items exist here
    # ------------------------------------------------------------------
    def process_products(self):
        print("\nProcessing products...")
        df = self.fetch("product_descriptions")
        if df.empty:
            return

        # Only keep English descriptions; deduplicate by product
        if "language" in df.columns:
            df_en = df[df["language"] == "EN"].copy()
            if df_en.empty:
                df_en = df  # fallback if no EN rows
        else:
            df_en = df

        df_en = df_en.drop_duplicates(subset=["product"])

        records = []
        for _, row in df_en.iterrows():
            mid = str(row.get("product") or "")
            if not mid or mid == "nan":
                continue
            records.append({
                "id": self.normalize_id("material", mid),
                "material_id": mid,
                "description": row.get("productDescription") or row.get("description"),
                "product_group": None,   # not in product_descriptions; available in sales_order_items.materialGroup
                "unit_of_measure": None, # not in product_descriptions
            })
        if records:
            self.db.table("products_clean").upsert(records).execute()
            print(f"  → {len(records)} products upserted")

    # ------------------------------------------------------------------
    # ADDRESSES  →  business_partner_addresses
    # Columns: businessPartner, addressId, streetName, cityName,
    #          postalCode, country, region
    # ✅ No change needed
    # ------------------------------------------------------------------
    def process_addresses(self):
        print("\nProcessing addresses...")
        df = self.fetch("business_partner_addresses")
        if df.empty:
            return
        records = []
        for _, row in df.iterrows():
            aid = str(row.get("addressId") or row.get("businessPartner") or "")
            if not aid or aid == "nan":
                continue
            records.append({
                "id": self.normalize_id("address", aid),
                "address_id": aid,
                "street": row.get("streetName"),
                "city": row.get("cityName"),
                "postal_code": row.get("postalCode"),
                "country": row.get("country"),
                "plant_code": None,
            })
        if records:
            self.db.table("addresses").upsert(records).execute()
            print(f"  → {len(records)} addresses upserted")

    # ------------------------------------------------------------------
    # SALES ORDERS  →  sales_order_headers
    # Columns: salesOrder, soldToParty, creationDate,
    #          requestedDeliveryDate, overallDeliveryStatus,
    #          totalNetAmount, transactionCurrency
    # ✅ No change needed — all 8 soldToParty values match customers
    # ------------------------------------------------------------------
    def process_sales_orders(self):
        print("\nProcessing sales orders...")
        df = self.fetch("sales_order_headers")
        if df.empty:
            return
        records = []
        for _, row in df.iterrows():
            oid = str(row.get("salesOrder") or "")
            cid = str(row.get("soldToParty") or "")
            if not oid or oid == "nan":
                continue
            records.append({
                "id": self.normalize_id("order", oid),
                "order_id": oid,
                "customer_id": self.normalize_id("customer", cid) if cid and cid != "nan" else None,
                "order_date": self.safe_date(row.get("creationDate")),
                "requested_delivery_date": self.safe_date(row.get("requestedDeliveryDate")),
                "status": row.get("overallDeliveryStatus") or row.get("overallOrdReltdBillgStatus"),
                "net_value": row.get("totalNetAmount"),
                "currency": row.get("transactionCurrency") or "USD",
            })
        if records:
            self.db.table("sales_orders").upsert(records).execute()
            print(f"  → {len(records)} sales orders upserted")

    # ------------------------------------------------------------------
    # ORDER ITEMS  →  sales_order_items
    # Columns: salesOrder, salesOrderItem, material,
    #          requestedQuantity, netAmount, materialGroup
    # ⚠️  FIX: salesOrderItem values are NOT zero-padded (e.g. "10", "20")
    #          Must store item_number as INTEGER safely
    # ------------------------------------------------------------------
    def process_order_items(self):
        print("\nProcessing order items...")
        df = self.fetch("sales_order_items")
        if df.empty:
            return
        records = []
        for _, row in df.iterrows():
            oid = str(row.get("salesOrder") or "")
            item_num_raw = str(row.get("salesOrderItem") or "")
            if not oid or oid == "nan":
                continue
            # Normalize item number — strip leading zeros for consistent composite key
            item_num = str(int(item_num_raw)) if item_num_raw and item_num_raw != "nan" else ""
            iid = f"{oid}-{item_num}"
            mid = str(row.get("material") or "")
            try:
                item_num_int = int(item_num_raw) if item_num_raw and item_num_raw != "nan" else None
            except ValueError:
                item_num_int = None
            records.append({
                "id": self.normalize_id("order_item", iid),
                "item_id": iid,
                "order_id": self.normalize_id("order", oid),
                "material_id": self.normalize_id("material", mid) if mid and mid != "nan" else None,
                "quantity": row.get("requestedQuantity"),
                "unit_price": None,   # not in raw data
                "net_value": row.get("netAmount"),
                "item_number": item_num_int,
            })
        if records:
            self.db.table("sales_order_items_clean").upsert(records).execute()
            print(f"  → {len(records)} order items upserted")

    # ------------------------------------------------------------------
    # DELIVERIES  →  outbound_delivery_headers
    # Columns: deliveryDocument, actualGoodsMovementDate,
    #          overallGoodsMovementStatus, shippingPoint, creationDate
    # ⚠️  KNOWN GAP: outbound_delivery_items table is MISSING from the zip
    #               so we cannot map delivery → sales_order via items.
    #               order_id will be NULL until that table is available.
    # ------------------------------------------------------------------
    def process_deliveries(self):
        print("\nProcessing deliveries...")
        df_hdr = self.fetch("outbound_delivery_headers")
        if df_hdr.empty:
            return

        # Attempt to build delivery→order map from delivery items if available
        delivery_to_order = {}
        try:
            df_items = self.fetch("outbound_delivery_items")
            if not df_items.empty:
                for _, row in df_items.iterrows():
                    did = str(row.get("deliveryDocument") or "").strip()
                    oid = str(row.get("referenceSdDocument") or "").strip()
                    if did and oid and did != "nan" and oid != "nan":
                        delivery_to_order[did] = oid
                print(f"  delivery→order map has {len(delivery_to_order)} entries")
            else:
                print("  WARNING: outbound_delivery_items is empty — order_id will be NULL in deliveries")
        except Exception:
            print("  WARNING: outbound_delivery_items table not found — order_id will be NULL in deliveries")

        records = []
        for _, row in df_hdr.iterrows():
            did = str(row.get("deliveryDocument") or "").strip()
            if not did or did == "nan":
                continue
            oid = delivery_to_order.get(did)
            records.append({
                "id": self.normalize_id("delivery", did),
                "delivery_id": did,
                "order_id": self.normalize_id("order", oid) if oid else None,
                "customer_id": None,         # not in header; needs delivery_items
                "ship_to_address_id": None,  # not in header
                "plant_code": str(row.get("shippingPoint") or ""),
                "delivery_date": self.safe_date(row.get("creationDate")),
                "actual_goods_issue_date": self.safe_date(row.get("actualGoodsMovementDate")),
                "status": str(row.get("overallGoodsMovementStatus") or ""),
            })

        print(f"  Built {len(records)} delivery records, attempting upsert...")
        if records:
            try:
                self.db.table("deliveries").upsert(records).execute()
                print(f"  → {len(records)} deliveries upserted")
            except Exception as e:
                print(f"  UPSERT ERROR: {e}")
                success = 0
                for r in records:
                    try:
                        self.db.table("deliveries").upsert([r]).execute()
                        success += 1
                    except Exception as row_err:
                        print(f"  Bad row delivery_id={r['delivery_id']}: {row_err}")
                print(f"  → {success} deliveries upserted (row-by-row fallback)")

    # ------------------------------------------------------------------
    # DELIVERY ITEMS  →  outbound_delivery_items
    # Columns: deliveryDocument, deliveryDocumentItem, actualDeliveryQuantity,
    #          plant, referenceSdDocument, referenceSdDocumentItem,
    #          storageLocation, batch, deliveryQuantityUnit
    #
    # ⚠️  KEY FIX: referenceSdDocumentItem is ZERO-PADDED ("000010", "000020")
    #              but sales_order_items uses UN-PADDED item numbers ("10", "20").
    #              Must strip zeros: int("000010") → "10" before building UUID.
    #
    # ⚠️  NOTE: outbound_delivery_items has NO material column.
    #           material_id is left NULL here — it can be backfilled later
    #           by joining delivery_items → sales_order_items_clean via order_item_id.
    # ------------------------------------------------------------------
    def process_delivery_items(self):
        print("\nProcessing delivery items...")
        df = self.fetch("outbound_delivery_items")
        if df.empty:
            return

        records = []
        skipped = 0
        for _, row in df.iterrows():
            did = str(row.get("deliveryDocument") or "").strip()
            item_num_raw = str(row.get("deliveryDocumentItem") or "").strip()

            if not did or did == "nan":
                skipped += 1
                continue

            # Composite key for this delivery item — keep raw padding in item_id
            # so it stays unique and traceable back to SAP
            iid = f"{did}-{item_num_raw}"

            # Reference to sales order + item
            oid = str(row.get("referenceSdDocument") or "").strip()
            o_item_raw = str(row.get("referenceSdDocumentItem") or "").strip()

            # ✅ CRITICAL FIX: strip leading zeros to match how process_order_items
            # builds its composite key (salesOrderItem "10" not "000010")
            if o_item_raw and o_item_raw != "nan":
                try:
                    o_item_norm = str(int(o_item_raw))
                except ValueError:
                    o_item_norm = o_item_raw  # fallback: use as-is if not numeric
            else:
                o_item_norm = ""

            order_item_composite = (
                f"{oid}-{o_item_norm}"
                if oid and o_item_norm and oid != "nan"
                else None
            )

            # Safe quantity conversion
            qty = row.get("actualDeliveryQuantity")
            try:
                qty = float(qty) if qty is not None else None
            except (ValueError, TypeError):
                qty = None

            records.append({
                "id": self.normalize_id("delivery_item", iid),
                "item_id": iid,
                "delivery_id": self.normalize_id("delivery", did),
                # material_id is NULL — outbound_delivery_items has no material column.
                # Backfill via: JOIN delivery_items → sales_order_items_clean ON order_item_id
                "material_id": None,
                "order_item_id": (
                    self.normalize_id("order_item", order_item_composite)
                    if order_item_composite else None
                ),
                "quantity_delivered": qty,
            })

        if skipped:
            print(f"  Skipped {skipped} rows with missing deliveryDocument")

        print(f"  Built {len(records)} delivery item records, attempting upsert...")
        if records:
            try:
                self.db.table("delivery_items").upsert(records).execute()
                print(f"  → {len(records)} delivery items upserted ✅")
            except Exception as e:
                print(f"  UPSERT ERROR: {e}")
                print("  Retrying row-by-row to isolate bad records...")
                success = 0
                for r in records:
                    try:
                        self.db.table("delivery_items").upsert([r]).execute()
                        success += 1
                    except Exception as row_err:
                        print(f"    ✗ Bad row item_id={r['item_id']}: {row_err}")
                print(f"  → {success}/{len(records)} delivery items upserted (row-by-row fallback)")

    # ------------------------------------------------------------------
    # BILLING DOCUMENTS  — FIXED
    # Uses billing_document_items.referenceSdDocument to get order_id
    # ------------------------------------------------------------------
    def process_billing_documents(self):
        print("\nProcessing billing documents...")

        # Build billing→order map from billing_document_items
        billing_to_order = {}
        billing_to_material = {}
        try:
            df_items = self.fetch("billing_document_items")
            if not df_items.empty:
                for _, row in df_items.iterrows():
                    bid = str(row.get("billingDocument") or "").strip()
                    oid = str(row.get("referenceSdDocument") or "").strip()
                    mid = str(row.get("material") or "").strip()
                    if bid and oid and bid != "nan" and oid != "nan":
                        billing_to_order[bid] = oid
                    if bid and mid and mid != "nan":
                        billing_to_material[bid] = mid
                print(f"  billing→order map: {len(billing_to_order)} entries")
        except Exception as e:
            print(f"  WARNING: could not read billing_document_items: {e}")

        # Try billing_document_headers first, fallback to cancellations
        df = self.fetch("billing_document_headers")
        if df.empty:
            print("  billing_document_headers empty — falling back to billing_document_cancellations")
            df = self.fetch("billing_document_cancellations")
        if df.empty:
            return

        records = []
        for _, row in df.iterrows():
            bid = str(row.get("billingDocument") or "")
            cid = str(row.get("soldToParty") or "")
            if not bid or bid == "nan":
                continue

            oid = billing_to_order.get(bid)

            records.append({
                "id": self.normalize_id("billing", bid),
                "billing_id": bid,
                "delivery_id": None,   # no direct delivery link in source data
                "order_id": self.normalize_id("order", oid) if oid else None,
                "customer_id": self.normalize_id("customer", cid) if cid and cid != "nan" else None,
                "billing_date": self.safe_date(row.get("billingDocumentDate")),
                "net_value": row.get("totalNetAmount"),
                "currency": row.get("transactionCurrency") or "USD",
                "document_type": row.get("billingDocumentType"),
            })

        if records:
            self.db.table("billing_documents").upsert(records).execute()
            populated = sum(1 for r in records if r["order_id"])
            print(f"  → {len(records)} billing documents upserted ({populated} with order_id)")

    # ------------------------------------------------------------------
    # JOURNAL ENTRIES  →  journal_entry_items_accounts_receivable
    # Columns: accountingDocument, fiscalYear, glAccount,
    #          postingDate, amountInTransactionCurrency,
    #          transactionCurrency, accountingDocumentType,
    #          referenceDocument (→ billingDocument), accountingDocumentItem
    # ⚠️  FIX: fiscalYear comes in as string "2025" — cast to int safely
    #          64 of 123 journal entries link to billing docs ✅
    # ------------------------------------------------------------------
    def process_journal_entries(self):
        print("\nProcessing journal entries...")
        df = self.fetch("journal_entry_items_accounts_receivable")
        if df.empty:
            return
        records = []
        for _, row in df.iterrows():
            jid = str(row.get("accountingDocument") or "")
            item = str(row.get("accountingDocumentItem") or "")
            jid_composite = f"{jid}-{item}" if item and item != "nan" else jid
            bid = str(row.get("referenceDocument") or "")
            if not jid or jid == "nan":
                continue

            # Safe fiscal year cast
            fiscal_year_raw = row.get("fiscalYear")
            try:
                fiscal_year = int(fiscal_year_raw) if fiscal_year_raw else None
            except (ValueError, TypeError):
                fiscal_year = None

            # Safe amount cast
            amount_raw = row.get("amountInTransactionCurrency") or row.get("amountInCompanyCodeCurrency")
            try:
                amount = float(amount_raw) if amount_raw is not None else None
            except (ValueError, TypeError):
                amount = None

            records.append({
                "id": self.normalize_id("journal", jid_composite),
                "journal_id": jid_composite,
                "billing_id": self.normalize_id("billing", bid) if bid and bid != "nan" else None,
                "accounting_document": row.get("accountingDocument"),
                "fiscal_year": fiscal_year,
                "posting_date": self.safe_date(row.get("postingDate")),
                "amount": amount,
                "currency": row.get("transactionCurrency") or row.get("companyCodeCurrency") or "USD",
                "gl_account": row.get("glAccount"),
                "document_type": row.get("accountingDocumentType"),
            })
        if records:
            self.db.table("journal_entries").upsert(records).execute()
            print(f"  → {len(records)} journal entries upserted")

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------
    def run_all(self):
        print("=" * 60)
        print("Starting OTC pipeline v2...")
        print("=" * 60)
        steps = [
            self.process_customers,
            self.process_products,
            self.process_addresses,
            self.process_sales_orders,
            self.process_order_items,
            self.process_deliveries,
            self.process_delivery_items,
            self.process_billing_documents,
            self.process_journal_entries,
        ]
        for step in steps:
            try:
                step()
            except Exception as e:
                print(f"  ERROR in {step.__name__}: {e}")
                import traceback
                traceback.print_exc()

        print("\n" + "=" * 60)
        print("Running integrity checks...")
        issues = self.validate_referential_integrity()
        if issues:
            print("Integrity warnings:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("All integrity checks passed.")
        print("=" * 60)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def validate_referential_integrity(self) -> list:
        issues = []
        checks = [
            ("sales_orders",               "order_id",    "customer_id"),
            ("sales_order_items_clean",    "item_id",     "order_id"),
            ("sales_order_items_clean",    "item_id",     "material_id"),
            ("deliveries",                 "delivery_id", "order_id"),
            ("billing_documents",          "billing_id",  "customer_id"),
            ("journal_entries",            "journal_id",  "billing_id"),
        ]
        for table, id_col, fk_col in checks:
            try:
                result = self.db.table(table).select(f"{id_col}, {fk_col}").is_(fk_col, "null").execute()
                count = len(result.data)
                total = self.db.table(table).select(id_col, count="exact").execute()
                total_count = total.count or 0
                if count > 0:
                    issues.append(f"{table}: {count}/{total_count} rows have null {fk_col}")
                else:
                    print(f"  ✓ {table}: all {fk_col} populated")
            except Exception as e:
                issues.append(f"Could not check {table}: {e}")
        return issues


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in environment / .env file")

    preprocessor = OTCPreprocessor(url, key)
    preprocessor.run_all()