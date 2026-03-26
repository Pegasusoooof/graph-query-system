# Order-to-Cash Graph Intelligence System

A full-stack application that converts an Order-to-Cash (O2C) business dataset into an interactive knowledge graph with a conversational AI query interface.

---

## Demo

[Watch the demo on Google Drive](https://drive.google.com/file/d/1eBFRuezYb7fDh_Fjha-fPQxp4Psx8Ei9/view?usp=sharing)

---

## Test Queries

Use these to verify the application end-to-end after setup.

### Entity Lookups
- `how many customers are in the dataset?`
- `list all products`
- `show me all sales orders`

### Order Flow / Tracing
- `trace the full order flow for the first sales order you can find`
- `show me the end-to-end chain for a delivered and billed order`
- `trace the flow of order SO-740518`

### Delivery Queries
- `how many deliveries have been completed?`
- `what is the delivery status breakdown across all orders?`

---

## Overview

The system ingests structured O2C data, builds a directed graph of business entities and their relationships, visualizes it in an interactive UI, and lets users query the data in natural language. The AI translates queries into structured graph operations and returns data-backed answers — it does not hallucinate or answer from general knowledge.

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Database | Supabase (PostgreSQL) | Managed Postgres with instant REST API, row-level security, and easy local dev via Docker |
| Graph engine | NetworkX (Python) | Native directed graph support, rich traversal APIs, loads entirely in-memory for sub-ms query latency |
| Backend API | FastAPI (Python) | Async-first, automatic OpenAPI docs, streaming response support via StreamingResponse |
| LLM | Groq — llama-3.3-70b-versatile | 128k context window (handles large graph dumps), strongest structured reasoning on Groq free tier |
| Frontend framework | Next.js 14 + React | App router, server components, zero-config deployment |
| Graph visualization | react-force-graph-2d | Force-directed layout, canvas rendering for performance at 300+ nodes, custom node draw callbacks |

---

## Architecture

```
Supabase (PostgreSQL)
        |
        v
  graph/cache.py          <- loads data at startup, builds NetworkX DiGraph
        |
        v
  graph/analyzer.py       <- typed queries: trace_order, detect_broken_flows,
        |                    top_products_by_order_value, get_customer_orders
        v
  services/
    context_builder.py    <- intent router: detects query type, fetches only
    |                        relevant graph data, builds LLM context string
    guardrails.py         <- 3-stage filter before any LLM call
        |
        v  SSE stream
  Next.js frontend
    ChatPanel.jsx         <- streaming chat UI, markdown + table rendering
    GraphCanvas.jsx       <- force-directed graph, node highlighting
    page.jsx              <- layout, state, resizable panel
```

---

## Graph Model

The O2C process is modelled as a directed acyclic graph (DAG) with 7 entity types:

```
Customer --> SalesOrder --> OrderItem --> Product
                 |
                 v
             Delivery --> BillingDocument --> JournalEntry
                 |
                 v
             Address
```

### Node types

| Entity | Key attributes |
|---|---|
| Customer | customer_id, name, region, country |
| SalesOrder | order_id, status, net_value, order_date |
| OrderItem | item_id, material_id, quantity, net_value |
| Product | material_id, description |
| Delivery | delivery_id, status, delivery_date |
| BillingDocument | billing_id, billing_date, net_value |
| JournalEntry | journal_id, gl_account, amount, fiscal_year |
| Address | address_id, street, city, country |

### Edge semantics

Edges are directed and typed, following the real business flow:

- Customer -> SalesOrder: customer places an order
- SalesOrder -> OrderItem: order contains line items
- OrderItem -> Product: item references a material
- SalesOrder -> Delivery: order triggers a delivery
- Delivery -> BillingDocument: delivery triggers billing
- BillingDocument -> JournalEntry: billing creates accounting entries
- SalesOrder -> Address: ship-to address

---

## Database & Storage Decisions

### Why Supabase + PostgreSQL

The source data is relational — orders, items, deliveries — so a relational database is the natural fit for persistent storage. Supabase provides managed PostgreSQL with no infrastructure overhead, an instant REST API for data ingestion scripts, and a local Docker dev environment that mirrors production.

The raw tables stay in Postgres. The graph is derived from them at startup.

### Why NetworkX in-memory

Graph traversals (finding all billing documents reachable from a product, detecting missing links in an O2C chain) are path-finding operations that relational SQL handles poorly. NetworkX gives O(1) node lookup by ID and native predecessor/successor traversal.

**Tradeoff:** The entire graph lives in RAM. At the current dataset size this is fine. At millions of nodes, a dedicated graph database (Neo4j, Amazon Neptune) would be the right call — but adds operational complexity not warranted here.

### Why not a graph database

Neo4j would give Cypher query language and persistent graph storage, but requires a separate service, a different query model, and more complex deployment. For this dataset size, NetworkX loaded from Postgres at startup gives equivalent query performance with much simpler ops.

---

## LLM Integration & Prompting Strategy

### Flow

1. User sends a message
2. Guardrails check runs — rejects unrelated queries before any LLM call
3. Intent router (context_builder.py) detects query type and fetches only relevant graph data
4. System prompt is assembled with the graph context injected
5. Groq streams tokens back via SSE
6. Frontend renders streamed markdown and tables in real time
7. highlight_nodes tags in the response trigger graph node highlighting

### Why streaming

Responses over graph data can be long. Streaming via StreamingResponse + Server-Sent Events means the user sees output immediately rather than waiting for the full response.

### Context management

The context builder uses intent detection to send only relevant data to the LLM — not a full graph dump. This keeps prompts focused and avoids hitting context limits:

- Product billing query: only billing count data + top products
- Trace query: only the specific order's O2C chain
- Broken flows query: only the broken flow analysis
- Delivery status: only delivery nodes with status breakdown

### System prompt design

The system prompt enforces strict grounding rules — only answer from the provided context, never invent or estimate data, always include entity IDs in responses, and end every response with highlight_nodes tags.

---

## Guardrails

Three-stage filter applied before every LLM call:

**Stage 1 — Hard blocklist:** Immediately rejects known off-topic domains (weather, recipes, movies, sports, crypto, general knowledge).

**Stage 2 — Domain allowlist:** Fast-passes any message containing O2C domain stems. Using stems means "deliver" matches delivery, deliveries, and delivered — a single stem covers all word forms.

**Stage 3 — Default pass:** Ambiguous short queries reach the LLM. The system prompt instructs the model to refuse off-topic questions, so blocking at the guardrail layer for ambiguous cases causes false negatives on valid analytical queries.

---

## Running Locally

### Prerequisites

- Python 3.11+
- Node.js 18+
- Supabase project (or local Supabase via Docker)
- Groq API key (free at console.groq.com)

### Backend

```bash
pip install -r requirements.txt
cp .env.example .env
# fill in SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY
uvicorn backend.main:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# open http://localhost:3000
```

---

## Example Queries

- "Which products have the most billing documents?"
- "Trace the full flow for order 740518"
- "Which orders have broken or incomplete flows?"
- "How many deliveries are done vs pending?"
- "Show me all customers and their order counts"
- "Which billing documents have no delivery attached?"

Off-topic queries such as "What is the capital of France?" are rejected with: "This system is designed to answer questions related to the provided dataset only."

---

## Project Structure

```
backend/
  main.py                    - FastAPI app entry point, startup graph loader
  config.py                  - Groq client init, graph globals, env vars
  routers/
    graph.py                 - /graph/ endpoints — serves nodes and edges to frontend
    query.py                 - /query/stream endpoint — handles SSE streaming responses
  services/
    context_builder.py       - Intent router + context assembly for LLM prompts
    guardrails.py            - 3-stage pre-LLM query filter

graph/
  analyzer.py                - Graph query methods: trace_order, detect_broken_flows,
                               top_products_by_order_value, get_customer_orders
  cache.py                   - Graph loader — pulls data from Supabase at startup,
                               builds NetworkX DiGraph in memory
  builder.py                 - Graph construction — maps raw DB rows to typed nodes and edges
  serializer.py              - Serializes NetworkX graph to JSON for frontend consumption

preprocessing/
  pipeline.py                - Cleans and transforms raw source data into relational tables
  audit_tables.py            - Validates table integrity before ingestion

scripts/
  ingest_data.py             - Loads cleaned data from raw-data/ into Supabase tables
  build_graph.py             - One-time script to verify graph builds correctly from DB

raw-data/                    - Source CSV/JSON files before preprocessing

frontend/
  src/app/
    page.jsx                 - Main layout, global state, resizable panel orchestration
    layout.jsx               - Root Next.js layout, font and metadata config
    globals.css              - Global Tailwind base styles
    components/
      ChatPanel.jsx          - Streaming chat UI, markdown + table rendering
      GraphCanvas.jsx        - Force-directed graph canvas, node highlighting
      Legend.jsx             - Entity type colour legend
      NodeInspector.jsx      - Side panel showing selected node's attributes
  next.config.js             - Next.js config (API proxy to FastAPI backend)
  tailwind.config.js         - Tailwind theme and content paths
  postcss.config.js          - PostCSS config for Tailwind
  package.json               - Frontend dependencies and scripts
  package-lock.json          - Lockfile for reproducible installs

.env                         - SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY
.gitignore                   - Ignores node_modules, __pycache__, .env, .next
README.md                    - Project documentation
requirements.txt             - Python dependencies
```