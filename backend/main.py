# backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import backend.config as cfg
from backend.routers import graph, query


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load graph once at startup
    print("Loading graph cache...")
    cfg.load_all()
    yield
    # Shutdown: nothing to clean up


app = FastAPI(
    title="OTC Graph Query API",
    description="Order-to-Cash process graph with natural language querying",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow Next.js frontend (adjust origin for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000","https://graph-query-systemm.onrender.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(graph.router)
app.include_router(query.router)


@app.get("/")
async def root():
    return {
        "status": "ok",
        "graph_nodes": cfg.G.number_of_nodes() if cfg.G else 0,
        "graph_edges": cfg.G.number_of_edges() if cfg.G else 0,
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}