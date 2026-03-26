# backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import backend.config as cfg
from backend.routers import graph, query
from backend.logger import log


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting OTC Graph Query API...", extra={"stage": "STARTUP"})
    log.info("Loading graph cache from disk...", extra={"stage": "STARTUP"})
    cfg.load_all()
    log.info(
        f"Graph ready: {cfg.G.number_of_nodes()} nodes, {cfg.G.number_of_edges()} edges",
        extra={"stage": "STARTUP"},
    )
    log.info(
        f"LLM model: {cfg.GROQ_MODEL}",
        extra={"stage": "STARTUP"},
    )
    log.info("API is ready to serve requests.", extra={"stage": "STARTUP"})
    yield
    log.info("Shutting down.", extra={"stage": "STARTUP"})


app = FastAPI(
    title="OTC Graph Query API",
    description="Order-to-Cash process graph with natural language querying",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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