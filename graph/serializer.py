# graph/serializer.py
import json
import networkx as nx


class OTCGraphSerializer:
    """
    Converts the NetworkX graph to the JSON format
    expected by react-force-graph on the frontend.
    """

    def __init__(self, G: nx.DiGraph):
        self.G = G

    def to_frontend_json(self) -> dict:
        """
        Returns {"nodes": [...], "links": [...]}
        Each node gets all its attributes.
        Each link gets source, target, relationship, edge_type.
        """
        nodes = []
        for node_id, attrs in self.G.nodes(data=True):
            nodes.append({"id": node_id, **self._clean(attrs)})

        links = []
        for src, tgt, attrs in self.G.edges(data=True):
            links.append({
                "source": src,
                "target": tgt,
                **self._clean(attrs),
            })

        return {"nodes": nodes, "links": links}

    def _clean(self, attrs: dict) -> dict:
        """Ensure all values are JSON-serializable."""
        clean = {}
        for k, v in attrs.items():
            if v is None:
                clean[k] = None
            elif isinstance(v, float) and (v != v):  # NaN check
                clean[k] = None
            else:
                try:
                    json.dumps(v)
                    clean[k] = v
                except (TypeError, ValueError):
                    clean[k] = str(v)
        return clean

    def to_json_string(self) -> str:
        return json.dumps(self.to_frontend_json(), default=str)