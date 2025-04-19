from typing import Dict, List, Any, Optional
import networkx as nx
import matplotlib.pyplot as plt
import os
from src.pipeline.core import Pipeline, Stage

class PipelineGraph:
    """Generate graph visualizations of pipeline structure."""
    
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.graph = nx.DiGraph()
        self._build_graph()
    
    def _build_graph(self):
        """Build a directed graph from the pipeline stages."""
        # Add nodes for each stage
        for idx, stage in enumerate(self.pipeline.stages):
            self.graph.add_node(stage.id, 
                               label=stage.name,
                               type=stage.__class__.__name__,
                               description=stage.description,
                               index=idx)
        
        # Add edges connecting sequential stages
        for idx in range(len(self.pipeline.stages) - 1):
            current_stage = self.pipeline.stages[idx]
            next_stage = self.pipeline.stages[idx + 1]
            self.graph.add_edge(current_stage.id, next_stage.id)
    
    def visualize(self, output_path: Optional[str] = None, show: bool = True):
        """Visualize the pipeline as a directed graph."""
        plt.figure(figsize=(12, 8))
        
        # Get node positions using a layout algorithm
        pos = nx.spring_layout(self.graph)
        
        # Draw nodes
        node_labels = {node: data['label'] for node, data in self.graph.nodes(data=True)}
        node_colors = ['skyblue' for _ in self.graph.nodes()]
        
        nx.draw_networkx_nodes(self.graph, pos, node_size=2000, node_color=node_colors, alpha=0.8)
        nx.draw_networkx_labels(self.graph, pos, labels=node_labels, font_size=10)
        
        # Draw edges
        nx.draw_networkx_edges(self.graph, pos, arrows=True, arrowsize=20, width=2, alpha=0.7)
        
        # Add title and adjust layout
        plt.title(f"Pipeline: {self.pipeline.name}", fontsize=15)
        plt.axis('off')
        plt.tight_layout()
        
        # Save if an output path is provided
        if output_path:
            directory = os.path.dirname(output_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        # Show the plot if requested
        if show:
            plt.show()
        else:
            plt.close()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the graph to a dictionary representation for web visualization."""
        nodes = []
        for node, data in self.graph.nodes(data=True):
            nodes.append({
                "id": node,
                "label": data.get("label", ""),
                "type": data.get("type", ""),
                "description": data.get("description", ""),
                "index": data.get("index", 0)
            })
        
        edges = []
        for source, target in self.graph.edges():
            edges.append({
                "source": source,
                "target": target
            })
        
        return {
            "nodes": nodes,
            "edges": edges,
            "pipeline_name": self.pipeline.name,
            "pipeline_id": self.pipeline.id
        }