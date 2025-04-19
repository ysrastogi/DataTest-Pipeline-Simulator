from typing import Dict, List, Any, Optional
import json
import os
from src.pipeline.core import Pipeline

class FlowDiagramGenerator:
    """Generate flow diagrams for pipelines in various formats."""
    
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
    
    def generate_mermaid(self) -> str:
        """Generate a Mermaid.js flowchart representation of the pipeline."""
        lines = ["graph TD"]
        
        # Add nodes for each stage
        for idx, stage in enumerate(self.pipeline.stages):
            stage_id = f"S{idx}"
            class_name = stage.__class__.__name__
            label = f"{stage.name}<br/>({class_name})"
            lines.append(f"    {stage_id}[\"{label}\"]")
        
        # Add edges connecting sequential stages
        for idx in range(len(self.pipeline.stages) - 1):
            lines.append(f"    S{idx} --> S{idx+1}")
        
        # Add title
        lines.append(f"    title[\"Pipeline Flow: {self.pipeline.name}\"]")
        lines.append("    style title fill:#fff,stroke:none")
        
        return "\n".join(lines)
    
    def generate_plantuml(self) -> str:
        """Generate a PlantUML activity diagram representation of the pipeline."""
        lines = ["@startuml", f"title Pipeline Flow: {self.pipeline.name}", ""]
        
        # Start
        lines.append("start")
        
        # Add activities for each stage
        for stage in self.pipeline.stages:
            class_name = stage.__class__.__name__
            lines.append(f":{stage.name} ({class_name});")
        
        # End
        lines.append("stop")
        lines.append("@enduml")
        
        return "\n".join(lines)
    
    def save_diagram(self, output_format: str = "mermaid", output_path: Optional[str] = None) -> str:
        """Generate and save a diagram in the specified format."""
        if output_format.lower() == "mermaid":
            diagram = self.generate_mermaid()
        elif output_format.lower() == "plantuml":
            diagram = self.generate_plantuml()
        else:
            raise ValueError(f"Unsupported diagram format: {output_format}")
        
        # Save the diagram if an output path is provided
        if output_path:
            directory = os.path.dirname(output_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
            
            with open(output_path, 'w') as f:
                f.write(diagram)
        
        return diagram
    
    def generate_html_report(self, output_path: Optional[str] = None) -> str:
        """Generate an HTML report with embedded Mermaid diagram."""
        mermaid_diagram = self.generate_mermaid()
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Pipeline Flow: {self.pipeline.name}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({{ startOnLoad: true }});
    </script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        .mermaid {{ margin: 20px 0; }}
        .info {{ margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>Pipeline Flow: {self.pipeline.name}</h1>
    <div class="info">
        <p><strong>Pipeline ID:</strong> {self.pipeline.id}</p>
        <p><strong>Number of Stages:</strong> {len(self.pipeline.stages)}</p>
        <p><strong>Description:</strong> {self.pipeline.description or "No description available"}</p>
    </div>
    <div class="mermaid">
{mermaid_diagram}
    </div>
</body>
</html>
"""
        
        # Save the HTML if an output path is provided
        if output_path:
            directory = os.path.dirname(output_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
            
            with open(output_path, 'w') as f:
                f.write(html)
        
        return html