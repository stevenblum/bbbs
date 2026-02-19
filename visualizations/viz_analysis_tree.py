#!/usr/bin/env python3
"""Render analysis and data-file Mermaid diagrams to a tabbed dashboard HTML file."""

from __future__ import annotations

import argparse
import html
from pathlib import Path
from typing import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = SCRIPT_DIR / "dash_data_analysis_tree.html"
ANALYSIS_MERMAID_CANDIDATES: list[Path] = [
    SCRIPT_DIR / "figure_data_analysis_tree.mmd",
    SCRIPT_DIR / "data_analysis_tree.mdd",
    SCRIPT_DIR / "data_analysis_tree.mmd",
]
DATA_TREE_MERMAID_CANDIDATES: list[Path] = [
    SCRIPT_DIR / "figure_data_file_tree.mmd",
]


def first_existing(paths: Sequence[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def build_mermaid_diagram_html(
    analysis_source_name: str,
    analysis_code: str,
    data_tree_source_name: str,
    data_tree_code: str,
) -> str:
    escaped_analysis_mermaid = html.escape(analysis_code)
    escaped_data_tree_mermaid = html.escape(data_tree_code)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Data Analysis Tree</title>
  <style>
    :root {{
      --bg: #0e1117;
      --panel: #171c24;
      --border: #2a3240;
      --text: #e6edf3;
      --muted: #a4b1c4;
      --accent: #5ad2f4;
    }}
    html, body {{
      margin: 0;
      min-height: 100%;
      background: var(--bg);
      color: var(--text);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    }}
    .page {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px 18px 36px;
    }}
    .title {{
      margin: 0 0 6px;
      font-size: 24px;
      font-weight: 600;
    }}
    .subtitle {{
      margin: 0 0 16px;
      color: var(--muted);
      font-size: 13px;
    }}
    .tabs {{
      display: inline-flex;
      gap: 8px;
      margin: 0 0 14px;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 6px;
      background: #131821;
    }}
    .tab-btn {{
      border: 1px solid transparent;
      background: transparent;
      color: var(--muted);
      font-size: 13px;
      font-weight: 600;
      padding: 6px 12px;
      border-radius: 8px;
      cursor: pointer;
    }}
    .tab-btn.active {{
      color: #08111b;
      background: var(--accent);
      border-color: #7bdff8;
    }}
    .tab-btn:focus-visible {{
      outline: 2px solid var(--accent);
      outline-offset: 1px;
    }}
    .tab-panel {{
      display: none;
    }}
    .tab-panel.active {{
      display: block;
    }}
    .tab-source {{
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 13px;
    }}
    .diagram-card {{
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
      background: var(--panel);
      overflow: auto;
    }}
    .mermaid {{
      min-width: 720px;
    }}
    .error {{
      display: none;
      margin-top: 12px;
      border: 1px solid #7f1d1d;
      border-radius: 8px;
      padding: 10px;
      background: #2a1010;
      color: #ffd6d6;
      font-size: 13px;
    }}
    .error.visible {{
      display: block;
    }}
  </style>
</head>
<body>
  <main class="page">
    <h1 class="title">Data Analysis Tree</h1>
    <p class="subtitle">Mermaid diagrams for workflow and file relationships</p>
    <nav class="tabs" aria-label="Diagram tabs">
      <button id="tab-analysis" class="tab-btn active" type="button" data-target="panel-analysis">Analysis Tree</button>
      <button id="tab-data" class="tab-btn" type="button" data-target="panel-data">Data Tree</button>
    </nav>
    <section class="diagram-card">
      <div id="panel-analysis" class="tab-panel active" role="region" aria-labelledby="tab-analysis">
        <p class="tab-source">Source: {html.escape(analysis_source_name)}</p>
        <div id="diagram-analysis" class="mermaid">
{escaped_analysis_mermaid}
        </div>
      </div>
      <div id="panel-data" class="tab-panel" role="region" aria-labelledby="tab-data">
        <p class="tab-source">Source: {html.escape(data_tree_source_name)}</p>
        <div id="diagram-data" class="mermaid">
{escaped_data_tree_mermaid}
        </div>
      </div>
      <div id="diagramError" class="error">
        Mermaid failed to render. Check network access for the Mermaid script or validate the .mmd/.mdd syntax.
      </div>
    </section>
  </main>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
  <script>
    (function () {{
      const tabs = Array.from(document.querySelectorAll('.tab-btn'));
      const panels = Array.from(document.querySelectorAll('.tab-panel'));

      function setActive(targetId) {{
        tabs.forEach((btn) => {{
          const active = btn.dataset.target === targetId;
          btn.classList.toggle('active', active);
          btn.setAttribute('aria-selected', active ? 'true' : 'false');
        }});
        panels.forEach((panel) => {{
          panel.classList.toggle('active', panel.id === targetId);
        }});
      }}

      tabs.forEach((btn) => {{
        btn.addEventListener('click', () => setActive(btn.dataset.target));
      }});

      try {{
        mermaid.initialize({{
          startOnLoad: false,
          theme: "dark",
          securityLevel: "loose",
          flowchart: {{ htmlLabels: false }}
        }});
        mermaid.run({{ querySelector: '.mermaid' }});
      }} catch (err) {{
        const el = document.getElementById("diagramError");
        if (el) {{
          el.classList.add("visible");
          el.textContent = "Mermaid failed to render: " + String(err);
        }}
      }}
    }})();
  </script>
</body>
</html>
"""


def render_analysis_tree(analysis_mermaid_path: Path, data_tree_mermaid_path: Path, output_html_path: Path) -> None:
    analysis_mermaid_code = analysis_mermaid_path.read_text(encoding="utf-8").strip()
    if not analysis_mermaid_code:
        raise SystemExit(f"Diagram source is empty: {analysis_mermaid_path}")

    data_tree_mermaid_code = data_tree_mermaid_path.read_text(encoding="utf-8").strip()
    if not data_tree_mermaid_code:
        raise SystemExit(f"Diagram source is empty: {data_tree_mermaid_path}")

    html_text = build_mermaid_diagram_html(
        analysis_mermaid_path.name,
        analysis_mermaid_code,
        data_tree_mermaid_path.name,
        data_tree_mermaid_code,
    )
    output_html_path.parent.mkdir(parents=True, exist_ok=True)
    output_html_path.write_text(html_text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create dashboard HTML for analysis-tree and data-tree Mermaid diagrams."
    )
    parser.add_argument(
        "--input",
        default="",
        help="Analysis-tree Mermaid source file (.mmd/.mdd). If omitted, default candidates are checked.",
    )
    parser.add_argument(
        "--data-tree-input",
        default="",
        help="Data-tree Mermaid source file (.mmd/.mdd). If omitted, default candidates are checked.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output HTML path",
    )
    args = parser.parse_args()

    if args.input.strip():
        analysis_mermaid_path = Path(args.input).expanduser().resolve()
        if not analysis_mermaid_path.exists():
            raise SystemExit(f"Diagram Mermaid file not found: {analysis_mermaid_path}")
    else:
        analysis_mermaid_path = first_existing(ANALYSIS_MERMAID_CANDIDATES)
        if analysis_mermaid_path is None:
            raise SystemExit(
                "No Mermaid source found. Tried: "
                + ", ".join(str(path) for path in ANALYSIS_MERMAID_CANDIDATES)
            )

    if args.data_tree_input.strip():
        data_tree_mermaid_path = Path(args.data_tree_input).expanduser().resolve()
        if not data_tree_mermaid_path.exists():
            raise SystemExit(f"Data-tree Mermaid file not found: {data_tree_mermaid_path}")
    else:
        data_tree_mermaid_path = first_existing(DATA_TREE_MERMAID_CANDIDATES)
        if data_tree_mermaid_path is None:
            raise SystemExit(
                "No data-tree Mermaid source found. Tried: "
                + ", ".join(str(path) for path in DATA_TREE_MERMAID_CANDIDATES)
            )

    output_html_path = Path(args.output).expanduser().resolve()
    render_analysis_tree(analysis_mermaid_path, data_tree_mermaid_path, output_html_path)
    print(f"Wrote analysis/data tree page: {output_html_path}")


if __name__ == "__main__":
    main()
