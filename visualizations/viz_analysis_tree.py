#!/usr/bin/env python3
"""Render the analysis-tree Mermaid diagram to a standalone dashboard HTML file."""

from __future__ import annotations

import argparse
import html
from pathlib import Path
from typing import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = SCRIPT_DIR / "dash_data_analysis_tree.html"
MERMAID_CANDIDATES: list[Path] = [
    SCRIPT_DIR / "figure_data_analysis_tree.mmd",
    SCRIPT_DIR / "data_analysis_tree.mdd",
    SCRIPT_DIR / "data_analysis_tree.mmd",
]


def first_existing(paths: Sequence[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def build_mermaid_diagram_html(mermaid_source_name: str, mermaid_code: str) -> str:
    escaped_mermaid = html.escape(mermaid_code)
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
    <p class="subtitle">Source: {html.escape(mermaid_source_name)}</p>
    <section class="diagram-card">
      <div id="diagram" class="mermaid">
{escaped_mermaid}
      </div>
      <div id="diagramError" class="error">
        Mermaid failed to render. Check network access for the Mermaid script or validate the .mmd/.mdd syntax.
      </div>
    </section>
  </main>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
  <script>
    (function () {{
      try {{
        mermaid.initialize({{
          startOnLoad: true,
          theme: "dark",
          securityLevel: "loose",
          flowchart: {{ htmlLabels: false }}
        }});
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


def render_analysis_tree(mermaid_path: Path, output_html_path: Path) -> None:
    mermaid_code = mermaid_path.read_text(encoding="utf-8").strip()
    if not mermaid_code:
        raise SystemExit(f"Diagram source is empty: {mermaid_path}")

    html_text = build_mermaid_diagram_html(mermaid_path.name, mermaid_code)
    output_html_path.parent.mkdir(parents=True, exist_ok=True)
    output_html_path.write_text(html_text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create dashboard HTML for the data analysis tree Mermaid diagram."
    )
    parser.add_argument(
        "--input",
        default="",
        help="Mermaid source file (.mmd/.mdd). If omitted, default candidates are checked.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output HTML path",
    )
    args = parser.parse_args()

    if args.input.strip():
        mermaid_path = Path(args.input).expanduser().resolve()
        if not mermaid_path.exists():
            raise SystemExit(f"Diagram Mermaid file not found: {mermaid_path}")
    else:
        mermaid_path = first_existing(MERMAID_CANDIDATES)
        if mermaid_path is None:
            raise SystemExit(
                "No Mermaid source found. Tried: "
                + ", ".join(str(path) for path in MERMAID_CANDIDATES)
            )

    output_html_path = Path(args.output).expanduser().resolve()
    render_analysis_tree(mermaid_path, output_html_path)
    print(f"Wrote data analysis tree page: {output_html_path}")


if __name__ == "__main__":
    main()
