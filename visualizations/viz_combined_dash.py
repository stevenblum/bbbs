#!/usr/bin/env python3
"""Bundle dashboard HTML files into a single standalone HTML viewer."""

from __future__ import annotations

import argparse
import base64
import html
import json
import re
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = "dash_all_in_one.html"
DEFAULT_PATTERNS = ["dash_*.html"]
DEFAULT_TITLE = "Dashboard Bundle"
PREFERRED_ORDER = ["dash_route.html", "dash_location.html", "dash_city.html"]


def natural_sort_key(value: str) -> List[object]:
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", value)]


def dashboard_label(filename: str) -> str:
    base = filename[:-5] if filename.endswith(".html") else filename
    if base.startswith("dash_"):
        base = base[5:]
    base = base.replace("-", " ")
    return " ".join(part.capitalize() for part in base.split("_"))


def parse_page_spec(spec: str) -> Tuple[str, str | None]:
    if "=" in spec:
        page, label = spec.split("=", 1)
        page = page.strip()
        label = label.strip()
        return page, label or None
    return spec.strip(), None


def resolve_page_path(directory: Path, page: str) -> Path:
    path = Path(page).expanduser()
    if not path.is_absolute():
        path = directory / path
    return path.resolve()


def discover_pages(directory: Path, patterns: Sequence[str], excluded_names: set[str]) -> List[Path]:
    discovered: Dict[str, Path] = {}
    for pattern in patterns:
        for path in directory.glob(pattern):
            if not path.is_file():
                continue
            if path.name in excluded_names:
                continue
            discovered[path.name] = path.resolve()

    preferred = []
    for filename in PREFERRED_ORDER:
        if filename in discovered:
            preferred.append(discovered.pop(filename))

    remaining = sorted(discovered.values(), key=lambda p: natural_sort_key(p.name))
    return preferred + remaining


def build_viewer_html(title: str, pages: List[dict]) -> str:
    if not pages:
        raise ValueError("No dashboard pages were provided.")

    buttons_html = "\n".join(
        (
            f'          <button class="nav-btn" data-page="{html.escape(page["file"], quote=True)}">'
            f'{html.escape(page["label"])}</button>'
        )
        for page in pages
    )
    pages_json = json.dumps(pages, separators=(",", ":"), ensure_ascii=False)
    default_page = pages[0]["file"]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #0f1720;
      --panel: #17222e;
      --panel-2: #0f1a26;
      --text: #ebf1f8;
      --muted: #9eb2c7;
      --border: #2a3a4a;
      --accent: #68d0f8;
    }}
    html, body {{
      margin: 0;
      width: 100%;
      height: 100%;
      background: var(--bg);
      color: var(--text);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    }}
    .app {{
      width: 100%;
      height: 100%;
      display: grid;
      grid-template-columns: minmax(220px, 300px) 1fr;
      grid-template-rows: auto 1fr;
      grid-template-areas:
        "sidebar topbar"
        "sidebar content";
    }}
    .sidebar {{
      grid-area: sidebar;
      border-right: 1px solid var(--border);
      background: linear-gradient(180deg, var(--panel), var(--panel-2));
      overflow: auto;
    }}
    .sidebar h1 {{
      margin: 0;
      padding: 18px 16px 10px;
      font-size: 16px;
      font-weight: 600;
      color: var(--muted);
    }}
    .nav-list {{
      display: flex;
      flex-direction: column;
      gap: 8px;
      padding: 0 12px 16px;
    }}
    .nav-btn {{
      width: 100%;
      text-align: left;
      font: inherit;
      color: var(--text);
      background: #16212e;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 9px 10px;
      cursor: pointer;
    }}
    .nav-btn:hover {{
      border-color: #4e647b;
    }}
    .nav-btn.active {{
      border-color: var(--accent);
      background: #133447;
      color: #e7f8ff;
    }}
    .topbar {{
      grid-area: topbar;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 14px;
      background: #0d1722;
      min-height: 44px;
      box-sizing: border-box;
    }}
    .status {{
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .content {{
      grid-area: content;
      min-height: 0;
    }}
    iframe {{
      width: 100%;
      height: 100%;
      border: 0;
      background: #0b1118;
    }}
    @media (max-width: 900px) {{
      .app {{
        grid-template-columns: 1fr;
        grid-template-rows: auto auto 1fr;
        grid-template-areas:
          "topbar"
          "sidebar"
          "content";
      }}
      .sidebar {{
        border-right: 0;
        border-bottom: 1px solid var(--border);
      }}
      .nav-list {{
        flex-direction: row;
        flex-wrap: wrap;
        padding-bottom: 12px;
      }}
      .nav-btn {{
        width: auto;
      }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <aside class="sidebar">
      <h1>{html.escape(title)}</h1>
      <div class="nav-list">
{buttons_html}
      </div>
    </aside>
    <header class="topbar">
      <div class="status" id="statusText"></div>
      <div class="status">{len(pages)} dashboards embedded</div>
    </header>
    <main class="content">
      <iframe id="dashboardFrame" title="Dashboard bundle"></iframe>
    </main>
  </div>

  <script>
    const pages = {pages_json};
    const fileToIndex = new Map(pages.map((page, index) => [page.file, index]));
    const frame = document.getElementById("dashboardFrame");
    const statusText = document.getElementById("statusText");
    const buttons = Array.from(document.querySelectorAll(".nav-btn"));

    function b64ToUtf8(base64Text) {{
      const binary = atob(base64Text);
      const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
      return new TextDecoder("utf-8").decode(bytes);
    }}

    function setActive(file) {{
      for (const button of buttons) {{
        button.classList.toggle("active", button.dataset.page === file);
      }}
    }}

    function loadPage(file) {{
      if (!fileToIndex.has(file)) {{
        return;
      }}
      const page = pages[fileToIndex.get(file)];
      if (!page.html) {{
        page.html = b64ToUtf8(page.b64);
        page.b64 = "";
      }}
      frame.srcdoc = page.html;
      setActive(page.file);
      const sizeMb = (page.bytes / (1024 * 1024)).toFixed(2);
      statusText.textContent = `${{page.label}} • ${{sizeMb}} MB`;
      window.location.hash = encodeURIComponent(page.file);
    }}

    for (const button of buttons) {{
      button.addEventListener("click", () => {{
        loadPage(button.dataset.page);
      }});
    }}

    const hashTarget = decodeURIComponent(window.location.hash.replace(/^#/, ""));
    if (hashTarget && fileToIndex.has(hashTarget)) {{
      loadPage(hashTarget);
    }} else {{
      loadPage({json.dumps(default_page)});
    }}
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Combine dashboard HTML files into one standalone downloadable HTML."
    )
    parser.add_argument(
        "--directory",
        default=str(SCRIPT_DIR),
        help="Directory containing dashboard HTML files (default: visualizations dir)",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output filename or absolute path for the combined HTML",
    )
    parser.add_argument(
        "--pattern",
        action="append",
        default=[],
        help="Glob pattern to discover dashboards (repeatable, default: dash_*.html)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="HTML filename to exclude (repeatable)",
    )
    parser.add_argument(
        "--page",
        action="append",
        default=[],
        help="Explicit dashboard spec: file.html or file.html=Label (repeatable, order preserved)",
    )
    parser.add_argument("--title", default=DEFAULT_TITLE, help="Viewer title")
    args = parser.parse_args()

    directory = Path(args.directory).expanduser().resolve()
    if not directory.exists() or not directory.is_dir():
        raise SystemExit(f"Dashboard directory not found: {directory}")

    output = Path(args.output).expanduser()
    if not output.is_absolute():
        output = directory / output
    output = output.resolve()

    excluded_names = {"dash_header.html", output.name, *args.exclude}
    pages_with_labels: List[Tuple[Path, str]] = []

    if args.page:
        for spec in args.page:
            page_spec, label = parse_page_spec(spec)
            if not page_spec:
                continue
            path = resolve_page_path(directory, page_spec)
            if not path.exists() or not path.is_file():
                raise SystemExit(f"Dashboard file not found: {path}")
            if path.name in excluded_names:
                continue
            pages_with_labels.append((path, label or dashboard_label(path.name)))
    else:
        patterns = args.pattern or DEFAULT_PATTERNS
        for path in discover_pages(directory, patterns, excluded_names):
            pages_with_labels.append((path, dashboard_label(path.name)))

    unique_pages: Dict[str, Tuple[Path, str]] = {}
    for path, label in pages_with_labels:
        unique_pages[path.name] = (path, label)
    pages_with_labels = list(unique_pages.values())

    if not pages_with_labels:
        raise SystemExit("No dashboard HTML files found to combine.")

    payload = []
    total_bytes = 0
    for path, label in pages_with_labels:
        raw = path.read_bytes()
        total_bytes += len(raw)
        payload.append(
            {
                "file": path.name,
                "label": label,
                "bytes": len(raw),
                "b64": base64.b64encode(raw).decode("ascii"),
            }
        )

    viewer_html = build_viewer_html(args.title, payload)
    output.write_text(viewer_html, encoding="utf-8")

    bundle_mb = output.stat().st_size / (1024 * 1024)
    source_mb = total_bytes / (1024 * 1024)
    print(
        f"Wrote {output} with {len(payload)} dashboards "
        f"(source {source_mb:.2f} MB, bundle {bundle_mb:.2f} MB)"
    )


if __name__ == "__main__":
    main()
