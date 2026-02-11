#!/usr/bin/env python3
"""Generate a data analysis hub page with a top navigation header and iframe content area."""

import argparse
import os
from typing import Dict, List, Tuple

DEFAULT_OUTPUT = "dash_header.html"
PREFERRED_PAGES = ["dash_route.html", "dash_location.html", "dash_city.html"]


def discover_pages(output_html: str) -> List[str]:
    pages: List[str] = []
    excluded = {DEFAULT_OUTPUT, os.path.basename(output_html)}

    for page in PREFERRED_PAGES:
        if page in excluded:
            continue
        if os.path.isfile(page):
            pages.append(page)

    for name in sorted(os.listdir(".")):
        if not name.endswith(".html"):
            continue
        if not name.startswith("dash_"):
            continue
        if name in excluded:
            continue
        if name not in pages:
            pages.append(name)

    return pages


def page_label(filename: str) -> str:
    base = filename[:-5] if filename.endswith(".html") else filename
    if base.startswith("dash_"):
        base = base[5:]
    return " ".join(part.capitalize() for part in base.split("_"))


def parse_page_specs(specs: List[str]) -> Tuple[List[str], Dict[str, str]]:
    pages: List[str] = []
    labels: Dict[str, str] = {}

    for spec in specs:
        item = spec.strip()
        if not item:
            continue

        if "=" in item:
            page, label = item.split("=", 1)
            page = page.strip()
            label = label.strip()
        else:
            page = item
            label = ""

        if not page:
            continue
        pages.append(page)
        if label:
            labels[page] = label

    return pages, labels


def build_html(output_html: str, pages: List[str], labels: Dict[str, str] | None = None) -> str:
    labels = labels or {}
    if not pages:
        pages = ["dash_route.html"]

    links = "\n".join(
        f'      <a href="#" class="nav-link" data-page="{page}">{labels.get(page, page_label(page))}</a>'
        for page in pages
    )

    options = ", ".join(f'"{p}"' for p in pages)
    default_page = pages[0]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Data Analysis Hub</title>
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
      width: 100%;
      height: 100%;
      background: var(--bg);
      color: var(--text);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    }}
    .app {{
      display: flex;
      flex-direction: column;
      height: 100%;
    }}
    .header {{
      height: 56px;
      display: flex;
      align-items: center;
      gap: 14px;
      padding: 0 16px;
      background: var(--panel);
      border-bottom: 1px solid var(--border);
      box-sizing: border-box;
      flex-shrink: 0;
    }}
    .title {{
      font-weight: 600;
      color: var(--muted);
      margin-right: 12px;
      white-space: nowrap;
    }}
    .nav {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .nav-link {{
      color: var(--text);
      text-decoration: none;
      padding: 7px 10px;
      border: 1px solid var(--border);
      border-radius: 8px;
      font-size: 13px;
      background: #131923;
    }}
    .nav-link.active {{
      border-color: var(--accent);
      color: #d9f7ff;
      background: #112a34;
    }}
    .frame-wrap {{
      flex: 1;
      min-height: 0;
    }}
    iframe {{
      width: 100%;
      height: 100%;
      border: 0;
      background: #0b1018;
    }}
  </style>
</head>
<body>
  <div class="app">
    <header class="header">
      <div class="title">Data Analysis</div>
      <nav class="nav">
{links}
      </nav>
    </header>
    <main class="frame-wrap">
      <iframe id="contentFrame" title="Data Analysis Content" src="{default_page}"></iframe>
    </main>
  </div>

  <script>
    const availablePages = [{options}];
    const frame = document.getElementById('contentFrame');
    const links = Array.from(document.querySelectorAll('.nav-link'));

    function setActive(page) {{
      links.forEach((a) => {{
        a.classList.toggle('active', a.dataset.page === page);
      }});
    }}

    function loadPage(page) {{
      if (!availablePages.includes(page)) return;
      frame.src = page;
      window.location.hash = page;
      setActive(page);
    }}

    links.forEach((a) => {{
      a.addEventListener('click', (e) => {{
        e.preventDefault();
        loadPage(a.dataset.page);
      }});
    }});

    const initial = window.location.hash.replace(/^#/, '');
    if (availablePages.includes(initial)) {{
      loadPage(initial);
    }} else {{
      setActive("{default_page}");
    }}
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a top-header data analysis hub page.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output HTML file")
    parser.add_argument(
        "--page",
        action="append",
        default=[],
        help="Optional page spec: file.html or file.html=Label (repeatable, order preserved)",
    )
    args = parser.parse_args()

    if args.page:
        pages, labels = parse_page_specs(args.page)
    else:
        pages = discover_pages(args.output)
        labels = {}

    excluded = {DEFAULT_OUTPUT, os.path.basename(args.output)}
    pages = [p for p in pages if os.path.basename(p) not in excluded]
    labels = {p: v for p, v in labels.items() if os.path.basename(p) not in excluded}

    html = build_html(args.output, pages, labels)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote data analysis hub to {args.output} with {len(pages)} linked pages")


if __name__ == "__main__":
    main()
