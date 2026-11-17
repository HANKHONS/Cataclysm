"""Build a dependency-free local handbook from canonical Markdown documents."""

from __future__ import annotations
import html
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
OUT = DOCS / "handbook"
PAGES = [
    "SYSTEM_ARCHITECTURE.md",
    "END_TO_END_TRACE.md",
    "CONCEPTS_AND_FORMULAS.md",
    "RUNBOOK.md",
    "FAILURE_SCENARIOS.md",
    "PERFORMANCE_AND_EXPERIMENTS.md",
    "VIVA_PREPARATION.md",
    "PROJECT_REPORT.md",
    "VERIFICATION.md",
    "IMPLEMENTATION_AUDIT.md",
    "CODE_WALKTHROUGH/README.md",
] + [
    str(path.relative_to(DOCS).as_posix())
    for path in sorted((DOCS / "CODE_WALKTHROUGH").glob("[0-9]*.md"))
]


def inline(text):
    text = html.escape(text)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)

    def link(m):
        label, url = m.groups()
        if url.startswith(("https://", "http://")):
            return (
                f'<a href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'
            )
        return f'<span title="See canonical Markdown for local source links">{label}</span>'

    return re.sub(r"\[([^]]+)\]\(([^)]+)\)", link, text)


def render(text):
    lines = text.splitlines()
    result = []
    paragraph = []
    fence = None
    code = []
    table = []
    listing = False

    def flush():
        if paragraph:
            result.append("<p>" + inline(" ".join(paragraph)) + "</p>")
            paragraph.clear()

    def close_list():
        nonlocal listing
        if listing:
            result.append("</ul>")
            listing = False

    def flush_table():
        if table:
            rows = []
            for index, line in enumerate(table):
                cells = line.strip().strip("|").split("|")
                if all(re.fullmatch(r"[: -]+", cell) for cell in cells):
                    continue
                tag = "th" if index == 0 else "td"
                rows.append(
                    "<tr>"
                    + "".join(
                        f"<{tag}>{inline(cell.strip())}</{tag}>" for cell in cells
                    )
                    + "</tr>"
                )
            result.append(
                '<div class="table-wrap"><table>' + "".join(rows) + "</table></div>"
            )
            table.clear()

    for line in lines:
        if line.startswith("```"):
            flush()
            close_list()
            flush_table()
            if fence is None:
                fence = line[3:]
                code = []
            else:
                result.append(
                    "<pre><code>" + html.escape("\n".join(code)) + "</code></pre>"
                )
                fence = None
            continue
        if fence is not None:
            code.append(line)
            continue
        if line.startswith("<!--"):
            continue
        if line.startswith("|"):
            flush()
            close_list()
            table.append(line)
            continue
        flush_table()
        if not line.strip():
            flush()
            close_list()
            continue
        match = re.match(r"^(#{1,6}) (.+)", line)
        if match:
            flush()
            close_list()
            level = len(match[1])
            title = match[2]
            anchor = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
            result.append(f'<h{level} id="{anchor}">{inline(title)}</h{level}>')
            continue
        if line.startswith("- "):
            flush()
            if not listing:
                result.append("<ul>")
                listing = True
            result.append("<li>" + inline(line[2:]) + "</li>")
            continue
        close_list()
        paragraph.append(line)
    flush()
    close_list()
    flush_table()
    return "\n".join(result)


def architecture_svg() -> str:
    """A clickable diagram of actual independent operational and lake branches."""
    nodes = [
        (20, 30, "Sources", "02_SOURCES_AND_GENERATION"),
        (200, 30, "FastAPI", "03_API_AND_SCHEMAS"),
        (380, 30, "Kafka", "04_KAFKA_CONSUMER"),
        (580, 30, "Consumer + ML", "04_KAFKA_CONSUMER"),
        (780, 30, "PostgreSQL", "05_POSTGRESQL"),
        (980, 30, "API / dashboard", "10_DASHBOARD"),
        (780, 120, "Raw Parquet", "07_MINIO_PARQUET_REPLAY"),
        (980, 120, "Offset commit", "04_KAFKA_CONSUMER"),
        (380, 220, "Spark dedup", "06_SPARK_STREAMING"),
        (580, 220, "Processed lake", "07_MINIO_PARQUET_REPLAY"),
        (780, 220, "Spark windows", "06_SPARK_STREAMING"),
        (980, 220, "Curated lake", "07_MINIO_PARQUET_REPLAY"),
    ]
    paths = [
        "M160 55 H200",
        "M340 55 H380",
        "M520 55 H580",
        "M720 55 H780",
        "M920 55 H980",
        "M850 80 V120",
        "M920 145 H980",
        "M450 80 V220",
        "M520 245 H580",
        "M720 245 H780",
        "M920 245 H980",
    ]
    diagram = '<div class="diagram-wrap"><svg class="architecture" viewBox="0 0 1140 300" role="group" aria-label="Actual pipeline branches. Select a component for its implementation."><defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0 L7 3.5 L0 7" fill="#a3432e"/></marker></defs>'
    diagram += "".join(
        f'<path d="{path}" fill="none" stroke="#a3432e" marker-end="url(#arrow)"/>'
        for path in paths
    )
    for x, y, label, chapter in nodes:
        diagram += f'<a href="#" data-chapter="CODE_WALKTHROUGH/{chapter}.md" aria-label="Read {label}"><rect x="{x}" y="{y}" width="140" height="50" rx="3"/><text x="{x + 70}" y="{y + 30}" text-anchor="middle">{label}</text></a>'
    return (
        diagram
        + '</svg></div><p class="diagram-note">Select a node to inspect its code. DB → raw → commit indicates consumer processing order; Spark independently reads Kafka. Every service still shares one host.</p>'
    )


def build():
    OUT.mkdir(exist_ok=True)
    entries = []
    for name in PAGES:
        path = DOCS / name
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8-sig")
        title = text.splitlines()[0].lstrip("# ")
        content = render(text)
        if name == "VIVA_PREPARATION.md":
            parts = re.split(r"^### (.+)$", text, flags=re.M)
            content = render(parts[0]) + "".join(
                "<details><summary>"
                + html.escape(parts[i])
                + "</summary>"
                + render(parts[i + 1])
                + "</details>"
                for i in range(1, len(parts) - 1, 2)
            )
        entries.append({"path": name, "title": title, "html": content, "search": text})
    data = json.dumps(entries, ensure_ascii=False).replace("<", "\\u003c")
    shell = (
        """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>CATACLYSM / Engineering field guide</title><link rel="stylesheet" href="handbook.css"><script defer src="handbook.js"></script><script>window.MathJax={tex:{inlineMath:[['\\\\(','\\\\)']],displayMath:[['\\\\[','\\\\]'],['$$','$$']]},options:{skipHtmlTags:['script','noscript','style','textarea','pre','code']}};</script><script defer src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script></head><body><header><a href="#" class="wordmark">C / FIELD GUIDE</a><span>CATACLYSM · THE ENGINEERING NOTEBOOK</span><a id="console" href="http://127.0.0.1:8000">Open console ↗</a></header><div class="layout"><aside><p class="kicker">READ / TRACE / QUESTION</p><label for="search">Find a concept or function</label><input type="search" id="search" placeholder="watermark, commit, TF-IDF…"><div id="searchResults" role="status"></div><nav id="chapters" aria-label="Learning chapters"></nav></aside><main><div class="intro"><p class="kicker">A SYSTEM YOU CAN EXPLAIN</p><h1>Follow the event.<br><em>Understand the decisions.</em></h1><p>Read the implementation, inspect its boundaries and challenge its evidence. This is a learning notebook, built from the project's canonical Markdown.</p></div><section aria-label="Interactive architecture"><h2>Choose a component</h2><div class="components"><button data-chapter="CODE_WALKTHROUGH/02_SOURCES_AND_GENERATION.md">01 / Observe<small>USGS · weather · simulation</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/03_API_AND_SCHEMAS.md">02 / Accept<small>Pydantic · FastAPI · producer</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/04_KAFKA_CONSUMER.md">03 / Persist<small>Kafka · consumer · transactions</small></button><button data-chapter="CODE_WALKTHROUGH/06_SPARK_STREAMING.md">04 / Analyze<small>Spark · watermarks · windows</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/07_MINIO_PARQUET_REPLAY.md">05 / Retain<small>PostgreSQL · MinIO · replay</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/10_DASHBOARD.md">06 / Inspect<small>Predictions · map · monitoring</small></button></div></section><section class="journey"><div class="section-head"><h2>One event, eight boundaries</h2><span>ACTUAL SMOKE ID / DEMO-a1449ee3220b471f</span></div><div id="steps" role="group" aria-label="Event journey stages"></div><div id="journeyDetail" aria-live="polite"></div><button id="next">Next boundary →</button></section><details class="concepts"><summary>Before you read: three distinctions that matter</summary><h3>Acknowledged is not persisted</h3><p>POST 200 confirms Kafka delivery. Database visibility, raw archival and Kafka source commit follow separately.</p><h3>Report count is not disaster count</h3><p>Several observations can describe one hazard. Sliding windows overlap; summing their counts double-counts reports.</p><h3>Computed score is not real-world validity</h3><p>The tiny text holdout and synthetic severity labels do not establish emergency-response usefulness.</p></details><div class="reading-head"><p class="kicker" id="chapterPath"></p><a id="canonical">Open canonical Markdown ↗</a></div><article id="reader"></article><footer>Generated from canonical docs. Run python scripts/build_handbook.py after documentation changes. Formulas render with MathJax when online; plain LaTeX remains readable offline. No live telemetry is simulated here.</footer></main></div><script type="application/json" id="documents">"""
        + data
        + """</script></body></html>"""
    )
    shell = shell.replace(
        '<div class="components">', architecture_svg() + '<div class="components">', 1
    )
    (OUT / "index.html").write_text(shell, encoding="utf-8")
    print(f"Handbook built: {len(entries)} canonical chapters")


if __name__ == "__main__":
    build()
