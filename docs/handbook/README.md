# Local educational handbook

Open `index.html` directly from this folder, or start CATACLYSM and open http://127.0.0.1:8000/handbook/ . No build tools, web framework or data service is required for the embedded chapters. CSS/JavaScript are local. Online MathJax improves LaTeX typesetting; without network access the actual LaTeX text remains visible.

The handbook is an educational reader, not a telemetry dashboard. Canonical Markdown is one directory above. Components select source chapters; the journey steps through actual pipeline boundaries; search finds concepts/functions across embedded chapters; viva answers expand individually. On mobile, the chapter navigation becomes a bounded scrolling shelf.

Regenerate after documentation changes from repository root with `python scripts/build_handbook.py`. Refresh source references first using `python scripts/sync_source_reference.py`. The builder updates the index only and preserves the handwritten stylesheet/behavior files. `Open canonical Markdown` works both from a local file and through the API's `/documentation/` route.
