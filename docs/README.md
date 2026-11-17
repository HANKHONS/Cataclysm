# CATACLYSM documentation index

Markdown is canonical. The [interactive handbook](handbook/index.html) embeds it for searchable local reading; while the application runs, open http://127.0.0.1:8000/handbook/ . Its diagrams select source walkthroughs, its event journey exposes separate boundaries and its viva answers collapse individually. LaTeX renders through MathJax when online; offline text remains readable.

## Recommended study sequence

1. [System architecture](SYSTEM_ARCHITECTURE.md): purpose, ownership, trade-offs, actual graph, reliability/scaling limits.
2. [End-to-end trace](END_TO_END_TRACE.md): follow one actual labelled event and distinguish acknowledgment/persistence/finalization.
3. [Code walkthrough](CODE_WALKTHROUGH/README.md): 13 ordered module chapters, detailed block explanations and exact source/definition ranges/hashes.
4. [Concepts and formulas](CONCEPTS_AND_FORMULAS.md): intuition, equations/derivations, examples, complexity and memory implications tied to this code.
5. [Runbook](RUNBOOK.md): execute and inspect each boundary using exact PowerShell commands.
6. [Failures](FAILURE_SCENARIOS.md): predict a result, reproduce a safe test, compare observed evidence.
7. [Performance and experiments](PERFORMANCE_AND_EXPERIMENTS.md): reproduce measurements and explain their confounds; historical [EXPERIMENTS.md](EXPERIMENTS.md) remains dated separately.
8. [Viva preparation](VIVA_PREPARATION.md): 68 questions from foundations to adversarial design defense.
9. [Academic project report](PROJECT_REPORT.md): implemented methodology/results and evidence-qualified limitations.

Use [VERIFICATION.md](VERIFICATION.md) to assess current claims, [IMPLEMENTATION_AUDIT.md](IMPLEMENTATION_AUDIT.md) for baseline defects/final disposition, [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for restartable state, [DEMO.md](DEMO.md) for the timed demonstration and [ML.md](ML.md) for model interpretation. Structured current observations and actual screenshots live in [evidence/](evidence/).

## Refresh source synchronization

```powershell
.\venv\Scripts\python.exe scripts/sync_source_reference.py
.\venv\Scripts\python.exe scripts/sync_source_reference.py --check
.\venv\Scripts\python.exe scripts/build_handbook.py
```

The walkthrough prose is maintained directly above its generated source-reference marker. Refresh updates only exact source snapshots/imports/definition ranges/hashes. The handbook builder reads canonical Markdown and regenerates `handbook/index.html`; it does not fabricate live status or replace source documents. Update prose when behavior changes; a matching hash alone does not prove an explanation remains correct.
