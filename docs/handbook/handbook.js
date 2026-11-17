"use strict";
const docs = JSON.parse(document.getElementById("documents").textContent);
const reader = document.getElementById("reader");
const nav = document.getElementById("chapters");
function openChapter(path, scroll = true) {
  const entry = docs.find((d) => d.path === path) || docs[0];
  reader.innerHTML = entry.html;
  document.getElementById("chapterPath").textContent = entry.path;
  document.getElementById("canonical").href =
    location.protocol === "file:"
      ? "../" + entry.path
      : "/documentation/" + entry.path;
  nav
    .querySelectorAll("button")
    .forEach((b) =>
      b.setAttribute("aria-current", String(b.dataset.path === entry.path)),
    );
  if (scroll)
    document
      .querySelector(".reading-head")
      .scrollIntoView({ behavior: "auto", block: "start" });
  if (window.MathJax?.typesetPromise)
    window.MathJax.typesetPromise([reader]).catch(() => {});
}
for (const entry of docs) {
  const button = document.createElement("button");
  button.textContent = entry.title;
  button.dataset.path = entry.path;
  button.addEventListener("click", () => {
    openChapter(entry.path);
    history.replaceState(null, "", "#" + encodeURIComponent(entry.path));
  });
  nav.append(button);
}
document.querySelectorAll("[data-chapter]").forEach((b) =>
  b.addEventListener("click", (event) => {
    event.preventDefault();
    openChapter(b.dataset.chapter);
  }),
);
const stages = [
  [
    "Source",
    "A labelled synthetic flood observation is constructed with ID DEMO-a1449ee3220b471f, current UTC time, coordinates and telemetry. This is controlled input, not a live disaster report.",
    "02_SOURCES_AND_GENERATION",
  ],
  [
    "API",
    "Pydantic validates the event. FastAPI serializes JSON and publishes with the location key. A delivery callback, not local enqueue, determines HTTP success.",
    "03_API_AND_SCHEMAS",
  ],
  [
    "Kafka",
    "This observed run acknowledged partition 0, offset 81. This establishes broker acceptance. It does not prove downstream persistence.",
    "04_KAFKA_CONSUMER",
  ],
  [
    "Consumer / ML",
    "The consumer validates again and obtains text/telemetry predictions. Confidence is uncalibrated; source severity and model output remain separate.",
    "09_ML_INFERENCE_AND_ANOMALIES",
  ],
  [
    "Database",
    "One transaction inserts event, report and two prediction records. First-observation identity prevents conflicting retries from replacing facts.",
    "05_POSTGRESQL",
  ],
  [
    "Raw / commit",
    "Raw Parquet retains the supported enriched payload. Only after its PUT succeeds does the consumer commit the Kafka source offset.",
    "07_MINIO_PARQUET_REPLAY",
  ],
  [
    "Spark",
    "An independent checkpointed query writes processed Parquet. A second query emits finalized sliding-window counts. Watermark delay means this is not instantaneous.",
    "06_SPARK_STREAMING",
  ],
  [
    "Dashboard",
    "The browser queries PostgreSQL-backed routes and refreshes on SSE notifications. The map/register share persisted records; form success alone cannot certify Spark completion.",
    "10_DASHBOARD",
  ],
];
let stageIndex = 0;
function showStage(index) {
  stageIndex = index;
  document
    .querySelectorAll("#steps button")
    .forEach((b, i) => b.setAttribute("aria-pressed", String(i === index)));
  const p = document.createElement("p");
  p.textContent = stages[index][1];
  const link = document.createElement("button");
  link.textContent = "Read the implementation →";
  link.className = "stage-link";
  link.addEventListener("click", () =>
    openChapter("CODE_WALKTHROUGH/" + stages[index][2] + ".md"),
  );
  link.style.cssText =
    "background:none;border:0;padding:0;color:var(--accent);cursor:pointer";
  document.getElementById("journeyDetail").replaceChildren(p, link);
}
stages.forEach(([name], index) => {
  const button = document.createElement("button");
  button.textContent = index + 1 + ". " + name;
  button.addEventListener("click", () => showStage(index));
  document.getElementById("steps").append(button);
});
document
  .getElementById("next")
  .addEventListener("click", () => showStage((stageIndex + 1) % stages.length));
showStage(0);
document.getElementById("search").addEventListener("input", (e) => {
  const value = e.target.value.trim().toLowerCase();
  const output = document.getElementById("searchResults");
  output.replaceChildren();
  if (value.length < 2) return;
  const results = docs.filter((d) => d.search.toLowerCase().includes(value));
  const count = document.createElement("p");
  count.textContent = results.length + " matching chapters";
  output.append(count);
  for (const entry of results.slice(0, 12)) {
    const b = document.createElement("button");
    b.textContent = entry.title;
    const small = document.createElement("small");
    small.textContent = entry.path;
    b.append(small);
    b.addEventListener("click", () => openChapter(entry.path));
    output.append(b);
  }
});
let initial;
try {
  initial = decodeURIComponent(location.hash.slice(1));
} catch {
  initial = "SYSTEM_ARCHITECTURE.md";
}
openChapter(initial || "SYSTEM_ARCHITECTURE.md", false);
