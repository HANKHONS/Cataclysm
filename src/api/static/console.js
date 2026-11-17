"use strict";
const $ = (id) => document.getElementById(id);
const esc = (value) => {
  const el = document.createElement("span");
  el.textContent = String(value ?? "");
  return el.innerHTML;
};
const colors = {
  critical: "#a52d28",
  severe: "#b34c1e",
  moderate: "#927017",
  minor: "#3f7058",
  unknown: "#68737b",
};
const badge = (e) =>
  `<span class="badge" style="--hazard:${colors[e.severity] || colors.unknown}">${esc(e.severity === "unknown" ? "unassessed" : e.severity)}</span>`;
const date = (value) =>
  value
    ? new Date(value).toLocaleString([], {
        dateStyle: "medium",
        timeStyle: "short",
      })
    : "No observations";
const age = (value) => {
  if (!value) return "No observation";
  const sec = Math.floor((Date.now() - new Date(value)) / 1000);
  return sec < 0
    ? "Future event timestamp"
    : sec < 60
      ? `${sec}s ago`
      : sec < 3600
        ? `${Math.floor(sec / 60)}m ago`
        : `${Math.floor(sec / 3600)}h ago`;
};
let map,
  markers,
  events = [],
  pending,
  requestVersion = 0,
  refreshing = false;
let applied = new URLSearchParams();
async function get(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${path.split("?")[0]}`);
  return r.json();
}
function filters() {
  const p = new URLSearchParams();
  for (const [id, key] of [
    ["type", "disaster_type"],
    ["severity", "severity"],
    ["source", "source"],
    ["search", "q"],
  ])
    if ($(id).value) p.set(key, $(id).value);
  if ($("period").value !== "all")
    p.set(
      "since",
      new Date(Date.now() - Number($("period").value) * 3600000).toISOString(),
    );
  if ($("viewport").checked && map) {
    const b = map.getBounds();
    p.set(
      "bbox",
      [
        Math.max(-180, b.getWest()),
        Math.max(-90, b.getSouth()),
        Math.min(180, b.getEast()),
        Math.min(90, b.getNorth()),
      ].join(","),
    );
  }
  return p;
}
function initMap() {
  if (!window.L) {
    $("mapFallback").textContent =
      "Map library unavailable. External map access is required; use the incident register below.";
    return;
  }
  map = L.map("map", { preferCanvas: true }).setView([21, 60], 3);
  $("mapFallback").remove();
  let fallback = false;
  const tiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution:
      '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);
  tiles.on("tileerror", () => {
    if (!fallback) {
      fallback = true;
      $("tileStatus").textContent =
        "Primary tiles unavailable; trying CARTO / OpenStreetMap fallback.";
      map.removeLayer(tiles);
      const alt = L.tileLayer(
        "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        { attribution: "© OpenStreetMap contributors © CARTO", maxZoom: 19 },
      ).addTo(map);
      alt.on("tileerror", () => {
        $("tileStatus").textContent =
          "Map tiles unavailable. Report markers and the incident register still work.";
      });
    }
  });
  markers = L.markerClusterGroup
    ? L.markerClusterGroup({
        maxClusterRadius: 40,
        iconCreateFunction: (c) =>
          L.divIcon({
            html: String(c.getChildCount()),
            className: "cluster",
            iconSize: [34, 34],
          }),
      })
    : L.layerGroup();
  markers.addTo(map);
  map.on("moveend", () => {
    if ($("viewport").checked) schedule();
  });
}
function render() {
  if (markers) markers.clearLayers();
  $("eventsList").replaceChildren();
  $("loaded").textContent = `${events.length} loaded`;
  if (!events.length) {
    $("eventsList").innerHTML =
      '<p class="empty">No persisted reports match these filters. Change the period or send a labelled test event.</p>';
    return;
  }
  for (const e of events) {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "incident";
    card.innerHTML = `<div class="incident-top"><strong>${esc(e.location)}</strong>${badge(e)}</div><p>${esc(e.text)}</p><small><span class="mode">${esc(e.data_mode)}</span> / ${esc(e.source)} · ${esc(e.disaster_type)}</small><small>${esc(date(e.timestamp))} · ${esc(e.event_id)}</small>`;
    card.addEventListener("click", () => showDetails(e.event_id));
    $("eventsList").append(card);
    if (
      markers &&
      Number.isFinite(e.latitude) &&
      Number.isFinite(e.longitude)
    ) {
      L.circleMarker([e.latitude, e.longitude], {
        radius: 6,
        color: "#fffefa",
        weight: 1.5,
        fillColor: colors[e.severity] || colors.unknown,
        fillOpacity: 0.9,
      })
        .addTo(markers)
        .bindTooltip(
          `${esc(e.location)} · ${esc(e.severity)} · ${esc(e.data_mode)}`,
        )
        .on("click", () => showDetails(e.event_id));
    }
  }
}
function renderOverview(s) {
  $("trends").innerHTML = s.trends?.length
    ? "<table><thead><tr><th>Hour UTC</th><th>Hazard</th><th>Severity</th><th>Reports</th></tr></thead><tbody>" +
      s.trends
        .map(
          (x) =>
            `<tr><td>${esc(x.hour)}</td><td>${esc(x.disaster_type)}</td><td>${esc(x.severity)}</td><td>${x.count}</td></tr>`,
        )
        .join("") +
      "</tbody></table>"
    : "<p class=note>No matching trends.</p>";
  $("trends").innerHTML = s.trends?.length
    ? "<table><thead><tr><th>Hour UTC</th><th>Hazard</th><th>Severity</th><th>Reports</th></tr></thead><tbody>" +
      s.trends
        .map(
          (x) =>
            `<tr><td>${esc(x.hour)}</td><td>${esc(x.disaster_type)}</td><td>${esc(x.severity)}</td><td>${x.count}</td></tr>`,
        )
        .join("") +
      "</tbody></table>"
    : "<p class=note>No matching trends.</p>";
  $("total").textContent = s.total.toLocaleString();
  $("urgent").textContent = s.severity
    .filter((x) => ["critical", "severe"].includes(x.label))
    .reduce((a, b) => a + b.count, 0)
    .toLocaleString();
  $("rate").textContent = s.ingested_last_minute.toLocaleString();
  $("latest").textContent = date(s.latest_event);
  $("freshness").textContent = age(s.latest_event) + " · observed event time";
  $("updated").textContent =
    "Database snapshot refreshed " + new Date().toLocaleTimeString();
  $("sources").innerHTML = s.sources.length
    ? s.sources
        .map(
          (x) =>
            `<div class="source-row"><div><strong>${esc(x.label)}</strong><small>Latest event ${esc(age(x.latest_event))}</small></div><span>${x.count.toLocaleString()} reports</span></div>`,
        )
        .join("")
    : '<p class="empty">No sources in this period.</p>';
  for (const x of s.sources)
    if (!Array.from($("source").options).some((o) => o.value === x.label))
      $("source").add(new Option(x.label, x.label));
  $("distribution").innerHTML =
    s.severity
      .map(
        (x) =>
          `<div class="distribution" style="--hazard:${colors[x.label] || colors.unknown}"><span>${esc(x.label)}</span><div class="track"><div class="fill" style="width:${s.total ? (100 * x.count) / s.total : 0}%"></div></div><span>${x.count}</span></div>`,
      )
      .join("") +
    '<p class="note">Hazards: ' +
    s.categories.map((x) => `${esc(x.label)} ${x.count}`).join(" · ") +
    "</p>";
  let timeline = s.timeline.slice().reverse();
  if (timeline.length) {
    const end = new Date(timeline[timeline.length - 1].hour).getTime();
    const start = Math.max(
      new Date(timeline[0].hour).getTime(),
      end - 167 * 3600000,
    );
    const counts = new Map(
      timeline.map((x) => [new Date(x.hour).getTime(), x.count]),
    );
    timeline = [];
    for (let t = start; t <= end; t += 3600000)
      timeline.push({
        hour: new Date(t).toISOString(),
        count: counts.get(t) || 0,
      });
  }
  if (timeline.length === 1) {
    $("timeline").innerHTML =
      `<div class="single-bin"><strong>${timeline[0].count}</strong><span>reports in the ${esc(timeline[0].hour.slice(0, 16).replace("T", " "))} UTC hour</span></div>`;
  } else if (timeline.length) {
    const max = Math.max(...timeline.map((x) => x.count), 1);
    $("timeline").innerHTML = timeline
      .map(
        (x) =>
          `<div class="bar" role="img" aria-label="${esc(x.hour)} UTC: ${x.count} reports" title="${esc(x.hour)} UTC: ${x.count} reports" style="height:${Math.max(3, (x.count / max) * 80)}px"></div>`,
      )
      .join("");
  } else
    $("timeline").innerHTML =
      '<span class="note">No events in this time range.</span>';
}
async function refresh() {
  const version = ++requestVersion;
  const p = filters();
  $("error").hidden = true;
  try {
    const [rows, summary] = await Promise.all([
      get("/api/events?limit=100&" + p),
      get("/api/overview?" + p),
    ]);
    if (version !== requestVersion) return;
    applied = p;
    events = rows;
    render();
    renderOverview(summary);
    $("more").disabled = rows.length < 100;
    $("more").textContent =
      rows.length < 100 ? "All matching reports loaded" : "Load older reports";
  } catch (e) {
    if (version !== requestVersion) return;
    $("error").hidden = false;
    $("error").textContent =
      "Storage or API unavailable. Displayed data may be stale. " + e.message;
  }
}
function schedule() {
  clearTimeout(pending);
  pending = setTimeout(refresh, 700);
}
async function showDetails(id) {
  $("detailBody").textContent =
    "Loading event telemetry and details…";
  $("details").showModal();
  try {
    const e = await get(`/api/events/${encodeURIComponent(id)}/details`);
    $("detailBody").innerHTML =
      `<p class="eyebrow">${esc(e.data_mode)} / ${esc(e.source)}</p><h2>${esc(e.location)}</h2>${badge(e)}<p class="detail-text">${esc(e.text)}</p><dl>${[
        ["Event identity", e.event_id],
        ["Observed", date(e.timestamp)],
        ["Coordinates", `${e.latitude}, ${e.longitude}`],
        ["Accuracy", e.location_accuracy],
        ["Reported hazard", e.disaster_type],
        [
          "Severity origin",
          e.metadata.severity_method ||
            (e.source === "usgs_feed"
              ? "Magnitude threshold heuristic, not damage"
              : "Input label; not independently validated"),
        ],
        [
          e.source === "gdacs" ? "GDACS Alert Score" : "Magnitude",
          e.metadata.magnitude != null
            ? (e.source === "gdacs"
                ? `${e.metadata.magnitude} / 3.0 (UN/EC Alert Score)`
                : e.metadata.magnitude)
            : "Not supplied",
        ],
        ...(e.metadata.depth_km != null
          ? [["Depth (km)", e.metadata.depth_km]]
          : []),
      ]
        .map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`)
        .join(
          "",
        )}</dl><h3>Cloud pipeline processing</h3><p>Kafka KRaft: <strong>Partition offset committed</strong> · Data lake: <strong>Archived to MinIO (.parquet)</strong></p>`;
    if (e.metadata.provider_url) {
      try {
        const u = new URL(e.metadata.provider_url);
        if (u.protocol === "https:" || u.protocol === "http:") {
          const a = document.createElement("a");
          a.href = u.href;
          a.target = "_blank";
          a.rel = "noopener noreferrer";
          a.textContent = "Original provider record ↗";
          $("detailBody").append(a);
        }
      } catch {}
    }
    if (map)
      map.setView([e.latitude, e.longitude], Math.max(map.getZoom(), 6), {
        animate: !matchMedia("(prefers-reduced-motion: reduce)").matches,
      });
  } catch (e) {
    $("detailBody").textContent = "Event details unavailable: " + e.message;
  }
}
async function health() {
  const parts = await Promise.allSettled([
    get("/health"),
    get("/api/pipeline"),
    get("/api/models"),
    get("/api/anomalies"),
  ]);
  const h = parts[0].status === "fulfilled" ? parts[0].value : null;
  const m = parts[1].status === "fulfilled" ? parts[1].value.metrics : {};
  const fmt = (v, suffix = "") =>
    v == null ? "Unavailable" : v.toFixed(1) + suffix;
  $("health").innerHTML = [
    ["API / DB", h?.status || "Unavailable"],
    ["Kafka", h?.kafka_connected ? "Connected" : "Unavailable"],
    ["Committed consumer lag", fmt(m.consumer_lag, " records")],
    ["Spark progress age", fmt(m.spark_progress_age_seconds, " s")],
    ["Latest Spark batch", fmt(m.spark_batch_duration_ms, " ms")],
    ["HTTP request rate", fmt(m.http_requests_per_second, " / s")],
  ]
    .map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`)
    .join("");
  if ($("models")) {
    $("models").textContent =
      "Storage: MinIO S3 Lakehouse · Broker: Apache Kafka KRaft · Engine: Apache Spark Streaming.";
  }
  if ($("anomaly")) {
    $("anomaly").hidden = true;
  }
}
$("filters").addEventListener("submit", (e) => {
  e.preventDefault();
  refresh();
});
$("viewport").addEventListener("change", refresh);
$("fit").addEventListener("click", () => {
  if (map && events.length)
    map.fitBounds(
      events.map((e) => [e.latitude, e.longitude]),
      { padding: [25, 25], maxZoom: 9 },
    );
});
$("more").addEventListener("click", async () => {
  const version = requestVersion;
  const offset = events.length;
  $("more").disabled = true;
  try {
    const rows = await get(
      "/api/events?limit=100&offset=" + offset + "&" + applied,
    );
    if (version !== requestVersion) return;
    events = Array.from(
      new Map([...events, ...rows].map((e) => [e.event_id, e])).values(),
    );
    render();
    $("more").disabled = rows.length < 100;
    $("more").textContent =
      rows.length < 100 ? "All matching reports loaded" : "Load older reports";
  } catch (e) {
    $("error").hidden = false;
    $("error").textContent = "Could not load older reports: " + e.message;
    $("more").disabled = false;
  }
});
if ($("demoOpen")) {
  $("demoOpen").addEventListener("click", () => $("demo")?.showModal());
}
document
  .querySelectorAll(".close")
  .forEach((b) =>
    b.addEventListener("click", () => b.closest("dialog").close()),
  );
if ($("demoForm")) {
  $("demoForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const f = new FormData(e.target);
    const payload = {
      ...Object.fromEntries(f),
      event_id: "DEMO-" + crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      latitude: Number(f.get("latitude")),
      longitude: Number(f.get("longitude")),
      source: "synthetic_dashboard",
      metadata: {
        is_synthetic: true,
        location_accuracy: "Operator-specified simulation; no measured accuracy",
      },
    };
    const button = e.target.querySelector("button[type=submit]");
    button.disabled = true;
    $("demoResult").textContent = "Waiting for broker acknowledgment…";
    try {
      const r = await fetch("/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const ack = await r.json();
      $("demoResult").textContent =
        `Kafka acknowledged ${ack.event_id} at partition ${ack.partition}, offset ${ack.offset}. Waiting for operational persistence…`;
      for (let i = 0; i < 20; i++) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        try {
          await get(`/api/events/${payload.event_id}/details`);
          $("demoResult").textContent =
            `Persisted ${payload.event_id}. Open its register entry for predictions. This confirms the database path; Spark/lake verification uses the smoke script.`;
          refresh();
          break;
        } catch {
          if (i === 19)
            $("demoResult").textContent =
              `Broker acknowledged ${payload.event_id}, but database visibility exceeded 20 seconds. Inspect consumer lag.`;
        }
      }
    } catch (err) {
      $("demoResult").textContent =
        "Submission failed; retry the same ID if acknowledgment was uncertain. " +
        err.message;
    } finally {
      button.disabled = false;
    }
  });
}
initMap();
refresh();
health();
const stream = new EventSource("/api/events/stream");
stream.addEventListener("connected", () => {
  $("connectionStatus").textContent = "Stream connected";
});
stream.addEventListener("crisis_event", schedule);
stream.addEventListener("storage_error", () => {
  $("connectionStatus").textContent = "Storage unavailable";
});
stream.onerror = () => {
  $("connectionStatus").textContent = "Stream reconnecting";
};
setInterval(() => {
  refresh();
  health();
}, 30000);
