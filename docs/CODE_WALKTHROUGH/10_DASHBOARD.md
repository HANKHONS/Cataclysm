# Dashboard: data ownership and interaction flow

The frontend remains buildless HTML/CSS/JavaScript plus pinned Leaflet/marker-cluster libraries. There is no framework replacement or fake telemetry source. HTML supplies semantic header/main/sections, labelled filters, register buttons and native modal dialogs. CSS tokens define warm paper, graphite, fine borders and hazard colors. Desktop uses a large map plus register; mobile stacks panels. Focus-visible outlines, native labels, skip link, readable textual severity and reduced-motion rules support keyboard/reduced-motion use. Color is not the sole severity signal.

`console.js` keeps row state, current query parameters, request generation and map/layer handles. `$` resolves known IDs; `esc` escapes through textContent before template insertion. `date` and `age` format actual timestamp values and identify future timestamps. `badge` chooses a fixed known color; event text never becomes raw CSS. `get` checks HTTP status before parsing JSON, so failed storage does not quietly look like an empty dataset.

`filters` builds URLSearchParams for source/hazard/severity/search and a moving lower event-time bound. A viewport filter reads the current map bounds as a bbox. `refresh` increments requestVersion, fetches rows and aggregate summary concurrently, ignores older completed responses, then replaces register/map/summary state. This generation guard prevents a slow old search from repainting a newer selection. Error text declares displayed data potentially stale. Server aggregate total is separate from the loaded 100 rows. Loading older pages uses the captured applied parameters; requestVersion discards a response if filters changed meanwhile. Offset pagination during concurrent insert can still shift rows; ID de-dup protects the local list, but a future keyset API would offer stronger browsing continuity.

`initMap` checks library availability, configures a canvas-preferred map and ordinary attributed OSM tiles, attempts a CARTO/OSM tile fallback on error, and leaves list/error context available without tiles. Marker clustering is used when its library is available; otherwise a plain layer group works. Each row becomes a severity-colored marker and escaped tooltip. `render` clears/rebuilds the layer and register from the same loaded rows. Coordinates use finite-number checks, so zero latitude/longitude work. `Fit reports` fits actual loaded coordinates; viewport filtering can intentionally change query scope after a move.

`renderOverview` displays period-wide API totals, high input-severity counts, recent unique-ID persistence and latest event time. Source watch uses reported source count/latest observation; it does not pretend that freshness proves provider uptime. The timeline fills zero-count hourly gaps, caps visible span to the latest 168 hours ending at the latest matching event, and supplies actual time/count tooltips. Historical periods can include more total data than the visible timeline. Severity/hazard distributions come from the same server filter scope. An expandable hourly table shows actual hazard/severity trends with an explicit 1,000-group limit. The model panel also displays the latest actual anomaly-window result without calling its flag ground-truth accuracy. Reports are explicitly not unique disasters.

`showDetails` opens a loading dialog, queries stored details and renders source/mode, original narrative, timestamp, exact coordinates, location-accuracy note, magnitude/depth and severity method. Model outputs appear separately with provenance/uncalibrated score and limitations. Provider links are only HTTP/HTTPS and use noopener/noreferrer. External provider navigation is a user action, not a hidden data upload.

`health` independently handles DB/broker health, actual Prometheus samples and model metadata through allSettled; absent values display Unavailable. A scrape query failure is not silently zero lag. EventSource automatically reconnects; crisis_event notifications debounce a fresh DB query, storage_error changes state, and the 30-second refresh reconciles filters and time scope. SSE is not a durable full-history source.

The demo form constructs one synthetic event with crypto UUID, current time, entered coordinates/narrative, labelled simulation metadata and no invented telemetry. POST response describes broker acknowledgment. It then polls persisted details for up to 20 seconds, showing database success separately. It does not push the submitted payload directly onto the map. Spark/raw completion is verified by scripts rather than inferred from UI visibility. A timed-out resubmission currently requires deliberately retaining the original ID through API tooling; the UI is not a general operator retry queue.

Browser QA must cover actual form persistence, search/empty filters, source labels, detail open/close, map movement, responsive overflow and reconnection. Source syntax and screenshots alone do not prove those interactions. CDN/map offline fallbacks are implemented but a fully vendored offline package is not claimed.


## Educational handbook implementation

`docs/handbook/handbook.js` reads the embedded canonical-document JSON, builds chapter buttons, filters searchable text, switches the escaped reader content, updates canonical links for file versus HTTP mode and walks the eight actual event boundaries. The SVG architecture nodes select matching source chapters. Viva answers use native details/summary controls. No live observations are fabricated. `scripts/build_handbook.py` escapes source/text, converts the supported Markdown blocks and embeds all content so local-file use needs no fetch server. `architecture_svg` draws the independent operational and Spark branches; `build` reads the fixed intended document list and writes only the generated index. The conservative converter is not a general untrusted Markdown execution engine; relative source links stay reference text and canonical Markdown retains the original links. MathJax is optional online enhancement, with raw LaTeX available offline. Regenerate after source/documentation edits and verify browser navigation, search, formulas and local links.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/api/static/index.html`

[Open source](../../src/api/static/index.html) · SHA-256 `e43b45fdd7acd032a2829e197e0d8b6efc229fb9dad1448c80db259d201e61d6`


```text
0001 | <!doctype html>
0002 | <html lang="en">
0003 |   <head>
0004 |     <meta charset="utf-8" />
0005 |     <meta name="viewport" content="width=device-width, initial-scale=1" />
0006 |     <title>CATACLYSM · Situation room</title>
0007 |     <link
0008 |       rel="stylesheet"
0009 |       href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
0010 |     />
0011 |     <link
0012 |       rel="stylesheet"
0013 |       href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"
0014 |     />
0015 |     <link rel="stylesheet" href="/static/console.css" />
0016 |     <script
0017 |       defer
0018 |       src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
0019 |     ></script>
0020 |     <script
0021 |       defer
0022 |       src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"
0023 |     ></script>
0024 |     <script defer src="/static/console.js"></script>
0025 |   </head>
0026 |   <body>
0027 |     <a href="#workspace" class="skip">Skip to incidents</a>
0028 |     <header>
0029 |       <a class="brand" href="/" aria-label="CATACLYSM home"
0030 |         ><span class="brand-mark">C /</span
0031 |         ><span
0032 |           >CATACLYSM<small>CRISIS INTELLIGENCE / FIELD CONSOLE</small></span
0033 |         ></a
0034 |       >
0035 |       <div class="header-right">
0036 |         <span id="connectionStatus" role="status">Connecting to stream…</span
0037 |         ><a href="/handbook/">Field guide ↗</a
0038 |         ><button id="demoOpen">+ Test event</button>
0039 |       </div>
0040 |     </header>
0041 |     <main>
0042 |       <div class="title-row">
0043 |         <div>
0044 |           <p class="eyebrow">OPERATIONS / SITUATION OVERVIEW</p>
0045 |           <h1>A clearer picture.<br /><em>A measured response.</em></h1>
0046 |         </div>
0047 |         <p class="context">
0048 |           Multi-source observations, one geographic workspace.<br /><strong
0049 |             >Academic prototype</strong
0050 |           >
0051 |           · Model output is experimental.<br /><span id="updated"
0052 |             >Waiting for persisted observations</span
0053 |           >
0054 |         </p>
0055 |       </div>
0056 |       <form id="filters" class="filters" aria-label="Incident filters">
0057 |         <label
0058 |           >Observation period<select id="period">
0059 |             <option value="24">Last 24 hours</option>
0060 |             <option value="168">Last 7 days</option>
0061 |             <option value="all">All persisted history</option>
0062 |             <option value="1">Last hour</option>
0063 |           </select></label
0064 |         >
0065 |         <label
0066 |           >Hazard<select id="type">
0067 |             <option value="">All hazards</option>
0068 |             <option>earthquake</option>
0069 |             <option>flood</option>
0070 |             <option>cyclone</option>
0071 |             <option>fire</option>
0072 |             <option>infrastructure</option>
0073 |             <option>weather</option>
0074 |           </select></label
0075 |         >
0076 |         <label
0077 |           >Severity<select id="severity">
0078 |             <option value="">All severities</option>
0079 |             <option>critical</option>
0080 |             <option>severe</option>
0081 |             <option>moderate</option>
0082 |             <option>minor</option>
0083 |             <option value="unknown">unassessed</option>
0084 |           </select></label
0085 |         >
0086 |         <label
0087 |           >Source<select id="source">
0088 |             <option value="">All sources</option>
0089 |           </select></label
0090 |         >
0091 |         <label class="search"
0092 |           >Search location, text or ID<input
0093 |             id="search"
0094 |             type="search"
0095 |             placeholder="Search observations…"
0096 |             maxlength="200"
0097 |         /></label>
0098 |         <button type="submit" class="apply">Apply filters</button>
0099 |       </form>
0100 |       <div id="error" role="alert" hidden></div>
0101 |       <section class="overview" aria-label="Period summary">
0102 |         <div>
0103 |           <span>01 / PERSISTED REPORTS</span><strong id="total">—</strong
0104 |           ><small>All matching records in period</small>
0105 |         </div>
0106 |         <div>
0107 |           <span>02 / CRITICAL + SEVERE</span><strong id="urgent">—</strong
0108 |           ><small>Input severity; may be heuristic</small>
0109 |         </div>
0110 |         <div>
0111 |           <span>03 / RECENT PERSISTENCE</span><strong id="rate">—</strong
0112 |           ><small>New matching IDs / last 60 seconds</small>
0113 |         </div>
0114 |         <div>
0115 |           <span>04 / LATEST OBSERVATION</span
0116 |           ><strong class="timestamp" id="latest">—</strong
0117 |           ><small id="freshness"
0118 |             >Event time; not a service-health guarantee</small
0119 |           >
0120 |         </div>
0121 |       </section>
0122 |       <section
0123 |         class="workspace"
0124 |         id="workspace"
0125 |         aria-label="Geospatial incident workspace"
0126 |       >
0127 |         <div class="map-panel">
0128 |           <div class="panel-head">
0129 |             <h2>Situation map <span> / SOURCE COORDINATES</span></h2>
0130 |             <button id="fit">Fit reports</button>
0131 |           </div>
0132 |           <div id="map">
0133 |             <p id="mapFallback">
0134 |               Loading map library… The incident list remains available if
0135 |               mapping is offline.
0136 |             </p>
0137 |           </div>
0138 |           <div class="map-footer">
0139 |             <div class="legend">
0140 |               <span><i style="--c: var(--critical)"></i>Critical</span
0141 |               ><span><i style="--c: var(--severe)"></i>Severe</span
0142 |               ><span><i style="--c: var(--moderate)"></i>Moderate</span
0143 |               ><span><i style="--c: var(--minor)"></i>Minor</span
0144 |               ><span><i style="--c: var(--unknown)"></i>Unassessed</span>
0145 |             </div>
0146 |             <label
0147 |               ><input type="checkbox" id="viewport" /> Filter to map area</label
0148 |             ><span id="tileStatus"
0149 |               >External tiles · accuracy supplied per report</span
0150 |             >
0151 |           </div>
0152 |         </div>
0153 |         <aside class="feed">
0154 |           <div class="panel-head">
0155 |             <h2>Incident register</h2>
0156 |             <span id="loaded">0 loaded</span>
0157 |           </div>
0158 |           <div id="eventsList" aria-live="polite">
0159 |             <p class="empty">Loading persisted reports…</p>
0160 |           </div>
0161 |           <button id="more">Load older reports</button>
0162 |         </aside>
0163 |       </section>
0164 |       <section class="bottom-grid" aria-label="Analytics and pipeline status">
0165 |         <article>
0166 |           <div class="panel-head">
0167 |             <h2>Activity over time</h2>
0168 |             <span>UTC / HOURLY BINS</span>
0169 |           </div>
0170 |           <div id="timeline" class="timeline"></div>
0171 |           <p class="note">
0172 |             Counts use event time. Latest 168 hours ending at the latest
0173 |             matching event; empty hours have zero reports. A report is not a
0174 |             unique disaster.
0175 |           </p>
0176 |           <div id="distribution"></div>
0177 |           <details class="trends">
0178 |             <summary>Hourly hazard &amp; severity trends</summary>
0179 |             <div id="trends"></div>
0180 |             <p class="note">
0181 |               Latest 1,000 nonempty hour / hazard / severity groups in the
0182 |               selected scope.
0183 |             </p>
0184 |           </details>
0185 |         </article>
0186 |         <article>
0187 |           <div class="panel-head">
0188 |             <h2>Source watch</h2>
0189 |             <span>FRESHNESS ≠ UPTIME</span>
0190 |           </div>
0191 |           <div id="sources"></div>
0192 |           <p class="note">
0193 |             Live means a named provider observation; synthetic and replay retain
0194 |             explicit labels. Provider updates may be delayed.
0195 |           </p>
0196 |         </article>
0197 |         <article>
0198 |           <div class="panel-head">
0199 |             <h2>Pipeline & models</h2>
0200 |             <span>MEASURED / REPORTED</span>
0201 |           </div>
0202 |           <dl id="health"></dl>
0203 |           <p class="note" id="models">Model status pending.</p>
0204 |           <p class="note" id="anomaly">Anomaly history pending.</p>
0205 |           <a href="/docs">Inspect API contracts ↗</a>
0206 |         </article>
0207 |       </section>
0208 |       <footer>
0209 |         CATACLYSM / LOCAL RESEARCH SYSTEM
0210 |         <span
0211 |           >No emergency dispatch capability. Verify observations with their
0212 |           provider.</span
0213 |         >
0214 |       </footer>
0215 |     </main>
0216 |     <dialog id="details">
0217 |       <button class="close" aria-label="Close event details">×</button>
0218 |       <div id="detailBody"></div>
0219 |     </dialog>
0220 |     <dialog id="demo">
0221 |       <button class="close" aria-label="Close test event form">×</button>
0222 |       <p class="eyebrow">CONTROLLED PIPELINE INSPECTION</p>
0223 |       <h2>Send a synthetic report</h2>
0224 |       <p>
0225 |         This event travels through the ingestion API, Kafka and actual sinks. It
0226 |         remains labelled synthetic.
0227 |       </p>
0228 |       <form id="demoForm">
0229 |         <label
0230 |           >Location<input
0231 |             name="location"
0232 |             value="Demo Mumbai"
0233 |             required
0234 |             maxlength="100"
0235 |         /></label>
0236 |         <div class="pair">
0237 |           <label
0238 |             >Latitude<input
0239 |               name="latitude"
0240 |               type="number"
0241 |               step="any"
0242 |               min="-90"
0243 |               max="90"
0244 |               value="19.076"
0245 |               required /></label
0246 |           ><label
0247 |             >Longitude<input
0248 |               name="longitude"
0249 |               type="number"
0250 |               step="any"
0251 |               min="-180"
0252 |               max="180"
0253 |               value="72.8777"
0254 |               required
0255 |           /></label>
0256 |         </div>
0257 |         <label
0258 |           >Narrative<textarea name="text" required maxlength="10000">
0259 | Synthetic test: river flood affecting a residential road.</textarea
0260 |           >
0261 |         </label>
0262 |         <div class="pair">
0263 |           <label
0264 |             >Hazard<select name="disaster_type">
0265 |               <option>flood</option>
0266 |               <option>earthquake</option>
0267 |               <option>fire</option>
0268 |               <option>cyclone</option>
0269 |               <option>infrastructure</option>
0270 |             </select></label
0271 |           ><label
0272 |             >Reported severity<select name="severity">
0273 |               <option>moderate</option>
0274 |               <option>severe</option>
0275 |               <option>critical</option>
0276 |               <option>minor</option>
0277 |             </select></label
0278 |           >
0279 |         </div>
0280 |         <button type="submit" class="apply">Send through Kafka</button>
0281 |       </form>
0282 |       <p id="demoResult" role="status"></p>
0283 |     </dialog>
0284 |   </body>
0285 | </html>
```

### `src/api/static/console.js`

[Open source](../../src/api/static/console.js) · SHA-256 `c0e1bede7129a6831a96d3b1b2425d3db262382b973a38d1b90470de4f304bc8`


```text
0001 | "use strict";
0002 | const $ = (id) => document.getElementById(id);
0003 | const esc = (value) => {
0004 |   const el = document.createElement("span");
0005 |   el.textContent = String(value ?? "");
0006 |   return el.innerHTML;
0007 | };
0008 | const colors = {
0009 |   critical: "#a52d28",
0010 |   severe: "#b34c1e",
0011 |   moderate: "#927017",
0012 |   minor: "#3f7058",
0013 |   unknown: "#68737b",
0014 | };
0015 | const badge = (e) =>
0016 |   `<span class="badge" style="--hazard:${colors[e.severity] || colors.unknown}">${esc(e.severity === "unknown" ? "unassessed" : e.severity)}</span>`;
0017 | const date = (value) =>
0018 |   value
0019 |     ? new Date(value).toLocaleString([], {
0020 |         dateStyle: "medium",
0021 |         timeStyle: "short",
0022 |       })
0023 |     : "No observations";
0024 | const age = (value) => {
0025 |   if (!value) return "No observation";
0026 |   const sec = Math.floor((Date.now() - new Date(value)) / 1000);
0027 |   return sec < 0
0028 |     ? "Future event timestamp"
0029 |     : sec < 60
0030 |       ? `${sec}s ago`
0031 |       : sec < 3600
0032 |         ? `${Math.floor(sec / 60)}m ago`
0033 |         : `${Math.floor(sec / 3600)}h ago`;
0034 | };
0035 | let map,
0036 |   markers,
0037 |   events = [],
0038 |   pending,
0039 |   requestVersion = 0,
0040 |   refreshing = false;
0041 | let applied = new URLSearchParams();
0042 | async function get(path) {
0043 |   const r = await fetch(path);
0044 |   if (!r.ok) throw new Error(`HTTP ${r.status}: ${path.split("?")[0]}`);
0045 |   return r.json();
0046 | }
0047 | function filters() {
0048 |   const p = new URLSearchParams();
0049 |   for (const [id, key] of [
0050 |     ["type", "disaster_type"],
0051 |     ["severity", "severity"],
0052 |     ["source", "source"],
0053 |     ["search", "q"],
0054 |   ])
0055 |     if ($(id).value) p.set(key, $(id).value);
0056 |   if ($("period").value !== "all")
0057 |     p.set(
0058 |       "since",
0059 |       new Date(Date.now() - Number($("period").value) * 3600000).toISOString(),
0060 |     );
0061 |   if ($("viewport").checked && map) {
0062 |     const b = map.getBounds();
0063 |     p.set(
0064 |       "bbox",
0065 |       [
0066 |         Math.max(-180, b.getWest()),
0067 |         Math.max(-90, b.getSouth()),
0068 |         Math.min(180, b.getEast()),
0069 |         Math.min(90, b.getNorth()),
0070 |       ].join(","),
0071 |     );
0072 |   }
0073 |   return p;
0074 | }
0075 | function initMap() {
0076 |   if (!window.L) {
0077 |     $("mapFallback").textContent =
0078 |       "Map library unavailable. External map access is required; use the incident register below.";
0079 |     return;
0080 |   }
0081 |   map = L.map("map", { preferCanvas: true }).setView([21, 60], 3);
0082 |   $("mapFallback").remove();
0083 |   let fallback = false;
0084 |   const tiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
0085 |     maxZoom: 19,
0086 |     attribution:
0087 |       '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
0088 |   }).addTo(map);
0089 |   tiles.on("tileerror", () => {
0090 |     if (!fallback) {
0091 |       fallback = true;
0092 |       $("tileStatus").textContent =
0093 |         "Primary tiles unavailable; trying CARTO / OpenStreetMap fallback.";
0094 |       map.removeLayer(tiles);
0095 |       const alt = L.tileLayer(
0096 |         "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
0097 |         { attribution: "© OpenStreetMap contributors © CARTO", maxZoom: 19 },
0098 |       ).addTo(map);
0099 |       alt.on("tileerror", () => {
0100 |         $("tileStatus").textContent =
0101 |           "Map tiles unavailable. Report markers and the incident register still work.";
0102 |       });
0103 |     }
0104 |   });
0105 |   markers = L.markerClusterGroup
0106 |     ? L.markerClusterGroup({
0107 |         maxClusterRadius: 40,
0108 |         iconCreateFunction: (c) =>
0109 |           L.divIcon({
0110 |             html: String(c.getChildCount()),
0111 |             className: "cluster",
0112 |             iconSize: [34, 34],
0113 |           }),
0114 |       })
0115 |     : L.layerGroup();
0116 |   markers.addTo(map);
0117 |   map.on("moveend", () => {
0118 |     if ($("viewport").checked) schedule();
0119 |   });
0120 | }
0121 | function render() {
0122 |   if (markers) markers.clearLayers();
0123 |   $("eventsList").replaceChildren();
0124 |   $("loaded").textContent = `${events.length} loaded`;
0125 |   if (!events.length) {
0126 |     $("eventsList").innerHTML =
0127 |       '<p class="empty">No persisted reports match these filters. Change the period or send a labelled test event.</p>';
0128 |     return;
0129 |   }
0130 |   for (const e of events) {
0131 |     const card = document.createElement("button");
0132 |     card.type = "button";
0133 |     card.className = "incident";
0134 |     card.innerHTML = `<div class="incident-top"><strong>${esc(e.location)}</strong>${badge(e)}</div><p>${esc(e.text)}</p><small><span class="mode">${esc(e.data_mode)}</span> / ${esc(e.source)} · ${esc(e.disaster_type)}</small><small>${esc(date(e.timestamp))} · ${esc(e.event_id)}</small>`;
0135 |     card.addEventListener("click", () => showDetails(e.event_id));
0136 |     $("eventsList").append(card);
0137 |     if (
0138 |       markers &&
0139 |       Number.isFinite(e.latitude) &&
0140 |       Number.isFinite(e.longitude)
0141 |     ) {
0142 |       L.circleMarker([e.latitude, e.longitude], {
0143 |         radius: 6,
0144 |         color: "#fffefa",
0145 |         weight: 1.5,
0146 |         fillColor: colors[e.severity] || colors.unknown,
0147 |         fillOpacity: 0.9,
0148 |       })
0149 |         .addTo(markers)
0150 |         .bindTooltip(
0151 |           `${esc(e.location)} · ${esc(e.severity)} · ${esc(e.data_mode)}`,
0152 |         )
0153 |         .on("click", () => showDetails(e.event_id));
0154 |     }
0155 |   }
0156 | }
0157 | function renderOverview(s) {
0158 |   $("trends").innerHTML = s.trends?.length
0159 |     ? "<table><thead><tr><th>Hour UTC</th><th>Hazard</th><th>Severity</th><th>Reports</th></tr></thead><tbody>" +
0160 |       s.trends
0161 |         .map(
0162 |           (x) =>
0163 |             `<tr><td>${esc(x.hour)}</td><td>${esc(x.disaster_type)}</td><td>${esc(x.severity)}</td><td>${x.count}</td></tr>`,
0164 |         )
0165 |         .join("") +
0166 |       "</tbody></table>"
0167 |     : "<p class=note>No matching trends.</p>";
0168 |   $("trends").innerHTML = s.trends?.length
0169 |     ? "<table><thead><tr><th>Hour UTC</th><th>Hazard</th><th>Severity</th><th>Reports</th></tr></thead><tbody>" +
0170 |       s.trends
0171 |         .map(
0172 |           (x) =>
0173 |             `<tr><td>${esc(x.hour)}</td><td>${esc(x.disaster_type)}</td><td>${esc(x.severity)}</td><td>${x.count}</td></tr>`,
0174 |         )
0175 |         .join("") +
0176 |       "</tbody></table>"
0177 |     : "<p class=note>No matching trends.</p>";
0178 |   $("total").textContent = s.total.toLocaleString();
0179 |   $("urgent").textContent = s.severity
0180 |     .filter((x) => ["critical", "severe"].includes(x.label))
0181 |     .reduce((a, b) => a + b.count, 0)
0182 |     .toLocaleString();
0183 |   $("rate").textContent = s.ingested_last_minute.toLocaleString();
0184 |   $("latest").textContent = date(s.latest_event);
0185 |   $("freshness").textContent = age(s.latest_event) + " · observed event time";
0186 |   $("updated").textContent =
0187 |     "Database snapshot refreshed " + new Date().toLocaleTimeString();
0188 |   $("sources").innerHTML = s.sources.length
0189 |     ? s.sources
0190 |         .map(
0191 |           (x) =>
0192 |             `<div class="source-row"><div><strong>${esc(x.label)}</strong><small>Latest event ${esc(age(x.latest_event))}</small></div><span>${x.count.toLocaleString()} reports</span></div>`,
0193 |         )
0194 |         .join("")
0195 |     : '<p class="empty">No sources in this period.</p>';
0196 |   for (const x of s.sources)
0197 |     if (!Array.from($("source").options).some((o) => o.value === x.label))
0198 |       $("source").add(new Option(x.label, x.label));
0199 |   $("distribution").innerHTML =
0200 |     s.severity
0201 |       .map(
0202 |         (x) =>
0203 |           `<div class="distribution" style="--hazard:${colors[x.label] || colors.unknown}"><span>${esc(x.label)}</span><div class="track"><div class="fill" style="width:${s.total ? (100 * x.count) / s.total : 0}%"></div></div><span>${x.count}</span></div>`,
0204 |       )
0205 |       .join("") +
0206 |     '<p class="note">Hazards: ' +
0207 |     s.categories.map((x) => `${esc(x.label)} ${x.count}`).join(" · ") +
0208 |     "</p>";
0209 |   let timeline = s.timeline.slice().reverse();
0210 |   if (timeline.length) {
0211 |     const end = new Date(timeline[timeline.length - 1].hour).getTime();
0212 |     const start = Math.max(
0213 |       new Date(timeline[0].hour).getTime(),
0214 |       end - 167 * 3600000,
0215 |     );
0216 |     const counts = new Map(
0217 |       timeline.map((x) => [new Date(x.hour).getTime(), x.count]),
0218 |     );
0219 |     timeline = [];
0220 |     for (let t = start; t <= end; t += 3600000)
0221 |       timeline.push({
0222 |         hour: new Date(t).toISOString(),
0223 |         count: counts.get(t) || 0,
0224 |       });
0225 |   }
0226 |   if (timeline.length === 1) {
0227 |     $("timeline").innerHTML =
0228 |       `<div class="single-bin"><strong>${timeline[0].count}</strong><span>reports in the ${esc(timeline[0].hour.slice(0, 16).replace("T", " "))} UTC hour</span></div>`;
0229 |   } else if (timeline.length) {
0230 |     const max = Math.max(...timeline.map((x) => x.count), 1);
0231 |     $("timeline").innerHTML = timeline
0232 |       .map(
0233 |         (x) =>
0234 |           `<div class="bar" role="img" aria-label="${esc(x.hour)} UTC: ${x.count} reports" title="${esc(x.hour)} UTC: ${x.count} reports" style="height:${Math.max(3, (x.count / max) * 80)}px"></div>`,
0235 |       )
0236 |       .join("");
0237 |   } else
0238 |     $("timeline").innerHTML =
0239 |       '<span class="note">No events in this time range.</span>';
0240 | }
0241 | async function refresh() {
0242 |   const version = ++requestVersion;
0243 |   const p = filters();
0244 |   $("error").hidden = true;
0245 |   try {
0246 |     const [rows, summary] = await Promise.all([
0247 |       get("/api/events?limit=100&" + p),
0248 |       get("/api/overview?" + p),
0249 |     ]);
0250 |     if (version !== requestVersion) return;
0251 |     applied = p;
0252 |     events = rows;
0253 |     render();
0254 |     renderOverview(summary);
0255 |     $("more").disabled = rows.length < 100;
0256 |     $("more").textContent =
0257 |       rows.length < 100 ? "All matching reports loaded" : "Load older reports";
0258 |   } catch (e) {
0259 |     if (version !== requestVersion) return;
0260 |     $("error").hidden = false;
0261 |     $("error").textContent =
0262 |       "Storage or API unavailable. Displayed data may be stale. " + e.message;
0263 |   }
0264 | }
0265 | function schedule() {
0266 |   clearTimeout(pending);
0267 |   pending = setTimeout(refresh, 700);
0268 | }
0269 | async function showDetails(id) {
0270 |   $("detailBody").textContent =
0271 |     "Loading original metadata and model provenance…";
0272 |   $("details").showModal();
0273 |   try {
0274 |     const e = await get(`/api/events/${encodeURIComponent(id)}/details`);
0275 |     $("detailBody").innerHTML =
0276 |       `<p class="eyebrow">${esc(e.data_mode)} / ${esc(e.source)}</p><h2>${esc(e.location)}</h2>${badge(e)}<p class="detail-text">${esc(e.text)}</p><dl>${[
0277 |         ["Event identity", e.event_id],
0278 |         ["Observed", date(e.timestamp)],
0279 |         ["Coordinates", `${e.latitude}, ${e.longitude}`],
0280 |         ["Accuracy", e.location_accuracy],
0281 |         ["Reported hazard", e.disaster_type],
0282 |         [
0283 |           "Severity origin",
0284 |           e.metadata.severity_method ||
0285 |             (e.source === "usgs_feed"
0286 |               ? "Magnitude threshold heuristic, not damage"
0287 |               : "Input label; not independently validated"),
0288 |         ],
0289 |         ["Magnitude", e.metadata.magnitude ?? "Not supplied"],
0290 |         ["Depth (km)", e.metadata.depth_km ?? "Not supplied"],
0291 |       ]
0292 |         .map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`)
0293 |         .join(
0294 |           "",
0295 |         )}</dl><h3>Experimental model outputs</h3>${e.predictions.map((p) => `<p>${esc(p.model_name)}: <strong>${esc(p.predicted_label)}</strong> / ${esc(p.provenance)}${p.confidence == null ? "" : ` / uncalibrated score ${Number(p.confidence).toFixed(3)}`}</p>`).join("")}<p class="note">Synthetic severity training does not validate physical damage prediction. Weather-only features cannot assess earthquake damage.</p>`;
0296 |     if (e.metadata.provider_url) {
0297 |       try {
0298 |         const u = new URL(e.metadata.provider_url);
0299 |         if (u.protocol === "https:" || u.protocol === "http:") {
0300 |           const a = document.createElement("a");
0301 |           a.href = u.href;
0302 |           a.target = "_blank";
0303 |           a.rel = "noopener noreferrer";
0304 |           a.textContent = "Original provider record ↗";
0305 |           $("detailBody").append(a);
0306 |         }
0307 |       } catch {}
0308 |     }
0309 |     if (map)
0310 |       map.setView([e.latitude, e.longitude], Math.max(map.getZoom(), 6), {
0311 |         animate: !matchMedia("(prefers-reduced-motion: reduce)").matches,
0312 |       });
0313 |   } catch (e) {
0314 |     $("detailBody").textContent = "Event details unavailable: " + e.message;
0315 |   }
0316 | }
0317 | async function health() {
0318 |   const parts = await Promise.allSettled([
0319 |     get("/health"),
0320 |     get("/api/pipeline"),
0321 |     get("/api/models"),
0322 |     get("/api/anomalies"),
0323 |   ]);
0324 |   const h = parts[0].status === "fulfilled" ? parts[0].value : null;
0325 |   const m = parts[1].status === "fulfilled" ? parts[1].value.metrics : {};
0326 |   const fmt = (v, suffix = "") =>
0327 |     v == null ? "Unavailable" : v.toFixed(1) + suffix;
0328 |   $("health").innerHTML = [
0329 |     ["API / DB", h?.status || "Unavailable"],
0330 |     ["Kafka", h?.kafka_connected ? "Connected" : "Unavailable"],
0331 |     ["Committed consumer lag", fmt(m.consumer_lag, " records")],
0332 |     ["Spark progress age", fmt(m.spark_progress_age_seconds, " s")],
0333 |     ["Latest Spark batch", fmt(m.spark_batch_duration_ms, " ms")],
0334 |     ["HTTP request rate", fmt(m.http_requests_per_second, " / s")],
0335 |   ]
0336 |     .map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`)
0337 |     .join("");
0338 |   if (parts[2].status === "fulfilled") {
0339 |     const model = parts[2].value;
0340 |     $("models").textContent =
0341 |       `Text: ${model.classifier}; severity: ${model.severity}. ${model.training_note} ${model.confidence_note}`;
0342 |   } else $("models").textContent = "Model status unavailable.";
0343 |   if (parts[3].status === "fulfilled" && parts[3].value.length) {
0344 |     const x = parts[3].value[0];
0345 |     $("anomaly").textContent =
0346 |       `Latest scored hour ${date(x.window_start)}: ${x.report_count} reports; decision score ${Number(x.score).toFixed(3)}; ${x.is_anomaly ? "flagged by forest/range guard" : "not flagged"}. No ground-truth accuracy is established.`;
0347 |   } else
0348 |     $("anomaly").textContent =
0349 |       "No anomaly result available. Scoring requires 20 completed training hours plus a scoring hour.";
0350 |   if (parts[3].status === "fulfilled" && parts[3].value.length) {
0351 |     const x = parts[3].value[0];
0352 |     $("anomaly").textContent =
0353 |       `Latest scored hour ${date(x.window_start)}: ${x.report_count} reports; decision score ${Number(x.score).toFixed(3)}; ${x.is_anomaly ? "flagged by forest/range guard" : "not flagged"}. No ground-truth accuracy is established.`;
0354 |   } else
0355 |     $("anomaly").textContent =
0356 |       "No anomaly result available. Scoring requires 20 completed training hours plus a scoring hour.";
0357 | }
0358 | $("filters").addEventListener("submit", (e) => {
0359 |   e.preventDefault();
0360 |   refresh();
0361 | });
0362 | $("viewport").addEventListener("change", refresh);
0363 | $("fit").addEventListener("click", () => {
0364 |   if (map && events.length)
0365 |     map.fitBounds(
0366 |       events.map((e) => [e.latitude, e.longitude]),
0367 |       { padding: [25, 25], maxZoom: 9 },
0368 |     );
0369 | });
0370 | $("more").addEventListener("click", async () => {
0371 |   const version = requestVersion;
0372 |   const offset = events.length;
0373 |   $("more").disabled = true;
0374 |   try {
0375 |     const rows = await get(
0376 |       "/api/events?limit=100&offset=" + offset + "&" + applied,
0377 |     );
0378 |     if (version !== requestVersion) return;
0379 |     events = Array.from(
0380 |       new Map([...events, ...rows].map((e) => [e.event_id, e])).values(),
0381 |     );
0382 |     render();
0383 |     $("more").disabled = rows.length < 100;
0384 |     $("more").textContent =
0385 |       rows.length < 100 ? "All matching reports loaded" : "Load older reports";
0386 |   } catch (e) {
0387 |     $("error").hidden = false;
0388 |     $("error").textContent = "Could not load older reports: " + e.message;
0389 |     $("more").disabled = false;
0390 |   }
0391 | });
0392 | $("demoOpen").addEventListener("click", () => $("demo").showModal());
0393 | document
0394 |   .querySelectorAll(".close")
0395 |   .forEach((b) =>
0396 |     b.addEventListener("click", () => b.closest("dialog").close()),
0397 |   );
0398 | $("demoForm").addEventListener("submit", async (e) => {
0399 |   e.preventDefault();
0400 |   const f = new FormData(e.target);
0401 |   const payload = {
0402 |     ...Object.fromEntries(f),
0403 |     event_id: "DEMO-" + crypto.randomUUID(),
0404 |     timestamp: new Date().toISOString(),
0405 |     latitude: Number(f.get("latitude")),
0406 |     longitude: Number(f.get("longitude")),
0407 |     source: "synthetic_dashboard",
0408 |     metadata: {
0409 |       is_synthetic: true,
0410 |       location_accuracy: "Operator-specified simulation; no measured accuracy",
0411 |     },
0412 |   };
0413 |   const button = e.target.querySelector("button[type=submit]");
0414 |   button.disabled = true;
0415 |   $("demoResult").textContent = "Waiting for broker acknowledgment…";
0416 |   try {
0417 |     const r = await fetch("/events", {
0418 |       method: "POST",
0419 |       headers: { "Content-Type": "application/json" },
0420 |       body: JSON.stringify(payload),
0421 |     });
0422 |     if (!r.ok) throw new Error("HTTP " + r.status);
0423 |     const ack = await r.json();
0424 |     $("demoResult").textContent =
0425 |       `Kafka acknowledged ${ack.event_id} at partition ${ack.partition}, offset ${ack.offset}. Waiting for operational persistence…`;
0426 |     for (let i = 0; i < 20; i++) {
0427 |       await new Promise((resolve) => setTimeout(resolve, 1000));
0428 |       try {
0429 |         await get(`/api/events/${payload.event_id}/details`);
0430 |         $("demoResult").textContent =
0431 |           `Persisted ${payload.event_id}. Open its register entry for predictions. This confirms the database path; Spark/lake verification uses the smoke script.`;
0432 |         refresh();
0433 |         break;
0434 |       } catch {
0435 |         if (i === 19)
0436 |           $("demoResult").textContent =
0437 |             `Broker acknowledged ${payload.event_id}, but database visibility exceeded 20 seconds. Inspect consumer lag.`;
0438 |       }
0439 |     }
0440 |   } catch (err) {
0441 |     $("demoResult").textContent =
0442 |       "Submission failed; retry the same ID if acknowledgment was uncertain. " +
0443 |       err.message;
0444 |   } finally {
0445 |     button.disabled = false;
0446 |   }
0447 | });
0448 | initMap();
0449 | refresh();
0450 | health();
0451 | const stream = new EventSource("/api/events/stream");
0452 | stream.addEventListener("connected", () => {
0453 |   $("connectionStatus").textContent = "Stream connected";
0454 | });
0455 | stream.addEventListener("crisis_event", schedule);
0456 | stream.addEventListener("storage_error", () => {
0457 |   $("connectionStatus").textContent = "Storage unavailable";
0458 | });
0459 | stream.onerror = () => {
0460 |   $("connectionStatus").textContent = "Stream reconnecting";
0461 | };
0462 | setInterval(() => {
0463 |   refresh();
0464 |   health();
0465 | }, 30000);
```

### `src/api/static/console.css`

[Open source](../../src/api/static/console.css) · SHA-256 `6ba70f4298eed548a6ea8cc86b08ab811edab4aecaad14f82bfb63f330414505`


```text
0001 | :root {
0002 |   --paper: #f5f3ed;
0003 |   --surface: #fffefa;
0004 |   --ink: #292e2b;
0005 |   --muted: #626a62;
0006 |   --line: #d7dbd1;
0007 |   --critical: #a52d28;
0008 |   --severe: #b34c1e;
0009 |   --moderate: #927017;
0010 |   --minor: #3f7058;
0011 |   --unknown: #68737b;
0012 | }
0013 | * {
0014 |   box-sizing: border-box;
0015 | }
0016 | body {
0017 |   margin: 0;
0018 |   background: var(--paper);
0019 |   color: var(--ink);
0020 |   font:
0021 |     14px/1.5 system-ui,
0022 |     -apple-system,
0023 |     Segoe UI,
0024 |     sans-serif;
0025 | }
0026 | a {
0027 |   color: var(--ink);
0028 |   text-underline-offset: 4px;
0029 | }
0030 | button,
0031 | input,
0032 | select,
0033 | textarea {
0034 |   font: inherit;
0035 | }
0036 | button {
0037 |   cursor: pointer;
0038 |   background: var(--surface);
0039 |   border: 1px solid var(--line);
0040 |   color: var(--ink);
0041 |   padding: 9px 14px;
0042 |   border-radius: 3px;
0043 | }
0044 | button:hover {
0045 |   background: #e9ebe2;
0046 | }
0047 | button:disabled {
0048 |   opacity: 0.6;
0049 |   cursor: wait;
0050 | }
0051 | :focus-visible {
0052 |   outline: 3px solid #4c705c;
0053 |   outline-offset: 3px;
0054 | }
0055 | header {
0056 |   min-height: 86px;
0057 |   padding: 16px 3.2%;
0058 |   border-bottom: 1px solid var(--line);
0059 |   display: flex;
0060 |   align-items: center;
0061 |   justify-content: space-between;
0062 |   gap: 20px;
0063 | }
0064 | .brand {
0065 |   display: flex;
0066 |   align-items: center;
0067 |   gap: 13px;
0068 |   text-decoration: none;
0069 |   font-weight: 800;
0070 |   font-size: 19px;
0071 |   letter-spacing: 2px;
0072 | }
0073 | .brand-mark {
0074 |   background: var(--ink);
0075 |   color: var(--paper);
0076 |   padding: 9px 12px;
0077 |   font:
0078 |     25px Georgia,
0079 |     serif;
0080 | }
0081 | .brand small {
0082 |   display: block;
0083 |   font-size: 9px;
0084 |   font-weight: 500;
0085 |   letter-spacing: 1.6px;
0086 |   margin-top: 3px;
0087 | }
0088 | .header-right {
0089 |   display: flex;
0090 |   align-items: center;
0091 |   gap: 22px;
0092 |   font-size: 12px;
0093 | }
0094 | #connectionStatus::before {
0095 |   content: "●";
0096 |   color: var(--minor);
0097 |   margin-right: 8px;
0098 | }
0099 | main {
0100 |   padding: 30px 3.2% 0;
0101 | }
0102 | .title-row {
0103 |   display: flex;
0104 |   align-items: flex-end;
0105 |   justify-content: space-between;
0106 |   margin-bottom: 28px;
0107 | }
0108 | .eyebrow {
0109 |   font-size: 10px;
0110 |   letter-spacing: 2px;
0111 |   color: var(--muted);
0112 |   font-weight: 650;
0113 |   margin: 0 0 12px;
0114 | }
0115 | h1 {
0116 |   font:
0117 |     42px/1.1 Georgia,
0118 |     serif;
0119 |   margin: 0;
0120 |   letter-spacing: -1px;
0121 | }
0122 | h1 em {
0123 |   font-weight: 400;
0124 |   color: #697163;
0125 | }
0126 | .context {
0127 |   font-size: 12px;
0128 |   line-height: 1.9;
0129 |   color: var(--muted);
0130 |   margin: 0;
0131 |   text-align: right;
0132 | }
0133 | .context strong {
0134 |   color: var(--ink);
0135 | }
0136 | .filters {
0137 |   display: flex;
0138 |   gap: 12px;
0139 |   align-items: flex-end;
0140 |   padding: 16px;
0141 |   background: #eaece3;
0142 |   border: 1px solid var(--line);
0143 |   border-radius: 3px;
0144 | }
0145 | label {
0146 |   display: flex;
0147 |   flex-direction: column;
0148 |   gap: 5px;
0149 |   font-size: 11px;
0150 |   font-weight: 600;
0151 |   color: var(--muted);
0152 | }
0153 | input,
0154 | select,
0155 | textarea {
0156 |   min-height: 36px;
0157 |   border: 1px solid #c8cec1;
0158 |   background: var(--surface);
0159 |   color: var(--ink);
0160 |   border-radius: 3px;
0161 |   padding: 7px 9px;
0162 |   max-width: 100%;
0163 | }
0164 | .search {
0165 |   flex: 1;
0166 | }
0167 | .apply {
0168 |   background: var(--ink);
0169 |   color: white;
0170 |   white-space: nowrap;
0171 | }
0172 | .apply:hover {
0173 |   background: #444d41;
0174 | }
0175 | .overview {
0176 |   display: grid;
0177 |   grid-template-columns: repeat(4, 1fr);
0178 |   padding: 24px 0;
0179 |   border-bottom: 1px solid var(--line);
0180 |   margin-bottom: 25px;
0181 | }
0182 | .overview > div {
0183 |   padding: 0 24px;
0184 |   border-right: 1px solid var(--line);
0185 | }
0186 | .overview > div:first-child {
0187 |   padding-left: 0;
0188 | }
0189 | .overview > div:last-child {
0190 |   border: 0;
0191 | }
0192 | .overview span {
0193 |   font-size: 9px;
0194 |   letter-spacing: 1.5px;
0195 |   font-weight: 700;
0196 |   color: var(--muted);
0197 | }
0198 | .overview strong {
0199 |   display: block;
0200 |   font:
0201 |     42px/1.4 Georgia,
0202 |     serif;
0203 |   font-variant-numeric: tabular-nums;
0204 | }
0205 | .overview strong.timestamp {
0206 |   font-size: 21px;
0207 |   padding: 11px 0;
0208 | }
0209 | .overview small {
0210 |   color: var(--muted);
0211 |   font-size: 10px;
0212 | }
0213 | .workspace {
0214 |   display: grid;
0215 |   grid-template-columns: minmax(0, 1fr) 365px;
0216 |   gap: 18px;
0217 | }
0218 | .map-panel,
0219 | .feed,
0220 | .bottom-grid article {
0221 |   border: 1px solid var(--line);
0222 |   background: var(--surface);
0223 |   border-radius: 3px;
0224 |   overflow: hidden;
0225 | }
0226 | .panel-head {
0227 |   display: flex;
0228 |   align-items: center;
0229 |   justify-content: space-between;
0230 |   gap: 12px;
0231 |   padding: 14px 16px;
0232 |   border-bottom: 1px solid var(--line);
0233 |   min-height: 55px;
0234 | }
0235 | h2 {
0236 |   font-size: 14px;
0237 |   margin: 0;
0238 |   font-weight: 650;
0239 | }
0240 | .panel-head span {
0241 |   font-size: 9px;
0242 |   letter-spacing: 1px;
0243 |   color: var(--muted);
0244 |   font-weight: 500;
0245 | }
0246 | .panel-head button {
0247 |   padding: 4px 8px;
0248 |   font-size: 11px;
0249 | }
0250 | #map {
0251 |   height: 490px;
0252 |   background: #e3e6dc;
0253 |   position: relative;
0254 | }
0255 | #mapFallback {
0256 |   padding: 30px;
0257 | }
0258 | .map-footer {
0259 |   padding: 12px;
0260 |   display: flex;
0261 |   align-items: center;
0262 |   flex-wrap: wrap;
0263 |   gap: 10px;
0264 |   font-size: 9px;
0265 |   color: var(--muted);
0266 | }
0267 | .map-footer label {
0268 |   flex-direction: row;
0269 |   align-items: center;
0270 |   font-size: 10px;
0271 | }
0272 | .map-footer input {
0273 |   min-height: unset;
0274 | }
0275 | .legend {
0276 |   display: flex;
0277 |   gap: 12px;
0278 |   flex-wrap: wrap;
0279 | }
0280 | .legend span {
0281 |   display: flex;
0282 |   align-items: center;
0283 |   gap: 4px;
0284 | }
0285 | .legend i {
0286 |   width: 7px;
0287 |   height: 7px;
0288 |   border-radius: 50%;
0289 |   background: var(--c);
0290 | }
0291 | #tileStatus {
0292 |   width: 100%;
0293 | }
0294 | .feed {
0295 |   display: flex;
0296 |   flex-direction: column;
0297 | }
0298 | #eventsList {
0299 |   height: 490px;
0300 |   overflow: auto;
0301 |   flex: 1;
0302 | }
0303 | .incident {
0304 |   display: block;
0305 |   width: 100%;
0306 |   border: 0;
0307 |   border-bottom: 1px solid var(--line);
0308 |   border-radius: 0;
0309 |   padding: 14px 16px;
0310 |   text-align: left;
0311 |   background: transparent;
0312 | }
0313 | .incident:hover {
0314 |   background: #f1f2e9;
0315 | }
0316 | .incident-top {
0317 |   display: flex;
0318 |   justify-content: space-between;
0319 |   gap: 8px;
0320 |   align-items: flex-start;
0321 | }
0322 | .incident strong {
0323 |   font-size: 12px;
0324 | }
0325 | .badge {
0326 |   font-size: 8px;
0327 |   font-weight: 750;
0328 |   letter-spacing: 0.6px;
0329 |   text-transform: uppercase;
0330 |   color: var(--hazard);
0331 |   background: color-mix(in srgb, var(--hazard) 9%, white);
0332 |   border: 1px solid color-mix(in srgb, var(--hazard) 28%, white);
0333 |   padding: 2px 5px;
0334 |   border-radius: 2px;
0335 |   white-space: nowrap;
0336 | }
0337 | .incident p {
0338 |   font-size: 11px;
0339 |   margin: 6px 0;
0340 |   line-height: 1.6;
0341 |   display: -webkit-box;
0342 |   -webkit-line-clamp: 2;
0343 |   -webkit-box-orient: vertical;
0344 |   overflow: hidden;
0345 | }
0346 | .incident small {
0347 |   display: block;
0348 |   font-size: 9px;
0349 |   color: var(--muted);
0350 | }
0351 | .mode {
0352 |   font-weight: 750;
0353 |   text-transform: uppercase;
0354 |   color: #785a27;
0355 | }
0356 | .empty {
0357 |   padding: 30px;
0358 |   text-align: center;
0359 |   color: var(--muted);
0360 | }
0361 | #more {
0362 |   border: 0;
0363 |   border-top: 1px solid var(--line);
0364 |   font-size: 11px;
0365 | }
0366 | .bottom-grid {
0367 |   display: grid;
0368 |   grid-template-columns: 1.2fr 1fr 1fr;
0369 |   gap: 18px;
0370 |   margin: 22px 0;
0371 | }
0372 | .bottom-grid article {
0373 |   padding-bottom: 15px;
0374 | }
0375 | .bottom-grid article > a {
0376 |   margin: 16px;
0377 |   font-size: 11px;
0378 | }
0379 | .note {
0380 |   font-size: 10px;
0381 |   color: var(--muted);
0382 |   margin: 12px 16px;
0383 |   line-height: 1.7;
0384 | }
0385 | .timeline {
0386 |   height: 110px;
0387 |   display: flex;
0388 |   align-items: flex-end;
0389 |   gap: 3px;
0390 |   padding: 16px;
0391 |   overflow: hidden;
0392 | }
0393 | .timeline .bar {
0394 |   background: #65765e;
0395 |   min-width: 2px;
0396 |   flex: 1;
0397 |   max-width: 32px;
0398 |   border-radius: 1px 1px 0 0;
0399 | }
0400 | .single-bin {
0401 |   display: flex;
0402 |   align-items: baseline;
0403 |   gap: 14px;
0404 |   align-self: center;
0405 | }
0406 | .single-bin strong {
0407 |   font:
0408 |     38px Georgia,
0409 |     serif;
0410 | }
0411 | .single-bin span {
0412 |   font-size: 11px;
0413 |   color: var(--muted);
0414 | }
0415 | .distribution {
0416 |   display: flex;
0417 |   align-items: center;
0418 |   gap: 9px;
0419 |   margin: 7px 16px;
0420 |   font-size: 11px;
0421 | }
0422 | .distribution .track {
0423 |   height: 5px;
0424 |   background: #eceee5;
0425 |   flex: 1;
0426 | }
0427 | .distribution .fill {
0428 |   height: 100%;
0429 |   background: var(--hazard);
0430 | }
0431 | .source-row {
0432 |   margin: 12px 16px;
0433 |   padding-bottom: 9px;
0434 |   border-bottom: 1px solid var(--line);
0435 |   font-size: 11px;
0436 |   display: flex;
0437 |   justify-content: space-between;
0438 |   gap: 12px;
0439 | }
0440 | .source-row small {
0441 |   display: block;
0442 |   color: var(--muted);
0443 |   font-size: 9px;
0444 | }
0445 | dl {
0446 |   margin: 12px 16px;
0447 |   font-size: 11px;
0448 | }
0449 | dl div {
0450 |   display: flex;
0451 |   justify-content: space-between;
0452 |   gap: 8px;
0453 |   border-bottom: 1px solid var(--line);
0454 |   padding: 7px 0;
0455 | }
0456 | dd {
0457 |   margin: 0;
0458 |   text-align: right;
0459 | }
0460 | footer {
0461 |   font-size: 9px;
0462 |   letter-spacing: 1px;
0463 |   color: var(--muted);
0464 |   padding: 20px 0;
0465 |   border-top: 1px solid var(--line);
0466 |   display: flex;
0467 |   justify-content: space-between;
0468 |   gap: 20px;
0469 | }
0470 | .skip {
0471 |   position: absolute;
0472 |   left: -9999px;
0473 | }
0474 | .skip:focus {
0475 |   left: 20px;
0476 |   top: 10px;
0477 |   z-index: 9999;
0478 |   background: white;
0479 |   padding: 15px;
0480 | }
0481 | dialog {
0482 |   max-width: 620px;
0483 |   width: calc(100% - 32px);
0484 |   border: 1px solid var(--line);
0485 |   background: var(--surface);
0486 |   color: var(--ink);
0487 |   border-radius: 4px;
0488 |   padding: 28px;
0489 |   max-height: 90vh;
0490 |   overflow: auto;
0491 | }
0492 | dialog::backdrop {
0493 |   background: #242b2666;
0494 | }
0495 | dialog h2 {
0496 |   font:
0497 |     28px Georgia,
0498 |     serif;
0499 |   margin: 12px 0;
0500 | }
0501 | dialog p {
0502 |   font-size: 12px;
0503 | }
0504 | dialog .close {
0505 |   float: right;
0506 |   font-size: 20px;
0507 |   padding: 0 8px;
0508 | }
0509 | dialog form {
0510 |   display: grid;
0511 |   gap: 13px;
0512 | }
0513 | .pair {
0514 |   display: grid;
0515 |   grid-template-columns: 1fr 1fr;
0516 |   gap: 14px;
0517 | }
0518 | .detail-text {
0519 |   white-space: pre-wrap;
0520 |   overflow-wrap: anywhere;
0521 | }
0522 | #error {
0523 |   background: #f6e4dd;
0524 |   color: #842e22;
0525 |   padding: 12px;
0526 |   margin-top: 12px;
0527 | }
0528 | #error[hidden] {
0529 |   display: none;
0530 | }
0531 | .cluster {
0532 |   border-radius: 50%;
0533 |   background: #394c3b;
0534 |   color: white;
0535 |   border: 3px solid #eef0e5;
0536 |   display: flex;
0537 |   align-items: center;
0538 |   justify-content: center;
0539 |   font-weight: 700;
0540 | }
0541 | .leaflet-container {
0542 |   font-family: inherit;
0543 | }
0544 | .leaflet-popup-content {
0545 |   font-size: 12px;
0546 | }
0547 | .leaflet-popup-content p {
0548 |   margin: 8px 0;
0549 | }
0550 | @media (min-width: 1700px) {
0551 |   main {
0552 |     max-width: 1900px;
0553 |     margin: auto;
0554 |   }
0555 |   #map {
0556 |     height: 580px;
0557 |   }
0558 |   #eventsList {
0559 |     height: 580px;
0560 |   }
0561 | }
0562 | @media (max-width: 1100px) {
0563 |   .filters {
0564 |     flex-wrap: wrap;
0565 |   }
0566 |   .filters label {
0567 |     flex: 1;
0568 |     min-width: 130px;
0569 |   }
0570 |   .filters .search {
0571 |     min-width: 200px;
0572 |   }
0573 |   .workspace {
0574 |     grid-template-columns: minmax(0, 1fr) 310px;
0575 |   }
0576 |   .header-right {
0577 |     gap: 10px;
0578 |   }
0579 |   .bottom-grid {
0580 |     grid-template-columns: 1fr 1fr;
0581 |   }
0582 |   .bottom-grid article:last-child {
0583 |     grid-column: 1/-1;
0584 |   }
0585 | }
0586 | @media (max-width: 760px) {
0587 |   header {
0588 |     align-items: flex-start;
0589 |     flex-wrap: wrap;
0590 |     padding: 15px;
0591 |   }
0592 |   .header-right {
0593 |     width: 100%;
0594 |     justify-content: space-between;
0595 |   }
0596 |   .brand {
0597 |     font-size: 17px;
0598 |   }
0599 |   main {
0600 |     padding: 22px 14px 0;
0601 |   }
0602 |   .title-row {
0603 |     display: block;
0604 |   }
0605 |   h1 {
0606 |     font-size: 35px;
0607 |   }
0608 |   .context {
0609 |     text-align: left;
0610 |     margin-top: 16px;
0611 |   }
0612 |   .overview {
0613 |     grid-template-columns: 1fr 1fr;
0614 |     gap: 18px;
0615 |   }
0616 |   .overview > div,
0617 |   .overview > div:first-child {
0618 |     padding: 0 12px;
0619 |   }
0620 |   .overview > div:nth-child(2) {
0621 |     border: 0;
0622 |   }
0623 |   .overview strong {
0624 |     font-size: 33px;
0625 |   }
0626 |   .overview strong.timestamp {
0627 |     font-size: 16px;
0628 |   }
0629 |   .workspace {
0630 |     grid-template-columns: 1fr;
0631 |   }
0632 |   .panel-head h2 span {
0633 |     display: none;
0634 |   }
0635 |   #map {
0636 |     height: 360px;
0637 |   }
0638 |   #eventsList {
0639 |     max-height: 420px;
0640 |   }
0641 |   .bottom-grid {
0642 |     grid-template-columns: 1fr;
0643 |   }
0644 |   .bottom-grid article:last-child {
0645 |     grid-column: auto;
0646 |   }
0647 |   footer {
0648 |     flex-direction: column;
0649 |     gap: 6px;
0650 |   }
0651 |   .pair {
0652 |     grid-template-columns: 1fr 1fr;
0653 |   }
0654 |   .filters {
0655 |     gap: 9px;
0656 |     padding: 12px;
0657 |   }
0658 | }
0659 | @media (prefers-reduced-motion: reduce) {
0660 |   *,
0661 |   *::before,
0662 |   *::after {
0663 |     scroll-behavior: auto !important;
0664 |     transition: none !important;
0665 |     animation: none !important;
0666 |   }
0667 | }
0668 | 
0669 | .trends {
0670 |   margin: 12px 16px;
0671 |   font-size: 11px;
0672 | }
0673 | .trends summary {
0674 |   cursor: pointer;
0675 |   font-weight: 600;
0676 | }
0677 | .trends > div {
0678 |   max-height: 240px;
0679 |   overflow: auto;
0680 | }
0681 | .trends table {
0682 |   border-collapse: collapse;
0683 |   width: 100%;
0684 |   font-size: 9px;
0685 |   white-space: nowrap;
0686 | }
0687 | .trends th,
0688 | .trends td {
0689 |   padding: 6px;
0690 |   border-bottom: 1px solid var(--line);
0691 |   text-align: left;
0692 | }
0693 | .feed {
0694 |   height: 615px;
0695 |   min-height: 0;
0696 | }
0697 | #eventsList {
0698 |   min-height: 0;
0699 | }
0700 | @media (min-width: 1700px) {
0701 |   .feed {
0702 |     height: 705px;
0703 |   }
0704 | }
0705 | @media (max-width: 760px) {
0706 |   .feed {
0707 |     height: 500px;
0708 |   }
0709 | }
```

### `docs/handbook/handbook.js`

[Open source](../../docs/handbook/handbook.js) · SHA-256 `8069b52d2f335d18f27f5c23b1b7e8dcf19b065325100d0ba6127443cafd9e3a`


```text
0001 | "use strict";
0002 | const docs = JSON.parse(document.getElementById("documents").textContent);
0003 | const reader = document.getElementById("reader");
0004 | const nav = document.getElementById("chapters");
0005 | function openChapter(path, scroll = true) {
0006 |   const entry = docs.find((d) => d.path === path) || docs[0];
0007 |   reader.innerHTML = entry.html;
0008 |   document.getElementById("chapterPath").textContent = entry.path;
0009 |   document.getElementById("canonical").href =
0010 |     location.protocol === "file:"
0011 |       ? "../" + entry.path
0012 |       : "/documentation/" + entry.path;
0013 |   nav
0014 |     .querySelectorAll("button")
0015 |     .forEach((b) =>
0016 |       b.setAttribute("aria-current", String(b.dataset.path === entry.path)),
0017 |     );
0018 |   if (scroll)
0019 |     document
0020 |       .querySelector(".reading-head")
0021 |       .scrollIntoView({ behavior: "auto", block: "start" });
0022 |   if (window.MathJax?.typesetPromise)
0023 |     window.MathJax.typesetPromise([reader]).catch(() => {});
0024 | }
0025 | for (const entry of docs) {
0026 |   const button = document.createElement("button");
0027 |   button.textContent = entry.title;
0028 |   button.dataset.path = entry.path;
0029 |   button.addEventListener("click", () => {
0030 |     openChapter(entry.path);
0031 |     history.replaceState(null, "", "#" + encodeURIComponent(entry.path));
0032 |   });
0033 |   nav.append(button);
0034 | }
0035 | document.querySelectorAll("[data-chapter]").forEach((b) =>
0036 |   b.addEventListener("click", (event) => {
0037 |     event.preventDefault();
0038 |     openChapter(b.dataset.chapter);
0039 |   }),
0040 | );
0041 | const stages = [
0042 |   [
0043 |     "Source",
0044 |     "A labelled synthetic flood observation is constructed with ID DEMO-a1449ee3220b471f, current UTC time, coordinates and telemetry. This is controlled input, not a live disaster report.",
0045 |     "02_SOURCES_AND_GENERATION",
0046 |   ],
0047 |   [
0048 |     "API",
0049 |     "Pydantic validates the event. FastAPI serializes JSON and publishes with the location key. A delivery callback, not local enqueue, determines HTTP success.",
0050 |     "03_API_AND_SCHEMAS",
0051 |   ],
0052 |   [
0053 |     "Kafka",
0054 |     "This observed run acknowledged partition 0, offset 81. This establishes broker acceptance. It does not prove downstream persistence.",
0055 |     "04_KAFKA_CONSUMER",
0056 |   ],
0057 |   [
0058 |     "Consumer / ML",
0059 |     "The consumer validates again and obtains text/telemetry predictions. Confidence is uncalibrated; source severity and model output remain separate.",
0060 |     "09_ML_INFERENCE_AND_ANOMALIES",
0061 |   ],
0062 |   [
0063 |     "Database",
0064 |     "One transaction inserts event, report and two prediction records. First-observation identity prevents conflicting retries from replacing facts.",
0065 |     "05_POSTGRESQL",
0066 |   ],
0067 |   [
0068 |     "Raw / commit",
0069 |     "Raw Parquet retains the supported enriched payload. Only after its PUT succeeds does the consumer commit the Kafka source offset.",
0070 |     "07_MINIO_PARQUET_REPLAY",
0071 |   ],
0072 |   [
0073 |     "Spark",
0074 |     "An independent checkpointed query writes processed Parquet. A second query emits finalized sliding-window counts. Watermark delay means this is not instantaneous.",
0075 |     "06_SPARK_STREAMING",
0076 |   ],
0077 |   [
0078 |     "Dashboard",
0079 |     "The browser queries PostgreSQL-backed routes and refreshes on SSE notifications. The map/register share persisted records; form success alone cannot certify Spark completion.",
0080 |     "10_DASHBOARD",
0081 |   ],
0082 | ];
0083 | let stageIndex = 0;
0084 | function showStage(index) {
0085 |   stageIndex = index;
0086 |   document
0087 |     .querySelectorAll("#steps button")
0088 |     .forEach((b, i) => b.setAttribute("aria-pressed", String(i === index)));
0089 |   const p = document.createElement("p");
0090 |   p.textContent = stages[index][1];
0091 |   const link = document.createElement("button");
0092 |   link.textContent = "Read the implementation →";
0093 |   link.className = "stage-link";
0094 |   link.addEventListener("click", () =>
0095 |     openChapter("CODE_WALKTHROUGH/" + stages[index][2] + ".md"),
0096 |   );
0097 |   link.style.cssText =
0098 |     "background:none;border:0;padding:0;color:var(--accent);cursor:pointer";
0099 |   document.getElementById("journeyDetail").replaceChildren(p, link);
0100 | }
0101 | stages.forEach(([name], index) => {
0102 |   const button = document.createElement("button");
0103 |   button.textContent = index + 1 + ". " + name;
0104 |   button.addEventListener("click", () => showStage(index));
0105 |   document.getElementById("steps").append(button);
0106 | });
0107 | document
0108 |   .getElementById("next")
0109 |   .addEventListener("click", () => showStage((stageIndex + 1) % stages.length));
0110 | showStage(0);
0111 | document.getElementById("search").addEventListener("input", (e) => {
0112 |   const value = e.target.value.trim().toLowerCase();
0113 |   const output = document.getElementById("searchResults");
0114 |   output.replaceChildren();
0115 |   if (value.length < 2) return;
0116 |   const results = docs.filter((d) => d.search.toLowerCase().includes(value));
0117 |   const count = document.createElement("p");
0118 |   count.textContent = results.length + " matching chapters";
0119 |   output.append(count);
0120 |   for (const entry of results.slice(0, 12)) {
0121 |     const b = document.createElement("button");
0122 |     b.textContent = entry.title;
0123 |     const small = document.createElement("small");
0124 |     small.textContent = entry.path;
0125 |     b.append(small);
0126 |     b.addEventListener("click", () => openChapter(entry.path));
0127 |     output.append(b);
0128 |   }
0129 | });
0130 | let initial;
0131 | try {
0132 |   initial = decodeURIComponent(location.hash.slice(1));
0133 | } catch {
0134 |   initial = "SYSTEM_ARCHITECTURE.md";
0135 | }
0136 | openChapter(initial || "SYSTEM_ARCHITECTURE.md", false);
```

### `docs/handbook/handbook.css`

[Open source](../../docs/handbook/handbook.css) · SHA-256 `0fa0afca55384b3cc80881f0ca82328fe32f81dcaf92711d34f7487524504cb6`


```text
0001 | :root {
0002 |   --paper: #fcfaf4;
0003 |   --ink: #2c302d;
0004 |   --muted: #676e66;
0005 |   --accent: #a3432e;
0006 |   --line: #dedfd5;
0007 | }
0008 | * {
0009 |   box-sizing: border-box;
0010 | }
0011 | body {
0012 |   background: var(--paper);
0013 |   color: var(--ink);
0014 |   font:
0015 |     15px/1.75 system-ui,
0016 |     Segoe UI,
0017 |     sans-serif;
0018 |   margin: 0;
0019 | }
0020 | header {
0021 |   padding: 18px 3%;
0022 |   border-bottom: 1px solid var(--line);
0023 |   display: flex;
0024 |   align-items: center;
0025 |   justify-content: space-between;
0026 |   gap: 20px;
0027 |   font-size: 10px;
0028 |   letter-spacing: 1px;
0029 | }
0030 | .wordmark {
0031 |   font-weight: 800;
0032 |   font-size: 16px;
0033 |   text-decoration: none;
0034 | }
0035 | a {
0036 |   color: var(--accent);
0037 |   text-underline-offset: 4px;
0038 | }
0039 | .layout {
0040 |   display: grid;
0041 |   grid-template-columns: 275px minmax(0, 1fr);
0042 |   max-width: 1600px;
0043 |   margin: auto;
0044 | }
0045 | aside {
0046 |   padding: 30px 24px;
0047 |   border-right: 1px solid var(--line);
0048 |   position: sticky;
0049 |   top: 0;
0050 |   align-self: start;
0051 |   max-height: 100vh;
0052 |   overflow: auto;
0053 | }
0054 | .kicker {
0055 |   font-size: 10px;
0056 |   letter-spacing: 1.8px;
0057 |   font-weight: 700;
0058 |   color: var(--muted);
0059 | }
0060 | aside label {
0061 |   font-size: 11px;
0062 |   display: block;
0063 | }
0064 | input {
0065 |   font: inherit;
0066 |   font-size: 12px;
0067 |   border: 1px solid #b8bdaf;
0068 |   background: white;
0069 |   padding: 10px;
0070 |   width: 100%;
0071 |   margin: 8px 0 18px;
0072 |   border-radius: 2px;
0073 | }
0074 | nav button {
0075 |   display: block;
0076 |   border: 0;
0077 |   background: none;
0078 |   text-align: left;
0079 |   padding: 9px 10px;
0080 |   font: 12px/1.5 system-ui;
0081 |   width: 100%;
0082 |   cursor: pointer;
0083 |   color: var(--ink);
0084 |   border-left: 2px solid transparent;
0085 | }
0086 | nav button[aria-current="true"] {
0087 |   border-color: var(--accent);
0088 |   color: var(--accent);
0089 |   background: #efe9dd;
0090 | }
0091 | nav button:hover {
0092 |   background: #eeeee4;
0093 | }
0094 | main {
0095 |   max-width: 1150px;
0096 |   padding: 40px 6% 20px;
0097 |   min-width: 0;
0098 | }
0099 | .intro {
0100 |   max-width: 750px;
0101 |   margin: 0 0 35px;
0102 | }
0103 | h1 {
0104 |   font:
0105 |     48px/1.1 Georgia,
0106 |     serif;
0107 |   letter-spacing: -1.4px;
0108 |   margin: 14px 0 22px;
0109 | }
0110 | h1 em {
0111 |   color: var(--accent);
0112 |   font-weight: 400;
0113 | }
0114 | .intro > p:last-child {
0115 |   max-width: 640px;
0116 |   color: var(--muted);
0117 | }
0118 | h2 {
0119 |   font:
0120 |     27px/1.25 Georgia,
0121 |     serif;
0122 |   margin: 30px 0 16px;
0123 | }
0124 | h3 {
0125 |   font-size: 17px;
0126 |   line-height: 1.5;
0127 |   margin: 30px 0 12px;
0128 | }
0129 | h4 {
0130 |   font-size: 15px;
0131 | }
0132 | .components {
0133 |   display: grid;
0134 |   grid-template-columns: 1fr 20px 1fr 20px 1fr;
0135 |   gap: 8px;
0136 |   align-items: center;
0137 | }
0138 | .components > button {
0139 |   padding: 16px 12px;
0140 |   border: 1px solid var(--line);
0141 |   background: #f0eee4;
0142 |   text-align: left;
0143 |   font-weight: 650;
0144 |   min-height: 85px;
0145 |   border-radius: 3px;
0146 |   cursor: pointer;
0147 | }
0148 | .components small {
0149 |   display: block;
0150 |   font-weight: 400;
0151 |   color: var(--muted);
0152 |   font-size: 10px;
0153 |   line-height: 1.6;
0154 |   margin-top: 5px;
0155 | }
0156 | .components > span {
0157 |   text-align: center;
0158 |   color: var(--accent);
0159 | }
0160 | button:hover {
0161 |   filter: brightness(0.97);
0162 | }
0163 | :focus-visible {
0164 |   outline: 3px solid #517461;
0165 |   outline-offset: 3px;
0166 | }
0167 | .journey {
0168 |   border-top: 1px solid var(--line);
0169 |   border-bottom: 1px solid var(--line);
0170 |   margin: 35px 0 20px;
0171 |   padding-bottom: 20px;
0172 | }
0173 | .section-head {
0174 |   display: flex;
0175 |   align-items: center;
0176 |   justify-content: space-between;
0177 |   gap: 20px;
0178 | }
0179 | .section-head span {
0180 |   font-size: 9px;
0181 |   color: var(--muted);
0182 | }
0183 | #steps {
0184 |   display: flex;
0185 |   gap: 6px;
0186 |   flex-wrap: wrap;
0187 | }
0188 | #steps button {
0189 |   border: 1px solid var(--line);
0190 |   background: white;
0191 |   color: var(--ink);
0192 |   padding: 6px 10px;
0193 |   font-size: 11px;
0194 |   border-radius: 2px;
0195 |   cursor: pointer;
0196 | }
0197 | #steps button[aria-pressed="true"] {
0198 |   background: var(--accent);
0199 |   color: white;
0200 | }
0201 | #journeyDetail {
0202 |   padding: 14px 0;
0203 |   font-size: 13px;
0204 |   min-height: 100px;
0205 | }
0206 | #next {
0207 |   border: 0;
0208 |   background: none;
0209 |   color: var(--accent);
0210 |   padding: 0;
0211 |   font: 12px system-ui;
0212 |   cursor: pointer;
0213 | }
0214 | details {
0215 |   padding: 16px 0;
0216 |   border-bottom: 1px solid var(--line);
0217 | }
0218 | summary {
0219 |   cursor: pointer;
0220 |   font-weight: 650;
0221 |   font-size: 14px;
0222 |   line-height: 1.6;
0223 | }
0224 | .reading-head {
0225 |   display: flex;
0226 |   align-items: center;
0227 |   justify-content: space-between;
0228 |   gap: 20px;
0229 |   border-top: 3px solid var(--ink);
0230 |   padding-top: 18px;
0231 |   margin-top: 35px;
0232 | }
0233 | .reading-head a {
0234 |   font-size: 11px;
0235 |   white-space: nowrap;
0236 | }
0237 | #chapterPath {
0238 |   overflow-wrap: anywhere;
0239 | }
0240 | article {
0241 |   overflow-wrap: anywhere;
0242 | }
0243 | article h1 {
0244 |   font-size: 36px;
0245 |   letter-spacing: -0.7px;
0246 | }
0247 | article p,
0248 | article li {
0249 |   max-width: 85ch;
0250 |   font-size: 14px;
0251 | }
0252 | article pre {
0253 |   background: #eeeee5;
0254 |   border: 1px solid var(--line);
0255 |   padding: 18px;
0256 |   overflow: auto;
0257 |   max-height: 650px;
0258 |   font:
0259 |     11px/1.7 Consolas,
0260 |     monospace;
0261 |   white-space: pre;
0262 |   overflow-wrap: normal;
0263 | }
0264 | code {
0265 |   font:
0266 |     12px Consolas,
0267 |     monospace;
0268 |   background: #eeeee5;
0269 |   padding: 2px 4px;
0270 | }
0271 | pre code {
0272 |   padding: 0;
0273 |   font: inherit;
0274 | }
0275 | table {
0276 |   border-collapse: collapse;
0277 |   font-size: 11px;
0278 |   line-height: 1.6;
0279 |   width: 100%;
0280 |   min-width: 600px;
0281 | }
0282 | td,
0283 | th {
0284 |   border: 1px solid var(--line);
0285 |   padding: 9px;
0286 |   vertical-align: top;
0287 |   text-align: left;
0288 | }
0289 | th {
0290 |   background: #ecece2;
0291 | }
0292 | .table-wrap {
0293 |   overflow: auto;
0294 | }
0295 | footer {
0296 |   font-size: 10px;
0297 |   color: var(--muted);
0298 |   border-top: 1px solid var(--line);
0299 |   margin-top: 40px;
0300 |   padding: 20px 0;
0301 | }
0302 | #searchResults button {
0303 |   display: block;
0304 |   text-align: left;
0305 |   border: 0;
0306 |   background: #eeeee5;
0307 |   padding: 10px;
0308 |   font-size: 11px;
0309 |   cursor: pointer;
0310 |   width: 100%;
0311 |   margin: 4px 0;
0312 | }
0313 | #searchResults small {
0314 |   display: block;
0315 |   color: var(--muted);
0316 | }
0317 | .concepts {
0318 |   background: #f2eee2;
0319 |   padding: 16px;
0320 | }
0321 | .concepts h3 {
0322 |   margin: 18px 0 5px;
0323 | }
0324 | .concepts p {
0325 |   font-size: 12px;
0326 |   margin: 0;
0327 | }
0328 | mjx-container {
0329 |   max-width: 100%;
0330 |   overflow-x: auto;
0331 |   overflow-y: hidden;
0332 | }
0333 | @media (max-width: 900px) {
0334 |   .layout {
0335 |     grid-template-columns: 220px minmax(0, 1fr);
0336 |   }
0337 |   main {
0338 |     padding: 25px 5%;
0339 |   }
0340 |   h1 {
0341 |     font-size: 39px;
0342 |   }
0343 |   .components {
0344 |     grid-template-columns: 1fr;
0345 |   }
0346 |   .components > span {
0347 |     display: none;
0348 |   }
0349 |   .section-head {
0350 |     display: block;
0351 |   }
0352 |   .section-head span {
0353 |     display: block;
0354 |     margin-bottom: 14px;
0355 |   }
0356 |   .reading-head {
0357 |     display: block;
0358 |   }
0359 | }
0360 | @media (max-width: 650px) {
0361 |   header {
0362 |     flex-wrap: wrap;
0363 |     gap: 12px;
0364 |   }
0365 |   header > span {
0366 |     display: none;
0367 |   }
0368 |   .layout {
0369 |     display: block;
0370 |   }
0371 |   aside {
0372 |     position: static;
0373 |     max-height: 260px;
0374 |     border-right: 0;
0375 |     border-bottom: 1px solid var(--line);
0376 |     padding: 20px;
0377 |   }
0378 |   main {
0379 |     padding: 24px 18px;
0380 |   }
0381 |   nav {
0382 |     display: grid;
0383 |     grid-template-columns: 1fr 1fr;
0384 |   }
0385 |   h1 {
0386 |     font-size: 36px;
0387 |   }
0388 |   article h1 {
0389 |     font-size: 30px;
0390 |   }
0391 |   .components {
0392 |     grid-template-columns: 1fr 1fr;
0393 |   }
0394 |   .components button {
0395 |     font-size: 11px;
0396 |     padding: 10px;
0397 |   }
0398 |   #journeyDetail {
0399 |     min-height: 150px;
0400 |   }
0401 | }
0402 | @media (prefers-reduced-motion: reduce) {
0403 |   * {
0404 |     scroll-behavior: auto !important;
0405 |     animation: none !important;
0406 |   }
0407 | }
0408 | .diagram-wrap {
0409 |   max-width: 100%;
0410 |   overflow: auto;
0411 |   border: 1px solid var(--line);
0412 |   background: #f4f1e7;
0413 |   margin-bottom: 12px;
0414 | }
0415 | .architecture {
0416 |   width: 100%;
0417 |   min-width: 760px;
0418 |   display: block;
0419 | }
0420 | .architecture rect {
0421 |   fill: #fffdfa;
0422 |   stroke: #bfc6b7;
0423 | }
0424 | .architecture text {
0425 |   font: 13px system-ui;
0426 |   fill: #2c302d;
0427 | }
0428 | .architecture a:hover rect,
0429 | .architecture a:focus rect {
0430 |   fill: #efe2ce;
0431 |   stroke: #a3432e;
0432 | }
0433 | .diagram-note {
0434 |   font-size: 10px;
0435 |   color: var(--muted);
0436 |   margin-bottom: 22px;
0437 | }
```

### `scripts/build_handbook.py`

[Open source](../../scripts/build_handbook.py) · SHA-256 `ca69c803ad2fa998eff0bc5cc33cfd58deb2363bf358a378d3365971cd9ce9bd`

Dependencies: `from __future__ import annotations`; `import html`; `import json`; `import re`; `from pathlib import Path`

- `inline`: lines 30–43. Read its control flow with the module guide above.

- `link`: lines 35–41. Read its control flow with the module guide above.

- `render`: lines 46–136. Read its control flow with the module guide above.

- `flush`: lines 55–58. Read its control flow with the module guide above.

- `close_list`: lines 60–64. Read its control flow with the module guide above.

- `flush_table`: lines 66–84. Read its control flow with the module guide above.

- `architecture_svg`: lines 139–178. A clickable diagram of actual independent operational and lake branches.

- `build`: lines 181–212. Read its control flow with the module guide above.


```text
0001 | """Build a dependency-free local handbook from canonical Markdown documents."""
0002 | 
0003 | from __future__ import annotations
0004 | import html
0005 | import json
0006 | import re
0007 | from pathlib import Path
0008 | 
0009 | ROOT = Path(__file__).resolve().parents[1]
0010 | DOCS = ROOT / "docs"
0011 | OUT = DOCS / "handbook"
0012 | PAGES = [
0013 |     "SYSTEM_ARCHITECTURE.md",
0014 |     "END_TO_END_TRACE.md",
0015 |     "CONCEPTS_AND_FORMULAS.md",
0016 |     "RUNBOOK.md",
0017 |     "FAILURE_SCENARIOS.md",
0018 |     "PERFORMANCE_AND_EXPERIMENTS.md",
0019 |     "VIVA_PREPARATION.md",
0020 |     "PROJECT_REPORT.md",
0021 |     "VERIFICATION.md",
0022 |     "IMPLEMENTATION_AUDIT.md",
0023 |     "CODE_WALKTHROUGH/README.md",
0024 | ] + [
0025 |     str(path.relative_to(DOCS).as_posix())
0026 |     for path in sorted((DOCS / "CODE_WALKTHROUGH").glob("[0-9]*.md"))
0027 | ]
0028 | 
0029 | 
0030 | def inline(text):
0031 |     text = html.escape(text)
0032 |     text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
0033 |     text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
0034 | 
0035 |     def link(m):
0036 |         label, url = m.groups()
0037 |         if url.startswith(("https://", "http://")):
0038 |             return (
0039 |                 f'<a href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'
0040 |             )
0041 |         return f'<span title="See canonical Markdown for local source links">{label}</span>'
0042 | 
0043 |     return re.sub(r"\[([^]]+)\]\(([^)]+)\)", link, text)
0044 | 
0045 | 
0046 | def render(text):
0047 |     lines = text.splitlines()
0048 |     result = []
0049 |     paragraph = []
0050 |     fence = None
0051 |     code = []
0052 |     table = []
0053 |     listing = False
0054 | 
0055 |     def flush():
0056 |         if paragraph:
0057 |             result.append("<p>" + inline(" ".join(paragraph)) + "</p>")
0058 |             paragraph.clear()
0059 | 
0060 |     def close_list():
0061 |         nonlocal listing
0062 |         if listing:
0063 |             result.append("</ul>")
0064 |             listing = False
0065 | 
0066 |     def flush_table():
0067 |         if table:
0068 |             rows = []
0069 |             for index, line in enumerate(table):
0070 |                 cells = line.strip().strip("|").split("|")
0071 |                 if all(re.fullmatch(r"[: -]+", cell) for cell in cells):
0072 |                     continue
0073 |                 tag = "th" if index == 0 else "td"
0074 |                 rows.append(
0075 |                     "<tr>"
0076 |                     + "".join(
0077 |                         f"<{tag}>{inline(cell.strip())}</{tag}>" for cell in cells
0078 |                     )
0079 |                     + "</tr>"
0080 |                 )
0081 |             result.append(
0082 |                 '<div class="table-wrap"><table>' + "".join(rows) + "</table></div>"
0083 |             )
0084 |             table.clear()
0085 | 
0086 |     for line in lines:
0087 |         if line.startswith("```"):
0088 |             flush()
0089 |             close_list()
0090 |             flush_table()
0091 |             if fence is None:
0092 |                 fence = line[3:]
0093 |                 code = []
0094 |             else:
0095 |                 result.append(
0096 |                     "<pre><code>" + html.escape("\n".join(code)) + "</code></pre>"
0097 |                 )
0098 |                 fence = None
0099 |             continue
0100 |         if fence is not None:
0101 |             code.append(line)
0102 |             continue
0103 |         if line.startswith("<!--"):
0104 |             continue
0105 |         if line.startswith("|"):
0106 |             flush()
0107 |             close_list()
0108 |             table.append(line)
0109 |             continue
0110 |         flush_table()
0111 |         if not line.strip():
0112 |             flush()
0113 |             close_list()
0114 |             continue
0115 |         match = re.match(r"^(#{1,6}) (.+)", line)
0116 |         if match:
0117 |             flush()
0118 |             close_list()
0119 |             level = len(match[1])
0120 |             title = match[2]
0121 |             anchor = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
0122 |             result.append(f'<h{level} id="{anchor}">{inline(title)}</h{level}>')
0123 |             continue
0124 |         if line.startswith("- "):
0125 |             flush()
0126 |             if not listing:
0127 |                 result.append("<ul>")
0128 |                 listing = True
0129 |             result.append("<li>" + inline(line[2:]) + "</li>")
0130 |             continue
0131 |         close_list()
0132 |         paragraph.append(line)
0133 |     flush()
0134 |     close_list()
0135 |     flush_table()
0136 |     return "\n".join(result)
0137 | 
0138 | 
0139 | def architecture_svg() -> str:
0140 |     """A clickable diagram of actual independent operational and lake branches."""
0141 |     nodes = [
0142 |         (20, 30, "Sources", "02_SOURCES_AND_GENERATION"),
0143 |         (200, 30, "FastAPI", "03_API_AND_SCHEMAS"),
0144 |         (380, 30, "Kafka", "04_KAFKA_CONSUMER"),
0145 |         (580, 30, "Consumer + ML", "04_KAFKA_CONSUMER"),
0146 |         (780, 30, "PostgreSQL", "05_POSTGRESQL"),
0147 |         (980, 30, "API / dashboard", "10_DASHBOARD"),
0148 |         (780, 120, "Raw Parquet", "07_MINIO_PARQUET_REPLAY"),
0149 |         (980, 120, "Offset commit", "04_KAFKA_CONSUMER"),
0150 |         (380, 220, "Spark dedup", "06_SPARK_STREAMING"),
0151 |         (580, 220, "Processed lake", "07_MINIO_PARQUET_REPLAY"),
0152 |         (780, 220, "Spark windows", "06_SPARK_STREAMING"),
0153 |         (980, 220, "Curated lake", "07_MINIO_PARQUET_REPLAY"),
0154 |     ]
0155 |     paths = [
0156 |         "M160 55 H200",
0157 |         "M340 55 H380",
0158 |         "M520 55 H580",
0159 |         "M720 55 H780",
0160 |         "M920 55 H980",
0161 |         "M850 80 V120",
0162 |         "M920 145 H980",
0163 |         "M450 80 V220",
0164 |         "M520 245 H580",
0165 |         "M720 245 H780",
0166 |         "M920 245 H980",
0167 |     ]
0168 |     diagram = '<div class="diagram-wrap"><svg class="architecture" viewBox="0 0 1140 300" role="group" aria-label="Actual pipeline branches. Select a component for its implementation."><defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0 L7 3.5 L0 7" fill="#a3432e"/></marker></defs>'
0169 |     diagram += "".join(
0170 |         f'<path d="{path}" fill="none" stroke="#a3432e" marker-end="url(#arrow)"/>'
0171 |         for path in paths
0172 |     )
0173 |     for x, y, label, chapter in nodes:
0174 |         diagram += f'<a href="#" data-chapter="CODE_WALKTHROUGH/{chapter}.md" aria-label="Read {label}"><rect x="{x}" y="{y}" width="140" height="50" rx="3"/><text x="{x + 70}" y="{y + 30}" text-anchor="middle">{label}</text></a>'
0175 |     return (
0176 |         diagram
0177 |         + '</svg></div><p class="diagram-note">Select a node to inspect its code. DB → raw → commit indicates consumer processing order; Spark independently reads Kafka. Every service still shares one host.</p>'
0178 |     )
0179 | 
0180 | 
0181 | def build():
0182 |     OUT.mkdir(exist_ok=True)
0183 |     entries = []
0184 |     for name in PAGES:
0185 |         path = DOCS / name
0186 |         if not path.exists():
0187 |             continue
0188 |         text = path.read_text(encoding="utf-8-sig")
0189 |         title = text.splitlines()[0].lstrip("# ")
0190 |         content = render(text)
0191 |         if name == "VIVA_PREPARATION.md":
0192 |             parts = re.split(r"^### (.+)$", text, flags=re.M)
0193 |             content = render(parts[0]) + "".join(
0194 |                 "<details><summary>"
0195 |                 + html.escape(parts[i])
0196 |                 + "</summary>"
0197 |                 + render(parts[i + 1])
0198 |                 + "</details>"
0199 |                 for i in range(1, len(parts) - 1, 2)
0200 |             )
0201 |         entries.append({"path": name, "title": title, "html": content, "search": text})
0202 |     data = json.dumps(entries, ensure_ascii=False).replace("<", "\\u003c")
0203 |     shell = (
0204 |         """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>CATACLYSM / Engineering field guide</title><link rel="stylesheet" href="handbook.css"><script defer src="handbook.js"></script><script>window.MathJax={tex:{inlineMath:[['\\\\(','\\\\)']],displayMath:[['\\\\[','\\\\]'],['$$','$$']]},options:{skipHtmlTags:['script','noscript','style','textarea','pre','code']}};</script><script defer src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script></head><body><header><a href="#" class="wordmark">C / FIELD GUIDE</a><span>CATACLYSM · THE ENGINEERING NOTEBOOK</span><a id="console" href="http://127.0.0.1:8000">Open console ↗</a></header><div class="layout"><aside><p class="kicker">READ / TRACE / QUESTION</p><label for="search">Find a concept or function</label><input type="search" id="search" placeholder="watermark, commit, TF-IDF…"><div id="searchResults" role="status"></div><nav id="chapters" aria-label="Learning chapters"></nav></aside><main><div class="intro"><p class="kicker">A SYSTEM YOU CAN EXPLAIN</p><h1>Follow the event.<br><em>Understand the decisions.</em></h1><p>Read the implementation, inspect its boundaries and challenge its evidence. This is a learning notebook, built from the project's canonical Markdown.</p></div><section aria-label="Interactive architecture"><h2>Choose a component</h2><div class="components"><button data-chapter="CODE_WALKTHROUGH/02_SOURCES_AND_GENERATION.md">01 / Observe<small>USGS · weather · simulation</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/03_API_AND_SCHEMAS.md">02 / Accept<small>Pydantic · FastAPI · producer</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/04_KAFKA_CONSUMER.md">03 / Persist<small>Kafka · consumer · transactions</small></button><button data-chapter="CODE_WALKTHROUGH/06_SPARK_STREAMING.md">04 / Analyze<small>Spark · watermarks · windows</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/07_MINIO_PARQUET_REPLAY.md">05 / Retain<small>PostgreSQL · MinIO · replay</small></button><span>→</span><button data-chapter="CODE_WALKTHROUGH/10_DASHBOARD.md">06 / Inspect<small>Predictions · map · monitoring</small></button></div></section><section class="journey"><div class="section-head"><h2>One event, eight boundaries</h2><span>ACTUAL SMOKE ID / DEMO-a1449ee3220b471f</span></div><div id="steps" role="group" aria-label="Event journey stages"></div><div id="journeyDetail" aria-live="polite"></div><button id="next">Next boundary →</button></section><details class="concepts"><summary>Before you read: three distinctions that matter</summary><h3>Acknowledged is not persisted</h3><p>POST 200 confirms Kafka delivery. Database visibility, raw archival and Kafka source commit follow separately.</p><h3>Report count is not disaster count</h3><p>Several observations can describe one hazard. Sliding windows overlap; summing their counts double-counts reports.</p><h3>Computed score is not real-world validity</h3><p>The tiny text holdout and synthetic severity labels do not establish emergency-response usefulness.</p></details><div class="reading-head"><p class="kicker" id="chapterPath"></p><a id="canonical">Open canonical Markdown ↗</a></div><article id="reader"></article><footer>Generated from canonical docs. Run python scripts/build_handbook.py after documentation changes. Formulas render with MathJax when online; plain LaTeX remains readable offline. No live telemetry is simulated here.</footer></main></div><script type="application/json" id="documents">"""
0205 |         + data
0206 |         + """</script></body></html>"""
0207 |     )
0208 |     shell = shell.replace(
0209 |         '<div class="components">', architecture_svg() + '<div class="components">', 1
0210 |     )
0211 |     (OUT / "index.html").write_text(shell, encoding="utf-8")
0212 |     print(f"Handbook built: {len(entries)} canonical chapters")
0213 | 
0214 | 
0215 | if __name__ == "__main__":
0216 |     build()
```
