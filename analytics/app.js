const hubLink = document.getElementById("hub-link");
if (hubLink) {
    hubLink.href = "/";
}

const daysEl = document.getElementById("days");
const symbolEl = document.getElementById("symbol");
const granularityEl = document.getElementById("granularity");
const statusNote = document.getElementById("status-note");
const kpiGrid = document.getElementById("kpi-grid");
const footer = document.getElementById("footer");
const exportBtn = document.getElementById("export-btn");
const refreshBtn = document.getElementById("refresh-btn");

const PLOTLY_LAYOUT = {
    paper_bgcolor: "#171a21",
    plot_bgcolor: "#171a21",
    font: { color: "#c7d0db", size: 12 },
    margin: { l: 48, r: 16, t: 16, b: 48 },
    legend: { orientation: "h", y: 1.08, x: 0 },
    xaxis: { gridcolor: "#2a2f3a", zerolinecolor: "#2a2f3a" },
    yaxis: { gridcolor: "#2a2f3a", zerolinecolor: "#2a2f3a" },
};

function queryParams() {
    const params = new URLSearchParams();
    params.set("days", daysEl.value);
    const symbol = symbolEl.value.trim().toUpperCase();
    if (symbol) {
        params.set("symbol", symbol);
    }
    return params;
}

async function fetchJson(path) {
    const response = await fetch(`${path}?${queryParams().toString()}`);
    if (!response.ok) {
        throw new Error(`${path} failed (${response.status})`);
    }
    return response.json();
}

function formatNumber(value) {
    if (value == null || Number.isNaN(Number(value))) {
        return "—";
    }
    return Number(value).toLocaleString();
}

function renderKpis(overview) {
    const cards = [
        ["Total events", overview.total_events],
        ["Valid entries", overview.valid_entries],
        ["Blocked entries", overview.valid_entry_blocked],
        ["Indicator blocks", overview.indicator_blocked],
        ["Order skips", overview.order_skips],
        ["Dry-run orders", overview.order_dry_runs],
        ["Live orders", overview.order_live_opens],
        ["Symbols", overview.symbols_seen],
    ];

    kpiGrid.innerHTML = cards
        .map(
            ([label, value]) => `
                <div class="kpi">
                    <div class="kpi-label">${label}</div>
                    <div class="kpi-value">${formatNumber(value)}</div>
                </div>
            `
        )
        .join("");
}

function renderBars(containerId, rows) {
    const container = document.getElementById(containerId);
    if (!rows.length) {
        container.innerHTML = `<div class="note">No data in this range.</div>`;
        return;
    }
    const max = Math.max(...rows.map((row) => row.count));
    container.innerHTML = rows
        .map((row) => {
            const width = max ? Math.max(4, (row.count / max) * 100) : 0;
            return `
                <div class="bar-row">
                    <div class="bar-label" title="${row.label}">${row.label}</div>
                    <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
                    <div class="bar-count">${row.count}</div>
                </div>
            `;
        })
        .join("");
}

function pivotSeries(rows) {
    const buckets = [];
    const bucketSet = new Set();
    const types = new Set();
    const matrix = {};

    rows.forEach((row) => {
        types.add(row.event_type);
        if (!bucketSet.has(row.bucket)) {
            bucketSet.add(row.bucket);
            buckets.push(row.bucket);
        }
        matrix[`${row.bucket}|${row.event_type}`] = row.count;
    });

    const traces = [...types].sort().map((eventType) => ({
        x: buckets,
        y: buckets.map((bucket) => matrix[`${bucket}|${eventType}`] || 0),
        name: eventType,
        type: "bar",
    }));

    return traces;
}

function renderTimeline(rows) {
    const traces = pivotSeries(rows);
    Plotly.newPlot(
        "timeline-chart",
        traces,
        {
            ...PLOTLY_LAYOUT,
            barmode: "stack",
            xaxis: { ...PLOTLY_LAYOUT.xaxis, title: granularityEl.value === "hourly" ? "Hour (UTC)" : "Day (UTC)" },
            yaxis: { ...PLOTLY_LAYOUT.yaxis, title: "Events" },
        },
        { responsive: true, displayModeBar: false }
    );
}

function renderRecent(rows) {
    const body = document.getElementById("recent-body");
    if (!rows.length) {
        body.innerHTML = `<tr><td colspan="9">No events yet.</td></tr>`;
        return;
    }
    body.innerHTML = rows
        .map(
            (row) => `
                <tr>
                    <td>${row.created_at || "—"}</td>
                    <td>${row.symbol || "—"}</td>
                    <td>${row.event_type || "—"}</td>
                    <td>${row.outcome || "—"}</td>
                    <td>${row.block_reason || "—"}</td>
                    <td>${row.signal || "—"}</td>
                    <td>${row.confidence ?? "—"}</td>
                    <td>${row.rsi ?? "—"}</td>
                    <td>${row.adx ?? row.htf_adx ?? "—"}</td>
                </tr>
            `
        )
        .join("");
}

function renderFeatureColumns(columns) {
    const container = document.getElementById("feature-columns");
    container.innerHTML = columns
        .map(
            (column) => `
                <div class="bar-row">
                    <div class="bar-label">${column}</div>
                    <div class="bar-track"><div class="bar-fill" style="width:100%; opacity:0.35"></div></div>
                    <div class="bar-count">col</div>
                </div>
            `
        )
        .join("");
}

async function refresh() {
    try {
        const health = await fetchJson("/api/health");
        if (!health.db_enabled) {
            statusNote.className = "note warn";
            statusNote.textContent =
                "DB_ENABLED=false — enable MySQL in .env and restart ./run-all.sh to collect decision_events.";
            return;
        }
        if (!health.db_ready) {
            statusNote.className = "note warn";
            statusNote.textContent = "MySQL is enabled but schema is not ready. Run python scripts/migrate_db.py.";
            return;
        }

        const overview = await fetchJson("/api/overview");
        if (overview.error) {
            statusNote.className = "note warn";
            statusNote.textContent = `Database error: ${overview.error}`;
            return;
        }

        renderKpis(overview);

        const seriesPath =
            granularityEl.value === "daily" ? "/api/series/daily" : "/api/series/hourly";
        const [series, eventTypes, blockReasons, symbols, recent, features] = await Promise.all([
            fetchJson(seriesPath),
            fetchJson("/api/breakdown/event_type"),
            fetchJson("/api/breakdown/block_reason"),
            fetchJson("/api/breakdown/symbol"),
            fetchJson("/api/recent"),
            fetchJson("/api/features"),
        ]);

        renderTimeline(series);
        renderBars("event-type-bars", eventTypes);
        renderBars("block-reason-bars", blockReasons);
        renderBars("symbol-bars", symbols);
        renderRecent(Array.isArray(recent) ? recent : recent.rows || []);
        renderFeatureColumns(features.columns || []);

        statusNote.className = "note";
        const rangeLabel = daysEl.options[daysEl.selectedIndex].text.toLowerCase();
        statusNote.textContent = `Showing ${formatNumber(overview.total_events)} events (${rangeLabel}). Data grows as the fleet runs with DB_ENABLED=true.`;
        footer.textContent = `Last refresh ${new Date().toLocaleString()} · Export up to 50k rows for ML pipelines.`;
    } catch (error) {
        statusNote.className = "note warn";
        statusNote.textContent = `Failed to load analytics: ${error.message}`;
    }
}

exportBtn.addEventListener("click", () => {
    window.location.href = `/api/export/features.csv?${queryParams().toString()}`;
});

refreshBtn.addEventListener("click", refresh);
daysEl.addEventListener("change", refresh);
symbolEl.addEventListener("change", refresh);
granularityEl.addEventListener("change", refresh);

refresh();
setInterval(refresh, 60000);
