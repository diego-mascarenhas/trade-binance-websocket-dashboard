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
const suggestionsBtn = document.getElementById("suggestions-btn");
const suggestionsNote = document.getElementById("suggestions-note");
const suggestionsContent = document.getElementById("suggestions-content");
let deepseekAvailable = false;
let lastSuggestions = [];

const PLOTLY_LAYOUT = {
    paper_bgcolor: "#171a21",
    plot_bgcolor: "#171a21",
    font: { color: "#c7d0db", size: 12 },
    margin: { l: 48, r: 16, t: 16, b: 48 },
    legend: { orientation: "h", y: 1.08, x: 0 },
    xaxis: { gridcolor: "#2a2f3a", zerolinecolor: "#2a2f3a" },
    yaxis: { gridcolor: "#2a2f3a", zerolinecolor: "#2a2f3a" },
};

/** Fixed colors per decision event — not Plotly’s default palette (which can look brown/orange). */
const EVENT_TYPE_COLORS = {
    valid_entry: "#22c55e",
    valid_entry_blocked: "#f59e0b",
    indicator_blocked: "#ef4444",
    order_skip: "#a855f7",
    order_dry_run: "#38bdf8",
    order_live_open: "#3b82f6",
};

function colorForEventType(eventType) {
    if (EVENT_TYPE_COLORS[eventType]) {
        return EVENT_TYPE_COLORS[eventType];
    }
    let hash = 0;
    const text = String(eventType || "other");
    for (let i = 0; i < text.length; i += 1) {
        hash = (hash * 31 + text.charCodeAt(i)) >>> 0;
    }
    const hue = hash % 360;
    return `hsl(${hue} 45% 52%)`;
}

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

    const traces = [...types].sort().map((eventType) => {
        const color = colorForEventType(eventType);
        return {
            x: buckets,
            y: buckets.map((bucket) => matrix[`${bucket}|${eventType}`] || 0),
            name: eventType,
            type: "bar",
            marker: { color },
        };
    });

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

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
}

function canApplySuggestion(item) {
    return Boolean(
        item &&
            item.symbol &&
            item.config_changes &&
            typeof item.config_changes === "object" &&
            Object.keys(item.config_changes).length > 0
    );
}

function renderSuggestionActions(item, index) {
    if (!canApplySuggestion(item)) {
        return `<div class="suggestion-actions"><span class="note" style="margin:0;font-size:0.75rem;">Sin override aplicable</span></div>`;
    }
    return `
        <div class="suggestion-actions">
            <button class="btn btn-primary" type="button" data-action="apply" data-index="${index}">Aplicar</button>
            <button class="btn" type="button" data-action="restore" data-index="${index}">Restaurar</button>
        </div>
    `;
}

function renderSuggestions(payload) {
    if (!payload || payload.error) {
        lastSuggestions = [];
        suggestionsContent.innerHTML = "";
        suggestionsNote.className = "note warn";
        suggestionsNote.textContent = payload?.error || "Could not load suggestions.";
        return;
    }

    lastSuggestions = payload.suggestions || [];

    const cacheHint = payload.cached
        ? ` (cached ${payload.cached_seconds_ago}s ago — use Analyze to refresh after cooldown)`
        : "";
    suggestionsNote.className = "note";
    suggestionsNote.textContent = `Model ${payload.model || "deepseek"} · ${payload.context_events || 0} events analyzed · ${payload.generated_at || ""}${cacheHint}`;

    const cards = lastSuggestions
        .map((item, index) => {
            const priority = (item.priority || "medium").toLowerCase();
            const symbol = item.symbol ? ` · ${escapeHtml(item.symbol)}` : "";
            const config = item.config_changes
                ? `<div class="suggestion-config">${escapeHtml(JSON.stringify(item.config_changes))}</div>`
                : "";
            return `
                <article class="suggestion-card priority-${priority}" data-index="${index}">
                    <div class="suggestion-body">
                        <div class="suggestion-title-row">
                            <span class="suggestion-badge">${escapeHtml(item.area || "GENERAL")}</span>
                            <span class="suggestion-badge">${escapeHtml(priority)}</span>
                            <span class="suggestion-title">${escapeHtml(item.title || "Suggestion")}${symbol}</span>
                        </div>
                        <div class="suggestion-detail">${escapeHtml(item.detail || "")}</div>
                        ${config}
                        <div class="suggestion-feedback" data-feedback="${index}"></div>
                    </div>
                    ${renderSuggestionActions(item, index)}
                </article>
            `;
        })
        .join("");

    const warnings = (payload.warnings || []).length
        ? `<div class="warning-list"><strong>Warnings</strong><ul>${payload.warnings
              .map((w) => `<li>${escapeHtml(w)}</li>`)
              .join("")}</ul></div>`
        : "";

    suggestionsContent.innerHTML = `
        <div class="suggestion-summary">${escapeHtml(payload.summary || "No summary returned.")}</div>
        <div class="suggestion-list">${cards || '<div class="note">No suggestions returned.</div>'}</div>
        ${warnings}
    `;
}

async function postJson(path, body) {
    const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });
    const payload = await response.json();
    if (!response.ok && !payload.error) {
        throw new Error(`${path} failed (${response.status})`);
    }
    return payload;
}

function setSuggestionFeedback(index, message, tone = "ok") {
    const card = suggestionsContent.querySelector(`.suggestion-card[data-index="${index}"]`);
    const feedback = suggestionsContent.querySelector(`[data-feedback="${index}"]`);
    if (feedback) {
        feedback.textContent = message;
        feedback.style.color = tone === "error" ? "#fca5a5" : tone === "info" ? "#93c5fd" : "#86efac";
    }
    if (card) {
        card.classList.remove("is-applied", "is-restored");
        if (tone === "ok") {
            card.classList.add("is-applied");
        } else if (tone === "info") {
            card.classList.add("is-restored");
        }
    }
}

async function applySuggestion(index) {
    const item = lastSuggestions[index];
    if (!canApplySuggestion(item)) {
        return;
    }
    const button = suggestionsContent.querySelector(`button[data-action="apply"][data-index="${index}"]`);
    if (button) {
        button.disabled = true;
        button.textContent = "Aplicando…";
    }
    try {
        const result = await postJson("/api/symbol-config/apply", {
            symbol: item.symbol,
            config_changes: item.config_changes,
            reason: `DeepSeek: ${item.title || "suggestion"}`,
        });
        if (!result.ok) {
            setSuggestionFeedback(index, result.error || "No se pudo aplicar.", "error");
            return;
        }
        setSuggestionFeedback(index, result.message || "Configuración aplicada.", "ok");
    } catch (error) {
        setSuggestionFeedback(index, error.message, "error");
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = "Aplicar";
        }
    }
}

async function restoreSuggestion(index) {
    const item = lastSuggestions[index];
    if (!canApplySuggestion(item)) {
        return;
    }
    const button = suggestionsContent.querySelector(`button[data-action="restore"][data-index="${index}"]`);
    if (button) {
        button.disabled = true;
        button.textContent = "Restaurando…";
    }
    try {
        const result = await postJson("/api/symbol-config/restore", {
            symbol: item.symbol,
            config_keys: Object.keys(item.config_changes),
            reason: `DeepSeek restore: ${item.title || "suggestion"}`,
        });
        if (!result.ok) {
            setSuggestionFeedback(index, result.error || "No se pudo restaurar.", "error");
            return;
        }
        setSuggestionFeedback(index, result.message || "Restaurado a defaults.", "info");
    } catch (error) {
        setSuggestionFeedback(index, error.message, "error");
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = "Restaurar";
        }
    }
}

async function loadSuggestions(force = false) {
    if (!deepseekAvailable) {
        suggestionsNote.className = "note warn";
        suggestionsNote.textContent =
            "DeepSeek disabled — set DEEPSEEK_ENABLED=true and DEEPSEEK_API_KEY in .env, then restart ./run-all.sh";
        return;
    }

    suggestionsBtn.disabled = true;
    suggestionsBtn.textContent = "Analyzing…";
    suggestionsNote.className = "note";
    suggestionsNote.textContent = "Calling DeepSeek with your decision_events summary…";

    try {
        const params = queryParams();
        if (force) {
            params.set("force", "1");
        }
        const response = await fetch(`/api/suggestions?${params.toString()}`);
        const payload = await response.json();
        renderSuggestions(payload);
    } catch (error) {
        suggestionsNote.className = "note warn";
        suggestionsNote.textContent = `Suggestions failed: ${error.message}`;
    } finally {
        suggestionsBtn.disabled = false;
        suggestionsBtn.textContent = "Analyze";
    }
}

async function refresh() {
    try {
        const health = await fetchJson("/api/health");
        deepseekAvailable = Boolean(health.deepseek_enabled);
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
        const total = overview.total_events || 0;
        const symbolsSeen = overview.symbols_seen || 0;
        const lastEvent = overview.last_event
            ? ` · last event ${overview.last_event.replace("T", " ").replace("+00:00", " UTC")}`
            : "";
        if (total > 0) {
            statusNote.textContent = `MySQL connected · ${formatNumber(total)} events (${rangeLabel}) · ${formatNumber(symbolsSeen)} symbol(s)${lastEvent}.`;
        } else {
            statusNote.textContent = `MySQL connected · no events in ${rangeLabel} yet. Keep the fleet running to collect decision_events.`;
        }
        footer.textContent = `Last refresh ${new Date().toLocaleString()} · Export up to 50k rows for ML pipelines.`;

        if (!deepseekAvailable) {
            suggestionsNote.className = "note";
            suggestionsNote.textContent =
                "Optional: set DEEPSEEK_ENABLED=true and DEEPSEEK_API_KEY in .env, then restart ./run-all.sh to enable AI suggestions.";
        } else if (!suggestionsContent.innerHTML) {
            suggestionsNote.className = "note";
            suggestionsNote.textContent =
                "Click Analyze to get DeepSeek config suggestions from your decision_events.";
        }
    } catch (error) {
        statusNote.className = "note warn";
        statusNote.textContent = `Failed to load analytics: ${error.message}`;
    }
}

exportBtn.addEventListener("click", () => {
    window.location.href = `/api/export/features.csv?${queryParams().toString()}`;
});

refreshBtn.addEventListener("click", refresh);
suggestionsBtn.addEventListener("click", () => loadSuggestions(true));
suggestionsContent.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) {
        return;
    }
    const index = Number(button.dataset.index);
    if (Number.isNaN(index)) {
        return;
    }
    if (button.dataset.action === "apply") {
        applySuggestion(index);
    } else if (button.dataset.action === "restore") {
        restoreSuggestion(index);
    }
});
daysEl.addEventListener("change", refresh);
symbolEl.addEventListener("change", refresh);
granularityEl.addEventListener("change", refresh);

refresh();
setInterval(refresh, 60000);
