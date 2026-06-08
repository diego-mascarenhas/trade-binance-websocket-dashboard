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
const exportOutcomesBtn = document.getElementById("export-outcomes-btn");
const refreshBtn = document.getElementById("refresh-btn");
const suggestionsBtn = document.getElementById("suggestions-btn");
const suggestionsApplyAllBtn = document.getElementById("suggestions-apply-all-btn");
const suggestionsRestoreAllBtn = document.getElementById("suggestions-restore-all-btn");
const suggestionsBulkFeedback = document.getElementById("suggestions-bulk-feedback");
const activeConfigStatusEl = document.getElementById("active-config-status");
const suggestionsNote = document.getElementById("suggestions-note");
const suggestionsContent = document.getElementById("suggestions-content");
const accountKpiGrid = document.getElementById("account-kpi-grid");
const accountNote = document.getElementById("account-note");
let deepseekAvailable = false;
let lastSuggestions = [];
let bulkApplied = false;
let activeConfigStatus = null;
/** @type {Map<string, number>} */
const symbolPorts = new Map();

async function loadSymbolPorts() {
    try {
        const response = await fetch("/pairs.json");
        if (!response.ok) {
            return;
        }
        const pairs = await response.json();
        if (!Array.isArray(pairs)) {
            return;
        }
        pairs.forEach((pair) => {
            const symbol = String(pair.symbol || "").toUpperCase();
            const port = Number(pair.port);
            if (symbol && Number.isFinite(port)) {
                symbolPorts.set(symbol, port);
            }
        });
    } catch {
        /* pairs.json optional */
    }
}

function symbolDashboardUrl(symbol) {
    const upper = String(symbol || "").trim().toUpperCase();
    if (!upper) {
        return null;
    }
    const port = symbolPorts.get(upper);
    if (!port) {
        return null;
    }
    const host = window.location.hostname || "127.0.0.1";
    return `http://${host}:${port}/`;
}

function renderSymbolLink(symbol) {
    const upper = String(symbol || "").trim().toUpperCase();
    if (!upper) {
        return "—";
    }
    const dashboardUrl = symbolDashboardUrl(upper);
    const label = escapeHtml(upper);
    if (dashboardUrl) {
        return `<a class="symbol-link" href="${dashboardUrl}" target="_blank" rel="noopener noreferrer" title="Open ${label} dashboard">${label}</a>`;
    }
    return `<a class="symbol-link symbol-link-analytics" href="/analytics/?symbol=${encodeURIComponent(upper)}" title="Filter analytics by ${label}">${label}</a>`;
}

function applySymbolFromQuery() {
    const params = new URLSearchParams(window.location.search);
    const symbol = (params.get("symbol") || "").trim().toUpperCase();
    if (symbol && symbolEl) {
        symbolEl.value = symbol;
    }
}

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

function formatPnl(value) {
    if (value == null || Number.isNaN(Number(value))) {
        return "—";
    }
    const num = Number(value);
    const sign = num >= 0 ? "+" : "";
    return `${sign}${num.toFixed(2)} USDT`;
}

function formatPct(value) {
    if (value == null || Number.isNaN(Number(value))) {
        return "—";
    }
    const num = Number(value);
    const sign = num >= 0 ? "+" : "";
    return `${sign}${num.toFixed(2)}%`;
}

function pnlClass(value) {
    if (value == null || Number.isNaN(Number(value))) {
        return "";
    }
    const num = Number(value);
    if (num > 0) {
        return "pnl-up";
    }
    if (num < 0) {
        return "pnl-down";
    }
    return "";
}

function formatGoalLabel(goal) {
    const num = Number(goal);
    if (!Number.isFinite(num)) {
        return "To goal";
    }
    if (num >= 1_000_000) {
        const millions = num / 1_000_000;
        return millions === Math.floor(millions) ? `To $${millions}M` : `To $${millions.toFixed(1)}M`;
    }
    return `To ${num.toLocaleString()} USDT`;
}

function formatDaysToGoal(perf) {
    const wallet = perf.wallet_usdt;
    const remaining = perf.million_remaining_usdt;
    const daily = perf.daily_avg_usdt;
    const days = perf.million_days_at_avg;
    const goal = perf.million_goal_usdt;

    if (wallet == null) {
        return { value: "—", sub: null };
    }
    if (remaining != null && remaining <= 0) {
        return { value: "Goal reached", sub: `${Number(goal).toLocaleString()} USDT` };
    }
    if (!daily || daily <= 0) {
        return {
            value: "Need +daily avg",
            sub: "Close trades in range with positive PnL",
        };
    }
    if (days == null) {
        return { value: "—", sub: null };
    }
    const dailyText = formatPnl(daily);
    if (days < 365) {
        return { value: `${Math.round(days)} days`, sub: `at ${dailyText}/day` };
    }
    return {
        value: `${(days / 365.25).toFixed(1)} years`,
        sub: `${Math.round(days)} days at ${dailyText}/day`,
    };
}

function formatBaselineHint(perf) {
    const baseline = perf.account_baseline_usdt;
    if (baseline == null) {
        return null;
    }
    const amount = `${Number(baseline).toLocaleString(undefined, { maximumFractionDigits: 2 })} USDT`;
    const source = perf.account_baseline_source || "file";
    if (source === "env") {
        return `baseline ${amount} · .env`;
    }
    if (source === "auto_estimated") {
        const when = perf.account_baseline_set_at ? ` · ${perf.account_baseline_set_at}` : "";
        return `baseline ${amount} · estimated from history${when}`;
    }
    if (source === "auto_first_snapshot") {
        const when = perf.account_baseline_set_at ? ` · since ${perf.account_baseline_set_at}` : "";
        return `baseline ${amount} · first snapshot${when}`;
    }
    const when = perf.account_baseline_set_at ? ` · since ${perf.account_baseline_set_at}` : "";
    return `baseline ${amount}${when}`;
}

function renderAccountKpis(perf) {
    if (!accountKpiGrid) {
        return;
    }
    if (!perf.configured) {
        accountKpiGrid.innerHTML = "";
        if (accountNote) {
            accountNote.className = "note warn account-panel-note";
            accountNote.textContent =
                "Binance API keys not configured — set BINANCE_API_KEY and BINANCE_SECRET_KEY in .env, then restart ./run-all.sh.";
        }
        return;
    }

    const lookback = perf.lookback_days || 30;
    const goalCard = formatDaysToGoal(perf);
    const sourceHint = {
        db: "trade_outcomes",
        orders_log: "orders.log",
        none: "no closes in range",
    }[perf.stats_source || "none"];

    const walletSub =
        perf.available_usdt != null
            ? `${formatPnl(perf.available_usdt).replace("+", "")} available`
            : null;
    const walletValue =
        perf.wallet_usdt != null
            ? formatPnl(perf.wallet_usdt).replace("+", "")
            : "—";
    const walletSubFinal =
        perf.stale && walletSub
            ? `${walletSub} · cached`
            : perf.stale
              ? "cached · API paused"
              : walletSub;

    const unrealizedSub =
        perf.unrealized_source === "fleet_cache" ? "from open positions · cached" : null;

    const cards = [
        {
            label: "Wallet",
            value: walletValue,
            sub: walletSubFinal,
        },
        {
            label: "Unrealized (all)",
            value: formatPnl(perf.unrealized_usdt),
            valueClass: pnlClass(perf.unrealized_usdt),
            sub: unrealizedSub,
        },
        {
            label: "ROI",
            value: perf.account_roi_pct != null ? formatPct(perf.account_roi_pct) : "—",
            valueClass: pnlClass(perf.account_roi_pct),
            sub: formatBaselineHint(perf),
        },
        {
            label: `Realized (${lookback}d)`,
            value: formatPnl(perf.realized_total_usdt),
            valueClass: pnlClass(perf.realized_total_usdt),
            sub: `${formatNumber(perf.closed_trades || 0)} closes · ${sourceHint}`,
        },
        {
            label: "Daily average",
            value: perf.daily_avg_usdt != null ? formatPnl(perf.daily_avg_usdt) : "—",
            valueClass: pnlClass(perf.daily_avg_usdt),
            sub: `total / ${lookback} calendar days`,
        },
        {
            label: formatGoalLabel(perf.million_goal_usdt),
            value: goalCard.value,
            valueClass: "goal-highlight",
            sub: goalCard.sub,
        },
    ];

    if (perf.pair_closed_trades > 0 && perf.pair_realized_total_usdt != null) {
        const sym = (symbolEl.value.trim().toUpperCase() || "Pair").slice(0, 12);
        cards.splice(4, 0, {
            label: `${sym} (${lookback}d)`,
            value: formatPnl(perf.pair_realized_total_usdt),
            valueClass: pnlClass(perf.pair_realized_total_usdt),
            sub:
                perf.pair_daily_avg_usdt != null
                    ? `${formatPnl(perf.pair_daily_avg_usdt)}/day`
                    : null,
        });
    }

    accountKpiGrid.innerHTML = cards
        .map((card) => {
            const valueClass = card.valueClass ? ` ${card.valueClass}` : "";
            const sub = card.sub ? `<div class="kpi-sub">${card.sub}</div>` : "";
            return `
                <div class="kpi">
                    <div class="kpi-label">${card.label}</div>
                    <div class="kpi-value${valueClass}">${card.value}</div>
                    ${sub}
                </div>
            `;
        })
        .join("");

    if (accountNote) {
        accountNote.className = "note account-panel-note";
        if (perf.wallet_usdt == null && perf.api_error) {
            accountNote.className = "note warn account-panel-note";
            accountNote.textContent = `Wallet unavailable (${perf.api_error}). Realized PnL is from MySQL history.`;
        } else if (perf.stale) {
            accountNote.className = "note warn account-panel-note";
            const blockHint = perf.api_error ? ` (${perf.api_error})` : "";
            accountNote.textContent = `Wallet from last successful Futures API read${blockHint} · Realized PnL from MySQL history.`;
        } else {
            accountNote.textContent =
                "Live from Binance Futures · daily average follows Range and Symbol filters · refreshes every 60s.";
        }
    }
}

async function loadAccountPerformance() {
    try {
        const perf = await fetchJson("/api/account-performance");
        renderAccountKpis(perf);
    } catch (error) {
        if (accountKpiGrid) {
            accountKpiGrid.innerHTML = "";
        }
        if (accountNote) {
            accountNote.className = "note warn account-panel-note";
            accountNote.textContent = `Account performance unavailable: ${error.message}`;
        }
    }
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
        ["Closed trades", overview.closed_trades],
        ["Wins", overview.trade_wins],
        ["Losses", overview.trade_losses],
        ["Realized PnL", overview.total_realized_pnl, "pnl"],
    ];

    kpiGrid.innerHTML = cards
        .map((entry) => {
            const [label, value, kind] = entry;
            const display = kind === "pnl" ? formatPnl(value) : formatNumber(value);
            return `
                <div class="kpi">
                    <div class="kpi-label">${label}</div>
                    <div class="kpi-value">${display}</div>
                </div>
            `;
        })
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

function formatAdxNumber(value) {
    if (value == null || value === "") {
        return null;
    }
    const num = Number(value);
    return Number.isFinite(num) ? num.toFixed(1) : null;
}

function isAdxUseHtf(row) {
    const value = row.adx_use_htf;
    if (value == null) {
        return true;
    }
    if (typeof value === "boolean") {
        return value;
    }
    return String(value).toLowerCase() in ("1", "true", "yes");
}

function renderAdxCell(row, kind) {
    const useHtf = isAdxUseHtf(row);
    const isFilterTarget =
        row.block_reason === "adx_low" && ((kind === "htf" && useHtf) || (kind === "ltf" && !useHtf));
    const value = kind === "htf" ? row.htf_adx : row.adx;
    const formatted = formatAdxNumber(value);
    const interval = kind === "htf" ? row.htf_interval || "HTF" : "1m";
    let title = kind === "htf" ? `ADX ${interval}` : "ADX 1m (LTF)";
    if (isFilterTarget) {
        const min = row.adx_min_trend;
        title +=
            min != null
                ? ` — usado en adx_low (min ${min})`
                : " — usado en adx_low";
    }
    const mark = isFilterTarget ? "*" : "";
    const cls = isFilterTarget ? "adx-used-for-filter" : "";
    return `<td class="${cls}" title="${escapeHtml(title)}">${formatted ?? "—"}${mark}</td>`;
}

function renderBlockSummary(payload) {
    const container = document.getElementById("block-summary");
    if (!container) {
        return;
    }
    if (!payload?.enabled || !payload.items?.length) {
        container.innerHTML = payload?.enabled
            ? '<p class="note" style="margin:0 0 12px;">Sin bloqueos en este rango.</p>'
            : "";
        return;
    }

    const total = payload.total_blocked || 0;
    const top = payload.items[0];
    const headline =
        top && total
            ? `Principal: <strong>${escapeHtml(top.block_reason)}</strong> — ${top.pct}% (${top.count}/${total})`
            : "";

    const rows = payload.items
        .map((item) => {
            const width = total ? Math.max(4, (item.count / total) * 100) : 0;
            return `
                <div class="block-summary-row">
                    <div class="block-summary-meta">
                        <span class="block-summary-reason">${escapeHtml(item.block_reason)}</span>
                        <span class="block-summary-count">${item.count} · ${item.pct}%</span>
                    </div>
                    <div class="bar-track"><div class="bar-fill block-summary-fill" style="width:${width}%"></div></div>
                    <p class="block-summary-text">${escapeHtml(item.summary || "")}</p>
                </div>
            `;
        })
        .join("");

    container.innerHTML = `
        <div class="block-summary-head">
            <h3>Resumen de bloqueos</h3>
            <p class="block-summary-total">${total} eventos con block_reason en el rango</p>
        </div>
        ${headline ? `<p class="block-summary-headline">${headline}</p>` : ""}
        <div class="block-summary-list">${rows}</div>
    `;
}

function renderRecent(rows) {
    const body = document.getElementById("recent-body");
    if (!rows.length) {
        body.innerHTML = `<tr><td colspan="10">No events yet.</td></tr>`;
        return;
    }
    body.innerHTML = rows
        .map(
            (row) => `
                <tr>
                    <td>${row.created_at || "—"}</td>
                    <td>${renderSymbolLink(row.symbol)}</td>
                    <td>${row.event_type || "—"}</td>
                    <td>${row.outcome || "—"}</td>
                    <td>${row.block_reason || "—"}</td>
                    <td>${row.signal || "—"}</td>
                    <td>${row.confidence ?? "—"}</td>
                    <td>${row.rsi ?? "—"}</td>
                    ${renderAdxCell(row, "ltf")}
                    ${renderAdxCell(row, "htf")}
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

function hasConfigChanges(item) {
    return Boolean(
        item &&
            item.config_changes &&
            typeof item.config_changes === "object" &&
            Object.keys(item.config_changes).length > 0
    );
}

function canApplySuggestion(item) {
    return hasConfigChanges(item);
}

function hasBulkApplicableSuggestions(suggestions = lastSuggestions) {
    return suggestions.some((item) => hasConfigChanges(item));
}

function hasActiveOverrides() {
    return Boolean(activeConfigStatus?.active);
}

function formatConfigValue(value) {
    if (typeof value === "boolean") {
        return value ? "true" : "false";
    }
    return String(value ?? "—");
}

function renderConfigKeyValues(entries) {
    const keys = Object.keys(entries || {});
    if (!keys.length) {
        return '<p class="note" style="margin:0;">—</p>';
    }
    return `
        <dl class="active-config-kv">
            ${keys
                .sort()
                .map(
                    (key) => `
                        <dt>${escapeHtml(key)}</dt>
                        <dd>${escapeHtml(formatConfigValue(entries[key]))}</dd>
                    `
                )
                .join("")}
        </dl>
    `;
}

function renderActiveConfigStatus(status) {
    activeConfigStatus = status;
    if (!activeConfigStatusEl) {
        updateBulkActionButtons();
        return;
    }
    if (!status?.active) {
        activeConfigStatusEl.hidden = true;
        activeConfigStatusEl.innerHTML = "";
        updateBulkActionButtons();
        return;
    }

    const fleetWide = status.fleet_wide || {};
    const perSymbol = status.per_symbol || {};
    const perSymbolBlocks = Object.keys(perSymbol)
        .sort()
        .map(
            (symbol) => `
                <div class="active-config-block">
                    <h4>${escapeHtml(symbol)}</h4>
                    ${renderConfigKeyValues(perSymbol[symbol])}
                </div>
            `
        )
        .join("");

    const updatedAt = status.updated_at ? escapeHtml(String(status.updated_at)) : "—";
    const updatedBy = status.updated_by ? escapeHtml(String(status.updated_by)) : "—";
    const symbolCount = status.symbols_with_overrides ?? 0;
    const fleetSize = status.fleet_size ?? 0;

    activeConfigStatusEl.hidden = false;
    activeConfigStatusEl.innerHTML = `
        <div class="active-config-head">
            <h3 class="active-config-title">Configuración activa (MySQL overrides)</h3>
            <p class="active-config-meta">${symbolCount}/${fleetSize} símbolos · ${updatedAt} · ${updatedBy}</p>
        </div>
        <div class="active-config-grid">
            <div class="active-config-block">
                <h4>Flota (mismo valor en todos)</h4>
                ${renderConfigKeyValues(fleetWide)}
            </div>
            ${perSymbolBlocks}
        </div>
        <p class="note" style="margin:10px 0 0;font-size:0.78rem;">
            Estos valores sustituyen al <code>.env</code> mientras estén activos.
            Usa <strong>Restaurar .env</strong> para volver a los defaults.
        </p>
    `;
    updateBulkActionButtons();
}

async function loadActiveConfigStatus() {
    try {
        const status = await fetchJson("/api/config-overrides");
        renderActiveConfigStatus(status);
    } catch (error) {
        renderActiveConfigStatus(null);
    }
}

function updateBulkActionButtons() {
    const applicable = hasBulkApplicableSuggestions();
    const hasOverrides = hasActiveOverrides();
    if (suggestionsApplyAllBtn) {
        suggestionsApplyAllBtn.hidden = !applicable || bulkApplied || hasOverrides;
    }
    if (suggestionsRestoreAllBtn) {
        suggestionsRestoreAllBtn.hidden = !hasOverrides && !bulkApplied;
    }
}

function setBulkFeedback(message, tone = "ok") {
    if (!suggestionsBulkFeedback) {
        return;
    }
    if (!message) {
        suggestionsBulkFeedback.hidden = true;
        suggestionsBulkFeedback.textContent = "";
        suggestionsBulkFeedback.className = "note suggestions-bulk-feedback";
        return;
    }
    suggestionsBulkFeedback.hidden = false;
    suggestionsBulkFeedback.textContent = message;
    suggestionsBulkFeedback.className = `note suggestions-bulk-feedback is-${tone}`;
}

function renderSuggestionActions(item, index) {
    if (!canApplySuggestion(item)) {
        return `<div class="suggestion-actions"><span class="note" style="margin:0;font-size:0.75rem;">Sin cambios en config_changes</span></div>`;
    }
    const scope = item.symbol ? escapeHtml(item.symbol) : "toda la flota";
    return `
        <div class="suggestion-actions">
            <span class="note" style="margin:0 0 6px;font-size:0.72rem;">${scope}</span>
            <button class="btn btn-primary" type="button" data-action="apply" data-index="${index}">Aplicar</button>
            <button class="btn" type="button" data-action="restore" data-index="${index}">Restaurar</button>
        </div>
    `;
}

function renderSuggestions(payload) {
    if (!payload || payload.error) {
        lastSuggestions = [];
        bulkApplied = false;
        suggestionsContent.innerHTML = "";
        suggestionsNote.className = "note warn";
        suggestionsNote.textContent = payload?.error || "Could not load suggestions.";
        setBulkFeedback("");
        updateBulkActionButtons();
        return;
    }

    lastSuggestions = payload.suggestions || [];
    bulkApplied = false;
    setBulkFeedback("");

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
    updateBulkActionButtons();
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
        const result = item.symbol
            ? await postJson("/api/symbol-config/apply", {
                  symbol: item.symbol,
                  config_changes: item.config_changes,
                  reason: `DeepSeek: ${item.title || "suggestion"}`,
              })
            : await postJson("/api/suggestions/apply-all", {
                  suggestions: [item],
                  reason: `DeepSeek: ${item.title || "suggestion"}`,
              });
        if (!result.ok && !result.partial) {
            setSuggestionFeedback(index, result.error || "No se pudo aplicar.", "error");
            return;
        }
        setSuggestionFeedback(index, result.message || "Configuración aplicada.", "ok");
        await loadActiveConfigStatus();
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
        const result = item.symbol
            ? await postJson("/api/symbol-config/restore", {
                  symbol: item.symbol,
                  config_keys: Object.keys(item.config_changes),
                  reason: `DeepSeek restore: ${item.title || "suggestion"}`,
              })
            : await postJson("/api/suggestions/restore-all", {
                  suggestions: [item],
                  reason: `DeepSeek restore: ${item.title || "suggestion"}`,
              });
        if (!result.ok && !result.partial) {
            setSuggestionFeedback(index, result.error || "No se pudo restaurar.", "error");
            return;
        }
        setSuggestionFeedback(index, result.message || "Restaurado a defaults.", "info");
        await loadActiveConfigStatus();
    } catch (error) {
        setSuggestionFeedback(index, error.message, "error");
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = "Restaurar";
        }
    }
}

async function applyAllSuggestions() {
    if (!hasBulkApplicableSuggestions()) {
        return;
    }
    if (suggestionsApplyAllBtn) {
        suggestionsApplyAllBtn.disabled = true;
        suggestionsApplyAllBtn.textContent = "Aplicando…";
    }
    setBulkFeedback("Aplicando sugerencias en MySQL y recargando dashboards…", "info");
    try {
        const result = await postJson("/api/suggestions/apply-all", {
            suggestions: lastSuggestions,
            reason: "DeepSeek bulk apply",
        });
        if (!result.ok && !result.partial) {
            setBulkFeedback(result.error || "No se pudo aplicar.", "error");
            return;
        }
        bulkApplied = true;
        setBulkFeedback(result.message || "Cambios aplicados.", result.partial ? "error" : "ok");
        lastSuggestions.forEach((_, index) => {
            setSuggestionFeedback(index, "Incluido en aplicación masiva.", "ok");
        });
        await loadActiveConfigStatus();
    } catch (error) {
        setBulkFeedback(error.message, "error");
    } finally {
        if (suggestionsApplyAllBtn) {
            suggestionsApplyAllBtn.disabled = false;
            suggestionsApplyAllBtn.textContent = "Aplicar cambios";
        }
        updateBulkActionButtons();
    }
}

async function restoreAllSuggestions() {
    if (!hasActiveOverrides() && !bulkApplied) {
        return;
    }
    if (suggestionsRestoreAllBtn) {
        suggestionsRestoreAllBtn.disabled = true;
        suggestionsRestoreAllBtn.textContent = "Restaurando…";
    }
    setBulkFeedback("Restaurando overrides a valores del .env…", "info");
    try {
        const result =
            hasBulkApplicableSuggestions() && bulkApplied
                ? await postJson("/api/suggestions/restore-all", {
                      suggestions: lastSuggestions,
                      reason: "DeepSeek bulk restore",
                  })
                : await postJson("/api/config-overrides/restore", {
                      reason: "Analytics restore all to .env",
                  });
        if (!result.ok && !result.partial) {
            setBulkFeedback(result.error || "No se pudo restaurar.", "error");
            return;
        }
        bulkApplied = false;
        setBulkFeedback(result.message || "Restaurado a .env.", result.partial ? "error" : "info");
        lastSuggestions.forEach((_, index) => {
            setSuggestionFeedback(index, "Restaurado a .env.", "info");
        });
        await loadActiveConfigStatus();
    } catch (error) {
        setBulkFeedback(error.message, "error");
    } finally {
        if (suggestionsRestoreAllBtn) {
            suggestionsRestoreAllBtn.disabled = false;
            suggestionsRestoreAllBtn.textContent = "Restaurar .env";
        }
        updateBulkActionButtons();
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
        await Promise.all([loadAccountPerformance(), loadActiveConfigStatus()]);

        if (!health.db_enabled) {
            statusNote.className = "note warn";
            statusNote.textContent =
                "DB_ENABLED=false — decision event charts need MySQL. Account performance above still works with Binance keys.";
            kpiGrid.innerHTML = "";
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
        const [series, eventTypes, blockReasons, blockSummary, symbols, recent, features] = await Promise.all([
            fetchJson(seriesPath),
            fetchJson("/api/breakdown/event_type"),
            fetchJson("/api/breakdown/block_reason"),
            fetchJson("/api/block-summary"),
            fetchJson("/api/breakdown/symbol"),
            fetchJson("/api/recent"),
            fetchJson("/api/features"),
        ]);

        renderTimeline(series);
        renderBars("event-type-bars", eventTypes);
        renderBars("block-reason-bars", blockReasons);
        renderBlockSummary(blockSummary);
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

if (exportOutcomesBtn) {
    exportOutcomesBtn.addEventListener("click", () => {
        window.location.href = `/api/export/trade-outcomes.csv?${queryParams().toString()}`;
    });
}

refreshBtn.addEventListener("click", refresh);
suggestionsBtn.addEventListener("click", () => loadSuggestions(true));
if (suggestionsApplyAllBtn) {
    suggestionsApplyAllBtn.addEventListener("click", () => applyAllSuggestions());
}
if (suggestionsRestoreAllBtn) {
    suggestionsRestoreAllBtn.addEventListener("click", () => restoreAllSuggestions());
}
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

applySymbolFromQuery();
loadSymbolPorts().then(() => {
    refresh();
});
setInterval(refresh, 60000);
