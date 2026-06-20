const statusNote = document.getElementById("status-note");
const configForm = document.getElementById("config-form");
const feedbackNote = document.getElementById("feedback-note");
const fleetPresetBar = document.getElementById("fleet-preset-bar");
const fleetPresetSelect = document.getElementById("fleet-preset-select");
const fleetPresetHint = document.getElementById("fleet-preset-hint");
const loadPresetBtn = document.getElementById("load-preset-btn");
const applyPresetBtn = document.getElementById("apply-preset-btn");
const overridesPanel = document.getElementById("overrides-panel");
const overridesSummary = document.getElementById("overrides-summary");
const perSymbolPanel = document.getElementById("per-symbol-panel");
const perSymbolJson = document.getElementById("per-symbol-json");

const urlParams = new URLSearchParams(window.location.search);
let configToken = urlParams.get("token") || sessionStorage.getItem("configToken") || "";
if (configToken) {
    sessionStorage.setItem("configToken", configToken);
    if (!urlParams.has("token")) {
        urlParams.set("token", configToken);
        const next = `${window.location.pathname}?${urlParams.toString()}`;
        window.history.replaceState({}, "", next);
    }
}

let editorState = null;
const baseline = new Map();

function apiHeaders(extra = {}) {
    const headers = { ...extra };
    if (configToken) {
        headers["X-Config-Token"] = configToken;
    }
    return headers;
}

async function fetchJson(url) {
    const response = await fetch(url, { headers: apiHeaders() });
    if (response.status === 403) {
        throw new Error("Acceso denegado. Usa /config/?token=TU_CONFIG_PAGE_TOKEN");
    }
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        throw new Error(data.error || `HTTP ${response.status}`);
    }
    return data;
}

async function postJson(url, body) {
    const response = await fetch(url, {
        method: "POST",
        headers: apiHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(body),
    });
    if (response.status === 403) {
        throw new Error("Acceso denegado. Token inválido o falta CONFIG_PAGE_TOKEN.");
    }
    const data = await response.json().catch(() => ({}));
    if (!response.ok && !data.partial) {
        throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }
    return data;
}

function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return "";
    }
    if (typeof value === "boolean") {
        return value ? "true" : "false";
    }
    return String(value);
}

function parseFieldValue(field, raw) {
    if (field.type === "bool") {
        return raw === "true";
    }
    if (field.type === "int") {
        return parseInt(raw, 10);
    }
    if (field.type === "float") {
        return parseFloat(raw);
    }
    return raw;
}

function valuesEqual(a, b) {
    return formatValue(a) === formatValue(b);
}

function setFeedback(message, kind = "info") {
    if (!feedbackNote) {
        return;
    }
    feedbackNote.hidden = !message;
    feedbackNote.textContent = message || "";
    feedbackNote.className = `note ${kind}`;
}

function setPresetControlsEnabled(enabled) {
    if (loadPresetBtn) {
        loadPresetBtn.disabled = !enabled;
    }
    if (applyPresetBtn) {
        applyPresetBtn.disabled = !enabled;
    }
    if (fleetPresetSelect) {
        fleetPresetSelect.disabled = !enabled;
    }
}

function renderOverridesSummary(status) {
    if (!status?.active) {
        overridesPanel.hidden = true;
        return;
    }
    overridesPanel.hidden = false;
    const fleetWide = status.fleet_wide || {};
    const keys = Object.keys(fleetWide);
    const lines = [
        `<strong>${status.symbols_with_overrides || 0}</strong> par(es) con overrides`,
        keys.length
            ? `Flota unificada: ${keys.map((k) => `<code>${escapeHtml(k)}</code>`).join(", ")}`
            : "Sin claves unificadas en toda la flota",
    ];
    if (status.updated_at) {
        lines.push(`Última actualización: ${escapeHtml(status.updated_at)} (${escapeHtml(status.updated_by || "?")})`);
    }
    overridesSummary.innerHTML = lines.map((line) => `<p class="note" style="margin:0 0 6px">${line}</p>`).join("");
}

function renderPerSymbol(status) {
    const perSymbol = status?.per_symbol || {};
    const keys = Object.keys(perSymbol);
    if (!keys.length) {
        perSymbolPanel.hidden = true;
        return;
    }
    perSymbolPanel.hidden = false;
    perSymbolJson.textContent = JSON.stringify(perSymbol, null, 2);
}

function buildField(field) {
    const wrap = document.createElement("div");
    wrap.className = "config-field";
    wrap.dataset.key = field.key;

    const label = document.createElement("label");
    label.htmlFor = `cfg-${field.key}`;
    label.textContent = field.label || field.key;

    const code = document.createElement("span");
    code.className = "key-code";
    code.textContent = field.key;

    const meta = document.createElement("div");
    meta.className = "config-meta";
    const sourceTag =
        field.source === "mysql_fleet"
            ? '<span class="tag tag-mysql">MySQL</span>'
            : '<span class="tag tag-env">.env</span>';
    meta.innerHTML = `${sourceTag} default .env: <code>${escapeHtml(formatValue(field.env_default))}</code>`;

    let input;
    if (field.type === "bool") {
        input = document.createElement("select");
        input.id = `cfg-${field.key}`;
        for (const val of ["true", "false"]) {
            const opt = document.createElement("option");
            opt.value = val;
            opt.textContent = val;
            input.appendChild(opt);
        }
        input.value = formatValue(field.effective ?? field.env_default) || "false";
    } else {
        input = document.createElement("input");
        input.id = `cfg-${field.key}`;
        input.type = field.type === "string" ? "text" : "number";
        if (field.type === "float") {
            input.step = "any";
        }
        input.value = formatValue(field.effective ?? field.env_default);
    }

    wrap.append(label, code, input, meta);

    if (field.help) {
        const help = document.createElement("span");
        help.className = "note config-help";
        help.textContent = field.help;
        wrap.appendChild(help);
    }

    const baselineValue = field.effective ?? field.env_default;
    baseline.set(field.key, baselineValue);

    const markChanged = () => {
        const current = parseFieldValue(field, input.value);
        wrap.classList.toggle("changed", !valuesEqual(current, baselineValue));
    };

    const applyPresetValue = (value) => {
        input.value = formatValue(value);
        markChanged();
        input.focus();
    };

    if (field.presets && field.presets.permissive != null && field.presets.conservative != null) {
        const presetsRow = document.createElement("div");
        presetsRow.className = "config-presets";

        const addPresetLine = (kind, value, hint, btnClass) => {
            const line = document.createElement("div");
            line.className = "config-preset-line";
            const labelSpan = document.createElement("span");
            labelSpan.className = `config-preset-label config-preset-${kind}`;
            labelSpan.textContent = kind === "permissive" ? "Permisivo" : "Conservador";
            const valueCode = document.createElement("code");
            valueCode.textContent = formatValue(value);
            const hintSpan = document.createElement("span");
            hintSpan.className = "config-preset-hint";
            hintSpan.textContent = hint ? ` — ${hint}` : "";
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = `config-preset-btn ${btnClass}`;
            btn.textContent = "Usar";
            btn.title = `Aplicar valor ${kind}`;
            btn.addEventListener("click", () => applyPresetValue(value));
            line.append(labelSpan, document.createTextNode(" "), valueCode, hintSpan, btn);
            presetsRow.appendChild(line);
        };

        addPresetLine(
            "permissive",
            field.presets.permissive,
            field.presets.permissive_hint,
            "config-preset-btn-loose",
        );
        addPresetLine(
            "conservative",
            field.presets.conservative,
            field.presets.conservative_hint,
            "config-preset-btn-tight",
        );
        wrap.appendChild(presetsRow);
    }

    input.addEventListener("input", markChanged);
    input.addEventListener("change", markChanged);

    return wrap;
}

function getSelectedPreset() {
    const id = fleetPresetSelect?.value;
    if (!id || !editorState?.fleet_presets) {
        return null;
    }
    return editorState.fleet_presets.find((preset) => preset.id === id) || null;
}

function updatePresetHint() {
    const preset = getSelectedPreset();
    if (!fleetPresetHint) {
        return;
    }
    if (!preset) {
        fleetPresetHint.textContent = "";
        return;
    }
    fleetPresetHint.textContent = preset.restore_only
        ? `${preset.hint} — vuelve al default del .env del servidor.`
        : preset.hint;
}

function renderFleetPresets(presets) {
    if (!fleetPresetSelect || !fleetPresetBar) {
        return;
    }
    fleetPresetSelect.innerHTML = "";
    for (const preset of presets || []) {
        const opt = document.createElement("option");
        opt.value = preset.id;
        opt.textContent = preset.label;
        fleetPresetSelect.appendChild(opt);
    }
    fleetPresetBar.hidden = !(presets || []).length;
    updatePresetHint();
}

function setFieldValue(field, value) {
    const input = document.getElementById(`cfg-${field.key}`);
    const wrap = configForm.querySelector(`[data-key="${field.key}"]`);
    if (!input || !wrap) {
        return;
    }
    input.value = formatValue(value);
    const baselineValue = baseline.get(field.key);
    wrap.classList.toggle("changed", !valuesEqual(value, baselineValue));
}

function loadPresetIntoForm(preset) {
    if (!preset || !editorState) {
        return 0;
    }
    const values = preset.values || {};
    let touched = 0;
    for (const field of editorState.keys || []) {
        if (!(field.key in values)) {
            continue;
        }
        const nextValue = values[field.key];
        const baselineValue = baseline.get(field.key);
        setFieldValue(field, nextValue);
        if (!valuesEqual(nextValue, baselineValue)) {
            touched += 1;
        }
    }
    return touched;
}

function loadSelectedPreset() {
    const preset = getSelectedPreset();
    if (!preset) {
        setFeedback("Selecciona un preset.", "error");
        return;
    }
    const touched = loadPresetIntoForm(preset);
    setFeedback(
        touched
            ? `Preset «${preset.label}» cargado (${touched} campo(s) distintos). Revisa y pulsa Aplicar preset.`
            : `Preset «${preset.label}» cargado — ya coincide con la flota activa.`,
        touched ? "info" : "ok",
    );
}

function collectChanges() {
    const changes = {};
    for (const field of editorState?.keys || []) {
        const input = document.getElementById(`cfg-${field.key}`);
        const wrap = configForm.querySelector(`[data-key="${field.key}"]`);
        if (!input || !wrap?.classList.contains("changed")) {
            continue;
        }
        changes[field.key] = parseFieldValue(field, input.value);
    }
    return changes;
}

async function applySelectedPreset() {
    const preset = getSelectedPreset();
    if (!preset) {
        setFeedback("Selecciona un preset.", "error");
        return;
    }

    const manualChanges = collectChanges();
    const useManual = Object.keys(manualChanges).length > 0;

    if (preset.restore_only && useManual) {
        setFeedback("«Default (.env)» no admite ajustes manuales. Carga el preset de nuevo o elige otro.", "error");
        return;
    }

    const confirmText = preset.restore_only
        ? `¿Aplicar «${preset.label}»? Se eliminarán los overrides MySQL y volverás al .env.`
        : useManual
          ? `¿Aplicar ${Object.keys(manualChanges).length} cambio(s) manual(es) a toda la flota?`
          : `¿Aplicar preset «${preset.label}» a toda la flota (${editorState?.fleet_size || "?"} pares)?`;
    if (!window.confirm(confirmText)) {
        return;
    }

    applyPresetBtn.disabled = true;
    applyPresetBtn.textContent = "Aplicando…";
    setFeedback("Guardando y recargando dashboards…", "info");
    try {
        let result;
        if (useManual) {
            result = await postJson("/api/config/apply-fleet", {
                config_changes: manualChanges,
                reason: `Config page manual (${preset.id})`,
            });
        } else {
            result = await postJson("/api/config/apply-preset", {
                preset_id: preset.id,
                reason: `Config page preset: ${preset.id}`,
            });
        }
        setFeedback(result.message || "Aplicado.", result.partial ? "error" : "ok");
        await loadEditor();
    } catch (error) {
        setFeedback(error.message, "error");
    } finally {
        applyPresetBtn.disabled = false;
        applyPresetBtn.textContent = "Aplicar preset";
    }
}

function renderForm(state) {
    configForm.innerHTML = "";
    baseline.clear();
    for (const field of state.keys || []) {
        configForm.appendChild(buildField(field));
    }
}

async function loadEditor() {
    statusNote.textContent = "Cargando configuración…";
    const state = await fetchJson("/api/config/editor");
    editorState = state;
    if (!state.enabled) {
        statusNote.textContent = "DB_ENABLED=false — activa MySQL para usar overrides.";
        setPresetControlsEnabled(false);
        return;
    }
    setPresetControlsEnabled(true);
    statusNote.textContent = `Flota: ${state.fleet_size} par(es). ${state.fleet_symbols?.join(", ") || ""}`;
    renderFleetPresets(state.fleet_presets);
    renderForm(state);
    renderOverridesSummary(state.overrides);
    renderPerSymbol(state.overrides);
}

loadPresetBtn?.addEventListener("click", loadSelectedPreset);
applyPresetBtn?.addEventListener("click", applySelectedPreset);
fleetPresetSelect?.addEventListener("change", updatePresetHint);

loadEditor().catch((error) => {
    const msg = error.message || "Error al cargar";
    statusNote.textContent = msg;
    setFeedback(msg, "error");
    if (msg.includes("Acceso denegado") || msg.includes("403")) {
        statusNote.textContent =
            "Falta token: abre /config/?token=TU_CONFIG_PAGE_TOKEN (valor de CONFIG_PAGE_TOKEN en .env)";
    }
});
