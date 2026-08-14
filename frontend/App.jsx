// ============================================================================
// Dr Shumard Analytics — Production Frontend (Redesigned)
// Connects to Express API. No window.storage.
// ============================================================================

import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import { parse, startOfDay, endOfDay, subDays } from "date-fns";
import { createClient } from "@supabase/supabase-js";
import { ThinkingOrb } from "thinking-orbs";

// ─── Supabase Client ─────────────────────────────────────────────────────────
// Auth links (password recovery, magic links) land as hash params — either
// tokens (#access_token=…&type=recovery) or an error (#error_code=otp_expired
// when the one-time link was already consumed, e.g. by an email security
// scanner). Read the hash BEFORE createClient: detectSessionInUrl strips it.
const AUTH_HASH = new URLSearchParams(window.location.hash.replace(/^#/, ""));
const LANDED_FROM_RECOVERY = AUTH_HASH.get("type") === "recovery";
const AUTH_LINK_ERROR_MSG = (() => {
  if (!AUTH_HASH.get("error") && !AUTH_HASH.get("error_code")) return "";
  const code = AUTH_HASH.get("error_code") || "";
  const desc = AUTH_HASH.get("error_description") || "";
  if (code === "otp_expired" || /expired|invalid/i.test(desc))
    return "That link has expired or was already used — some email security scanners consume links before you click them. Use “Forgot password?” below to send a fresh one, and open it promptly.";
  return desc || "Sign-in link error — request a fresh link below.";
})();
if (AUTH_LINK_ERROR_MSG) window.history.replaceState(null, "", window.location.pathname + window.location.search);

const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL,
  import.meta.env.VITE_SUPABASE_ANON_KEY
);

// ─── API Client ──────────────────────────────────────────────────────────────
const API_BASE = window.location.origin;

// Active funnel (persisted to localStorage). Reads sync; writes go through
// setActiveFunnel so React state can re-render. Default 'analytics' preserves
// behavior for users with no native access.
const FUNNEL_LS_KEY = "dr-shumard-active-funnel";
const getActiveFunnel = () => {
  try { return localStorage.getItem(FUNNEL_LS_KEY) || "analytics"; }
  catch { return "analytics"; }
};
const setActiveFunnelLS = (f) => {
  try { localStorage.setItem(FUNNEL_LS_KEY, f); } catch {}
};
const readChatUiState = (key) => {
  try {
    const parsed = JSON.parse(localStorage.getItem(key) || "{}");
    return {
      activeId: typeof parsed.activeId === "string" ? parsed.activeId : null,
      draft: typeof parsed.draft === "string" ? parsed.draft : "",
    };
  } catch { return { activeId: null, draft: "" }; }
};

const getAuthHeaders = async (funnel = getActiveFunnel()) => {
  const { data: { session } } = await supabase.auth.getSession();
  const headers = { "Content-Type": "application/json", "X-Funnel": funnel };
  if (session?.access_token) headers["Authorization"] = `Bearer ${session.access_token}`;
  return headers;
};

// For unauthenticated GETs that still need funnel scoping (metrics, events, etc.)
const getFunnelHeaders = () => ({ "X-Funnel": getActiveFunnel() });

// Parse MM/DD/YYYY → Date at midnight (component-based, no browser TZ dependency)
const parseMDate = (d) => { const [m, dy, y] = d.split('/').map(Number); return new Date(y, m - 1, dy); };

const api = {
  async getMyFunnels() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/me/funnels`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch funnels: ${res.status}`);
    return res.json();
  },
  async getMetrics(limit = 90, offset = 0) {
    // expand=variants embeds each day's A/B/undetected breakdown so the split-test
    // toggle switches client-side (no refetch). Fetched once per load.
    const url = `${API_BASE}/api/metrics?limit=${limit}&offset=${offset}&expand=variants`;
    const res = await fetch(url, { headers: getFunnelHeaders() });
    if (!res.ok) throw new Error(`Failed to fetch metrics: ${res.status}`);
    return res.json();
  },
  async getAbTestStart() {
    const res = await fetch(`${API_BASE}/api/ab-test/start`, { headers: getFunnelHeaders() });
    if (!res.ok) throw new Error(`Failed to fetch A/B start: ${res.status}`);
    return res.json();
  },
  async setAbTestStart(body) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/ab-test/start`, { method: "PUT", headers, body: JSON.stringify(body) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Failed: ${res.status}`); }
    return res.json();
  },
  async linkIdentity(body) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/crm/link`, { method: "POST", headers, body: JSON.stringify(body) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Failed: ${res.status}`); }
    return res.json();
  },
  async getRegPageVariants() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/reg-page-variants`, { headers });
    if (!res.ok) throw new Error(`Failed: ${res.status}`);
    return res.json();
  },
  async setRegPageVariants(map) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/reg-page-variants`, { method: "PUT", headers, body: JSON.stringify({ map }) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Failed: ${res.status}`); }
    return res.json();
  },
  async getSalesPages() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/sales-pages`, { headers });
    if (!res.ok) throw new Error(`Failed: ${res.status}`);
    return res.json();
  },
  async setSalesPages(map) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/sales-pages`, { method: "PUT", headers, body: JSON.stringify({ map }) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Failed: ${res.status}`); }
    return res.json();
  },
  async upsertMetric(entry) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/metrics`, {
      method: "POST",
      headers,
      body: JSON.stringify(entry),
    });
    if (!res.ok) throw new Error(`Failed to save: ${res.status}`);
    return res.json();
  },
  async updateMetric(date, data) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/metrics/${encodeURIComponent(date)}`, {
      method: "PUT",
      headers,
      body: JSON.stringify(data),
    });
    if (!res.ok) throw new Error(`Failed to update: ${res.status}`);
    return res.json();
  },
  async deleteMetric(date) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/metrics/${encodeURIComponent(date)}`, { method: "DELETE", headers });
    if (!res.ok) throw new Error(`Failed to delete: ${res.status}`);
    return res.json();
  },
  async getCustomMetrics() {
    const res = await fetch(`${API_BASE}/api/custom-metrics`, { headers: getFunnelHeaders() });
    if (!res.ok) throw new Error(`Failed to fetch custom metrics: ${res.status}`);
    return res.json();
  },
  async saveCustomMetric(cm) {
    const method = cm.id ? "PUT" : "POST";
    const url = cm.id ? `${API_BASE}/api/custom-metrics/${cm.id}` : `${API_BASE}/api/custom-metrics`;
    const headers = await getAuthHeaders();
    const res = await fetch(url, { method, headers, body: JSON.stringify({ name: cm.name, formula: cm.formula, format: cm.format }) });
    if (!res.ok) throw new Error(`Failed to save custom metric: ${res.status}`);
    return res.json();
  },
  async deleteCustomMetric(id) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/custom-metrics/${id}`, { method: "DELETE", headers });
    if (!res.ok) throw new Error(`Failed to delete: ${res.status}`);
    return res.json();
  },
  async getEvents(limit = 100, type = "") {
    const url = `${API_BASE}/api/activity?limit=${limit}${type ? `&type=${type}` : ""}`;
    const res = await fetch(url, { headers: getFunnelHeaders() });
    if (!res.ok) throw new Error(`Failed to fetch events: ${res.status}`);
    return res.json();
  },
  async chat(messages, funnel) {
    const headers = await getAuthHeaders(funnel);
    const res = await fetch(`${API_BASE}/api/insights/chat`, { method: "POST", headers, body: JSON.stringify({ messages }) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Chat failed: ${res.status}`); }
    return res.json();
  },
  async workerChat(messages, funnel) {
    const headers = await getAuthHeaders(funnel);
    const res = await fetch(`${API_BASE}/api/worker/chat`, { method: "POST", headers, body: JSON.stringify({ messages }) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Worker chat failed: ${res.status}`); }
    return res.json();
  },
  async adminResetLink(email, funnel) {
    const headers = await getAuthHeaders(funnel);
    const res = await fetch(`${API_BASE}/api/admin/reset-link`, { method: "POST", headers, body: JSON.stringify({ email }) });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(body.error || `Failed: ${res.status}`);
    return body;
  },
  async getConversations(funnel) {
    const headers = await getAuthHeaders(funnel);
    const res = await fetch(`${API_BASE}/api/insights/conversations`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch conversations: ${res.status}`);
    return res.json();
  },
  async saveConversation(id, title, messages, funnel) {
    const headers = await getAuthHeaders(funnel);
    const res = await fetch(`${API_BASE}/api/insights/conversations/${id}`, { method: "PUT", headers, body: JSON.stringify({ title, messages }) });
    if (!res.ok) throw new Error(`Failed to save conversation: ${res.status}`);
    return res.json();
  },
  async deleteConversation(id, funnel) {
    const headers = await getAuthHeaders(funnel);
    const res = await fetch(`${API_BASE}/api/insights/conversations/${id}`, { method: "DELETE", headers });
    if (!res.ok) throw new Error(`Failed to delete conversation: ${res.status}`);
    return res.json();
  },
  async clearCache() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/cache/clear`, { method: "POST", headers });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Cache clear failed: ${res.status}`); }
    return res.json();
  },
  async finalizePastDays() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/admin/finalize-past-days`, { method: "POST", headers, body: JSON.stringify({}) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Finalize failed: ${res.status}`); }
    return res.json();
  },
  async finalizeDate(date) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/admin/finalize-date`, { method: "POST", headers, body: JSON.stringify({ date }) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Finalize failed: ${res.status}`); }
    return res.json();
  },
  async getCrmContacts(search = "", stage = "", limit = 100, offset = 0, salesPage = "") {
    const headers = await getAuthHeaders();
    const qs = new URLSearchParams({ limit, offset });
    if (search) qs.set("search", search);
    if (stage) qs.set("stage", stage);
    if (salesPage) qs.set("sales_page", salesPage);
    const res = await fetch(`${API_BASE}/api/crm/contacts?${qs}`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch contacts: ${res.status}`);
    return res.json();
  },
  async getCrmContact(id) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/crm/contacts/${encodeURIComponent(id)}`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch contact: ${res.status}`);
    return res.json();
  },
  async getCrmStats() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/crm/stats`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch CRM stats: ${res.status}`);
    return res.json();
  },
  async getEmailReport() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/crm/email-report`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch email report: ${res.status}`);
    return res.json();
  },
  async getEmailReportClicks(source) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/crm/email-report/clicks?source=${encodeURIComponent(source)}`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch clicks: ${res.status}`);
    return res.json();
  },
};

function getLocalKey() {
  return localStorage.getItem("dr-shumard-api-key") || "";
}

// ─── Date Helpers (LA timezone) ──────────────────────────────────────────────
const getLADate = () => new Date().toLocaleDateString("en-US", { timeZone: "America/Los_Angeles", year: "numeric", month: "2-digit", day: "2-digit" });
const getLADay = (d) => { try { const [m, dy, y] = d.split("/").map(Number); return new Date(y, m - 1, dy).toLocaleDateString("en-US", { weekday: "long" }); } catch { return ""; } };
const getLADayShort = (d) => { try { const [m, dy, y] = d.split("/").map(Number); return new Date(y, m - 1, dy).toLocaleDateString("en-US", { weekday: "short" }); } catch { return ""; } };
const fmtDateNice = (d) => { try { const [m, dy, y] = d.split("/").map(Number); return new Date(y, m - 1, dy).toLocaleDateString("en-US", { month: "short", day: "numeric" }); } catch { return d; } };

// ─── Formula Engine ──────────────────────────────────────────────────────────
const MK = ["fb_spend", "fb_link_clicks", "reg_page_visits", "registrations", "replays", "viewedcta", "clickedcta", "purchases", "purchases_fb", "purchases_native", "purchases_youtube", "purchases_aibot", "purchases_aibot_b", "purchases_postwebinar", "purchases_cpa", "purchases_sales_a", "purchases_sales_b", "purchases_retargeting", "stayed_45", "stayed_60", "stayed_80", "total_purchases", "attended"];
const COL_LABELS = { fb_spend: "FB Spend", fb_link_clicks: "Total Reg. Page Visited", reg_page_visits: "Page Views", registrations: "Registra​tions", attended: "Attended", replays: "Replays", viewedcta: "Viewed CTA", clickedcta: "Clicked CTA", purchases_fb: "FB Purchases", purchases_native: "Native Ads", purchases_youtube: "Youtube/Organic", purchases_aibot: "AI Chat Bot", purchases_aibot_b: "AI Chat Bot B", purchases_postwebinar: "Post Webinar", purchases_cpa: "CPA Traffic Funnel", purchases_sales_a: "Sales A", purchases_sales_b: "Sales B", purchases_retargeting: "Retargeting", stayed_45: "45 min", stayed_60: "60 min", stayed_80: "80 min", total_purchases: "Total Purchases" };
const DEFAULT_HIDDEN = [];

// Summary card defaults and metric options for the configurable summary strip
const DEFAULT_SUMMARY_CARDS = [
  { label: "Total Spend", key: "fb_spend", agg: "total", format: "currency" },
  { label: "Reg. Page Visits", key: "fb_link_clicks", agg: "total", format: "number" },
  { label: "Registrations", key: "registrations", agg: "total", format: "number" },
  { label: "Purchases", key: "purchases", agg: "total", format: "number" },
  { label: "Total Replays", key: "replays", agg: "total", format: "number" },
];
const SUMMARY_METRIC_OPTIONS = [
  { key: "fb_spend", label: "FB Spend", defaultFormat: "currency" },
  { key: "fb_link_clicks", label: "Reg. Page Visits", defaultFormat: "number" },
  { key: "registrations", label: "Registrations", defaultFormat: "number" },
  { key: "attended", label: "Attended", defaultFormat: "number" },
  { key: "replays", label: "Replays", defaultFormat: "number" },
  { key: "viewedcta", label: "Viewed CTA", defaultFormat: "number" },
  { key: "clickedcta", label: "Clicked CTA", defaultFormat: "number" },
  { key: "purchases", label: "Total Purchases", defaultFormat: "number" },
  { key: "purchases_fb", label: "FB Purchases", defaultFormat: "number" },
  { key: "purchases_native", label: "Native Ads", defaultFormat: "number" },
  { key: "purchases_youtube", label: "Youtube/Organic", defaultFormat: "number" },
  { key: "purchases_aibot", label: "AI Chat Bot", defaultFormat: "number" },
  { key: "purchases_aibot_b", label: "AI Chat Bot B", defaultFormat: "number" },
  { key: "purchases_postwebinar", label: "Post Webinar", defaultFormat: "number" },
  { key: "purchases_cpa", label: "CPA Traffic Funnel", defaultFormat: "number" },
  { key: "purchases_sales_a", label: "Sales A", defaultFormat: "number" },
  { key: "purchases_sales_b", label: "Sales B", defaultFormat: "number" },
  { key: "purchases_retargeting", label: "Retargeting", defaultFormat: "number" },
  { key: "stayed_45", label: "45 min", defaultFormat: "number" },
  { key: "stayed_60", label: "60 min", defaultFormat: "number" },
  { key: "stayed_80", label: "80 min", defaultFormat: "number" },
  { key: "total_purchases", label: "Total Purchases (alt)", defaultFormat: "number" },
];
const evalFormula = (f, row, ctx = {}) => { try { let e = f.trim(); for (const k of MK) e = e.replace(new RegExp(`\\b${k}\\b`, "gi"), String(Number(row[k]) || 0)); for (const [k, v] of Object.entries(ctx)) e = e.replace(new RegExp(`\\b${k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, "gi"), String(Number(v) || 0)); if (/[^0-9+\-*/().%\s]/.test(e)) return null; e = e.replace(/[_%]/g, m => m === '%' ? '/100*' : ''); const r = Function('"use strict"; return (' + e + ")")(); return isFinite(r) ? Math.round(r * 100) / 100 : null; } catch { return null; } };
const fmtVal = (v, fmt) => v === null ? "\u2014" : fmt === "percent" ? `${v}%` : fmt === "currency" ? `$${v.toLocaleString("en-US", { minimumFractionDigits: 2 })}` : v.toLocaleString("en-US", { maximumFractionDigits: 2 });

// Fields that have no A/B dimension \u2014 FB reports spend and reg-page link clicks
// at the account/day level, so they can't be attributed to a split-test variant.
// In a single-variant view (A/B/undetected) we hide these columns and any custom
// metric that depends on them, since a per-variant value would be misleading.
// CRM contacts list page size (server paginates via limit/offset).
const CRM_PAGE = 100;
const UNSPLITTABLE_FIELDS = ["fb_spend", "fb_link_clicks"];
const formulaUsesUnsplittable = (formula) =>
  UNSPLITTABLE_FIELDS.some(k => new RegExp(`\\b${k}\\b`, "i").test(formula || ""));

// Evaluate all custom metrics for a row in dependency order (topo-sort)
const evalAllCustoms = (customs, row) => {
  const ctx = {};
  // Build a name->formula+format map
  const byName = {};
  customs.forEach(cm => { byName[cm.name.toLowerCase()] = cm; });
  // Topo-sort: detect which customs reference other customs
  const resolved = new Set();
  const visiting = new Set();
  const order = [];
  const visit = (cm) => {
    const key = cm.name.toLowerCase();
    if (resolved.has(key)) return true;
    if (visiting.has(key)) return false; // circular
    visiting.add(key);
    // Check if this formula references other custom metric names
    for (const other of customs) {
      if (other.name === cm.name) continue;
      const re = new RegExp(`\\b${other.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, "i");
      if (re.test(cm.formula)) {
        if (!visit(other)) return false; // circular dep
      }
    }
    visiting.delete(key);
    resolved.add(key);
    order.push(cm);
    return true;
  };
  customs.forEach(cm => visit(cm));
  // Evaluate in order
  for (const cm of order) {
    ctx[cm.name] = evalFormula(cm.formula, row, ctx);
  }
  return ctx;
};

const I = ({ d, size = 16, stroke = "currentColor", sw = 1.8 }) => (<svg aria-hidden="true" focusable="false" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={stroke} strokeWidth={sw} strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>);
const FullScreenLoading = ({ label }) => (
  <div style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100dvh" }}>
    <div role="status" aria-live="polite" aria-label={label} style={{ textAlign: "center" }}>
      <ThinkingOrb state="breathing" size={64} speed={0.75} theme="light" aria-hidden="true" style={{ display: "block", margin: "0 auto 16px" }} />
      <div style={{ color: "var(--ds-gray-700)", fontSize: 13, fontWeight: 500 }}>{label}</div>
    </div>
  </div>
);
const fn = "'Geist',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
const mono = "'Geist Mono',ui-monospace,SFMono-Regular,Menlo,monospace";
const SUMMARY_CHART_COLORS = ["#0070f3", "#0a7f43", "#6d28d9", "#a35200", "#be123c", "#0f766e"];
const DATE_FILTER_OPTIONS = [
  { value: "today", label: "Today" },
  { value: "yesterday", label: "Yesterday" },
  { value: "7", label: "Last 7 days" },
  { value: "30", label: "Last 30 days" },
  { value: "all", label: "All time" },
  { value: "custom", label: "Custom range" },
];

const getSummaryRowValue = (card, row, customs) => {
  if (String(card.key).startsWith("cm:")) {
    const metric = customs.find(custom => `cm:${custom.id}` === card.key);
    if (!metric) return 0;
    return Number(evalAllCustoms(customs, row)[metric.name]) || 0;
  }
  return Number(row[card.key]) || 0;
};

function SummarySparkline({ card, rows, customs, formatValue, accent, gradientId }) {
  const [activeIndex, setActiveIndex] = useState(null);
  const series = useMemo(() => [...rows]
    .sort((a, b) => parseMDate(a.date) - parseMDate(b.date))
    .map(row => ({ date: row.date, value: getSummaryRowValue(card, row, customs) })), [card.key, customs, rows]);

  if (!series.length) {
    return <div className="summary-sparkline is-empty" aria-label={`${card.label}: no trend data`}><span>No trend data</span></div>;
  }

  const width = 240;
  const height = 64;
  const top = 7;
  const bottom = 58;
  const values = series.map(point => point.value);
  const minimum = Math.min(0, ...values);
  const rawMaximum = Math.max(0, ...values);
  const maximum = rawMaximum === minimum ? rawMaximum + 1 : rawMaximum;
  const span = maximum - minimum || 1;
  const yFor = value => top + ((maximum - value) / span) * (bottom - top);
  const xFor = index => series.length === 1 ? width / 2 : (index / (series.length - 1)) * width;
  const hasTrendLine = series.length > 1;
  const plotted = series.map((point, index) => ({ x: xFor(index), y: yFor(point.value) }));
  const linePath = plotted.map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const baselineY = yFor(0);
  const areaPath = hasTrendLine ? `${linePath} L${plotted[plotted.length - 1].x.toFixed(2)} ${baselineY.toFixed(2)} L${plotted[0].x.toFixed(2)} ${baselineY.toFixed(2)} Z` : "";
  const resolvedActiveIndex = activeIndex === null ? null : Math.min(activeIndex, series.length - 1);
  const activePoint = resolvedActiveIndex === null ? null : series[resolvedActiveIndex];
  const activeX = resolvedActiveIndex === null ? 0 : xFor(resolvedActiveIndex);
  const activePercent = resolvedActiveIndex === null ? 0 : series.length === 1 ? 50 : (resolvedActiveIndex / (series.length - 1)) * 100;
  const tooltipTransform = activePercent < 18 ? "translateX(0)" : activePercent > 82 ? "translateX(-100%)" : "translateX(-50%)";

  const updateFromPointer = event => {
    const bounds = event.currentTarget.getBoundingClientRect();
    const position = Math.max(0, Math.min(bounds.width, event.clientX - bounds.left));
    setActiveIndex(series.length === 1 ? 0 : Math.round((position / bounds.width) * (series.length - 1)));
  };

  return (
    <div
      className="summary-sparkline"
      role="group"
      tabIndex={0}
      aria-label={`${card.label} trend, ${series.length} data point${series.length === 1 ? "" : "s"}. Use the left and right arrow keys to inspect values.`}
      onPointerMove={updateFromPointer}
      onPointerDown={updateFromPointer}
      onPointerLeave={() => setActiveIndex(null)}
      onFocus={() => setActiveIndex(current => current === null ? series.length - 1 : current)}
      onBlur={() => setActiveIndex(null)}
      onKeyDown={event => {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        event.preventDefault();
        const direction = event.key === "ArrowRight" ? 1 : -1;
        setActiveIndex(current => Math.max(0, Math.min(series.length - 1, (current ?? series.length - 1) + direction)));
      }}
      style={{ "--summary-chart-accent": accent }}
    >
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true" focusable="false">
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={accent} stopOpacity="0.26" />
            <stop offset="100%" stopColor={accent} stopOpacity="0.03" />
          </linearGradient>
        </defs>
        <line className="summary-sparkline-baseline" x1="0" x2={width} y1={baselineY} y2={baselineY} />
        {hasTrendLine ? (
          <>
            <path className="summary-sparkline-area" d={areaPath} fill={`url(#${gradientId})`} />
            <path className="summary-sparkline-line" d={linePath} />
          </>
        ) : (
          <circle className="summary-sparkline-single-dot" cx={width / 2} cy={yFor(series[0].value)} r="3.5" />
        )}
        {activePoint && (
          <>
            <line className="summary-sparkline-rule" x1={activeX} x2={activeX} y1={top} y2={bottom} />
            <circle className="summary-sparkline-dot" cx={activeX} cy={yFor(activePoint.value)} r="3.5" />
          </>
        )}
      </svg>
      {activePoint && (
        <div className="summary-sparkline-readout" role="status" aria-live="polite" style={{ left: `${activePercent}%`, transform: tooltipTransform }}>
          <span>{fmtDateNice(activePoint.date)}</span>
          <strong>{formatValue(activePoint.value)}</strong>
        </div>
      )}
    </div>
  );
}

function GeistSelect({ value, options, onChange, ariaLabel, className = "", icon }) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const triggerRef = useRef(null);
  const optionRefs = useRef([]);
  const selectedIndex = Math.max(0, options.findIndex(option => option.value === value));
  const selectedOption = options[selectedIndex] || options[0];

  useEffect(() => {
    if (!open) return;
    const focusFrame = requestAnimationFrame(() => optionRefs.current[selectedIndex]?.focus());
    const handlePointerDown = (event) => {
      if (!rootRef.current?.contains(event.target)) setOpen(false);
    };
    document.addEventListener("pointerdown", handlePointerDown);
    return () => {
      cancelAnimationFrame(focusFrame);
      document.removeEventListener("pointerdown", handlePointerDown);
    };
  }, [open, selectedIndex]);

  const selectOption = (option) => {
    onChange(option.value);
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  };

  const openFromKeyboard = (event) => {
    if (["ArrowDown", "ArrowUp", "Enter", " "].includes(event.key)) {
      event.preventDefault();
      setOpen(true);
    }
  };

  const handleOptionKeyDown = (event, index) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      setOpen(false);
      triggerRef.current?.focus();
      return;
    }
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      selectOption(options[index]);
      return;
    }
    let nextIndex = null;
    if (event.key === "ArrowDown") nextIndex = (index + 1) % options.length;
    if (event.key === "ArrowUp") nextIndex = (index - 1 + options.length) % options.length;
    if (event.key === "Home") nextIndex = 0;
    if (event.key === "End") nextIndex = options.length - 1;
    if (nextIndex !== null) {
      event.preventDefault();
      optionRefs.current[nextIndex]?.focus();
    }
  };

  return (
    <div ref={rootRef} className={`geist-select${open ? " is-open" : ""}${className ? ` ${className}` : ""}`} onBlur={() => requestAnimationFrame(() => { if (!rootRef.current?.contains(document.activeElement)) setOpen(false); })}>
      <button
        ref={triggerRef}
        type="button"
        className="geist-select-trigger"
        aria-label={`${ariaLabel}: ${selectedOption?.label || ""}`}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen(current => !current)}
        onKeyDown={openFromKeyboard}
      >
        {icon && <I d={icon} size={14} />}
        <span className="geist-select-value">{selectedOption?.label}</span>
        <I d="M6 9l6 6 6-6" size={12} />
      </button>
      {open && (
        <div className="geist-select-menu" role="listbox" aria-label={ariaLabel}>
          {options.map((option, index) => (
            <button
              key={option.value}
              ref={element => { optionRefs.current[index] = element; }}
              type="button"
              role="option"
              aria-selected={option.value === value}
              tabIndex={option.value === value ? 0 : -1}
              className={`geist-select-option${option.value === value ? " is-selected" : ""}`}
              onClick={() => selectOption(option)}
              onKeyDown={(event) => handleOptionKeyDown(event, index)}
            >
              <span>{option.label}</span>
              {option.value === value && <I d="M5 12l4 4L19 6" size={14} sw={2} />}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function GeistDateRangePicker({ filter, onFilterChange, range, onRangeChange }) {
  const [open, setOpen] = useState(false);
  const [draftFilter, setDraftFilter] = useState(filter);
  const [draftRange, setDraftRange] = useState(range);
  const rootRef = useRef(null);
  const triggerRef = useRef(null);
  const panelRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    const focusFrame = requestAnimationFrame(() => panelRef.current?.querySelector("button")?.focus());
    const handlePointerDown = (event) => {
      if (!rootRef.current?.contains(event.target)) setOpen(false);
    };
    const handleKeyDown = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setOpen(false);
        triggerRef.current?.focus();
      }
    };
    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      cancelAnimationFrame(focusFrame);
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [open]);

  const openPicker = () => {
    setDraftFilter(filter);
    setDraftRange(range);
    setOpen(true);
  };

  const selectPreset = (option) => {
    if (option.value === "custom") {
      setDraftFilter("custom");
      return;
    }
    onFilterChange(option.value);
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  };

  const applyCustomRange = () => {
    if (!draftRange[0] || !draftRange[1]) return;
    onRangeChange(draftRange);
    onFilterChange("custom");
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  };

  const cancelPicker = () => {
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  };

  const label = (() => {
    if (filter !== "custom") return DATE_FILTER_OPTIONS.find(option => option.value === filter)?.label || "Date range";
    if (!range[0] || !range[1]) return "Custom range";
    const formatDate = (date) => date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    return `${formatDate(range[0])} – ${formatDate(range[1])}`;
  })();

  return (
    <div ref={rootRef} className={`geist-date-picker${open ? " is-open" : ""}${draftFilter === "custom" ? " shows-calendar" : ""}`} onBlur={() => requestAnimationFrame(() => { if (!rootRef.current?.contains(document.activeElement)) setOpen(false); })}>
      <button
        ref={triggerRef}
        type="button"
        className="geist-date-picker-trigger"
        aria-label={`Date range: ${label}`}
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => open ? setOpen(false) : openPicker()}
        onKeyDown={(event) => {
          if (!open && ["ArrowDown", "Enter", " "].includes(event.key)) {
            event.preventDefault();
            openPicker();
          }
        }}
      >
        <I d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" size={14} />
        <span>{label}</span>
        <I d="M6 9l6 6 6-6" size={12} />
      </button>
      {open && (
        <div ref={panelRef} className="geist-date-picker-panel" role="dialog" aria-label="Choose a date range">
          <div className="geist-date-presets" role="group" aria-label="Date range presets">
            {DATE_FILTER_OPTIONS.map(option => (
              <button key={option.value} type="button" aria-pressed={draftFilter === option.value} className={draftFilter === option.value ? "is-selected" : ""} onClick={() => selectPreset(option)}>
                <span>{option.label}</span>
                {draftFilter === option.value && <I d="M5 12l4 4L19 6" size={14} sw={2} />}
              </button>
            ))}
          </div>
          {draftFilter === "custom" && (
            <div className="geist-date-calendar">
              <DatePicker inline selectsRange startDate={draftRange[0]} endDate={draftRange[1]} onChange={(update) => setDraftRange(update)} />
              <div className="geist-date-actions">
                <button type="button" className="geist-date-cancel" onClick={cancelPicker}>Cancel</button>
                <button type="button" className="geist-date-apply" disabled={!draftRange[0] || !draftRange[1]} onClick={applyCustomRange}>Apply range</button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function SplitTestSettings({ start, onStartNow, onClear, onRegPages }) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const triggerRef = useRef(null);
  const panelRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    const focusFrame = requestAnimationFrame(() => panelRef.current?.querySelector("button")?.focus());
    const handlePointerDown = (event) => {
      if (!rootRef.current?.contains(event.target)) setOpen(false);
    };
    const handleKeyDown = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setOpen(false);
        triggerRef.current?.focus();
      }
    };
    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      cancelAnimationFrame(focusFrame);
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [open]);

  const runAndClose = (action) => {
    triggerRef.current?.focus();
    setOpen(false);
    action();
  };
  const formattedStart = start
    ? new Date(start).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
    : "All time";

  return (
    <div ref={rootRef} className={`split-test-settings${open ? " is-open" : ""}`} onBlur={() => requestAnimationFrame(() => { if (!rootRef.current?.contains(document.activeElement)) setOpen(false); })}>
      <button ref={triggerRef} type="button" className="split-test-trigger" aria-label="A/B test settings" title="A/B test settings" aria-haspopup="dialog" aria-expanded={open} onClick={() => setOpen(current => !current)}>
        <I d="M4 7h10M18 7h2M4 17h2m4 0h10M14 4v6M8 14v6" size={14} />
        <span className="split-test-label-desktop">A/B settings</span>
        <span className="split-test-label-mobile" aria-hidden="true">A/B</span>
        <I d="M6 9l6 6 6-6" size={12} />
      </button>
      {open && (
        <div ref={panelRef} className="split-test-popover" role="dialog" aria-label="A/B test settings">
          <div className="split-test-popover-header"><strong>Split-test settings</strong><span>Earlier test traffic is excluded from A/B reports; totals are unaffected.</span></div>
          <div className="split-test-current"><span>A/B from</span><strong>{formattedStart}</strong></div>
          <div className="split-test-popover-actions">
            <button type="button" onClick={() => runAndClose(onStartNow)}>Start now</button>
            {start && <button type="button" onClick={() => runAndClose(onClear)}>Clear</button>}
          </div>
          <div className="split-test-popover-divider" />
          <button type="button" className="split-test-reg-pages" onClick={() => runAndClose(onRegPages)}><span><strong>Registration pages</strong><small>Map landing-page URLs to A/B variants.</small></span><I d="M9 18l6-6-6-6" size={14} /></button>
        </div>
      )}
    </div>
  );
}

function useIsMobile(bp = 768) {
  const [m, setM] = useState(() => typeof window !== 'undefined' && window.innerWidth <= bp);
  useEffect(() => {
    const mq = window.matchMedia(`(max-width: ${bp}px)`);
    const h = (e) => setM(e.matches);
    mq.addEventListener('change', h);
    return () => mq.removeEventListener('change', h);
  }, [bp]);
  return m;
}

function useEscapeKey(onClose) {
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    const handleKey = (event) => {
      if (event.key === "Escape") closeRef.current?.();
    };
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, []);
}

function useDialogFocus() {
  const dialogRef = useRef(null);
  useEffect(() => {
    const previousFocus = document.activeElement;
    const appLayout = document.querySelector(".app-layout");
    const previousAriaHidden = appLayout?.getAttribute("aria-hidden");
    const wasInert = appLayout?.hasAttribute("inert");
    appLayout?.setAttribute("inert", "");
    appLayout?.setAttribute("aria-hidden", "true");

    const dialog = dialogRef.current;
    const getFocusable = () => dialog ? [...dialog.querySelectorAll(
      'button:not(:disabled), [href], input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])'
    )].filter(el => !el.hasAttribute("hidden")) : [];
    const focusFrame = requestAnimationFrame(() => {
      const preferred = dialog?.querySelector("[data-dialog-initial-focus], [autofocus]");
      (preferred || dialog)?.focus();
    });
    const trapFocus = (event) => {
      if (event.key !== "Tab") return;
      const focusable = getFocusable();
      if (!focusable.length) {
        event.preventDefault();
        dialog?.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && (document.activeElement === first || document.activeElement === dialog)) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", trapFocus);
    return () => {
      cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", trapFocus);
      if (appLayout) {
        if (!wasInert) appLayout.removeAttribute("inert");
        if (previousAriaHidden === null) appLayout.removeAttribute("aria-hidden");
        else appLayout.setAttribute("aria-hidden", previousAriaHidden);
      }
      if (previousFocus instanceof HTMLElement) previousFocus.focus();
    };
  }, []);
  return dialogRef;
}

export default function App() {
  const isMobile = useIsMobile();
  const isCompactNav = useIsMobile(1180);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const navMenuRef = useRef(null);
  const navMenuButtonRef = useRef(null);
  const [session, setSession] = useState(null);
  const [userRole, setUserRole] = useState(null);
  const [activeFunnel, setActiveFunnelState] = useState(() => getActiveFunnel());
  const [allowedFunnels, setAllowedFunnels] = useState([activeFunnel]);
  const [variantFilter, setVariantFilter] = useState("all"); // 'all' | 'A' | 'B' | 'undetected' — split-test toggle (funnels with variant data)
  const [abTestStart, setAbTestStartState] = useState(null); // ISO string or null — variants only count from here
  const [regMapOpen, setRegMapOpen] = useState(false); // reg-page → variant map editor (admin)
  const [authLoading, setAuthLoading] = useState(true);
  const [authEmail, setAuthEmail] = useState("");
  const [authPass, setAuthPass] = useState("");
  const [authError, setAuthError] = useState("");
  const [authSubmitting, setAuthSubmitting] = useState(false);
  // Password-recovery flow: arriving via a reset link must show a set-new-password
  // screen before the dashboard — otherwise the person never gets to choose one.
  const [recoveryMode, setRecoveryMode] = useState(LANDED_FROM_RECOVERY);
  const [authNotice, setAuthNotice] = useState(AUTH_LINK_ERROR_MSG);
  const [newPass, setNewPass] = useState("");
  const [newPass2, setNewPass2] = useState("");
  const [pwError, setPwError] = useState("");
  const [pwSubmitting, setPwSubmitting] = useState(false);
  const [forgotOpen, setForgotOpen] = useState(false);
  const [resetLinkOpen, setResetLinkOpen] = useState(false);
  const [metrics, setMetrics] = useState([]);
  const [customs, setCustoms] = useState([]);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("dash");
  const [viewMode, setViewMode] = useState("list");
  // Default: Last 7 Days on desktop, Today on mobile.
  const [dateFilter, setDateFilter] = useState(() =>
    (typeof window !== 'undefined' && window.innerWidth <= 768) ? "today" : "7"
  );
  const [dateRange, setDateRange] = useState([null, null]);
  const [toast, setToast] = useState(null);
  const [editCM, setEditCM] = useState(null);
  const [delConfirm, setDelConfirm] = useState(null);
  const [delCM, setDelCM] = useState(null);
  const [editRow, setEditRow] = useState(null);
  const [search, setSearch] = useState("");
  useEffect(() => {
    if (isMobile) setSearch("");
  }, [isMobile]);
  const [events, setEvents] = useState([]);
  const [evFilter, setEvFilter] = useState("");
  // CRM (people spine: tracking_contacts ⋈ events by email)
  const [crmContacts, setCrmContacts] = useState([]);
  const [crmStats, setCrmStats] = useState(null);
  const [crmTotal, setCrmTotal] = useState(0);
  const [crmOffset, setCrmOffset] = useState(0);
  const [crmSearch, setCrmSearch] = useState("");
  const [crmStage, setCrmStage] = useState("");
  const [crmSalesPage, setCrmSalesPage] = useState("");   // sales-page filter (label) or ""
  const [salesPages, setSalesPages] = useState(null);     // { map, isDefault, default } or null
  const [salesEditorOpen, setSalesEditorOpen] = useState(false);
  const [crmLoading, setCrmLoading] = useState(false);
  const [crmContact, setCrmContact] = useState(null);
  const [crmContactTab, setCrmContactTab] = useState("journey");
  const [emailReport, setEmailReport] = useState(null);
  const [emailReportLoading, setEmailReportLoading] = useState(false);
  const [emailDrill, setEmailDrill] = useState(null);        // { source, ...clicks } or null
  const [emailDrillLoading, setEmailDrillLoading] = useState(false);
  const [lenses, setLenses] = useState([]);
  const [lensesLoaded, setLensesLoaded] = useState(false);
  const [activeLensId, setActiveLensId] = useState("default-all");
  const [lensMenuOpen, setLensMenuOpen] = useState(false);
  const [lensEditing, setLensEditing] = useState(null); // null | { id?, name, metrics }
  const [summaryCards, setSummaryCards] = useState(DEFAULT_SUMMARY_CARDS);
  const [summaryEditorOpen, setSummaryEditorOpen] = useState(false);
  const lensMenuRef = useRef(null);
  const activeLens = (() => {
    const found = lenses.find(l => l.id === activeLensId);
    if (found) return found;
    // Until lenses load, show NO metric columns rather than all of MK — otherwise
    // columns the active lens hides (e.g. stayed_45/60/80) flash in, then vanish.
    if (!lensesLoaded) return { id: activeLensId, name: "All Metrics", metrics: [] };
    return lenses[0] || { id: "default-all", name: "All Metrics", metrics: MK };
  })();
  const isColVisible = (col) => activeLens.metrics.includes(col);

  // In a single-variant view, spend and reg-page clicks (and any custom metric
  // built on them) have no A/B breakdown, so we drop them from the table/cards.
  const inVariantView = variantFilter !== "all";
  const isHiddenInVariant = (col) => {
    if (!inVariantView) return false;
    if (col.type === "base") return UNSPLITTABLE_FIELDS.includes(col.key);
    if (col.type === "custom") return formulaUsesUnsplittable(col.cm?.formula);
    return false;
  };
  const cardHiddenInVariant = (card) => {
    if (!inVariantView) return false;
    if (String(card.key).startsWith("cm:")) {
      const cm = customs.find(c => `cm:${c.id}` === card.key);
      return formulaUsesUnsplittable(cm?.formula);
    }
    return UNSPLITTABLE_FIELDS.includes(card.key);
  };

  // ─── Column ordering ────────────────────────────────────────────
  const [colOrder, setColOrder] = useState(null);
  const [colEditorOpen, setColEditorOpen] = useState(false);
  // Row selection: clicking rows scopes the summary cards to those rows.
  // Set semantics — non-contiguous selections aggregate only the picked days.
  const [selectedDates, setSelectedDates] = useState(() => new Set());
  // Anchor for shift-click range selection (last clicked row's date).
  const selectionAnchorRef = useRef(null);

  // Build the ordered list of column descriptors
  const buildColList = useCallback(() => {
    const baseCols = MK.filter(k => COL_LABELS[k]).map(k => ({ key: k, label: COL_LABELS[k], type: "base" }));
    const customCols = customs.map(cm => ({ key: `cm:${cm.id}`, label: cm.name, type: "custom", cm }));
    const all = [...baseCols, ...customCols];
    if (!colOrder) return all;
    // Sort by saved order, putting unknowns at the end
    const orderMap = {};
    colOrder.forEach((k, i) => { orderMap[k] = i; });
    const ordered = [...all].sort((a, b) => {
      const ai = orderMap[a.key] !== undefined ? orderMap[a.key] : 9999;
      const bi = orderMap[b.key] !== undefined ? orderMap[b.key] : 9999;
      return ai - bi;
    });
    return ordered;
  }, [customs, colOrder]);

  const orderedCols = buildColList();

  const saveColOrder = async (keys) => {
    setColOrder(keys);
    try {
      const headers = await getAuthHeaders();
      await fetch(`${API_BASE}/api/me/preferences`, { method: "PUT", headers, body: JSON.stringify({ preferences: { col_order: keys } }) });
    } catch { }
  };
  const fetchLenses = async () => {
    try {
      const headers = await getAuthHeaders();
      const res = await fetch(`${API_BASE}/api/lenses`, { headers });
      const data = await res.json();
      setLenses(data.data || []);
    } catch { /* silent */ }
    finally { setLensesLoaded(true); }
  };
  const saveLens = async (lens) => {
    try {
      const headers = await getAuthHeaders();
      if (lens.id) {
        await fetch(`${API_BASE}/api/lenses/${lens.id}`, { method: "PUT", headers, body: JSON.stringify({ name: lens.name, metrics: lens.metrics }) });
      } else {
        await fetch(`${API_BASE}/api/lenses`, { method: "POST", headers, body: JSON.stringify({ name: lens.name, metrics: lens.metrics }) });
      }
      await fetchLenses();
      setLensEditing(null);
      flash(lens.id ? "Lens updated" : "Lens created");
    } catch (e) { flash(e.message, "err"); }
  };
  const deleteLens = async (id) => {
    try {
      const headers = await getAuthHeaders();
      await fetch(`${API_BASE}/api/lenses/${id}`, { method: "DELETE", headers });
      if (activeLensId === id) setActiveLensId("default-all");
      await fetchLenses();
      flash("Lens deleted");
    } catch (e) { flash(e.message, "err"); }
  };
  const setDefaultLens = async (id) => {
    try {
      const headers = await getAuthHeaders();
      await fetch(`${API_BASE}/api/me/preferences`, { method: "PUT", headers, body: JSON.stringify({ preferences: { default_lens_id: id } }) });
      flash("Default lens set");
    } catch { /* silent */ }
  };
  useEffect(() => {
    const h = (e) => {
      if (lensMenuRef.current && !lensMenuRef.current.contains(e.target)) setLensMenuOpen(false);
      document.querySelectorAll(".navbar-dropdown[open]").forEach(menu => {
        if (!menu.contains(e.target)) menu.removeAttribute("open");
      });
    };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  // Close mobile menu on nav
  useEffect(() => { setMobileMenuOpen(false); }, [view]);
  useEffect(() => {
    if (!isCompactNav) {
      if (mobileMenuOpen) setMobileMenuOpen(false);
      return;
    }
    if (!mobileMenuOpen) return;
    const menu = navMenuRef.current;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const getFocusable = () => menu ? [...menu.querySelectorAll(
      'button:not(:disabled), select:not(:disabled), [href], [tabindex]:not([tabindex="-1"])'
    )] : [];
    const focusFrame = requestAnimationFrame(() => {
      const closeButton = menu?.querySelector(".nav-menu-close");
      (closeButton || getFocusable()[0])?.focus();
    });
    const handleKey = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setMobileMenuOpen(false);
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = getFocusable();
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", handleKey);
    return () => {
      cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", handleKey);
      document.body.style.overflow = previousOverflow;
      navMenuButtonRef.current?.focus();
    };
  }, [isCompactNav, mobileMenuOpen]);
  const tt = useRef(null);

  const flash = useCallback((msg, type = "ok") => { if (tt.current) clearTimeout(tt.current); setToast({ msg, type }); tt.current = setTimeout(() => setToast(null), 3000); }, []);

  const loadData = useCallback(async () => {
    try {
      const [mRes, cRes] = await Promise.all([api.getMetrics(90, 0), api.getCustomMetrics()]);
      setMetrics(mRes.data || []);
      setCustoms(cRes.data || []);
    } catch (e) {
      console.error("Load error:", e);
      flash("Failed to load data", "err");
    } finally {
      setLoading(false);
    }
  }, [flash]);

  // ─── Auth ───────────────────────────────────────────────────────────────────
  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session: s } }) => {
      setSession(s);
      if (s) fetchRole(s.access_token);
      else setAuthLoading(false);
    });
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, s) => {
      if (_event === "PASSWORD_RECOVERY") setRecoveryMode(true);
      setSession(s);
      if (s) fetchRole(s.access_token);
      else { setUserRole(null); setAuthLoading(false); }
    });
    return () => subscription.unsubscribe();
  }, []);

  const fetchRole = async (token) => {
    try {
      // ── Step 1: Discover allowed funnels ────────────────────────────
      // /api/me/funnels does NOT enforce funnel access (it's the bootstrap
      // endpoint). We must hit this FIRST so we know which funnel to send
      // on subsequent authenticated requests — otherwise a stale localStorage
      // funnel value can lock the user out of their own dashboard.
      let allowed = ["analytics"];
      try {
        const fr = await fetch(`${API_BASE}/api/me/funnels`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (fr.ok) {
          const fd = await fr.json();
          if (Array.isArray(fd.funnels) && fd.funnels.length > 0) allowed = fd.funnels;
        }
      } catch { /* fall back to analytics */ }
      setAllowedFunnels(allowed);

      // Snap activeFunnel to a valid one before any other authenticated call
      let active = getActiveFunnel();
      if (!allowed.includes(active)) {
        active = allowed[0];
        setActiveFunnelLS(active);
        setActiveFunnelState(active);
      }

      // ── Step 2: Now safely call /api/me with a valid funnel ─────────
      const res = await fetch(`${API_BASE}/api/me`, {
        headers: { Authorization: `Bearer ${token}`, "X-Funnel": active },
      });
      if (res.ok) {
        const data = await res.json();
        setUserRole(data.role || "viewer");
        if (data.preferences?.default_lens_id) setActiveLensId(data.preferences.default_lens_id);
        if (data.preferences?.col_order) setColOrder(data.preferences.col_order);
        else if (data.default_col_order) setColOrder(data.default_col_order);
        if (data.preferences?.summary_cards) setSummaryCards(data.preferences.summary_cards);
      } else {
        setUserRole("viewer");
      }
    } catch { setUserRole("viewer"); }
    setAuthLoading(false);
  };

  // Switch the active funnel: persist, update state, and refetch everything.
  const switchFunnel = useCallback((f) => {
    if (f === activeFunnel) return;
    if (!allowedFunnels.includes(f)) return;
    setActiveFunnelLS(f);
    setActiveFunnelState(f);
  }, [activeFunnel, allowedFunnels]);

  const isAdmin = userRole === "admin";

  // CRM loaders. The list reloads (debounced) when entering the CRM view or when
  // the search/stage filter changes; opening a row fetches that person's journey.
  const loadCrm = useCallback(async () => {
    setCrmLoading(true);
    try {
      const list = await api.getCrmContacts(crmSearch, crmStage, CRM_PAGE, crmOffset, crmSalesPage);
      setCrmContacts(list.data || []);
      setCrmTotal(list.total || 0);
    } catch (e) { flash(e.message, "err"); }
    setCrmLoading(false);
  }, [crmSearch, crmStage, crmOffset, crmSalesPage]);
  // Stats scan the whole view, so fetch them once on entering CRM / changing
  // filters — not on every page turn.
  const loadCrmStats = useCallback(async () => {
    try { setCrmStats((await api.getCrmStats()) || null); } catch { /* non-fatal */ }
  }, []);
  useEffect(() => {
    if (view !== "crm") return;
    const t = setTimeout(loadCrm, 250);
    return () => clearTimeout(t);
  }, [view, loadCrm]);
  useEffect(() => { if (view === "crm") loadCrmStats(); }, [view, loadCrmStats]);
  // Load the configured sales pages once on entering CRM (for the filter + badges + editor).
  useEffect(() => { if (view === "crm" && !salesPages) api.getSalesPages().then(setSalesPages).catch(() => {}); }, [view, salesPages]);
  // Reset to the first page whenever the search/stage/sales-page filter changes.
  useEffect(() => { setCrmOffset(0); }, [crmSearch, crmStage, crmSalesPage]);
  const openCrmContact = async (row) => {
    try {
      const data = await api.getCrmContact(row.contact_id || row.email);
      setCrmContactTab("journey");
      setCrmContact(data);
    } catch (e) { flash(e.message, "err"); }
  };
  const loadEmailReport = useCallback(async () => {
    setEmailReportLoading(true);
    try { setEmailReport(await api.getEmailReport()); } catch (e) { flash(e.message, "err"); }
    setEmailReportLoading(false);
  }, []);
  useEffect(() => { if (view === "emailreport") loadEmailReport(); }, [view, loadEmailReport]);
  const openEmailDrill = async (source) => {
    setEmailDrillLoading(true); setEmailDrill({ source, clicks: [] });
    try { setEmailDrill(await api.getEmailReportClicks(source)); } catch (e) { flash(e.message, "err"); setEmailDrill(null); }
    setEmailDrillLoading(false);
  };

  const handleLogin = async (e) => {
    e.preventDefault();
    setAuthError("");
    setAuthSubmitting(true);
    try {
      const { error } = await supabase.auth.signInWithPassword({ email: authEmail, password: authPass });
      if (error) setAuthError(error.message);
    } finally {
      setAuthSubmitting(false);
    }
  };

  const handleLogout = async () => {
    setMobileMenuOpen(false);
    await supabase.auth.signOut();
    setSession(null);
    setUserRole(null);
  };

  const handleSetPassword = async (e) => {
    e.preventDefault();
    setPwError("");
    if (newPass.length < 8) { setPwError("Use at least 8 characters."); return; }
    if (newPass !== newPass2) { setPwError("Passwords don't match."); return; }
    setPwSubmitting(true);
    try {
      const { error } = await supabase.auth.updateUser({ password: newPass });
      if (error) setPwError(error.message);
      else {
        setRecoveryMode(false);
        setNewPass(""); setNewPass2("");
        flash("Password updated — you're signed in");
      }
    } finally {
      setPwSubmitting(false);
    }
  };

  useEffect(() => { loadData(); fetchLenses(); }, [loadData]);
  useEffect(() => { const i = setInterval(loadData, 30000); return () => clearInterval(i); }, [loadData]);
  // A/B test start cutoff (per funnel) — refetch on mount + funnel switch.
  useEffect(() => {
    let cancelled = false;
    api.getAbTestStart().then(r => { if (!cancelled) setAbTestStartState(r.start || null); }).catch(() => {});
    return () => { cancelled = true; };
  }, [activeFunnel]);
  const startAbTestNow = async () => {
    if (!window.confirm("Start the A/B test now?\n\nVariant data collected before this moment (your test traffic) will be excluded from the A/B reports — totals are unaffected. You can change or clear this anytime.")) return;
    try { const r = await api.setAbTestStart({ now: true }); setAbTestStartState(r.start || null); await loadData(); flash("A/B test now counts from now"); }
    catch (e) { flash(e.message, "err"); }
  };
  const clearAbTest = async () => {
    try { const r = await api.setAbTestStart({ start: null }); setAbTestStartState(r.start || null); await loadData(); flash("Cleared — counting all variant data"); }
    catch (e) { flash(e.message, "err"); }
  };
  // Admin: link a sale/event (alias email) to its real registrant so it counts in the
  // correct variant. Recomputes the dashboard afterward.
  const linkSaleToRegistrant = async (aliasEmail, canonicalEmail) => {
    try {
      const r = await api.linkIdentity({ alias_email: aliasEmail, canonical_email: canonicalEmail });
      flash(`Linked ${aliasEmail} → ${canonicalEmail}${r.variant ? ` · counts as variant ${r.variant}` : " (registrant has no variant yet)"}`);
      loadData();
      if (view === "crm") loadCrm();
    } catch (e) { flash(e.message, "err"); }
  };

  // When the active funnel changes (post-mount), refetch role + data + lenses.
  // The initial fetches above already run on mount with the persisted funnel.
  const funnelMountedRef = useRef(false);
  useEffect(() => {
    if (!funnelMountedRef.current) { funnelMountedRef.current = true; return; }
    setVariantFilter("all"); // variant toggle is per-funnel; reset on switch
    if (session?.access_token) fetchRole(session.access_token);
    loadData();
    fetchLenses();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeFunnel]);

  const refreshWebhook = useCallback(async () => {
    flash("Refreshing spend data…", "ok");
    try {
      const res = await fetch(`${API_BASE}/api/refresh`, { method: "POST" });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        const msg = body.error || body.message || JSON.stringify(body);
        flash(msg, "err");
        return;
      }
      await loadData();
      flash(body.message || "Spend data refreshed", "ok");
    } catch (e) {
      flash(e.message || "Refresh failed", "err");
    }
  }, [loadData, flash]);

  const clearCache = useCallback(async () => {
    flash("Clearing server cache…", "ok");
    try {
      const body = await api.clearCache();
      await loadData();
      flash(body.message || "Cache cleared", "ok");
    } catch (e) {
      flash(e.message || "Cache clear failed", "err");
    }
  }, [loadData, flash]);

  const [finalizing, setFinalizing] = useState(false);
  const finalizePastDays = useCallback(async () => {
    if (finalizing) return;
    if (!window.confirm("Finalize all past days? This freezes their values from the events table. Safe to run multiple times — already-finalized days are skipped.")) return;
    setFinalizing(true);
    flash("Finalizing past days…", "ok");
    try {
      const body = await api.finalizePastDays();
      await loadData();
      const msg = body.total === 0
        ? "All past days are already finalized."
        : `Finalized ${body.finalized}/${body.total} past day${body.total === 1 ? "" : "s"}.`;
      flash(msg, "ok");
    } catch (e) {
      flash(e.message || "Finalize failed", "err");
    } finally {
      setFinalizing(false);
    }
  }, [finalizing, loadData, flash]);

  const finalizeDay = useCallback(async (dateRaw) => {
    const iso = dateRaw.includes('/') ? dateRaw.replace(/(\d{2})\/(\d{2})\/(\d{4})/, '$3-$1-$2') : dateRaw;
    if (!window.confirm(`Refinalize ${iso}? Recomputes deduped values from events and overwrites the canonical columns.`)) return;
    flash(`Finalizing ${iso}…`, "ok");
    try {
      const body = await api.finalizeDate(iso);
      await loadData();
      const fields = body.written ? Object.keys(body.written).length : 0;
      flash(fields > 0 ? `Finalized ${iso} (${fields} field${fields === 1 ? "" : "s"} updated)` : `Finalized ${iso} (no events found)`, "ok");
    } catch (e) {
      flash(e.message || "Finalize failed", "err");
    }
  }, [flash, loadData]);

  const recalcSpend = useCallback(async (dateRaw) => {
    const iso = dateRaw.includes('/') ? dateRaw.replace(/(\d{2})\/(\d{2})\/(\d{4})/, '$3-$1-$2') : dateRaw;
    flash(`Recalculating spend for ${iso}…`, "ok");
    try {
      const res = await fetch(`${API_BASE}/api/refresh-date`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ date: iso }) });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) { flash(body.error || "Recalculation failed", "err"); return; }
      await loadData();
      flash(body.message || "Spend updated", "ok");
    } catch (e) { flash(e.message || "Recalculation failed", "err"); }
  }, [loadData, flash]);

  const submitEntry = useCallback(async (entry) => {
    try {
      if (editRow) { await api.updateMetric(entry.date, entry); }
      else { await api.upsertMetric(entry); }
      await loadData();
      flash(editRow ? "Entry updated" : "Entry added");
      setView("dash"); setEditRow(null);
    } catch (e) { flash(e.message, "err"); }
  }, [editRow, loadData, flash]);

  const deleteEntry = useCallback(async (date) => {
    try { await api.deleteMetric(date); await loadData(); flash("Entry deleted"); setDelConfirm(null); }
    catch (e) { flash(e.message, "err"); }
  }, [loadData, flash]);

  const saveCM = useCallback(async (cm) => {
    try { await api.saveCustomMetric(cm); await loadData(); flash(cm.id ? "Metric updated" : "Custom metric created"); setView("dash"); setEditCM(null); }
    catch (e) { flash(e.message, "err"); }
  }, [loadData, flash]);

  const deleteCM = useCallback(async (id) => {
    try { await api.deleteCustomMetric(id); await loadData(); flash("Custom metric deleted"); setDelCM(null); }
    catch (e) { flash(e.message, "err"); }
  }, [loadData, flash]);

  // Resolve the active split-test variant CLIENT-SIDE: each row carries a
  // `variants` breakdown (expand=variants), so toggling A/B/undetected merges
  // that bucket's counts over the row — no refetch, no flicker. Custom metrics
  // then evaluate against the selected variant's own numbers (e.g. attendance %
  // = attended_A / registrations_A). 'all' uses the row as-is.
  const viewMetrics = variantFilter === "all"
    ? metrics
    : metrics.map(m => (m.variants && m.variants[variantFilter]) ? { ...m, ...m.variants[variantFilter] } : m);

  const filtered = viewMetrics.filter(m => {
    if (search) {
      const s = search.toLowerCase();
      if (!(m.date.includes(s) || (m.day || "").toLowerCase().includes(s) || fmtDateNice(m.date).toLowerCase().includes(s))) return false;
    }
    return true;
  });

  // tableRows = rows matching the search box only. The table always shows
  // the full list so users can pick any day regardless of the card filter.
  const tableRows = filtered;

  // displayRows = search + date filter. Drives summary cards / totals when
  // no rows are explicitly selected.
  const displayRows = filtered.filter(m => {
    const d = parseMDate(m.date);
    const today = parseMDate(getLADate()); // LA-pinned "today", not browser-local
    if (dateFilter === "today" && d.getTime() !== today.getTime()) return false;
    if (dateFilter === "yesterday" && d.getTime() !== subDays(today, 1).getTime()) return false;
    if (dateFilter === "7" && d < subDays(today, 7)) return false;
    if (dateFilter === "30" && d < subDays(today, 30)) return false;
    if (dateFilter === "custom") {
      const [start, end] = dateRange;
      if (start && d < startOfDay(start)) return false;
      if (end && d > endOfDay(end)) return false;
    }
    return true;
  });

  // When rows are selected, scope cards to that set only.
  // Otherwise fall back to the date-filtered card view. Selection is sourced
  // from tableRows (which ignores the date filter), so a selected day is
  // honored even if it's outside the current card-scope filter.
  const effectiveRows = selectedDates.size > 0
    ? tableRows.filter(r => selectedDates.has(r.date))
    : displayRows;

  // Toggle/extend selection. Shift-click extends from the anchor row to the
  // clicked row across the currently visible rows; plain click toggles.
  const toggleRowSelection = useCallback((date, shiftKey) => {
    setSelectedDates(prev => {
      const next = new Set(prev);
      if (shiftKey && selectionAnchorRef.current && selectionAnchorRef.current !== date) {
        const dates = tableRows.map(r => r.date);
        const a = dates.indexOf(selectionAnchorRef.current);
        const b = dates.indexOf(date);
        if (a !== -1 && b !== -1) {
          const [lo, hi] = a < b ? [a, b] : [b, a];
          for (let i = lo; i <= hi; i++) next.add(dates[i]);
        } else if (next.has(date)) next.delete(date); else next.add(date);
      } else if (next.has(date)) {
        next.delete(date);
      } else {
        next.add(date);
      }
      return next;
    });
    selectionAnchorRef.current = date;
  }, [tableRows]);

  const clearSelection = useCallback(() => {
    setSelectedDates(new Set());
    selectionAnchorRef.current = null;
  }, []);

  // Drop the selection when the search changes (selected rows may have
  // dropped out of the visible table). Date filter changes don't clear it
  // since the table no longer narrows by date.
  useEffect(() => {
    setSelectedDates(new Set());
    selectionAnchorRef.current = null;
  }, [search]);

  const saveSummaryCards = async (cards) => {
    setSummaryCards(cards);
    try {
      const headers = await getAuthHeaders();
      await fetch(`${API_BASE}/api/me/preferences`, { method: "PUT", headers, body: JSON.stringify({ preferences: { summary_cards: cards } }) });
    } catch { }
  };

  const formatSummaryVal = (val, format) => {
    if (format === "currency") return `$${val.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
    if (format === "decimal") return val.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return val.toLocaleString("en-US", { maximumFractionDigits: 1 });
  };

  const getSummaryCardVal = (card) => {
    if (effectiveRows.length === 0) return formatSummaryVal(0, card.format);
    const values = effectiveRows.map(row => getSummaryRowValue(card, row, customs));
    const sum = values.reduce((total, value) => total + value, 0);
    const raw = card.agg === "avg" ? sum / effectiveRows.length : sum;
    return formatSummaryVal(Math.round(raw * 100) / 100, card.format);
  };

  // % change compares recent half vs older half. With an active selection
  // it's not meaningful (sample is too small / arbitrary), so we suppress it.
  const half = Math.floor(displayRows.length / 2);
  const recent = displayRows.slice(0, half || 1);
  const older = displayRows.slice(half || 1);
  const pctChange = (card) => {
    const rSum = recent.reduce((sum, row) => sum + getSummaryRowValue(card, row, customs), 0);
    const oSum = older.reduce((sum, row) => sum + getSummaryRowValue(card, row, customs), 0);
    if (oSum === 0) return null;
    return Math.round(((rSum - oSum) / oSum) * 100);
  };

  // Auth loading
  if (authLoading) return (
    <div style={S.app}><style>{CSS}</style>
      <FullScreenLoading label="Preparing your workspace…" />
    </div>
  );

  // Set-new-password screen — arriving via a recovery link signs the person in
  // with a one-time session; they MUST set a password here before the dashboard.
  if (session && recoveryMode) return (
    <div style={S.app}><style>{CSS}</style>
      <div className="auth-shell" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100dvh", padding: "32px 16px" }}>
        <form onSubmit={handleSetPassword} className="login-form" style={{ width: "100%", maxWidth: 380, background: "#fff", borderRadius: 16, padding: "48px 36px", boxShadow: "0 8px 32px rgba(0,0,0,0.08)", border: "1px solid var(--ds-border)" }}>
          <div style={{ textAlign: "center", marginBottom: 32 }}>
            <img src="https://portal-drshumard.b-cdn.net/trans_sized.png" alt="Logo" style={{ height: 36, objectFit: "contain", marginBottom: 16 }} />
            <h1 style={{ fontSize: 24, fontWeight: 600, color: "#171717", margin: 0, letterSpacing: "-0.03em" }}>Set a new password</h1>
            <p style={{ fontSize: 14, color: "#666", marginTop: 8 }}>for {session.user?.email}</p>
          </div>
          {pwError && <div role="alert" style={{ background: "#FFF7F7", color: "#C00", padding: "10px 14px", borderRadius: 8, fontSize: 13, marginBottom: 16, border: "1px solid #F5B7B7" }}>{pwError}</div>}
          <div style={{ marginBottom: 16 }}>
            <label htmlFor="new-password" style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#333", marginBottom: 6 }}>New password</label>
            <input id="new-password" type="password" autoComplete="new-password" value={newPass} onChange={e => setNewPass(e.target.value)} required minLength={8} style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", fontSize: 14, outline: "none", boxSizing: "border-box" }} />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label htmlFor="new-password-2" style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#333", marginBottom: 6 }}>Confirm new password</label>
            <input id="new-password-2" type="password" autoComplete="new-password" value={newPass2} onChange={e => setNewPass2(e.target.value)} required minLength={8} style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", fontSize: 14, outline: "none", boxSizing: "border-box" }} />
          </div>
          <button type="submit" disabled={pwSubmitting} aria-busy={pwSubmitting} style={{ ...S.btnDark, width: "100%", justifyContent: "center", padding: "11px 0", borderRadius: 8, fontSize: 14, fontWeight: 600 }}>{pwSubmitting ? "Saving…" : "Save password"}</button>
          <p style={{ fontSize: 12, color: "var(--ds-gray-600)", textAlign: "center", marginTop: 16, marginBottom: 0 }}>At least 8 characters. You'll stay signed in after saving.</p>
        </form>
      </div>
    </div>
  );

  // Login screen
  if (!session) return (
    <div style={S.app}><style>{CSS}</style>
      <div className="auth-shell" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100dvh", padding: "32px 16px" }}>
        <form onSubmit={handleLogin} className="login-form" style={{ width: "100%", maxWidth: 380, background: "#fff", borderRadius: 16, padding: "48px 36px", boxShadow: "0 8px 32px rgba(0,0,0,0.08)", border: "1px solid var(--ds-border)" }}>
          <div style={{ textAlign: "center", marginBottom: 32 }}>
            <img src="https://portal-drshumard.b-cdn.net/trans_sized.png" alt="Logo" style={{ height: 36, objectFit: "contain", marginBottom: 16 }} />
            <h1 style={{ fontSize: 24, fontWeight: 600, color: "#171717", margin: 0, letterSpacing: "-0.03em" }}>Welcome back</h1>
            <p style={{ fontSize: 14, color: "#666", marginTop: 8 }}>Sign in to Shumard Analytics</p>
          </div>
          {authNotice && <div role="status" style={{ background: "#F0F7FF", color: "#0A4A8F", padding: "10px 14px", borderRadius: 8, fontSize: 13, marginBottom: 16, border: "1px solid #BBD6F5" }}>{authNotice}</div>}
          {authError && <div role="alert" style={{ background: "#FFF7F7", color: "#C00", padding: "10px 14px", borderRadius: 8, fontSize: 13, marginBottom: 16, border: "1px solid #F5B7B7" }}>{authError}</div>}
          <div style={{ marginBottom: 16 }}>
            <label htmlFor="auth-email" style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#333", marginBottom: 6 }}>Email</label>
            <input id="auth-email" type="email" autoComplete="email" value={authEmail} onChange={e => setAuthEmail(e.target.value)} required style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", fontSize: 14, outline: "none", boxSizing: "border-box" }} />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label htmlFor="auth-password" style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#333", marginBottom: 6 }}>Password</label>
            <input id="auth-password" type="password" autoComplete="current-password" value={authPass} onChange={e => setAuthPass(e.target.value)} required style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", fontSize: 14, outline: "none", boxSizing: "border-box" }} />
          </div>
          <button type="submit" disabled={authSubmitting} aria-busy={authSubmitting} style={{ ...S.btnDark, width: "100%", justifyContent: "center", padding: "11px 0", borderRadius: 8, fontSize: 14, fontWeight: 600 }}>{authSubmitting ? "Signing in…" : "Sign in"}</button>
          <div style={{ textAlign: "center", marginTop: 12 }}>
            <button type="button" onClick={() => { setAuthError(""); setForgotOpen(true); }} style={{ background: "none", border: "none", padding: 0, fontSize: 13, color: "var(--ds-gray-600)", cursor: "pointer", textDecoration: "underline", textUnderlineOffset: 3 }}>Forgot password?</button>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 12, margin: "20px 0" }}>
            <div style={{ flex: 1, height: 1, background: "var(--ds-border)" }} />
            <span style={{ fontSize: 12, color: "var(--ds-gray-600)", fontWeight: 500 }}>or</span>
            <div style={{ flex: 1, height: 1, background: "var(--ds-border)" }} />
          </div>
          <button type="button" onClick={() => supabase.auth.signInWithOAuth({ provider: "google", options: { redirectTo: window.location.origin } })} style={{ ...S.btnLight, width: "100%", justifyContent: "center", padding: "11px 0", borderRadius: 10, fontSize: 14, fontWeight: 600, gap: 10 }}>
            <svg width="18" height="18" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z" /><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z" /><path fill="#FBBC05" d="M10.53 28.59a14.5 14.5 0 010-9.18l-7.98-6.19a24.01 24.01 0 000 21.56l7.98-6.19z" /><path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z" /></svg>
            Continue with Google
          </button>
        </form>
      </div>
      {forgotOpen && <ForgotPasswordModal initialEmail={authEmail} onCancel={() => setForgotOpen(false)} onSent={em => { setForgotOpen(false); setAuthNotice(`Password reset link sent to ${em}. Open it promptly — links are single-use.`); }} />}
    </div>
  );

  if (loading) return (
    <div style={S.app}><style>{CSS}</style>
      <FullScreenLoading label="Loading analytics…" />
    </div>
  );

  const effectiveViewMode = isMobile ? "board" : viewMode;

  const navigationGroups = [
    {
      label: "Workspace",
      items: [
        { id: "dash", label: "Overview", icon: "M4 13h6V4H4v9zm10 7h6V11h-6v9zM4 20h6v-3H4v3zm10-13h6V4h-6v3z" },
        { id: "crm", label: "People", icon: "M16 21v-2a4 4 0 00-4-4H6a4 4 0 00-4 4v2M9 11a4 4 0 100-8 4 4 0 000 8zm13 10v-2a4 4 0 00-3-3.87m-2-11.96a4 4 0 010 7.75" },
        { id: "emailreport", label: "Email report", icon: "M4 4h16a2 2 0 012 2v12a2 2 0 01-2 2H4a2 2 0 01-2-2V6a2 2 0 012-2zm18 2l-10 7L2 6" },
        { id: "events", label: "Activity", icon: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" },
        { id: "insights", label: "AI insights", icon: "M9.7 17h4.6M12 3v1m6.4 1.6l-.7.7M21 12h-1M4 12H3m3.3-5.7l-.7-.7m2.8 9.9a5 5 0 117.1 0l-.6.6A3.4 3.4 0 0014 18.5V19a2 2 0 11-4 0v-.5c0-.9-.4-1.8-1-2.4l-.6-.6z" },
      ],
    },
    ...(isAdmin ? [{
      label: "Admin",
      items: [
        { id: "query", label: "Data explorer", icon: "M4 7v10c0 2.2 3.6 4 8 4s8-1.8 8-4V7M4 7c0 2.2 3.6 4 8 4s8-1.8 8-4M4 7c0-2.2 3.6-4 8-4s8 1.8 8 4" },
        { id: "worker", label: "AI worker", icon: "M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.8-3.8a6 6 0 01-8 8l-6.9 6.9a2.1 2.1 0 01-3-3l6.9-6.9a6 6 0 018-8l-3.8 3.8z" },
        { id: "custom-list", label: "Metric settings", icon: "M12 15.5a3.5 3.5 0 100-7 3.5 3.5 0 000 7zm0-12v2m0 13v2m8.5-8.5h-2m-13 0h-2m14.5-6l-1.4 1.4M7.4 16.6L6 18m12 0l-1.4-1.4M7.4 7.4L6 6" },
      ],
    }] : []),
  ];

  const viewTitles = {
    dash: "Overview", entry: editRow ? "Edit entry" : "New entry", crm: "People",
    emailreport: "Email report", events: "Activity", insights: "AI insights",
    worker: "AI worker", query: "Data explorer", custom: editCM ? "Edit metric" : "New metric",
    "custom-list": "Metric settings",
  };

  const isNavActive = (id) => {
    if (id === "dash") return view === "dash" || view === "entry";
    if (id === "custom-list") return view === "custom-list" || view === "custom";
    return view === id;
  };

  const navigateTo = async (id) => {
    setMobileMenuOpen(false);
    document.querySelectorAll(".navbar-dropdown[open]").forEach(menu => menu.removeAttribute("open"));
    if (id === "events") {
      try { const r = await api.getEvents(100, evFilter); setEvents(r.data || []); }
      catch (e) { flash(e.message, "err"); }
    }
    setView(id);
    setEditRow(null);
    if (id !== "custom") setEditCM(null);
  };

  return (
    <div style={S.app}><style>{CSS}</style>
      <a className="skip-link" href="#app-main">Skip to content</a>
      <div className="app-layout">
        <header className="app-navbar">
          <div className="navbar-main" aria-hidden={isCompactNav && mobileMenuOpen ? "true" : undefined} inert={isCompactNav && mobileMenuOpen ? "" : undefined}>
            <div className="navbar-left">
              <div className="navbar-brand">
                <div className="navbar-brand-mark" aria-hidden="true">S</div>
                <div className="navbar-brand-copy"><strong>Shumard</strong><span>Analytics</span></div>
              </div>
              {allowedFunnels.length > 1 && (
                <div className="navbar-workspace">
                  <span className="navbar-workspace-label">Workspace</span>
                  <GeistSelect
                    className="workspace-geist-select"
                    value={activeFunnel}
                    onChange={switchFunnel}
                    ariaLabel="Active workspace"
                    options={allowedFunnels.map(f => ({ value: f, label: f === "analytics" ? "Main funnel" : f === "native" ? "Native funnel" : f }))}
                  />
                </div>
              )}
              <span className="navbar-current">{viewTitles[view] || "Analytics"}</span>
            </div>

            <nav className="navbar-routes" aria-label="Primary navigation">
              {navigationGroups[0].items.map(item => (
                <button key={item.id} className={`navbar-route${isNavActive(item.id) ? " is-active" : ""}`} onClick={() => navigateTo(item.id)} aria-current={isNavActive(item.id) ? "page" : undefined}>
                  <span>{item.label}</span>
                </button>
              ))}
            </nav>

            <div className="navbar-actions">
              <button className="icon-button" onClick={refreshWebhook} aria-label="Refresh data" title="Refresh data"><I d="M23 4v6h-6M20.5 15a9 9 0 11-2.1-9.4L23 10" size={15} /></button>
              <button className="icon-button desktop-only" onClick={clearCache} aria-label="Clear cache" title="Clear cache"><I d="M3 6h18M8 6V4h8v2m-9 0l1 15h8l1-15" size={14} /></button>

              {isAdmin && (
                <details className={`navbar-dropdown desktop-only${navigationGroups[1].items.some(item => isNavActive(item.id)) ? " has-active" : ""}`}>
                  <summary className="navbar-dropdown-trigger">Admin <I d="M6 9l6 6 6-6" size={13} /></summary>
                  <div className="navbar-popover admin-popover">
                    <div className="navbar-popover-label">Administration</div>
                    {navigationGroups[1].items.map(item => (
                      <button key={item.id} className={isNavActive(item.id) ? "is-active" : ""} onClick={() => navigateTo(item.id)}><I d={item.icon} size={15} /><span>{item.label}</span></button>
                    ))}
                    <div className="navbar-popover-divider" />
                    <button disabled={finalizing} onClick={(e) => { e.currentTarget.closest("details")?.removeAttribute("open"); finalizePastDays(); }}><I d="M12 3v12m0 0l4-4m-4 4l-4-4M5 21h14" size={15} /><span>{finalizing ? "Finalizing…" : "Finalize data"}</span></button>
                  </div>
                </details>
              )}

              {view === "dash" && isAdmin && (
                <button className="topbar-text-action" style={S.btnDark} onClick={() => { setEditRow(null); setView("entry"); }}><I d="M12 5v14M5 12h14" size={14} stroke="#fff" sw={2} /><span>New entry</span></button>
              )}

              <details className="navbar-dropdown account-dropdown desktop-only">
                <summary className="account-trigger" aria-label="Account menu" title="Account menu"><span className="account-avatar">{(session?.user?.email || "U").charAt(0).toUpperCase()}</span></summary>
                <div className="navbar-popover account-popover">
                  <div className="account-popover-identity"><strong>{session?.user?.email?.split("@")[0] || "Account"}</strong><span>{session?.user?.email || ""}</span><small>{isAdmin ? "Administrator" : "Viewer"}</small></div>
                  <div className="navbar-popover-divider" />
                  {isAdmin && <button onClick={e => { e.currentTarget.closest("details")?.removeAttribute("open"); setResetLinkOpen(true); }}><I d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.74 5.74L9 19H7v2H3v-4l7.26-7.26A6 6 0 0121 9z" size={15} /><span>Password reset link</span></button>}
                  <button onClick={handleLogout}><I d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9" size={15} /><span>Sign out</span></button>
                </div>
              </details>

              <button ref={navMenuButtonRef} className="mobile-menu-toggle" onClick={() => setMobileMenuOpen(p => !p)} aria-label={mobileMenuOpen ? "Close navigation" : "Open navigation"} aria-expanded={mobileMenuOpen}>
                <I d="M4 6h16M4 12h16M4 18h16" size={18} sw={2} />
              </button>
            </div>
          </div>

          {isCompactNav && mobileMenuOpen && (
            <>
              <button className="nav-menu-scrim" tabIndex={-1} aria-label="Close navigation" onClick={() => setMobileMenuOpen(false)} />
              <div ref={navMenuRef} className="nav-menu-panel" role="dialog" aria-modal="true" aria-label="Navigation menu">
                <div className="nav-menu-header"><div><strong>Navigation</strong><span>{viewTitles[view] || "Analytics"}</span></div><button className="nav-menu-close" aria-label="Close navigation" onClick={() => setMobileMenuOpen(false)}><I d="M6 18L18 6M6 6l12 12" size={18} sw={2} /></button></div>
                {allowedFunnels.length > 1 && (
                  <div className="menu-workspace"><span>Workspace</span><GeistSelect className="menu-workspace-geist-select" value={activeFunnel} onChange={(nextFunnel) => { setMobileMenuOpen(false); switchFunnel(nextFunnel); }} ariaLabel="Active workspace" options={allowedFunnels.map(f => ({ value: f, label: f === "analytics" ? "Main funnel" : f === "native" ? "Native funnel" : f }))} /></div>
                )}
                <nav className="nav-menu-groups" aria-label="Application navigation">
                  {navigationGroups.map(group => (
                    <div className="nav-menu-group" key={group.label}><div className="nav-menu-label">{group.label}</div>{group.items.map(item => <button key={item.id} className={isNavActive(item.id) ? "is-active" : ""} onClick={() => navigateTo(item.id)} aria-current={isNavActive(item.id) ? "page" : undefined}><I d={item.icon} size={16} /><span>{item.label}</span></button>)}</div>
                  ))}
                </nav>
                <div className="nav-menu-footer">
                  <div className="nav-menu-utilities"><button onClick={() => { setMobileMenuOpen(false); clearCache(); }}><I d="M3 6h18M8 6V4h8v2m-9 0l1 15h8l1-15" size={15} />Clear cache</button>{isAdmin && <button disabled={finalizing} onClick={() => { setMobileMenuOpen(false); finalizePastDays(); }}><I d="M12 3v12m0 0l4-4m-4 4l-4-4M5 21h14" size={15} />{finalizing ? "Finalizing…" : "Finalize data"}</button>}{isAdmin && <button onClick={() => { setMobileMenuOpen(false); setResetLinkOpen(true); }}><I d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.74 5.74L9 19H7v2H3v-4l7.26-7.26A6 6 0 0121 9z" size={15} />Password reset link</button>}</div>
                  <div className="nav-menu-account"><span className="account-avatar">{(session?.user?.email || "U").charAt(0).toUpperCase()}</span><div><strong>{session?.user?.email?.split("@")[0] || "Account"}</strong><span>{isAdmin ? "Administrator" : "Viewer"}</span></div><button onClick={handleLogout} aria-label="Sign out" title="Sign out"><I d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9" size={16} /></button></div>
                </div>
              </div>
            </>
          )}
        </header>

        <section className="workspace-shell" aria-hidden={isCompactNav && mobileMenuOpen ? "true" : undefined} inert={isCompactNav && mobileMenuOpen ? "" : undefined}>
          <main id="app-main" className={`main-content${view === "insights" || view === "worker" ? " chat-main" : ""}`} style={view === "insights" || view === "worker" ? { padding: 0, maxWidth: "none", margin: 0 } : S.main}>
        {view === "dash" && (
          <div className="fi">
            <div className="title-row" style={S.titleRow}>
              <div><h1 style={S.pageTitle}>Performance overview</h1></div>
              <div className="overview-date-controls">
                <GeistDateRangePicker filter={dateFilter} onFilterChange={setDateFilter} range={dateRange} onRangeChange={setDateRange} />
              </div>
            </div>

            {selectedDates.size > 0 && (() => {
              const sorted = [...selectedDates].sort((a, b) => parseMDate(a) - parseMDate(b));
              // "X – Y" only if the selection is the full contiguous range.
              // Otherwise enumerate (e.g. "Apr 26 and Apr 28").
              let contiguous = true;
              for (let i = 1; i < sorted.length; i++) {
                if ((parseMDate(sorted[i]) - parseMDate(sorted[i - 1])) / 86400000 !== 1) { contiguous = false; break; }
              }
              const labels = sorted.map(fmtDateNice);
              let span;
              if (labels.length === 1) span = labels[0];
              else if (contiguous) span = `${labels[0]} – ${labels[labels.length - 1]}`;
              else if (labels.length === 2) span = `${labels[0]} and ${labels[1]}`;
              else if (labels.length <= 4) span = `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}`;
              else span = `${labels.slice(0, 3).join(", ")} +${labels.length - 3} more`;
              return (
                <div role="status" style={{ display: "flex", alignItems: "center", gap: 10, padding: "9px 12px", marginBottom: 12, background: "var(--ds-blue-100)", border: "1px solid var(--ds-border)", borderRadius: 8, fontSize: 13, color: "var(--ds-blue-700)" }}>
                  <span style={{ fontWeight: 600 }}>Showing {selectedDates.size} selected row{selectedDates.size === 1 ? "" : "s"}</span>
                  <span aria-hidden="true" style={{ color: "var(--ds-blue-600)" }}>•</span>
                  <span>{span}</span>
                  <button onClick={clearSelection} style={{ marginLeft: "auto", background: "transparent", border: "1px solid var(--ds-border-hover)", borderRadius: 6, padding: "3px 10px", fontSize: 12, fontWeight: 500, color: "var(--ds-blue-700)", cursor: "pointer", fontFamily: fn }}>Clear</button>
                </div>
              );
            })()}

            <div className="summary-strip" style={{ ...S.strip, position: "relative" }}>
              {summaryCards.filter(c => !cardHiddenInVariant(c)).map((c, i) => {
                const pct = selectedDates.size > 0 ? null : pctChange(c);
                const chartAccent = SUMMARY_CHART_COLORS[i % SUMMARY_CHART_COLORS.length];
                return (
                  <div key={`${c.key}-${i}`} className="summary-card" style={S.stripCell}>
                    <div style={S.stripLabel}>
                      {c.label}
                      {c.agg === "avg" && <span style={{ fontSize: 10, color: "var(--ds-gray-600)", fontWeight: 400, marginLeft: 4 }}>(avg)</span>}
                    </div>
                    <div className="strip-val-row" style={S.stripValRow}>
                      <span style={S.stripVal}>{getSummaryCardVal(c)}</span>
                      {pct !== null && (
                        <span style={{ ...S.pctBadge, background: pct >= 0 ? "#EAFCFA" : "#FFF1F2", color: pct >= 0 ? "#047857" : "#BE123C" }}>
                          {pct >= 0 ? "+" : ""}{pct}%
                          <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d={pct >= 0 ? "M3 8L6 4L9 8" : "M3 4L6 8L9 4"} stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
                        </span>
                      )}
                    </div>
                    <SummarySparkline
                      card={c}
                      rows={effectiveRows}
                      customs={customs}
                      accent={chartAccent}
                      gradientId={`summary-gradient-${i}`}
                      formatValue={value => formatSummaryVal(value, c.format)}
                    />
                  </div>
                );
              })}
              {/* Pad the last row with empty cells (white bg) so the grid lines stay
                  consistent for any card count. Pad to a multiple of 4 (desktop) so
                  4-col and 2-col layouts both end on a full row. */}
              {Array.from({ length: (4 - (summaryCards.filter(c => !cardHiddenInVariant(c)).length % 4)) % 4 }).map((_, i) => (
                <div key={`pad-${i}`} className="strip-pad" style={S.stripCell} aria-hidden="true" />
              ))}
              <button
                onClick={() => setSummaryEditorOpen(true)}
                title="Edit summary cards"
                aria-label="Edit summary cards"
                style={{ position: "absolute", top: 8, right: 8, background: "transparent", border: "1px solid transparent", borderRadius: 6, padding: 4, cursor: "pointer", color: "var(--ds-gray-600)", display: "flex", alignItems: "center", justifyContent: "center", transition: "all 0.15s" }}
                onMouseEnter={e => { e.currentTarget.style.background = "var(--ds-gray-100)"; e.currentTarget.style.borderColor = "var(--ds-border)"; e.currentTarget.style.color = "var(--ds-gray-700)"; }}
                onMouseLeave={e => { e.currentTarget.style.background = "transparent"; e.currentTarget.style.borderColor = "transparent"; e.currentTarget.style.color = "var(--ds-gray-600)"; }}
              >
                <I d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" size={14} />
                <I d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" size={14} />
              </button>
            </div>

            <div className="toolbar-row overview-toolbar" style={S.toolbar}>
              <div className="toolbar-controls" style={{ display: "flex", gap: "10px", alignItems: "center", justifyContent: "center", flexWrap: "wrap", width: "100%" }}>
                <div className="list-board-toggle" style={S.listBoardToggle}>
                  <button type="button" aria-pressed={viewMode === "list"} style={viewMode === "list" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("list")}><I d="M4 6h16M4 12h16M4 18h16" size={14} /> List</button>
                  <button type="button" aria-pressed={viewMode === "board"} style={viewMode === "board" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("board")}><I d="M4 4h4v16H4zM10 4h4v16h-4zM16 4h4v16h-4z" size={14} /> Board</button>
                </div>
                {(activeFunnel === "native" || activeFunnel === "analytics") && (
                  <div className="variant-toggle" style={S.listBoardToggle} title="Split test variant filter">
                    {["all", "A", "B", "undetected"].map(v => (
                      <button
                        type="button"
                        key={v}
                        aria-pressed={variantFilter === v}
                        style={variantFilter === v ? S.listToggleActive : S.listToggleInactive}
                        onClick={() => setVariantFilter(v)}
                      >
                        {v === "all" ? "All" : v === "undetected" ? "Undetected" : v}
                      </button>
                    ))}
                  </div>
                )}
                <div className="overview-utility-actions">
                  {isAdmin && (activeFunnel === "native" || activeFunnel === "analytics") && (
                    <SplitTestSettings start={abTestStart} onStartNow={startAbTestNow} onClear={clearAbTest} onRegPages={() => setRegMapOpen(true)} />
                  )}
                  <button type="button" className="column-editor-trigger" style={S.btnLight} aria-label="Edit columns" title="Edit columns" onClick={() => setColEditorOpen(true)}>
                    <I d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01" size={14} stroke="var(--ds-gray-700)" />
                    <span className="toolbar-action-label">Edit Columns</span>
                  </button>
                  <div
                    className="lens-menu-root"
                    ref={lensMenuRef}
                    onBlur={() => requestAnimationFrame(() => { if (!lensMenuRef.current?.contains(document.activeElement)) setLensMenuOpen(false); })}
                    onKeyDown={event => {
                      if (event.key !== "Escape" || !lensMenuOpen) return;
                      event.preventDefault();
                      event.stopPropagation();
                      lensMenuRef.current?.querySelector(".lens-menu-trigger")?.focus();
                      setLensMenuOpen(false);
                    }}
                  >
                  <button
                    type="button"
                    className="lens-menu-trigger"
                    style={S.btnLight}
                    aria-haspopup="dialog"
                    aria-expanded={lensMenuOpen}
                    aria-controls="lens-menu-popover"
                    aria-label={`Metric lens: ${activeLens.name}`}
                    title={`Metric lens: ${activeLens.name}`}
                    onClick={() => {
                      const nextOpen = !lensMenuOpen;
                      setLensMenuOpen(nextOpen);
                      if (nextOpen) requestAnimationFrame(() => lensMenuRef.current?.querySelector(".lens-menu-popover button")?.focus());
                    }}
                  >
                    <I d="M15 12a3 3 0 11-6 0 3 3 0 016 0zM2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" size={14} stroke="var(--ds-gray-700)" />
                    <span className="toolbar-action-label">{activeLens.name}</span>
                    <span className="toolbar-action-caret" aria-hidden="true">▾</span>
                  </button>
                  {lensMenuOpen && (
                    <div id="lens-menu-popover" className="lens-menu-popover" role="dialog" aria-label="Metric lenses">
                      <div style={{ padding: "6px 14px", fontSize: 10, fontWeight: 700, color: "var(--ds-gray-600)", textTransform: "uppercase", letterSpacing: "0.05em" }}>Lenses</div>
                      {lenses.map(lens => (
                        <div key={lens.id} style={{ display: "flex", alignItems: "center", gap: 6, padding: "5px 8px", fontSize: 13, color: "var(--ds-gray-800)", fontWeight: activeLensId === lens.id ? 600 : 500, background: activeLensId === lens.id ? "var(--ds-gray-100)" : "transparent" }}>
                          <button type="button" style={{ flex: 1, minHeight: 28, padding: "0 6px", border: 0, background: "transparent", color: "inherit", fontWeight: "inherit", textAlign: "left" }} onClick={() => { setActiveLensId(lens.id); lensMenuRef.current?.querySelector(".lens-menu-trigger")?.focus(); setLensMenuOpen(false); }}>{lens.name}</button>
                          {isAdmin && (
                            <div style={{ display: "flex", alignItems: "center", gap: 2 }}>
                              {lens.id !== "default-all" && <button type="button" aria-label={`Set ${lens.name} as default`} title="Set as default" style={{ ...S.btnGhost, minHeight: 28, padding: 5 }} onClick={() => { setDefaultLens(lens.id); setActiveLensId(lens.id); }}><I d="M12 2l3.1 6.3L22 9.3l-5 4.9 1.2 6.8-6.2-3.2L5.8 21 7 14.2 2 9.3l6.9-1L12 2z" size={13} /></button>}
                              <button type="button" aria-label={`Edit ${lens.name}`} title="Edit lens" style={{ ...S.btnGhost, minHeight: 28, padding: 5 }} onClick={() => { lensMenuRef.current?.querySelector(".lens-menu-trigger")?.focus(); setLensEditing({ id: lens.id, name: lens.name, metrics: [...(lens.metrics || [])] }); setLensMenuOpen(false); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.1 2.1 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={13} /></button>
                              {lens.id !== "default-all" && <button type="button" aria-label={`Delete ${lens.name}`} title="Delete lens" style={{ ...S.btnGhost, minHeight: 28, padding: 5, color: "var(--ds-red-600)" }} onClick={() => { deleteLens(lens.id); }}><I d="M19 7l-1 14H6L5 7m5 4v6m4-6v6M9 7V4h6v3M4 7h16" size={13} /></button>}
                            </div>
                          )}
                        </div>
                      ))}
                      {isAdmin && (
                        <>
                          <div style={{ height: 1, background: "var(--ds-border)", margin: "4px 0" }} />
                          <button type="button" style={{ width: "100%", minHeight: 34, padding: "7px 14px", border: 0, background: "transparent", textAlign: "left", fontSize: 13, color: "var(--ds-blue-700)", fontWeight: 600 }} onClick={() => { lensMenuRef.current?.querySelector(".lens-menu-trigger")?.focus(); setLensEditing({ name: "", metrics: MK.filter(k => COL_LABELS[k]) }); setLensMenuOpen(false); }}>+ Create lens</button>
                        </>
                      )}
                    </div>
                  )}
                  </div>
                </div>
                {!isMobile && <div className="search-wrap overview-search" style={S.searchWrap}><I d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" size={15} stroke="var(--ds-gray-600)" /><input aria-label="Search records" style={S.searchInput} placeholder="Search records…" value={search} onChange={e => setSearch(e.target.value)} /></div>}
              </div>
            </div>

            {effectiveViewMode === "list" ? (
              <div className="table-wrap overview-table-wrap" style={S.tableWrap}>
                <table style={S.table}>
                  <thead><tr>
                    <th className="sticky-col sticky-col-1" style={S.th}>Day</th><th className="sticky-col sticky-col-2" style={S.th}>Date</th>
                    {orderedCols.filter(c => (c.type === "base" ? isColVisible(c.key) : true) && !isHiddenInVariant(c)).map(c => (
                      <th key={c.key} style={{ ...S.th, ...(c.type === "custom" ? { color: "var(--ds-green-600)" } : {}), ...(c.key === "total_purchases" ? S.thHighlight : {}) }}>{c.label}</th>
                    ))}
                    <th style={{ ...S.th, width: 72 }}>Actions</th>
                  </tr></thead>
                  <tbody>
                    {tableRows.length === 0 ? (
                      <tr><td colSpan={2 + MK.filter(k => isColVisible(k)).length + customs.length + 1} style={S.emptyTd}>No entries found for this period.</td></tr>
                    ) : tableRows.map((row) => {
                      const isToday = row.date === getLADate();
                      const isSelected = selectedDates.has(row.date);
                      const ctx = evalAllCustoms(customs, row);
                      return (
                        <tr
                          key={row.date}
                          className={`trow${isSelected ? " selected" : ""}`}
                          tabIndex={0}
                          aria-selected={isSelected}
                          style={{
                            cursor: "pointer",
                            userSelect: isSelected ? "none" : undefined,
                            ...(!isSelected && isToday ? { background: "#F8FDF9" } : {}),
                          }}
                          onClick={(e) => toggleRowSelection(row.date, e.shiftKey)}
                          onKeyDown={(e) => { if (e.target === e.currentTarget && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); toggleRowSelection(row.date, e.shiftKey); } }}
                        >
                          <td className="sticky-col sticky-col-1" style={S.td}><span style={S.dayPill}>{getLADayShort(row.date)}</span></td>
                          <td className="sticky-col sticky-col-2" style={{ ...S.td, whiteSpace: "nowrap", minWidth: 90 }}><span style={{ color: "var(--ds-gray-900)", fontWeight: 500, fontSize: 14 }}>{fmtDateNice(row.date)}</span></td>
                          {orderedCols.filter(c => (c.type === "base" ? isColVisible(c.key) : true) && !isHiddenInVariant(c)).map(c => {
                            if (c.type === "custom") {
                              return <td key={c.key} style={{ ...S.tdNum, color: "var(--ds-green-600)", fontWeight: 600 }}>{fmtVal(ctx[c.cm.name], c.cm.format)}</td>;
                            }
                            if (c.key === "total_purchases") return <td key={c.key} style={S.tdNum}><span style={S.purchBadge}>{(Number(row.total_purchases) || 0).toLocaleString()}</span></td>;
                            if (c.key === "fb_spend") return <td key={c.key} style={S.tdMoney}>{"$" + (Number(row.fb_spend) || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })}</td>;
                            return <td key={c.key} style={S.tdNum}>{(Number(row[c.key]) || 0).toLocaleString()}</td>;
                          })}
                          <td style={S.td} onClick={(e) => e.stopPropagation()}>
                            <div style={{ display: "flex", gap: 2, justifyContent: "center" }}>
                              <button aria-label={`Recalculate Facebook spend for ${fmtDateNice(row.date)}`} className="rowBtn" style={{ ...S.rowAct, color: "var(--ds-blue-600)" }} title="Recalculate Facebook spend" onClick={() => recalcSpend(row.date)}><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={14} /></button>
                              {isAdmin && <button aria-label={`Finalize ${fmtDateNice(row.date)}`} className="rowBtn" style={{ ...S.rowAct, color: "var(--ds-green-600)" }} title="Finalize this day" onClick={() => finalizeDay(row.date)}><I d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" size={14} /></button>}
                              {isAdmin && <button aria-label={`Edit ${fmtDateNice(row.date)}`} className="rowBtn" style={S.rowAct} onClick={() => { setEditRow(row); setView("entry"); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={14} /></button>}
                              {isAdmin && <button aria-label={`Delete ${fmtDateNice(row.date)}`} className="rowBtn" style={{ ...S.rowAct, color: "var(--ds-red-600)" }} onClick={() => setDelConfirm(row.date)}><I d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" size={14} /></button>}
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="boardGrid" style={S.boardGrid}>
                {tableRows.length === 0 ? (
                  <div style={{ ...S.emptyTd, gridColumn: "1 / -1", border: "1px dashed var(--ds-border)", borderRadius: 12 }}>No entries found for this period.</div>
                ) : tableRows.map((row) => {
                  const boardCtx = evalAllCustoms(customs, row);
                  const isSelected = selectedDates.has(row.date);
                  const cardStyle = {
                    ...S.boardCard,
                    cursor: "pointer",
                    ...(isSelected ? { background: "var(--ds-blue-100)", borderColor: "var(--ds-border-hover)", boxShadow: "none" } : {}),
                  };
                  return (
                    <article key={row.date} aria-label={`Metrics for ${fmtDateNice(row.date)}`} style={cardStyle} onClick={(e) => toggleRowSelection(row.date, e.shiftKey)}>
                      <div style={S.boardCardHeader}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={S.dayPill}>{getLADayShort(row.date)}</span>
                          <div style={{ fontWeight: 600, color: "var(--ds-gray-900)", fontSize: 14 }}>{fmtDateNice(row.date)}</div>
                        </div>
                        <button
                          type="button"
                          aria-pressed={isSelected}
                          aria-label={`${isSelected ? "Deselect" : "Select"} ${fmtDateNice(row.date)}`}
                          style={{ ...S.rowAct, width: 28, height: 28, padding: 0, justifyContent: "center", color: isSelected ? "#fff" : "var(--ds-gray-600)", background: isSelected ? "var(--ds-blue-600)" : "var(--ds-background-100)", borderColor: isSelected ? "var(--ds-blue-600)" : "var(--ds-border)" }}
                          onClick={(e) => { e.stopPropagation(); toggleRowSelection(row.date, e.shiftKey); }}
                        >
                          <I d={isSelected ? "M5 12l4 4L19 6" : "M12 5v14M5 12h14"} size={14} />
                        </button>
                      </div>
                      <div style={S.boardCardBody}>
                        {orderedCols.filter(c => (c.type === "base" ? isColVisible(c.key) : true) && !isHiddenInVariant(c)).map(c => {
                          if (c.type === "custom") {
                            return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={{ ...S.bcVal, color: "var(--ds-green-600)" }}>{fmtVal(boardCtx[c.cm.name], c.cm.format)}</span></div>;
                          }
                          if (c.key === "fb_spend") return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={S.bcVal}>{"$" + (Number(row.fb_spend) || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })}</span></div>;
                          if (c.key === "total_purchases") return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={S.purchBadge}>{(Number(row.total_purchases) || 0).toLocaleString()}</span></div>;
                          return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={S.bcVal}>{(Number(row[c.key]) || 0).toLocaleString()}</span></div>;
                        })}
                      </div>
                      <div style={S.boardCardActions} onClick={(e) => e.stopPropagation()}>
                        <button aria-label={`Recalculate Facebook spend for ${fmtDateNice(row.date)}`} style={{ ...S.rowAct, color: "var(--ds-blue-600)" }} title="Recalculate Facebook spend" onClick={() => recalcSpend(row.date)}><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={14} /></button>
                        {isAdmin && <button aria-label={`Finalize ${fmtDateNice(row.date)}`} style={{ ...S.rowAct, color: "var(--ds-green-600)" }} title="Finalize this day" onClick={() => finalizeDay(row.date)}><I d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" size={14} /></button>}
                        {isAdmin && <button aria-label={`Edit ${fmtDateNice(row.date)}`} style={S.rowAct} title="Edit row" onClick={() => { setEditRow(row); setView("entry"); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={14} /></button>}
                        {isAdmin && <button aria-label={`Delete ${fmtDateNice(row.date)}`} style={{ ...S.rowAct, color: "var(--ds-red-600)" }} title="Delete row" onClick={() => setDelConfirm(row.date)}><I d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" size={14} /></button>}
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </div>
        )}
        {view === "entry" && <EntryForm initial={editRow} onSubmit={submitEntry} onCancel={() => { setView("dash"); setEditRow(null); }} isMobile={isMobile} />}
        {view === "custom" && <CMForm initial={editCM} onSubmit={saveCM} onCancel={() => { setView("custom-list"); setEditCM(null); }} metrics={metrics} customs={customs} />}
        {view === "custom-list" && (
          <div className="fi" style={S.fc}>
            <div style={{ ...S.fh, marginBottom: 16 }}><h2 style={S.ft}>Metric settings</h2></div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "20px" }}>
              <p style={{ ...S.fs, margin: 0 }}>Define calculated columns for your table.</p>
              <button style={S.btnDark} onClick={() => { setEditCM(null); setView("custom"); }}><I d="M12 5v14M5 12h14" stroke="#fff" /> New metric</button>
            </div>
            <div style={S.fcard}>
              {customs.length === 0 ? (
                <div style={{ textAlign: "center", padding: "40px 0", color: "var(--ds-gray-600)" }}>No custom metrics created yet.</div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  {customs.map(cm => (
                    <div key={cm.id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "16px", border: "1px solid var(--ds-border)", borderRadius: 12, background: "var(--ds-background-200)" }}>
                      <div>
                        <div style={{ fontWeight: 600, color: "var(--ds-gray-900)", fontSize: 14 }}>{cm.name}</div>
                        <div style={{ fontFamily: mono, fontSize: 12, color: "var(--ds-gray-700)", marginTop: 4 }}>{cm.formula}</div>
                      </div>
                      <div style={{ display: "flex", gap: 6 }}>
                        <button aria-label={`Edit ${cm.name}`} title="Edit metric" style={S.btnLight} onClick={() => { setEditCM(cm); setView("custom"); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={14} /></button>
                        <button aria-label={`Delete ${cm.name}`} title="Delete metric" style={{ ...S.btnLight, color: "var(--ds-red-600)" }} onClick={() => setDelCM(cm.id)}><I d="M18 6L6 18M6 6l12 12" size={14} /></button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
        {view === "events" && (
          <div className="fi">
            <div className="events-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
              <div><h1 style={S.pageTitle}>Activity</h1><div style={S.pageSub}>A live timeline of registrations, engagement, and purchases.</div></div>
              <div style={{ ...S.searchWrap, width: "auto", padding: "6px 12px" }}>
                <select aria-label="Filter activity by event type" style={S.searchInput} value={evFilter} onChange={async e => { setEvFilter(e.target.value); try { const r = await api.getEvents(100, e.target.value); setEvents(r.data || []); } catch { } }}>
                  <option value="">All Events</option>
                  <option value="registrations">Registrations</option>
                  <option value="replays">Replays</option>
                  <option value="viewedcta">Viewed CTA</option>
                  <option value="clickedcta">Clicked CTA</option>
                  <option value="purchases">Purchases</option>
                </select>
              </div>
            </div>
            <div style={S.fcard}>
              {events.length === 0 ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "var(--ds-gray-600)" }}>No events recorded yet. Events will appear when your webhooks send data.</div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column" }}>
                  {events.map((ev, i) => {
                    const typeColors = { registrations: "#1D4ED8", replays: "#6D28D9", viewedcta: "#A35200", clickedcta: "#0A7F43", purchases: "#B42318", fb_spend: "#4338CA" };
                    const tc = typeColors[ev.event_type] || "var(--ds-gray-700)";
                    const ago = ((Date.now() - new Date(ev.event_time).getTime()) / 1000);
                    const agoStr = ago < 60 ? "just now" : ago < 3600 ? `${Math.floor(ago / 60)}m ago` : ago < 86400 ? `${Math.floor(ago / 3600)}h ago` : `${Math.floor(ago / 86400)}d ago`;
                    return (
                      <div key={ev.id || i} style={{ display: "flex", alignItems: "flex-start", gap: 16, padding: "16px 0", borderBottom: i < events.length - 1 ? "1px solid var(--ds-gray-100)" : "none" }}>
                        <div style={{ width: 8, height: 8, borderRadius: "50%", background: tc, marginTop: 6, flexShrink: 0 }} />
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                            <span style={{ fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em", color: tc, background: `color-mix(in srgb, ${tc} 8%, transparent)`, padding: "2px 8px", borderRadius: 4 }}>{ev.event_type.replace("cta", " CTA")}</span>
                            <span style={{ fontSize: 12, color: "var(--ds-gray-600)" }}>{agoStr}</span>
                          </div>
                          <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
                            {ev.name && <span style={{ fontSize: 14, fontWeight: 600, color: "var(--ds-gray-900)" }}>{ev.name}</span>}
                            {ev.email && <span style={{ fontSize: 13, color: "var(--ds-gray-700)" }}>{ev.email}</span>}
                            {ev.phone && <span style={{ fontSize: 13, color: "var(--ds-gray-700)" }}>{ev.phone}</span>}
                            {!ev.name && !ev.email && !ev.phone && <span style={{ fontSize: 13, color: "var(--ds-gray-600)", fontStyle: "italic" }}>No contact info</span>}
                          </div>
                          {ev.metadata && Object.keys(ev.metadata).length > 0 && (
                            <div style={{ fontSize: 12, color: "var(--ds-gray-600)", marginTop: 4, fontFamily: mono }}>{Object.entries(ev.metadata).map(([k, v]) => `${k}: ${v}`).join(" · ")}</div>
                          )}
                        </div>
                        <div style={{ fontSize: 11, color: "var(--ds-gray-600)", whiteSpace: "nowrap", flexShrink: 0 }}>{new Date(ev.event_time).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}</div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        )}
        {view === "crm" && (
          <div className="fi">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24, gap: 16, flexWrap: "wrap" }}>
              <div><h1 style={S.pageTitle}>People</h1><div style={S.pageSub}>{crmTotal.toLocaleString()} people across your funnel. Select a person to view their full journey.</div></div>
              <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
                {/* Sales-page filter — only when pages are configured */}
                {salesPages && Object.keys(salesPages.map || {}).length > 0 && (
                  <select
                    aria-label="Filter people by sales page"
                    value={crmSalesPage}
                    onChange={e => setCrmSalesPage(e.target.value)}
                    style={{ ...S.btnLight, padding: "8px 12px", borderRadius: 8, cursor: "pointer", fontFamily: fn }}
                    title="Filter to people who hit a sales page"
                  >
                    <option value="">All sales pages</option>
                    {[...new Set(Object.values(salesPages.map))].map(label => (
                      <option key={label} value={label}>Hit: {label}</option>
                    ))}
                  </select>
                )}
                {isAdmin && <button style={{ ...S.btnLight, padding: "8px 12px", borderRadius: 8 }} onClick={() => setSalesEditorOpen(true)} title="Configure sales pages"><I d="M12 15.5a3.5 3.5 0 100-7 3.5 3.5 0 000 7zm0-12v2m0 13v2m8.5-8.5h-2m-13 0h-2m14.5-6l-1.4 1.4M7.4 16.6L6 18m12 0l-1.4-1.4M7.4 7.4L6 6" size={14} /> Sales pages</button>}
                <div style={{ ...S.searchWrap, width: "auto", minWidth: 240, padding: "6px 12px" }}>
                  <I d="M21 21l-4.35-4.35M11 19a8 8 0 100-16 8 8 0 000 16z" size={14} stroke="var(--ds-gray-700)" />
                  <input aria-label="Search people" style={S.searchInput} placeholder="Search name, email, phone…" value={crmSearch} onChange={e => setCrmSearch(e.target.value)} />
                </div>
              </div>
            </div>

            {/* Funnel strip — click a stage to filter */}
            <div style={{ ...S.strip, gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))" }}>
              {[
                { key: "", label: "People", val: crmStats?.total },
                { key: "registration", label: "Registered", val: crmStats?.registrations, stage: "registration" },
                { key: "attended", label: "Attended", val: crmStats?.attended, stage: "attended" },
                { key: "replay", label: "Replay", val: crmStats?.replays, stage: "replay" },
                { key: "viewedcta", label: "Saw CTA", val: crmStats?.viewedcta, stage: "viewedcta" },
                { key: "clickedcta", label: "Clicked CTA", val: crmStats?.clickedcta, stage: "clickedcta" },
                { key: "purchase", label: "Purchased", val: crmStats?.purchases, stage: "purchase" },
              ].map(card => {
                const active = crmStage === card.key;
                const color = card.stage ? (CRM_STAGE_META[card.stage] || {}).color : "var(--ds-gray-900)";
                return (
                  <button type="button" aria-pressed={active} key={card.label} onClick={() => setCrmStage(active ? "" : card.key)} style={{ ...S.stripCell, cursor: "pointer", padding: "16px 18px", border: 0, textAlign: "left", boxShadow: active ? `inset 0 -3px 0 ${color}` : "none", background: active ? "var(--ds-blue-100)" : "var(--ds-background-100)" }}>
                    <span style={{ ...S.stripLabel, color }}>{card.label}</span>
                    <span style={{ ...S.stripVal, fontSize: 22 }}>{card.val != null ? card.val.toLocaleString() : "—"}</span>
                  </button>
                );
              })}
            </div>

            <div style={S.fcard}>
              {crmLoading ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "var(--ds-gray-600)" }}>Loading…</div>
              ) : crmContacts.length === 0 ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "var(--ds-gray-600)" }}>No contacts found.</div>
              ) : (
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                    <thead>
                      <tr>{["Name", "Email", "Phone", "Stage", "Source", "Visits", "Sales pages", "Last seen"].map(h => <th key={h} style={{ ...S.th, textAlign: "left" }}>{h}</th>)}</tr>
                    </thead>
                    <tbody>
                      {crmContacts.map((c, i) => (
                        <tr key={c.contact_id || c.email || i} tabIndex={0} onClick={() => openCrmContact(c)} onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openCrmContact(c); } }} style={{ cursor: "pointer" }}>
                          <td style={{ ...S.td, textAlign: "left", fontWeight: 600, color: "var(--ds-gray-900)" }}>{c.name || <span style={{ color: "var(--ds-gray-600)", fontStyle: "italic", fontWeight: 400 }}>Unknown</span>}{c.is_shared_ip && <span title="Seen on a shared/NAT IP — identity not auto-merged" style={{ marginLeft: 6, fontSize: 9, fontWeight: 700, letterSpacing: "0.04em", color: "#B91C1C", background: "#FEE2E2", padding: "1px 5px", borderRadius: 4, verticalAlign: "middle" }}>SHARED IP</span>}</td>
                          <td style={{ ...S.td, textAlign: "left", color: "var(--ds-gray-800)" }}>{c.email}{c.has_linked && <span title="Has linked identities — another email/phone was fused into this person (same browser/IP). Open to see the Linked tab." style={{ marginLeft: 6, fontSize: 9, fontWeight: 700, letterSpacing: "0.04em", color: "var(--ds-blue-700)", background: "var(--ds-blue-100)", padding: "1px 5px", borderRadius: 4, verticalAlign: "middle" }}>L</span>}</td>
                          <td style={{ ...S.td, textAlign: "left", color: "var(--ds-gray-700)" }}>{c.phone || "—"}</td>
                          <td style={{ ...S.td, textAlign: "left" }}><CrmStageBadge stage={c.stage} /></td>
                          <td style={{ ...S.td, textAlign: "left", color: "var(--ds-gray-700)" }}>{crmSourceLabel(c.attribution) || "—"}</td>
                          <td style={{ ...S.td, textAlign: "left", color: c.is_tracked ? "var(--ds-gray-800)" : "var(--ds-gray-600)" }}>{c.is_tracked ? c.visit_count : "LEGACY"}</td>
                          <td style={{ ...S.td, textAlign: "left" }}>{(c.sales_pages_hit && c.sales_pages_hit.length) ? <span style={{ display: "inline-flex", gap: 4, flexWrap: "wrap" }}>{c.sales_pages_hit.map(l => <span key={l} title={`Hit ${l}`} style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.03em", color: "#065F46", background: "#D1FAE5", padding: "2px 6px", borderRadius: 4, whiteSpace: "nowrap" }}>{l}</span>)}</span> : <span style={{ color: "var(--ds-gray-600)" }}>—</span>}</td>
                          <td style={{ ...S.td, textAlign: "left", color: "var(--ds-gray-600)", whiteSpace: "nowrap" }}>{c.last_activity ? crmRelTime(c.last_activity) : "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {!crmLoading && crmTotal > CRM_PAGE && (
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", paddingTop: 16, marginTop: 4, borderTop: "1px solid var(--ds-gray-100)", fontSize: 13, color: "var(--ds-gray-700)" }}>
                  <span>{(crmOffset + 1).toLocaleString()}–{Math.min(crmOffset + CRM_PAGE, crmTotal).toLocaleString()} of {crmTotal.toLocaleString()}</span>
                  <div style={{ display: "flex", gap: 8 }}>
                    <button style={{ ...S.btnLight, opacity: crmOffset === 0 ? 0.4 : 1, cursor: crmOffset === 0 ? "default" : "pointer" }} disabled={crmOffset === 0} onClick={() => setCrmOffset(Math.max(0, crmOffset - CRM_PAGE))}>← Prev</button>
                    <button style={{ ...S.btnLight, opacity: crmOffset + CRM_PAGE >= crmTotal ? 0.4 : 1, cursor: crmOffset + CRM_PAGE >= crmTotal ? "default" : "pointer" }} disabled={crmOffset + CRM_PAGE >= crmTotal} onClick={() => setCrmOffset(crmOffset + CRM_PAGE)}>Next →</button>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
        {view === "emailreport" && (
          <div className="fi">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24, gap: 16, flexWrap: "wrap" }}>
              <div><h1 style={S.pageTitle}>Email report</h1><div style={S.pageSub}>Click-to-purchase performance by source · {emailReport?.totals?.window_days ? `buyers within ${emailReport.totals.window_days} days of a click` : "buyers who purchased after clicking"}</div></div>
              <button style={S.btnLight} onClick={loadEmailReport}><I d="M23 4v6h-6M20.5 15a9 9 0 11-2.1-9.4L23 10" size={14} /> Refresh</button>
            </div>

            <div style={{ ...S.strip, gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))" }}>
              {[
                { label: "Clicks", val: emailReport?.totals?.clicks },
                { label: "People", val: emailReport?.totals?.people },
                { label: "Buyers", val: emailReport?.totals?.buyers },
                { label: "Conversion", val: emailReport?.totals?.conversion, pct: true },
              ].map(c => (
                <div key={c.label} style={{ ...S.stripCell, padding: "16px 18px" }}>
                  <span style={S.stripLabel}>{c.label}</span>
                  <span style={{ ...S.stripVal, fontSize: 22 }}>{c.val != null ? (c.pct ? c.val + "%" : c.val.toLocaleString()) : "—"}</span>
                </div>
              ))}
            </div>

            <div style={S.fcard}>
              {emailReportLoading ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "var(--ds-gray-600)" }}>Loading…</div>
              ) : !emailReport?.sources?.length ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "var(--ds-gray-600)" }}>
                  No email clicks tracked yet.
                  <div style={{ fontSize: 13, marginTop: 8 }}>They appear once your emails link with <code style={{ background: "var(--ds-gray-100)", padding: "1px 5px", borderRadius: 4 }}>?he=&#123;&#123;contact.email&#125;&#125;&amp;el=&lt;source&gt;&amp;htrafficsource=email</code></div>
                </div>
              ) : (
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                    <thead>
                      <tr>{["Source (el)", "Clicks", "People", "Buyers", "Conversion"].map((h, i) => <th key={h} style={{ ...S.th, textAlign: i === 0 ? "left" : "right" }}>{h}</th>)}</tr>
                    </thead>
                    <tbody>
                      {emailReport.sources.map((r, i) => (
                        <tr key={r.source || i} tabIndex={0} onClick={() => openEmailDrill(r.source)} onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openEmailDrill(r.source); } }} style={{ cursor: "pointer" }} title="View individual clicks">
                          <td style={{ ...S.td, textAlign: "left", fontWeight: 600, color: "var(--ds-gray-900)" }}>
                            <span style={{ color: "var(--ds-gray-700)", marginRight: 6 }}>›</span>{r.source}
                          </td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{r.clicks.toLocaleString()}</td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{r.people.toLocaleString()}</td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{r.buyers.toLocaleString()}</td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums", fontWeight: 600, color: r.conversion > 0 ? "#047857" : "var(--ds-gray-600)" }}>{r.conversion}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
        {view === "insights" && <InsightsChat key={`${session?.user?.id || "user"}:${activeFunnel}:insights`} flash={flash} isMobile={isMobile} activeFunnel={activeFunnel} userId={session?.user?.id} />}
        {view === "worker" && isAdmin && <InsightsChat key={`${session?.user?.id || "user"}:${activeFunnel}:worker`} flash={flash} isMobile={isMobile} activeFunnel={activeFunnel} userId={session?.user?.id} mode="worker" />}
        {view === "query" && <QueryBuilder flash={flash} />}
          </main>
          {view !== "insights" && view !== "worker" && (
            <FloatingInsights key={`${session?.user?.id || "user"}:${activeFunnel}`} flash={flash} activeFunnel={activeFunnel} userId={session?.user?.id} />
          )}
        </section>
      </div>

      {resetLinkOpen && <ResetLinkModal funnel={activeFunnel} onCancel={() => setResetLinkOpen(false)} />}
      {delConfirm && <Modal title="Delete entry" msg={`Remove the entry for ${fmtDateNice(delConfirm)}?`} onCancel={() => setDelConfirm(null)} onConfirm={() => deleteEntry(delConfirm)} />}
      {delCM && <Modal title="Delete custom metric" msg="This will remove the column from your table." onCancel={() => setDelCM(null)} onConfirm={() => deleteCM(delCM)} />}
      {lensEditing && <LensEditor lens={lensEditing} onSave={saveLens} onCancel={() => setLensEditing(null)} />}
      {regMapOpen && <RegPageMapModal flash={flash} onClose={() => setRegMapOpen(false)} onSaved={() => loadData()} />}
      {colEditorOpen && <ColumnEditor columns={orderedCols.filter(c => c.type !== "base" || isColVisible(c.key))} isAdmin={isAdmin} onSave={(keys) => { saveColOrder(keys); setColEditorOpen(false); }} onCancel={() => setColEditorOpen(false)} />}
      {crmContact && <CrmContactModal contact={crmContact} tab={crmContactTab} setTab={setCrmContactTab} onClose={() => setCrmContact(null)} isAdmin={isAdmin} onLink={linkSaleToRegistrant} />}
      {emailDrill && <EmailDrillModal drill={emailDrill} loading={emailDrillLoading} onClose={() => setEmailDrill(null)} onOpenContact={(idf) => { setEmailDrill(null); openCrmContact({ email: idf }); }} />}
      {summaryEditorOpen && <SummaryEditor cards={summaryCards} customs={customs} onSave={(cards) => { saveSummaryCards(cards); setSummaryEditorOpen(false); flash("Summary cards updated"); }} onCancel={() => setSummaryEditorOpen(false)} />}
      {salesEditorOpen && <SalesPagesModal flash={flash} onClose={() => setSalesEditorOpen(false)} onSaved={() => { setSalesPages(null); setCrmSalesPage(""); }} />}
      {toast && (<div className="fi toast-el" role={toast.type === "ok" ? "status" : "alert"} aria-live="polite" style={{ ...S.toast, borderLeft: `3px solid ${toast.type === "ok" ? "#0A7F43" : "#B42318"}` }}><I d={toast.type === "ok" ? "M20 6L9 17l-5-5" : "M12 2a10 10 0 100 20 10 10 0 000-20zM12 8v4M12 16h.01"} size={16} stroke={toast.type === "ok" ? "#0A7F43" : "#B42318"} sw={2.2} />{toast.msg}</div>)}
    </div>
  );
}

function SummaryEditor({ cards: initialCards, customs, onSave, onCancel }) {
  useEscapeKey(onCancel);
  const dialogRef = useDialogFocus();
  const [cards, setCards] = useState(initialCards.map(c => ({ ...c })));
  const [addOpen, setAddOpen] = useState(false);

  // Build full options list: base metrics + custom metrics
  const allOptions = [
    ...SUMMARY_METRIC_OPTIONS,
    ...(customs || []).map(cm => ({ key: `cm:${cm.id}`, label: cm.name, defaultFormat: cm.format === "currency" ? "currency" : cm.format === "percent" ? "decimal" : "number" })),
  ];

  const updateCard = (idx, field, val) => {
    setCards(prev => prev.map((c, i) => i === idx ? { ...c, [field]: val } : c));
  };
  const removeCard = (idx) => setCards(prev => prev.filter((_, i) => i !== idx));
  const moveCard = (idx, dir) => {
    setCards(prev => {
      const arr = [...prev];
      const target = idx + dir;
      if (target < 0 || target >= arr.length) return arr;
      [arr[idx], arr[target]] = [arr[target], arr[idx]];
      return arr;
    });
  };
  const addCard = (key) => {
    const opt = allOptions.find(o => o.key === key);
    if (!opt) return;
    setCards(prev => [...prev, { label: opt.label, key: opt.key, agg: "total", format: opt.defaultFormat }]);
    setAddOpen(false);
  };

  const SE = {
    cardRow: { display: "flex", alignItems: "center", gap: 8, padding: "12px 14px", border: "1px solid var(--ds-border)", borderRadius: 8, background: "var(--ds-background-200)", marginBottom: 8 },
    fieldGroup: { display: "flex", flexDirection: "column", gap: 3 },
    miniLabel: { fontSize: 10, fontWeight: 600, color: "var(--ds-gray-600)", textTransform: "uppercase", letterSpacing: "0.04em" },
    miniSelect: { minHeight: 32, padding: "5px 8px", fontSize: 12, fontWeight: 500, border: "1px solid var(--ds-border-hover)", borderRadius: 6, background: "var(--ds-background-100)", color: "var(--ds-gray-800)", outline: "none", cursor: "pointer", fontFamily: "inherit" },
    miniInput: { minHeight: 32, padding: "5px 8px", fontSize: 12, fontWeight: 500, border: "1px solid var(--ds-border-hover)", borderRadius: 6, background: "var(--ds-background-100)", color: "var(--ds-gray-800)", outline: "none", fontFamily: "inherit", width: 120 },
    moveBtn: { width: 28, height: 28, background: "transparent", border: "none", borderRadius: 6, cursor: "pointer", padding: 0, color: "var(--ds-gray-600)", fontSize: 14, lineHeight: 1, display: "flex", alignItems: "center", justifyContent: "center" },
    removeBtn: { background: "transparent", border: "none", cursor: "pointer", padding: "2px 4px", color: "var(--ds-red-600)", fontSize: 16, lineHeight: 1, display: "flex", alignItems: "center", borderRadius: 4 },
  };

  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onCancel}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner" role="dialog" aria-modal="true" aria-label="Edit summary cards" style={{ ...S.modal, maxWidth: 560, textAlign: "left", padding: "28px 32px" }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
          <div style={{ width: 40, height: 40, borderRadius: 10, background: "var(--ds-blue-100)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
            <I d="M4 6h16M4 12h16M4 18h7" size={20} stroke="var(--ds-blue-700)" sw={2} />
          </div>
          <div>
            <div style={{ fontSize: 17, fontWeight: 600, color: "var(--ds-gray-900)" }}>Edit summary cards</div>
            <div style={{ fontSize: 13, color: "var(--ds-gray-700)", marginTop: 2 }}>Choose metrics, labels, and aggregation type</div>
          </div>
        </div>

        <div style={{ maxHeight: 400, overflowY: "auto", marginBottom: 16, paddingRight: 4 }}>
          {cards.length === 0 && (
            <div style={{ textAlign: "center", padding: "32px 0", color: "var(--ds-gray-600)", fontSize: 13 }}>No cards configured. Add one below.</div>
          )}
          {cards.map((card, idx) => (
            <div key={idx} style={SE.cardRow}>
              <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
                <button aria-label={`Move ${card.label} up`} style={SE.moveBtn} onClick={() => moveCard(idx, -1)} disabled={idx === 0} title="Move up">▲</button>
                <button aria-label={`Move ${card.label} down`} style={SE.moveBtn} onClick={() => moveCard(idx, 1)} disabled={idx === cards.length - 1} title="Move down">▼</button>
              </div>
              <div style={{ flex: 1, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "end" }}>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Label</span>
                  <input aria-label={`Label for card ${idx + 1}`} style={SE.miniInput} value={card.label} onChange={e => updateCard(idx, "label", e.target.value)} />
                </div>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Metric</span>
                  <select aria-label={`Metric for ${card.label}`} style={SE.miniSelect} value={card.key} onChange={e => {
                    const opt = allOptions.find(o => o.key === e.target.value);
                    updateCard(idx, "key", e.target.value);
                    if (opt && card.label === (allOptions.find(o => o.key === card.key)?.label || "")) updateCard(idx, "label", opt.label);
                  }}>
                    {allOptions.map(o => <option key={o.key} value={o.key}>{o.label}</option>)}
                  </select>
                </div>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Type</span>
                  <select aria-label={`Aggregation for ${card.label}`} style={SE.miniSelect} value={card.agg} onChange={e => updateCard(idx, "agg", e.target.value)}>
                    <option value="total">Total</option>
                    <option value="avg">Average</option>
                  </select>
                </div>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Format</span>
                  <select aria-label={`Display format for ${card.label}`} style={SE.miniSelect} value={card.format} onChange={e => updateCard(idx, "format", e.target.value)}>
                    <option value="number">Number</option>
                    <option value="currency">Currency ($)</option>
                    <option value="decimal">Decimal</option>
                  </select>
                </div>
              </div>
              <button aria-label={`Remove ${card.label} card`} style={SE.removeBtn} onClick={() => removeCard(idx)} title="Remove card">×</button>
            </div>
          ))}
        </div>

        {!addOpen ? (
          <button
            style={{ ...S.btnLight, width: "100%", justifyContent: "center", marginBottom: 16, padding: "10px 0", borderStyle: "dashed", color: "var(--ds-gray-700)" }}
            onClick={() => setAddOpen(true)}
          >
            + Add card
          </button>
        ) : (
          <div style={{ display: "flex", gap: 8, marginBottom: 16, alignItems: "center" }}>
            <select
              aria-label="Metric to add"
              autoFocus
              style={{ ...SE.miniSelect, flex: 1, padding: "8px 10px", fontSize: 13 }}
              defaultValue=""
              onChange={e => { if (e.target.value) addCard(e.target.value); }}
            >
              <option value="" disabled>Select a metric…</option>
              {allOptions.map(o => <option key={o.key} value={o.key}>{o.label}</option>)}
            </select>
            <button style={{ ...S.btnGhost, padding: "6px 10px", fontSize: 12, color: "var(--ds-gray-700)" }} onClick={() => setAddOpen(false)}>Cancel</button>
          </div>
        )}

        <div style={{ display: "flex", gap: 10, justifyContent: "space-between" }}>
          <button style={{ ...S.btnGhost, fontSize: 13, color: "var(--ds-gray-700)" }} onClick={() => setCards(DEFAULT_SUMMARY_CARDS.map(c => ({ ...c })))}>Reset to default</button>
          <div style={{ display: "flex", gap: 10 }}>
            <button style={{ ...S.btnLight, justifyContent: "center" }} onClick={onCancel}>Cancel</button>
            <button style={{ ...S.btnDark, justifyContent: "center" }} onClick={() => onSave(cards)} disabled={cards.length === 0}>Save</button>
          </div>
        </div>
      </div>
    </div>
  );
}

function RegPageMapModal({ flash, onClose, onSaved }) {
  useEscapeKey(onClose);
  const dialogRef = useDialogFocus();
  const [rows, setRows] = useState(null); // [{ url, variant }]
  const [isDefault, setIsDefault] = useState(false);
  const [saving, setSaving] = useState(false);
  useEffect(() => {
    api.getRegPageVariants()
      .then(r => { setRows(Object.entries(r.map || {}).map(([url, variant]) => ({ url, variant }))); setIsDefault(!!r.isDefault); })
      .catch(e => { flash(e.message, "err"); setRows([]); });
  }, []);
  const update = (i, k, v) => setRows(rs => rs.map((row, j) => j === i ? { ...row, [k]: v } : row));
  const add = () => setRows(rs => [...(rs || []), { url: "", variant: "A" }]);
  const remove = (i) => setRows(rs => rs.filter((_, j) => j !== i));
  const save = async () => {
    const map = {};
    for (const r of (rows || [])) { const u = (r.url || "").trim(); if (u) map[u] = r.variant; }
    setSaving(true);
    try { await api.setRegPageVariants(map); flash(Object.keys(map).length ? "Reg-page map saved" : "Reverted to default"); onSaved && onSaved(); onClose(); }
    catch (e) { flash(e.message, "err"); }
    finally { setSaving(false); }
  };
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onClose}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner structured-dialog" role="dialog" aria-modal="true" aria-label="Registration page variant map" style={{ ...S.modal, maxWidth: 660, width: "92%", textAlign: "left", padding: 0, display: "flex", flexDirection: "column", maxHeight: "85vh" }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: "20px 24px", borderBottom: "1px solid var(--ds-border)" }}>
          <h2 style={{ fontSize: 18, fontWeight: 600, margin: 0, color: "var(--ds-gray-900)" }}>Registration page variants</h2>
          <div style={{ fontSize: 13, color: "var(--ds-gray-700)", marginTop: 4 }}>Maps each landing-page URL to its split-test variant for the <strong>Reg Page Visits</strong> metric. Use the full URL with no query string (e.g. <code>https://drshumardworkshop.com/webinar</code>).{isDefault && <span style={{ color: "var(--ds-gray-600)" }}> · currently using the built-in default</span>}</div>
        </div>
        <div style={{ padding: 24, overflowY: "auto", flex: 1 }}>
          {rows === null ? <div style={{ color: "var(--ds-gray-600)" }}>Loading…</div> : (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {rows.map((r, i) => (
                <div className="repeater-row" key={i} style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <input aria-label={`Registration page URL ${i + 1}`} style={{ ...S.inp, flex: 1, minWidth: 0, fontSize: 13 }} placeholder="https://drshumardworkshop.com/webinar" value={r.url} onChange={e => update(i, "url", e.target.value)} />
                  <select aria-label={`Variant for registration page ${i + 1}`} style={{ ...S.inp, width: 130, fontSize: 13 }} value={r.variant} onChange={e => update(i, "variant", e.target.value)}>
                    <option value="A">A</option><option value="B">B</option><option value="undetected">Undetected</option>
                  </select>
                  <button aria-label={`Remove registration page ${i + 1}`} style={{ ...S.btnGhost, padding: "6px 9px" }} onClick={() => remove(i)} title="Remove">✕</button>
                </div>
              ))}
              <button style={{ ...S.btnLight, alignSelf: "flex-start", fontSize: 13 }} onClick={add}>+ Add page</button>
              {rows.length === 0 && <div style={{ fontSize: 12, color: "var(--ds-gray-600)" }}>No pages mapped — saving empty reverts to the default (/webinar→A, /webinar-page→B).</div>}
            </div>
          )}
        </div>
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, padding: "16px 24px", borderTop: "1px solid var(--ds-border)" }}>
          <button style={S.btnGhost} onClick={onClose}>Cancel</button>
          <button style={{ ...S.btnDark, opacity: (saving || rows === null) ? 0.6 : 1 }} disabled={saving || rows === null} onClick={save}>{saving ? "Saving…" : "Save"}</button>
        </div>
      </div>
    </div>
  );
}

function SalesPagesModal({ flash, onClose, onSaved }) {
  useEscapeKey(onClose);
  const dialogRef = useDialogFocus();
  const [rows, setRows] = useState(null); // [{ url, label }]
  const [isDefault, setIsDefault] = useState(false);
  const [saving, setSaving] = useState(false);
  useEffect(() => {
    api.getSalesPages()
      .then(r => { setRows(Object.entries(r.map || {}).map(([url, label]) => ({ url, label }))); setIsDefault(!!r.isDefault); })
      .catch(e => { flash(e.message, "err"); setRows([]); });
  }, []);
  const update = (i, k, v) => setRows(rs => rs.map((row, j) => j === i ? { ...row, [k]: v } : row));
  const add = () => setRows(rs => [...(rs || []), { url: "", label: "" }]);
  const remove = (i) => setRows(rs => rs.filter((_, j) => j !== i));
  const save = async () => {
    const map = {};
    for (const r of (rows || [])) { const u = (r.url || "").trim(); const l = (r.label || "").trim(); if (u && l) map[u] = l; }
    setSaving(true);
    try { await api.setSalesPages(map); flash(Object.keys(map).length ? "Sales pages saved" : "Reverted to default"); onSaved && onSaved(); onClose(); }
    catch (e) { flash(e.message, "err"); }
    finally { setSaving(false); }
  };
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onClose}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner structured-dialog" role="dialog" aria-modal="true" aria-label="Sales pages" style={{ ...S.modal, maxWidth: 660, width: "92%", textAlign: "left", padding: 0, display: "flex", flexDirection: "column", maxHeight: "85vh" }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: "20px 24px", borderBottom: "1px solid var(--ds-border)" }}>
          <h2 style={{ fontSize: 18, fontWeight: 600, margin: 0, color: "var(--ds-gray-900)" }}>Sales pages</h2>
          <div style={{ fontSize: 13, color: "var(--ds-gray-700)", marginTop: 4 }}>Name each sales/checkout page by its full URL (no query string, e.g. <code>https://drshumardworkshop.com/checkout2</code>). These labels drive the CRM journey, the <strong>Sales pages</strong> column/filter, and AI Insights questions like “how many people hit Sales B”.{isDefault && <span style={{ color: "var(--ds-gray-600)" }}> · currently using the built-in default</span>}</div>
        </div>
        <div style={{ padding: 24, overflowY: "auto", flex: 1 }}>
          {rows === null ? <div style={{ color: "var(--ds-gray-600)" }}>Loading…</div> : (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {rows.map((r, i) => (
                <div className="repeater-row" key={i} style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <input aria-label={`Sales page URL ${i + 1}`} style={{ ...S.inp, flex: 1, minWidth: 0, fontSize: 13 }} placeholder="https://drshumardworkshop.com/checkout2" value={r.url} onChange={e => update(i, "url", e.target.value)} />
                  <input aria-label={`Sales page label ${i + 1}`} style={{ ...S.inp, width: 150, fontSize: 13 }} placeholder="Sales B" value={r.label} onChange={e => update(i, "label", e.target.value)} />
                  <button aria-label={`Remove sales page ${i + 1}`} style={{ ...S.btnGhost, padding: "6px 9px" }} onClick={() => remove(i)} title="Remove">✕</button>
                </div>
              ))}
              <button style={{ ...S.btnLight, alignSelf: "flex-start", fontSize: 13 }} onClick={add}>+ Add page</button>
              {rows.length === 0 && <div style={{ fontSize: 12, color: "var(--ds-gray-600)" }}>No pages mapped — saving empty reverts to the default (/checkout→Legacy, /checkout1→Sales A, /checkout2→Sales B).</div>}
            </div>
          )}
        </div>
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, padding: "16px 24px", borderTop: "1px solid var(--ds-border)" }}>
          <button style={S.btnGhost} onClick={onClose}>Cancel</button>
          <button style={{ ...S.btnDark, opacity: (saving || rows === null) ? 0.6 : 1 }} disabled={saving || rows === null} onClick={save}>{saving ? "Saving…" : "Save"}</button>
        </div>
      </div>
    </div>
  );
}

function LensEditor({ lens, onSave, onCancel }) {
  useEscapeKey(onCancel);
  const dialogRef = useDialogFocus();
  const [name, setName] = useState(lens.name || "");
  const [metrics, setMetrics] = useState((lens.metrics || MK).filter(k => COL_LABELS[k]));
  const toggle = (m) => setMetrics(prev => prev.includes(m) ? prev.filter(x => x !== m) : [...prev, m]);
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onCancel}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner" role="dialog" aria-modal="true" aria-label={lens.id ? "Edit lens" : "Create lens"} style={{ ...S.modal, maxWidth: 420 }} onClick={e => e.stopPropagation()}>
        <div style={{ width: 44, height: 44, borderRadius: 12, background: "var(--ds-blue-100)", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}><I d="M15 12a3 3 0 11-6 0 3 3 0 016 0zM2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" size={22} stroke="var(--ds-blue-700)" sw={2} /></div>
        <div style={{ fontSize: 18, fontWeight: 600, color: "var(--ds-gray-900)", marginBottom: 6, textAlign: "center" }}>{lens.id ? "Edit lens" : "Create lens"}</div>
        <div style={{ color: "var(--ds-gray-700)", fontSize: 13, marginBottom: 20, textAlign: "center" }}>Choose a name and the metrics to include</div>
        <div style={{ marginBottom: 16 }}>
          <label htmlFor="lens-name" style={{ fontSize: 12, fontWeight: 600, color: "var(--ds-gray-800)", display: "block", marginBottom: 6 }}>Lens name</label>
          <input id="lens-name" style={S.inp} value={name} onChange={e => setName(e.target.value)} placeholder="e.g. Funnel Overview" autoFocus />
        </div>
        <div style={{ marginBottom: 20 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: "var(--ds-gray-800)", display: "block", marginBottom: 8 }}>Metrics</label>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
            {MK.filter(m => COL_LABELS[m]).map(m => (
              <label key={m} style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", borderRadius: 8, border: `1px solid ${metrics.includes(m) ? "var(--ds-border-hover)" : "var(--ds-border)"}`, background: metrics.includes(m) ? "var(--ds-blue-100)" : "#fff", cursor: "pointer", fontSize: 13, fontWeight: 500, color: metrics.includes(m) ? "var(--ds-blue-700)" : "var(--ds-gray-700)", transition: "all 0.15s" }}>
                <input type="checkbox" checked={metrics.includes(m)} onChange={() => toggle(m)} style={{ accentColor: "var(--ds-blue-700)" }} />
                {COL_LABELS[m]}
              </label>
            ))}
          </div>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button style={{ ...S.btnLight, flex: 1, justifyContent: "center" }} onClick={onCancel}>Cancel</button>
          <button style={{ ...S.btnDark, flex: 1, justifyContent: "center" }} disabled={!name.trim() || metrics.length === 0} onClick={() => onSave({ id: lens.id, name: name.trim(), metrics })}>{lens.id ? "Update" : "Create"}</button>
        </div>
      </div>
    </div>
  );
}

function ColumnEditor({ columns, onSave, onCancel, isAdmin }) {
  useEscapeKey(onCancel);
  const dialogRef = useDialogFocus();
  const [items, setItems] = useState(columns.map(c => ({ ...c })));
  const [propagating, setPropagating] = useState(false);
  const dragItem = useRef(null);
  const dragOverItem = useRef(null);
  const [dragIdx, setDragIdx] = useState(null);
  const [overIdx, setOverIdx] = useState(null);
  const [statusMsg, setStatusMsg] = useState(null);

  const handleDragStart = (idx) => { dragItem.current = idx; setDragIdx(idx); };
  const handleDragEnter = (idx) => { dragOverItem.current = idx; setOverIdx(idx); };
  const handleDragEnd = () => {
    if (dragItem.current !== null && dragOverItem.current !== null && dragItem.current !== dragOverItem.current) {
      const next = [...items];
      const [dragged] = next.splice(dragItem.current, 1);
      next.splice(dragOverItem.current, 0, dragged);
      setItems(next);
    }
    dragItem.current = null;
    dragOverItem.current = null;
    setDragIdx(null);
    setOverIdx(null);
  };
  const moveItem = (index, delta) => {
    const target = index + delta;
    if (target < 0 || target >= items.length) return;
    setItems(prev => {
      const next = [...prev];
      const [item] = next.splice(index, 1);
      next.splice(target, 0, item);
      return next;
    });
  };

  // Reset = natural MK order, filtered to whatever base columns the current
  // lens shows (so "Reset" doesn't bring hidden columns back).
  const visibleBaseKeys = new Set(columns.filter(c => c.type === "base").map(c => c.key));
  const reset = () => setItems(
    MK.filter(k => COL_LABELS[k] && visibleBaseKeys.has(k))
      .map(k => ({ key: k, label: COL_LABELS[k], type: "base" }))
      .concat(columns.filter(c => c.type === "custom"))
  );
  const CE = {
    list: { display: "flex", flexDirection: "column", gap: 2, maxHeight: 400, overflowY: "auto", marginBottom: 20 },
    item: (isDragging, isOver) => ({ display: "flex", alignItems: "center", gap: 10, padding: "10px 12px", background: isDragging ? "var(--ds-blue-100)" : isOver ? "var(--ds-gray-100)" : "var(--ds-background-200)", border: `1px solid ${isDragging || isOver ? "var(--ds-border-hover)" : "var(--ds-border)"}`, borderRadius: 8, cursor: "grab", transition: "all 150ms ease", opacity: isDragging ? 0.65 : 1, transform: isDragging ? "scale(0.985)" : "scale(1)" }),
    grip: { color: "var(--ds-gray-600)", fontSize: 14, cursor: "grab", userSelect: "none", display: "flex", flexDirection: "column", lineHeight: 0.5, letterSpacing: 2 },
    label: { flex: 1, fontSize: 14, fontWeight: 500, color: "var(--ds-gray-900)" },
    badge: { fontSize: 10, fontWeight: 600, padding: "2px 6px", borderRadius: 4, textTransform: "uppercase", letterSpacing: "0.04em" },
  };
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onCancel}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner" role="dialog" aria-modal="true" aria-label="Reorder columns" style={{ ...S.modal, maxWidth: 440, textAlign: "left" }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <div>
            <div style={{ fontSize: 18, fontWeight: 600, color: "var(--ds-gray-900)" }}>Edit column order</div>
            <div style={{ fontSize: 13, color: "var(--ds-gray-700)", marginTop: 4 }}>Drag or use the arrow buttons. Day &amp; Date are always first.</div>
          </div>
        </div>
        <div style={CE.list}>
          {items.map((col, i) => (
            <div
              key={col.key}
              draggable
              onDragStart={() => handleDragStart(i)}
              onDragEnter={() => handleDragEnter(i)}
              onDragEnd={handleDragEnd}
              onDragOver={e => e.preventDefault()}
              style={CE.item(dragIdx === i, overIdx === i && dragIdx !== i)}
            >
              <span style={CE.grip}>⠿</span>
              <span style={{ color: "var(--ds-gray-600)", fontSize: 12, fontWeight: 600, width: 20, textAlign: "center" }}>{i + 1}</span>
              <span style={CE.label}>{col.label}</span>
              {col.type === "custom" && <span style={{ ...CE.badge, background: "#ECFDF5", color: "#047857" }}>Custom</span>}
              <div style={{ display: "flex", alignItems: "center", gap: 2 }}>
                <button type="button" draggable={false} aria-label={`Move ${col.label} up`} title="Move up" disabled={i === 0} style={{ ...S.btnGhost, minHeight: 28, padding: "4px 6px" }} onMouseDown={e => e.stopPropagation()} onClick={() => moveItem(i, -1)}>↑</button>
                <button type="button" draggable={false} aria-label={`Move ${col.label} down`} title="Move down" disabled={i === items.length - 1} style={{ ...S.btnGhost, minHeight: 28, padding: "4px 6px" }} onMouseDown={e => e.stopPropagation()} onClick={() => moveItem(i, 1)}>↓</button>
              </div>
            </div>
          ))}
        </div>
        <div style={{ display: "flex", gap: 10, justifyContent: "space-between", flexWrap: "wrap" }}>
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <button style={{ ...S.btnGhost, fontSize: 13, color: "var(--ds-gray-700)" }} onClick={reset}>Reset to default</button>
            {isAdmin && (
              <button
                style={{ ...S.btnGhost, fontSize: 12, color: "var(--ds-blue-700)", opacity: propagating ? 0.5 : 1 }}
                disabled={propagating}
                onClick={async () => {
                  setPropagating(true);
                  setStatusMsg(null);
                  try {
                    // First save the current order
                    onSave(items.map(c => c.key));
                    // Then propagate to all users
                    const headers = await getAuthHeaders();
                    const res = await fetch(`${API_BASE}/api/settings/propagate-col-order`, { method: "POST", headers });
                    const data = await res.json();
                    if (data.success) setStatusMsg(`Pushed to ${data.updated} user${data.updated !== 1 ? "s" : ""}`);
                    else setStatusMsg(data.error || "Failed");
                  } catch { setStatusMsg("Network error"); }
                  setPropagating(false);
                }}
              >
                {propagating ? "Pushing…" : "Set as default for all"}
              </button>
            )}
            {statusMsg && <span style={{ fontSize: 12, color: "#047857", fontWeight: 500 }}>{statusMsg}</span>}
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <button style={{ ...S.btnLight, justifyContent: "center" }} onClick={onCancel}>Cancel</button>
            <button style={{ ...S.btnDark, justifyContent: "center" }} onClick={() => onSave(items.map(c => c.key))}>Save order</button>
          </div>
        </div>
      </div>
    </div>
  );
}

function ForgotPasswordModal({ initialEmail, onCancel, onSent }) {
  useEscapeKey(onCancel);
  const dialogRef = useDialogFocus();
  const [email, setEmail] = useState(initialEmail || "");
  const [err, setErr] = useState("");
  const [sending, setSending] = useState(false);
  const send = async (e) => {
    e.preventDefault();
    setErr("");
    setSending(true);
    try {
      const { error } = await supabase.auth.resetPasswordForEmail(email.trim(), { redirectTo: window.location.origin });
      if (error) setErr(error.message);
      else onSent(email.trim());
    } finally {
      setSending(false);
    }
  };
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onCancel}>
      <form ref={dialogRef} tabIndex={-1} className="modal-inner" role="dialog" aria-modal="true" aria-label="Reset password" style={{ ...S.modal, maxWidth: 400, textAlign: "left" }} onClick={e => e.stopPropagation()} onSubmit={send}>
        <div style={{ fontSize: 18, fontWeight: 600, color: "var(--ds-gray-900)", marginBottom: 6 }}>Reset your password</div>
        <div style={{ color: "var(--ds-gray-700)", fontSize: 13, marginBottom: 18, lineHeight: 1.5 }}>Enter your account email and we'll send you a link to set a new password.</div>
        {err && <div role="alert" style={{ background: "#FFF7F7", color: "#C00", padding: "10px 14px", borderRadius: 8, fontSize: 13, marginBottom: 14, border: "1px solid #F5B7B7" }}>{err}</div>}
        <label htmlFor="forgot-email" style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#333", marginBottom: 6 }}>Email</label>
        <input id="forgot-email" data-dialog-initial-focus type="email" autoComplete="email" value={email} onChange={e => setEmail(e.target.value)} required style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", fontSize: 14, outline: "none", boxSizing: "border-box", marginBottom: 20 }} />
        <div style={{ display: "flex", gap: 10 }}>
          <button type="button" style={{ ...S.btnLight, flex: 1, justifyContent: "center" }} onClick={onCancel}>Cancel</button>
          <button type="submit" disabled={sending} aria-busy={sending} style={{ ...S.btnDark, flex: 1, justifyContent: "center" }}>{sending ? "Sending…" : "Send reset link"}</button>
        </div>
      </form>
    </div>
  );
}

function ResetLinkModal({ funnel, onCancel }) {
  useEscapeKey(onCancel);
  const dialogRef = useDialogFocus();
  const [email, setEmail] = useState("");
  const [result, setResult] = useState(null); // { email, link }
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState(false);
  const generate = async (e) => {
    e.preventDefault();
    setErr(""); setResult(null); setCopied(false);
    setBusy(true);
    try { setResult(await api.adminResetLink(email.trim(), funnel)); }
    catch (ex) { setErr(ex.message); }
    finally { setBusy(false); }
  };
  const copy = async () => {
    try { await navigator.clipboard.writeText(result.link); setCopied(true); setTimeout(() => setCopied(false), 2000); }
    catch { setErr("Copy failed — select the link text and copy manually."); }
  };
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onCancel}>
      <form ref={dialogRef} tabIndex={-1} className="modal-inner" role="dialog" aria-modal="true" aria-label="Password reset link" style={{ ...S.modal, maxWidth: 460, textAlign: "left" }} onClick={e => e.stopPropagation()} onSubmit={generate}>
        <div style={{ fontSize: 18, fontWeight: 600, color: "var(--ds-gray-900)", marginBottom: 6 }}>Password reset link</div>
        <div style={{ color: "var(--ds-gray-700)", fontSize: 13, marginBottom: 18, lineHeight: 1.5 }}>Generate a link you can send to a user directly (text, Slack, …). It opens the set-a-new-password screen. Single-use, expires in about an hour.</div>
        {err && <div role="alert" style={{ background: "#FFF7F7", color: "#C00", padding: "10px 14px", borderRadius: 8, fontSize: 13, marginBottom: 14, border: "1px solid #F5B7B7" }}>{err}</div>}
        <label htmlFor="reset-link-email" style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#333", marginBottom: 6 }}>User's email</label>
        <input id="reset-link-email" data-dialog-initial-focus type="email" autoComplete="off" value={email} onChange={e => setEmail(e.target.value)} required style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", fontSize: 14, outline: "none", boxSizing: "border-box", marginBottom: 16 }} />
        {result && (
          <div style={{ background: "var(--ds-gray-100)", border: "1px solid var(--ds-border)", borderRadius: 8, padding: "10px 12px", marginBottom: 16 }}>
            <div style={{ fontSize: 12, color: "var(--ds-gray-600)", marginBottom: 6 }}>Reset link for {result.email}</div>
            <div style={{ fontSize: 12, fontFamily: "monospace", wordBreak: "break-all", color: "var(--ds-gray-900)", marginBottom: 10, userSelect: "all" }}>{result.link}</div>
            <button type="button" onClick={copy} style={{ ...S.btnDark, padding: "7px 14px", fontSize: 13 }}>{copied ? "Copied ✓" : "Copy link"}</button>
          </div>
        )}
        <div style={{ display: "flex", gap: 10 }}>
          <button type="button" style={{ ...S.btnLight, flex: 1, justifyContent: "center" }} onClick={onCancel}>{result ? "Done" : "Cancel"}</button>
          <button type="submit" disabled={busy} aria-busy={busy} style={{ ...S.btnDark, flex: 1, justifyContent: "center" }}>{busy ? "Generating…" : result ? "Generate another" : "Generate link"}</button>
        </div>
      </form>
    </div>
  );
}

function Modal({ title, msg, onCancel, onConfirm }) {
  useEscapeKey(onCancel);
  const dialogRef = useDialogFocus();
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onCancel}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner" role="alertdialog" aria-modal="true" aria-label={title} style={S.modal} onClick={e => e.stopPropagation()}>
        <div style={{ width: 44, height: 44, borderRadius: 12, background: "var(--ds-red-100)", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}><I d="M12 2a10 10 0 100 20 10 10 0 000-20zM12 8v4M12 16h.01" size={22} stroke="var(--ds-red-600)" sw={2} /></div>
        <div style={{ fontSize: 18, fontWeight: 600, color: "var(--ds-gray-900)", marginBottom: 6 }}>{title}</div>
        <div style={{ color: "var(--ds-gray-700)", fontSize: 14, marginBottom: 28, lineHeight: 1.5 }}>{msg}</div>
        <div style={{ display: "flex", gap: 10 }}><button style={{ ...S.btnLight, flex: 1, justifyContent: "center" }} onClick={onCancel}>Cancel</button><button style={{ ...S.btnDark, background: "var(--ds-red-600)", borderColor: "var(--ds-red-600)", flex: 1, justifyContent: "center" }} onClick={onConfirm}>Delete</button></div>
      </div>
    </div>
  );
}

// ─── CRM presentational helpers ──────────────────────────────────────────────
const CRM_STAGE_META = {
  lead:         { label: "Lead",        color: "var(--ds-gray-700)" },
  registration: { label: "Registered",  color: "#1D4ED8" },
  attended:     { label: "Attended",    color: "#0369A1" },
  replay:       { label: "Replay",      color: "#6D28D9" },
  viewedcta:    { label: "Saw CTA",     color: "#A35200" },
  clickedcta:   { label: "Clicked CTA", color: "#0A7F43" },
  purchase:     { label: "Purchased",   color: "#B42318" },
};
const crmTimelineColor = (item) => {
  if (item.kind === "pageview") return item.sales_page ? "#059669" : "var(--ds-gray-600)";
  if (item.kind === "tag") return "#14B8A6";
  const ec = { registrations: "#1D4ED8", attended: "#0369A1", replays: "#6D28D9", viewedcta: "#A35200", clickedcta: "#0A7F43", purchases: "#B42318", stayeduntil: "#4338CA" };
  return ec[item.event_type] || "var(--ds-gray-700)";
};
const crmRelTime = (ts) => {
  const s = (Date.now() - new Date(ts).getTime()) / 1000;
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
};
const crmSourceLabel = (attr) => {
  if (!attr) return null;
  return attr.utm_source || ((attr.fbclid || attr.fbc) ? "facebook" : null) || (attr.gclid ? "google" : null) || (attr.ttclid ? "tiktok" : null) || null;
};
function CrmStageBadge({ stage }) {
  const m = CRM_STAGE_META[stage] || CRM_STAGE_META.lead;
  return <span style={{ fontSize: 11, fontWeight: 650, letterSpacing: "0.01em", color: m.color, background: `color-mix(in srgb, ${m.color} 9%, transparent)`, padding: "3px 8px", borderRadius: 5, whiteSpace: "nowrap" }}>{m.label}</span>;
}
function CrmDetailRows({ rows }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {rows.filter(([, v]) => v != null && v !== "").map(([k, v]) => (
        <div key={k} style={{ display: "flex", fontSize: 13, gap: 12 }}>
          <span style={{ color: "var(--ds-gray-600)", minWidth: 110, flexShrink: 0 }}>{k}</span>
          <span style={{ color: "var(--ds-gray-800)", wordBreak: "break-all" }}>{v}</span>
        </div>
      ))}
    </div>
  );
}
function CrmContactModal({ contact, tab, setTab, onClose, isAdmin, onLink }) {
  useEscapeKey(onClose);
  const dialogRef = useDialogFocus();
  const c = contact.contact || {};
  const timeline = contact.timeline || [];
  const visits = contact.visits || [];
  const events = contact.events || [];
  const linked = contact.linked || [];
  const attr = c.attribution || {};
  const title = c.name || c.email || "Contact";
  const hasPurchase = events.some(e => e.event_type === "purchases");
  const [linkOpen, setLinkOpen] = useState(false);
  const [linkTo, setLinkTo] = useState("");
  const submitLink = () => {
    const canon = linkTo.trim().toLowerCase();
    if (!canon || !c.email) return;
    onLink(c.email, canon);
    setLinkOpen(false); setLinkTo("");
  };
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onClose}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner structured-dialog" role="dialog" aria-modal="true" aria-label="Contact details" style={{ ...S.modal, maxWidth: 720, width: "92%", maxHeight: "85vh", padding: 0, textAlign: "left", display: "flex", flexDirection: "column", overflow: "hidden" }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: "20px 24px", borderBottom: "1px solid var(--ds-border)", display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
          <div style={{ minWidth: 0 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4, flexWrap: "wrap" }}>
              <h2 style={{ fontSize: 18, fontWeight: 600, color: "var(--ds-gray-900)", margin: 0 }}>{title}</h2>
              <CrmStageBadge stage={c.stage} />
              {!c.is_tracked && <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.06em", color: "var(--ds-gray-600)", background: "var(--ds-gray-100)", padding: "2px 7px", borderRadius: 4 }}>LEGACY</span>}
              {c.is_shared_ip && <span title="Seen on a shared/NAT IP — identity was not auto-merged to avoid fusing strangers" style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.06em", color: "#B91C1C", background: "#FEE2E2", padding: "2px 7px", borderRadius: 4 }}>SHARED IP</span>}
              {linked.length > 0 && <button type="button" title="Other emails or phones fused into this person" style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.04em", color: "var(--ds-blue-700)", background: "var(--ds-blue-100)", border: "1px solid var(--ds-border)", padding: "2px 7px", borderRadius: 5, cursor: "pointer" }} onClick={() => setTab("linked")}>+{linked.length} linked</button>}
            </div>
            <div style={{ fontSize: 13, color: "var(--ds-gray-700)", display: "flex", gap: 12, flexWrap: "wrap" }}>
              {c.email && <span>{c.email}</span>}
              {c.phone && <span>{c.phone}</span>}
            </div>
            {isAdmin && c.email && (
              <div style={{ marginTop: 10 }}>
                {!linkOpen ? (
                  <button style={{ ...S.btnLight, padding: "4px 10px", fontSize: 12 }} onClick={() => setLinkOpen(true)} title="Attribute this person's events to their real registrant."><I d="M10 13a5 5 0 007.1 0l2-2a5 5 0 00-7.1-7.1l-1.1 1.1M14 11a5 5 0 00-7.1 0l-2 2A5 5 0 0012 20.1l1.1-1.1" size={13} /> Link to registrant</button>
                ) : (
                  <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
                    <input aria-label="Registrant email to link" autoFocus style={{ ...S.inp, padding: "6px 10px", fontSize: 12, minWidth: 240, width: "auto" }} placeholder="registrant email to attribute to…" value={linkTo} onChange={e => setLinkTo(e.target.value)} onKeyDown={e => { if (e.key === "Enter") submitLink(); }} />
                    <button style={{ ...S.btnDark, padding: "6px 12px", fontSize: 12 }} onClick={submitLink} disabled={!linkTo.trim()}>Link</button>
                    <button style={{ ...S.btnGhost, padding: "6px 8px", fontSize: 12 }} onClick={() => { setLinkOpen(false); setLinkTo(""); }}>Cancel</button>
                  </div>
                )}
              </div>
            )}
          </div>
          <button aria-label="Close contact details" style={S.btnGhost} onClick={onClose}><I d="M6 18L18 6M6 6l12 12" size={18} stroke="var(--ds-gray-700)" /></button>
        </div>
        <div className="dialog-tabs" role="tablist" aria-label="Contact details" style={{ display: "flex", gap: 4, padding: "0 24px", borderBottom: "1px solid var(--ds-border)" }}>
          {[["journey", `Journey (${timeline.length})`], ["clicks", `Clicks (${visits.length})`], ["linked", `Linked (${linked.length})`], ["details", "Details"]].map(([k, label]) => (
            <button role="tab" aria-selected={tab === k} key={k} onClick={() => setTab(k)} style={{ padding: "12px 14px", fontSize: 13, fontWeight: 600, background: "transparent", border: "none", borderBottom: tab === k ? "2px solid var(--ds-gray-900)" : "2px solid transparent", color: tab === k ? "var(--ds-gray-900)" : "var(--ds-gray-700)", cursor: "pointer", marginBottom: -1 }}>{label}</button>
          ))}
        </div>
        <div style={{ padding: 24, overflowY: "auto", flex: 1 }}>
          {tab === "journey" && (
            timeline.length === 0 ? <div style={{ textAlign: "center", padding: "32px 0", color: "var(--ds-gray-600)" }}>No journey events yet.</div> : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                {timeline.map((it, i) => {
                  const color = crmTimelineColor(it);
                  const last = i === timeline.length - 1;
                  return (
                    <div key={i} style={{ display: "flex", gap: 14 }}>
                      <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
                        <div style={{ width: 10, height: 10, borderRadius: "50%", background: color, flexShrink: 0, marginTop: 4 }} />
                        {!last && <div style={{ width: 2, flex: 1, background: "var(--ds-border)", marginTop: 2 }} />}
                      </div>
                      <div style={{ flex: 1, minWidth: 0, paddingBottom: last ? 0 : 18 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                          <span style={{ fontSize: 13, fontWeight: 600, color: "var(--ds-gray-900)" }}>{it.label}</span>
                          {it.source && <span style={{ fontSize: 11, color: "var(--ds-gray-700)", background: "var(--ds-gray-100)", padding: "1px 6px", borderRadius: 4 }}>{it.source}</span>}
                          <span style={{ fontSize: 12, color: "var(--ds-gray-600)" }}>{crmRelTime(it.ts)}</span>
                        </div>
                        {it.url && <div style={{ fontSize: 12, color: "var(--ds-gray-700)", marginTop: 2, wordBreak: "break-all" }}>{it.url}</div>}
                        <div style={{ fontSize: 11, color: "var(--ds-gray-600)", marginTop: 2 }}>{new Date(it.ts).toLocaleString("en-US", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" })}</div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )
          )}
          {tab === "clicks" && (
            visits.length === 0 ? <div style={{ textAlign: "center", padding: "32px 0", color: "var(--ds-gray-600)" }}>{c.is_tracked ? "No page views tracked yet." : "This contact wasn't tracked by shumard.js, so there's no on-site click activity."}</div> : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                {visits.map((v, i) => (
                  <div key={v.id || i} style={{ padding: "12px 0", borderBottom: i < visits.length - 1 ? "1px solid var(--ds-gray-100)" : "none" }}>
                    <div style={{ fontSize: 13, fontWeight: 500, color: "var(--ds-gray-900)", display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>{v.sales_page ? `Visited ${v.sales_page}` : (v.page_title || "Page view")}{v.sales_page && <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.03em", color: "#065F46", background: "#D1FAE5", padding: "2px 6px", borderRadius: 4 }}>SALES PAGE</span>}</div>
                    <div style={{ fontSize: 12, color: "var(--ds-gray-700)", wordBreak: "break-all", marginTop: 2 }}>{v.current_url}</div>
                    <div style={{ fontSize: 11, color: "var(--ds-gray-600)", marginTop: 2 }}>{new Date(v.timestamp).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}{v.referrer_url ? ` · from ${v.referrer_url}` : ""}</div>
                  </div>
                ))}
              </div>
            )
          )}
          {tab === "linked" && (
            linked.length === 0 ? <div style={{ textAlign: "center", padding: "32px 0", color: "var(--ds-gray-600)" }}>No other identities linked to this person.<div style={{ fontSize: 12, marginTop: 6 }}>Linked identities appear when the same person is seen under another email/phone on the same browser or IP (e.g. a purchase under a different checkout email).</div></div> : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                <div style={{ fontSize: 12, color: "var(--ds-gray-700)", marginBottom: 12 }}>These identities were fused into this person by the tracker (same browser / IP). Their events and clicks are already merged into the Journey above.</div>
                {linked.map((l, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, padding: "12px 0", borderBottom: i < linked.length - 1 ? "1px solid var(--ds-gray-100)" : "none" }}>
                    <div style={{ minWidth: 0 }}>
                      <div style={{ fontSize: 13, fontWeight: 500, color: "var(--ds-gray-900)", wordBreak: "break-all" }}>{l.email || l.phone || l.contact_id}</div>
                      {l.email && l.phone && <div style={{ fontSize: 12, color: "var(--ds-gray-700)", marginTop: 2 }}>{l.phone}</div>}
                    </div>
                    <span style={{ flexShrink: 0, fontSize: 10, fontWeight: 700, letterSpacing: "0.06em", padding: "2px 7px", borderRadius: 4, color: l.tracked ? "var(--ds-blue-700)" : "var(--ds-gray-600)", background: l.tracked ? "var(--ds-blue-100)" : "var(--ds-gray-100)" }}>{l.tracked ? "TRACKED" : "EVENT-ONLY"}</span>
                  </div>
                ))}
              </div>
            )
          )}
          {tab === "details" && (
            <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
              <div>
                <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "var(--ds-gray-600)", marginBottom: 8 }}>Identity</div>
                <CrmDetailRows rows={[["Name", c.name], ["Email", c.email], ["Phone", c.phone], ["First name", c.first_name], ["Last name", c.last_name], ["Tracked", c.is_tracked ? "yes" : "no"], ["Contact ID", c.contact_id]]} />
              </div>
              {Object.keys(attr).filter(k => k !== "extra").length > 0 && (
                <div>
                  <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "var(--ds-gray-600)", marginBottom: 8 }}>Attribution</div>
                  <CrmDetailRows rows={Object.entries(attr).filter(([k]) => k !== "extra").map(([k, v]) => [k, String(v)])} />
                  {attr.extra && Object.keys(attr.extra).length > 0 && <div style={{ marginTop: 6 }}><CrmDetailRows rows={Object.entries(attr.extra).map(([k, v]) => [`extra.${k}`, String(v)])} /></div>}
                </div>
              )}
              {(c.tags || []).length > 0 && (
                <div>
                  <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "var(--ds-gray-600)", marginBottom: 8 }}>Tags</div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>{c.tags.map(t => <span key={t} style={{ fontSize: 11, background: "var(--ds-gray-100)", color: "var(--ds-gray-800)", padding: "3px 8px", borderRadius: 4 }}>{t}</span>)}</div>
                </div>
              )}
              <div>
                <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "var(--ds-gray-600)", marginBottom: 8 }}>Funnel events ({events.length})</div>
                {events.length === 0 ? <div style={{ fontSize: 13, color: "var(--ds-gray-600)" }}>None</div> : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {events.map((e, i) => (
                      <div key={e.id || i} style={{ display: "flex", justifyContent: "space-between", fontSize: 13, gap: 12 }}>
                        <span style={{ color: "var(--ds-gray-800)" }}>{e.event_type}{e.metadata && e.metadata.source ? ` · ${e.metadata.source}` : ""}</span>
                        <span style={{ color: "var(--ds-gray-600)", whiteSpace: "nowrap" }}>{new Date(e.event_time).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function EmailDrillModal({ drill, loading, onClose, onOpenContact }) {
  useEscapeKey(onClose);
  const dialogRef = useDialogFocus();
  const clicks = drill.clicks || [];
  const fmtWhen = (ts) => new Date(ts).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
  const fmtGap = (click, buy) => {
    const h = (new Date(buy).getTime() - new Date(click).getTime()) / 3600000;
    if (h < 1) return `${Math.max(1, Math.round(h * 60))}m later`;
    if (h < 48) return `${Math.round(h)}h later`;
    return `${Math.round(h / 24)}d later`;
  };
  return (
    <div className="modal-backdrop" style={S.overlay} onClick={onClose}>
      <div ref={dialogRef} tabIndex={-1} className="modal-inner structured-dialog" role="dialog" aria-modal="true" aria-label="Email click details" style={{ ...S.modal, maxWidth: 860, width: "94%", maxHeight: "85vh", padding: 0, textAlign: "left", display: "flex", flexDirection: "column", overflow: "hidden" }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: "20px 24px", borderBottom: "1px solid var(--ds-border)", display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "var(--ds-gray-700)" }}>Email source</div>
            <h2 style={{ fontSize: 18, fontWeight: 600, color: "var(--ds-gray-900)", margin: "2px 0 6px" }}>{drill.source}</h2>
            <div style={{ fontSize: 13, color: "var(--ds-gray-700)", display: "flex", gap: 14, flexWrap: "wrap" }}>
              <span><b style={{ color: "var(--ds-gray-900)" }}>{drill.total_clicks ?? clicks.length}</b> clicks</span>
              <span><b style={{ color: "var(--ds-gray-900)" }}>{drill.people ?? "—"}</b> people</span>
              <span><b style={{ color: "#047857" }}>{drill.buyers ?? 0}</b> buyers</span>
            </div>
          </div>
          <button aria-label="Close email click details" style={S.btnGhost} onClick={onClose}><I d="M6 18L18 6M6 6l12 12" size={18} stroke="var(--ds-gray-700)" /></button>
        </div>
        {/* Funnel-stage breakdown — of the people who clicked this source, how far they got */}
        {!loading && drill.funnel && drill.funnel.clickers > 0 && (() => {
          const f = drill.funnel;
          const stages = [
            { key: "clickers", label: "Clicked", color: "#0F766E" },
            { key: "registered", label: "Registered", color: "#1D4ED8" },
            { key: "attended", label: "Attended", color: "#0369A1" },
            { key: "saw_cta", label: "Saw CTA", color: "#A35200" },
            { key: "clicked_cta", label: "Clicked CTA", color: "#0A7F43" },
            { key: "purchased", label: "Purchased", color: "#B42318" },
          ];
          const base = f.clickers || 1;
          return (
            <div style={{ padding: "16px 24px", borderBottom: "1px solid var(--ds-border)", background: "var(--ds-background-200)" }}>
              <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "var(--ds-gray-700)", marginBottom: 12 }}>Funnel reach of these clickers</div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                {stages.map(s => {
                  const v = f[s.key] || 0;
                  const pct = Math.round((v / base) * 100);
                  return (
                    <div key={s.key} style={{ flex: "1 1 110px", minWidth: 100, background: "#fff", border: "1px solid var(--ds-border)", borderRadius: 8, padding: "10px 12px" }}>
                      <div style={{ fontSize: 11, fontWeight: 600, color: s.color, marginBottom: 4 }}>{s.label}</div>
                      <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
                        <span style={{ fontSize: 20, fontWeight: 600, color: "var(--ds-gray-900)", fontVariantNumeric: "tabular-nums" }}>{v.toLocaleString()}</span>
                        <span style={{ fontSize: 12, color: "var(--ds-gray-600)" }}>{pct}%</span>
                      </div>
                      <div style={{ height: 3, background: "var(--ds-gray-100)", borderRadius: 2, marginTop: 6, overflow: "hidden" }}>
                        <div style={{ width: `${pct}%`, height: "100%", background: s.color }} />
                      </div>
                    </div>
                  );
                })}
              </div>
              <div style={{ fontSize: 11, color: "var(--ds-gray-600)", marginTop: 10 }}>% of the {base.toLocaleString()} identified people who clicked this email. Post‑webinar emails reach people who already registered/attended — this is their overall funnel position.</div>
            </div>
          );
        })()}
        <div style={{ padding: 0, overflowY: "auto", flex: 1 }}>
          {loading ? (
            <div style={{ textAlign: "center", padding: "48px 0", color: "var(--ds-gray-600)" }}>Loading…</div>
          ) : clicks.length === 0 ? (
            <div style={{ textAlign: "center", padding: "48px 0", color: "var(--ds-gray-600)" }}>No clicks recorded for this source yet.</div>
          ) : (
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr>{["Person", "Clicked", "Landing page", "Purchase"].map((h, i) => <th key={h} style={{ ...S.th, textAlign: i === 1 || i === 3 ? "right" : "left", position: "sticky", top: 0 }}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {clicks.map((c, i) => {
                  const who = c.name || c.email || "Anonymous";
                  let path = c.current_url;
                  try { const u = new URL(c.current_url); path = u.hostname.replace(/^www\./, "") + u.pathname; } catch { }
                  return (
                    <tr key={i} tabIndex={(c.email || c.contact_id) ? 0 : undefined} onClick={() => (c.email || c.contact_id) && onOpenContact(c.email || c.contact_id)} onKeyDown={e => { if ((c.email || c.contact_id) && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); onOpenContact(c.email || c.contact_id); } }} style={{ cursor: (c.email || c.contact_id) ? "pointer" : "default" }} title={c.email ? "Open full journey" : ""}>
                      <td style={{ ...S.td, textAlign: "left" }}>
                        <div style={{ fontWeight: 600, color: "var(--ds-gray-900)" }}>{who}</div>
                        {c.email && c.name && <div style={{ fontSize: 12, color: "var(--ds-gray-700)" }}>{c.email}</div>}
                      </td>
                      <td style={{ ...S.td, textAlign: "right", color: "var(--ds-gray-700)", whiteSpace: "nowrap" }}>{fmtWhen(c.click_ts)}</td>
                      <td style={{ ...S.td, textAlign: "left", color: "var(--ds-gray-700)", maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={c.current_url}>{path}</td>
                      <td style={{ ...S.td, textAlign: "right", whiteSpace: "nowrap" }}>
                        {c.purchased_at
                          ? <span style={{ color: "#047857", fontWeight: 600 }}>✓ {fmtGap(c.click_ts, c.purchased_at)}</span>
                          : <span style={{ color: "var(--ds-gray-600)" }}>—</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
        {clicks.length >= 500 && <div style={{ padding: "10px 24px", borderTop: "1px solid var(--ds-border)", fontSize: 12, color: "var(--ds-gray-600)" }}>Showing the most recent 500 clicks.</div>}
      </div>
    </div>
  );
}

function EntryForm({ initial, onSubmit, onCancel, isMobile }) {
  const today = getLADate();
  const defaults = initial
    ? { fb_spend: initial.fb_spend ?? 0, fb_link_clicks: initial.fb_link_clicks ?? 0, registrations: initial.registrations ?? 0, replays: initial.replays ?? 0, viewedcta: initial.viewedcta ?? 0, clickedcta: initial.clickedcta ?? 0, purchases_fb: initial.purchases_fb ?? 0, purchases_native: initial.purchases_native ?? 0, purchases_youtube: initial.purchases_youtube ?? 0, purchases_aibot: initial.purchases_aibot ?? 0, purchases_aibot_b: initial.purchases_aibot_b ?? 0, purchases_postwebinar: initial.purchases_postwebinar ?? 0, purchases_cpa: initial.purchases_cpa ?? 0, purchases_sales_a: initial.purchases_sales_a ?? 0, purchases_sales_b: initial.purchases_sales_b ?? 0, purchases_retargeting: initial.purchases_retargeting ?? 0, stayed_45: initial.stayed_45 ?? 0, stayed_60: initial.stayed_60 ?? 0, stayed_80: initial.stayed_80 ?? 0, attended: initial.attended ?? 0 }
    : { fb_spend: "", fb_link_clicks: "", registrations: "", replays: "", viewedcta: "", clickedcta: "", purchases_fb: "", purchases_native: "", purchases_youtube: "", purchases_aibot: "", purchases_aibot_b: "", purchases_postwebinar: "", purchases_cpa: "", purchases_sales_a: "", purchases_sales_b: "", purchases_retargeting: "", stayed_45: "", stayed_60: "", stayed_80: "", attended: "" };
  const [f, setF] = useState({ date: initial?.date || today, ...defaults });
  const set = (k, v) => setF(p => ({ ...p, [k]: v }));
  const go = () => onSubmit({ date: f.date, day: getLADay(f.date), fb_spend: parseFloat(f.fb_spend) || 0, fb_link_clicks: parseInt(f.fb_link_clicks) || 0, registrations: parseInt(f.registrations) || 0, replays: parseInt(f.replays) || 0, viewedcta: parseInt(f.viewedcta) || 0, clickedcta: parseInt(f.clickedcta) || 0, purchases_fb: parseInt(f.purchases_fb) || 0, purchases_native: parseInt(f.purchases_native) || 0, purchases_youtube: parseInt(f.purchases_youtube) || 0, purchases_aibot: parseInt(f.purchases_aibot) || 0, purchases_aibot_b: parseInt(f.purchases_aibot_b) || 0, purchases_postwebinar: parseInt(f.purchases_postwebinar) || 0, purchases_cpa: parseInt(f.purchases_cpa) || 0, purchases_sales_a: parseInt(f.purchases_sales_a) || 0, purchases_sales_b: parseInt(f.purchases_sales_b) || 0, purchases_retargeting: parseInt(f.purchases_retargeting) || 0, stayed_45: parseInt(f.stayed_45) || 0, stayed_60: parseInt(f.stayed_60) || 0, stayed_80: parseInt(f.stayed_80) || 0, attended: parseInt(f.attended) || 0 });
  const fieldGroups = [
    { title: "Traffic", description: "Spend, visits, and registrations", fields: [{ k: "fb_spend", l: "Facebook spend ($)", step: "0.01", ph: "0.00" }, { k: "fb_link_clicks", l: "Registration page visits", ph: "0" }, { k: "registrations", l: "Registrations", ph: "0" }] },
    { title: "Engagement", description: "Attendance and call-to-action behavior", fields: [{ k: "attended", l: "Attended", ph: "0" }, { k: "replays", l: "Replays", ph: "0" }, { k: "viewedcta", l: "Viewed CTA", ph: "0" }, { k: "clickedcta", l: "Clicked CTA", ph: "0" }] },
    { title: "Purchase sources", description: "Attribute purchases to their source", fields: [{ k: "purchases_fb", l: "Facebook", ph: "0" }, { k: "purchases_native", l: "Native ads", ph: "0" }, { k: "purchases_youtube", l: "YouTube / organic", ph: "0" }, { k: "purchases_aibot", l: "AI chatbot", ph: "0" }, { k: "purchases_aibot_b", l: "AI chatbot B", ph: "0" }, { k: "purchases_postwebinar", l: "Post-webinar", ph: "0" }, { k: "purchases_cpa", l: "CPA funnel", ph: "0" }, { k: "purchases_sales_a", l: "Sales A", ph: "0" }, { k: "purchases_sales_b", l: "Sales B", ph: "0" }, { k: "purchases_retargeting", l: "Retargeting", ph: "0" }] },
    { title: "Retention", description: "How long attendees stayed", fields: [{ k: "stayed_45", l: "Stayed 45 minutes", ph: "0" }, { k: "stayed_60", l: "Stayed 60 minutes", ph: "0" }, { k: "stayed_80", l: "Stayed 80 minutes", ph: "0" }] },
  ];
  return (
    <div className="fi form-container" style={S.fc}>
      <div style={S.fh}><div style={{ ...S.formBadge, background: initial ? "var(--ds-blue-100)" : "var(--ds-green-100)", color: initial ? "var(--ds-blue-700)" : "var(--ds-green-600)" }}>{initial ? "EDIT ENTRY" : "NEW ENTRY"}</div><h1 style={S.ft}>{initial ? `Update ${fmtDateNice(initial.date)}` : "Add daily metrics"}</h1><p style={S.fs}>Record the day’s traffic, engagement, purchases, and retention. Empty fields are saved as zero.</p></div>
      <div className="form-card" style={S.fcard}>
        <div style={{ marginBottom: 24 }}><label htmlFor="entry-date" style={S.fl}>Date <span style={{ color: "var(--ds-gray-600)", fontWeight: 400 }}>(MM/DD/YYYY)</span></label><input id="entry-date" style={S.inp} value={f.date} onChange={e => set("date", e.target.value)} placeholder="03/14/2026" disabled={!!initial} />{!initial && f.date && <div style={S.hint}>{getLADay(f.date)}</div>}</div>
        {fieldGroups.map(group => (
          <fieldset className="form-section" key={group.title}>
            <legend>{group.title}</legend>
            <p>{group.description}</p>
            <div className="form-section-grid">
              {group.fields.map(fi => (<div key={fi.k}><label htmlFor={`entry-${fi.k}`} style={S.fl}>{fi.l}</label><input id={`entry-${fi.k}`} style={{ ...S.inp, textAlign: "right" }} type="number" inputMode={fi.step ? "decimal" : "numeric"} min="0" step={fi.step || "1"} placeholder={fi.ph} value={f[fi.k]} onChange={e => set(fi.k, e.target.value)} /></div>))}
            </div>
          </fieldset>
        ))}
        <div style={S.fa}><button type="button" style={S.btnLight} onClick={onCancel}>Cancel</button><button type="button" style={S.btnDark} onClick={go}>{initial ? "Update entry" : "Add entry"}</button></div>
      </div>
    </div>
  );
}

function CMForm({ initial, onSubmit, onCancel, metrics, customs = [] }) {
  const [f, setF] = useState({ id: initial?.id || "", name: initial?.name || "", formula: initial?.formula || "", format: initial?.format || "number" });
  // Build context from other custom metrics for preview
  const otherCustoms = customs.filter(cm => cm.name !== f.name);
  const previewCtx = metrics.length > 0 ? evalAllCustoms(otherCustoms, metrics[0]) : {};
  const preview = f.formula && metrics.length > 0 ? evalFormula(f.formula, metrics[0], previewCtx) : null;
  const allVars = [...MK, ...otherCustoms.map(cm => cm.name)];
  return (
    <div className="fi" style={S.fc}>
      <div style={S.fh}><h1 style={S.ft}>{initial ? `Edit “${initial.name}”` : "Create custom metric"}</h1><p style={S.fs}>Build a reusable metric from your existing funnel data.</p></div>
      <div style={S.fcard}>
        <div style={{ marginBottom: 20 }}><label htmlFor="metric-name" style={S.fl}>Metric name</label><input id="metric-name" style={S.inp} value={f.name} onChange={e => setF(p => ({ ...p, name: e.target.value }))} placeholder="e.g. CTA click rate" /></div>
        <div style={{ marginBottom: 20 }}><label htmlFor="metric-formula" style={S.fl}>Formula</label><input id="metric-formula" spellCheck="false" style={{ ...S.inp, fontFamily: mono }} value={f.formula} onChange={e => setF(p => ({ ...p, formula: e.target.value }))} placeholder="e.g. clickedcta / viewedcta * 100" /><div style={S.hint}>Available variables: {allVars.join(", ")}</div></div>
        <div style={{ marginBottom: 20 }}><span style={S.fl}>Display format</span><div role="group" aria-label="Display format" style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>{[{ v: "number", l: "Number" }, { v: "percent", l: "Percent (%)" }, { v: "currency", l: "Currency ($)" }].map(o => (<button type="button" aria-pressed={f.format === o.v} key={o.v} style={f.format === o.v ? S.fmtA : S.fmtB} onClick={() => setF(p => ({ ...p, format: o.v }))}>{o.l}</button>))}</div></div>
        {metrics.length > 0 && f.formula && (<div style={S.prev}><div style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase", color: "var(--ds-gray-700)", marginBottom: 8 }}>Preview ({fmtDateNice(metrics[0].date)})</div><div style={{ fontSize: 28, fontWeight: 600, color: preview !== null ? "var(--ds-gray-900)" : "var(--ds-red-600)" }}>{fmtVal(preview, f.format)}</div></div>)}
        <div style={S.fa}><button type="button" style={S.btnGhost} onClick={onCancel}>Cancel</button><button type="button" style={{ ...S.btnDark, opacity: f.name && f.formula ? 1 : 0.4 }} onClick={() => f.name && f.formula && onSubmit(f)} disabled={!f.name || !f.formula}>{initial ? "Update metric" : "Create metric"}</button></div>
      </div>
    </div>
  );
}

// Chat data-viz primitives. Specs follow the dataviz method: categorical hues
// in a FIXED validated order (never cycled past 8, CVD-checked on white),
// single hue when one series carries magnitude, thin marks with rounded
// data-ends, hairline grid, legend only for ≥2 series, tooltips on every mark,
// values in ink (never the series color).
const CHART_COLORS = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948'];
const VIZ = { grid: 'var(--ds-border)', axis: 'var(--ds-gray-400)', muted: 'var(--ds-gray-600)', ink: 'var(--ds-gray-900)', sub: 'var(--ds-gray-700)', surface: 'var(--ds-background-100)', good: 'var(--ds-green-600)', bad: 'var(--ds-red-600)' };

function niceCeil(n) {
  if (n <= 0) return 1;
  const exp = Math.pow(10, Math.floor(Math.log10(n)));
  const norm = n / exp;
  let nice;
  if (norm <= 1) nice = 1;
  else if (norm <= 2) nice = 2;
  else if (norm <= 5) nice = 5;
  else nice = 10;
  return nice * exp;
}
function fmtChartNum(v) {
  if (!Number.isFinite(v)) return '';
  const abs = Math.abs(v);
  if (abs >= 1e6) return (v / 1e6).toFixed(abs >= 1e7 ? 0 : 1) + 'M';
  if (abs >= 1e3) return (v / 1e3).toFixed(abs >= 1e4 ? 0 : 1) + 'k';
  if (Number.isInteger(v)) return String(v);
  if (abs >= 10) return v.toFixed(0);
  if (abs >= 1) return v.toFixed(1);
  if (abs > 0) return v.toFixed(2);
  return '0';
}

const fmtTipNum = (v) => Number.isFinite(v) ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—';

// One fixed-position tooltip shared by all marks of a chart.
function useVizTip() {
  const [tip, setTip] = useState(null); // { x, y, title, lines:[{swatch,text}] }
  const show = (e, title, lines) => setTip({ x: e.clientX, y: e.clientY, title, lines });
  const move = (e) => setTip(t => (t ? { ...t, x: e.clientX, y: e.clientY } : t));
  const hide = () => setTip(null);
  const node = tip ? (
    <div style={{ position: 'fixed', left: Math.min(tip.x + 12, (typeof window !== 'undefined' ? window.innerWidth : 1000) - 250), top: tip.y + 14, zIndex: 1000, background: 'var(--ds-gray-900)', color: '#fff', borderRadius: 8, padding: '8px 11px', fontSize: 12, lineHeight: 1.55, pointerEvents: 'none', boxShadow: 'var(--ds-shadow-menu)', maxWidth: 240 }}>
      {tip.title && <div style={{ fontWeight: 600 }}>{tip.title}</div>}
      {(tip.lines || []).map((l, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {l.swatch && <span style={{ width: 8, height: 8, borderRadius: 2, background: l.swatch, display: 'inline-block', flexShrink: 0 }} />}
          <span>{l.text}</span>
        </div>
      ))}
    </div>
  ) : null;
  return { show, move, hide, node };
}

const CHART_WRAP = { margin: "12px 0", padding: 16, border: "1px solid var(--ds-border)", borderRadius: 8, background: "var(--ds-background-100)" };
const CHART_TITLE = { fontSize: 13, fontWeight: 600, color: VIZ.ink, marginBottom: 8 };
const CHART_LEGEND = { display: "flex", flexWrap: "wrap", gap: 14, marginTop: 8, justifyContent: "center" };
const CHART_LEGEND_ITEM = { display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: VIZ.sub };

// Rounded data-end, square baseline (4px cap; plain rect when too small).
function barPath(x, y, w, h, up) {
  const r = Math.min(4, w / 2, h);
  if (h <= 0 || r <= 0) return `M${x},${y} h${w} v${h} h${-w} Z`;
  if (up) return `M${x},${y + h} v${-(h - r)} q0,${-r} ${r},${-r} h${w - 2 * r} q${r},0 ${r},${r} v${h - r} Z`;
  return `M${x},${y} v${h - r} q0,${r} ${r},${r} h${w - 2 * r} q${r},0 ${r},${-r} v${-(h - r)} Z`;
}

// Part-to-whole donut: slices in fixed categorical order (sorted by size,
// tail folded into "Other"), 2px surface gaps, total in the center, legend
// carries label + value + share (identity is never color-alone).
function DonutChart({ spec, tip }) {
  let rows = (spec.data || [])
    .map(d => ({ label: String(d.label ?? d[spec.x] ?? '?'), value: Number(d.value ?? d.y) }))
    .filter(r => Number.isFinite(r.value) && r.value > 0);
  if (!rows.length) return <div style={{ padding: 12, color: VIZ.muted, fontSize: 13 }}>(empty chart)</div>;
  rows.sort((a, b) => b.value - a.value);
  if (rows.length > 6) {
    const rest = rows.slice(5).reduce((s, r) => s + r.value, 0);
    rows = [...rows.slice(0, 5), { label: 'Other', value: rest }];
  }
  const total = rows.reduce((s, r) => s + r.value, 0);
  const H = 210, cx = 105, cy = 105, R = 88, ir = 56;
  let a = -Math.PI / 2;
  const slices = rows.map((r, i) => {
    const a0 = a; a += (r.value / total) * 2 * Math.PI;
    return { ...r, a0, a1: a, pct: Math.round(r.value / total * 1000) / 10, color: CHART_COLORS[i % 8] };
  });
  const pt = (ang, rad) => [cx + rad * Math.cos(ang), cy + rad * Math.sin(ang)];
  const arc = (s) => {
    if (s.a1 - s.a0 >= 2 * Math.PI - 0.001) return null; // single full slice → ring
    const large = s.a1 - s.a0 > Math.PI ? 1 : 0;
    const [x0, y0] = pt(s.a0, R), [x1, y1] = pt(s.a1, R), [x2, y2] = pt(s.a1, ir), [x3, y3] = pt(s.a0, ir);
    return `M${x0},${y0} A${R},${R} 0 ${large} 1 ${x1},${y1} L${x2},${y2} A${ir},${ir} 0 ${large} 0 ${x3},${y3} Z`;
  };
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 20, flexWrap: 'wrap' }}>
      <svg viewBox={`0 0 210 ${H}`} style={{ width: 190, maxWidth: '45%', height: 'auto', flexShrink: 0 }}>
        {slices.map((s, i) => {
          const d = arc(s);
          const handlers = {
            onMouseEnter: (e) => tip.show(e, s.label, [{ swatch: s.color, text: `${fmtTipNum(s.value)} · ${s.pct}%` }]),
            onMouseMove: tip.move, onMouseLeave: tip.hide,
          };
          if (!d) return <circle key={i} cx={cx} cy={cy} r={(R + ir) / 2} fill="none" stroke={s.color} strokeWidth={R - ir} {...handlers} />;
          return <path key={i} d={d} fill={s.color} stroke={VIZ.surface} strokeWidth={2} {...handlers} />;
        })}
        <text x={cx} y={cy - 2} textAnchor="middle" fontSize={22} fontWeight={600} fill={VIZ.ink}>{fmtChartNum(total)}</text>
        <text x={cx} y={cy + 16} textAnchor="middle" fontSize={11} fill={VIZ.muted}>{spec.center_label || 'total'}</text>
      </svg>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6, minWidth: 150, flex: 1 }}>
        {slices.map((s, i) => (
          <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13 }}>
            <span style={{ width: 10, height: 10, borderRadius: 3, background: s.color, flexShrink: 0 }} />
            <span style={{ color: VIZ.sub, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{s.label}</span>
            <span style={{ color: VIZ.ink, fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{fmtTipNum(s.value)}</span>
            <span style={{ color: VIZ.muted, fontVariantNumeric: 'tabular-nums', minWidth: 44, textAlign: 'right' }}>{s.pct}%</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// Ranked categories (top states, campaigns by spend): horizontal bars, ONE hue
// (magnitude is the job), value in ink at each bar end, labels never clipped.
function HBarChart({ spec, tip }) {
  const xKey = spec.x || 'label';
  const sKey = (spec.series && spec.series[0] && spec.series[0].key) || 'value';
  const rows = (spec.data || [])
    .map(d => ({ label: String(d[xKey] ?? d.label ?? '?'), value: Number(d[sKey] ?? d.value) }))
    .filter(r => Number.isFinite(r.value))
    .slice(0, 20);
  if (!rows.length) return <div style={{ padding: 12, color: VIZ.muted, fontSize: 13 }}>(empty chart)</div>;
  const max = Math.max(...rows.map(r => r.value), 0) || 1;
  const W = 640, rowH = 30, barH = 18, LAB = 132, VAL = 64;
  const H = rows.length * rowH + 4;
  const innerW = W - LAB - VAL - 16;
  const color = (spec.series && spec.series[0] && spec.series[0].color) || CHART_COLORS[0];
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height: 'auto', display: 'block' }}>
      {rows.map((r, i) => {
        const y = i * rowH + 2;
        const w = Math.max((r.value / max) * innerW, 1.5);
        const handlers = {
          onMouseEnter: (e) => tip.show(e, r.label, [{ swatch: color, text: fmtTipNum(r.value) }]),
          onMouseMove: tip.move, onMouseLeave: tip.hide,
        };
        return (
          <g key={i}>
            <text x={LAB - 8} y={y + barH / 2 + 4} textAnchor="end" fontSize={12} fill={VIZ.sub}>
              {r.label.length > 18 ? r.label.slice(0, 17) + '…' : r.label}
            </text>
            <path d={`M${LAB},${y} h${w - Math.min(4, w / 2)} q${Math.min(4, w / 2)},0 ${Math.min(4, w / 2)},${Math.min(4, barH / 2)} v${barH - 2 * Math.min(4, barH / 2)} q0,${Math.min(4, barH / 2)} ${-Math.min(4, w / 2)},${Math.min(4, barH / 2)} h${-(w - Math.min(4, w / 2))} Z`} fill={color} {...handlers} />
            <text x={LAB + w + 8} y={y + barH / 2 + 4} fontSize={12} fontWeight={600} fill={VIZ.ink} fontVariantNumeric="tabular-nums">{fmtChartNum(r.value)}</text>
          </g>
        );
      })}
    </svg>
  );
}

// XY chart (line / area / bar over an ordered x). Single series wears one hue
// and needs no legend; grouped series follow the fixed categorical order.
function XYChart({ spec, tip }) {
  const type = spec.type === 'bar' ? 'bar' : (spec.type === 'area' ? 'area' : 'line');
  const xKey = spec.x || 'x';
  const series = (Array.isArray(spec.series) ? spec.series : []).filter(s => s && s.key).slice(0, 8);
  if (series.length === 0) return <div style={{ padding: 12, color: VIZ.muted, fontSize: 13 }}>(chart has no series)</div>;
  const colorFor = (s, si) => s.color || CHART_COLORS[si % 8];

  const W = 640, H = 280, PAD_L = 48, PAD_R = 16, PAD_T = 12, PAD_B = 44;
  const innerW = W - PAD_L - PAD_R;
  const innerH = H - PAD_T - PAD_B;

  let yMin = 0, yMax = 0;
  spec.data.forEach(d => series.forEach(s => {
    const v = Number(d[s.key]);
    if (Number.isFinite(v)) { if (v < yMin) yMin = v; if (v > yMax) yMax = v; }
  }));
  if (yMax === yMin) yMax = yMin + 1;
  const niceMax = niceCeil(yMax * 1.1);
  const niceMin = yMin >= 0 ? 0 : -niceCeil(-yMin * 1.1);
  const range = niceMax - niceMin || 1;
  const clipId = `chartclip-${Math.random().toString(36).slice(2, 9)}`;

  const yScale = (v) => PAD_T + innerH - ((v - niceMin) / range) * innerH;
  const xCount = spec.data.length;
  const xScale = (i) => PAD_L + (xCount === 1 ? innerW / 2 : (i / (xCount - 1)) * innerW);
  const groupW = innerW / Math.max(xCount, 1);
  const barW = type === 'bar' ? Math.min(24, Math.max(2, (groupW * 0.8) / series.length)) : 0;

  const yTicks = [];
  for (let t = 0; t <= 5; t++) {
    const v = niceMin + range * (t / 5);
    yTicks.push({ v, y: yScale(v) });
  }
  const xLabelEvery = Math.max(1, Math.ceil(xCount / 8));
  const zeroY = yScale(0);
  const colW = innerW / Math.max(xCount, 1);

  const tipForIndex = (e, i) => {
    const d = spec.data[i];
    tip.show(e, String(d[xKey] ?? ''), series.map((s, si) => ({ swatch: colorFor(s, si), text: `${s.label || s.key}: ${fmtTipNum(Number(d[s.key]))}` })));
  };
  const showBarLabels = type === 'bar' && series.length === 1 && xCount <= 8;

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto", display: "block" }}>
        <defs><clipPath id={clipId}><rect x={PAD_L} y={PAD_T - 6} width={innerW} height={innerH + 6} /></clipPath></defs>
        {yTicks.map((t, i) => (
          <g key={`t${i}`}>
            <line x1={PAD_L} y1={t.y} x2={W - PAD_R} y2={t.y} stroke={VIZ.grid} strokeWidth={1} />
            <text x={PAD_L - 6} y={t.y + 3} fontSize={10} fill={VIZ.muted} textAnchor="end">{fmtChartNum(t.v)}</text>
          </g>
        ))}
        <line x1={PAD_L} y1={H - PAD_B} x2={W - PAD_R} y2={H - PAD_B} stroke={VIZ.axis} />
        {spec.data.map((d, i) => (i % xLabelEvery === 0 || i === xCount - 1) && (
          <text key={`x${i}`} x={xScale(i)} y={H - PAD_B + 14} fontSize={10} fill={VIZ.muted} textAnchor="middle">
            {(() => { const s = String(d[xKey] ?? ''); return s.length > 10 ? s.slice(0, 10) : s; })()}
          </text>
        ))}
        <g clipPath={`url(#${clipId})`}>
          {series.map((s, si) => {
            const color = colorFor(s, si);
            if (type === 'bar') {
              return spec.data.map((d, i) => {
                const v = Number(d[s.key]);
                if (!Number.isFinite(v)) return null;
                const x = xScale(i) - (barW * series.length) / 2 + si * barW;
                const y = yScale(v);
                const h = Math.max(Math.abs(zeroY - y), 1);
                return (
                  <path key={`${si}-${i}`} d={barPath(x, Math.min(y, zeroY), barW * 0.86, h, v >= 0)} fill={color}
                    onMouseEnter={(e) => tipForIndex(e, i)} onMouseMove={tip.move} onMouseLeave={tip.hide} />
                );
              });
            }
            const pts = spec.data
              .map((d, i) => {
                const v = Number(d[s.key]);
                return Number.isFinite(v) ? { x: xScale(i), y: yScale(v), i } : null;
              })
              .filter(Boolean);
            if (pts.length === 0) return null;
            const polyPts = pts.map(p => `${p.x},${p.y}`).join(' ');
            return (
              <g key={`s${si}`}>
                {type === 'area' && pts.length >= 2 && (
                  <path d={`M${pts[0].x},${zeroY} L${pts.map(p => `${p.x},${p.y}`).join(' L')} L${pts[pts.length - 1].x},${zeroY} Z`} fill={color} opacity={0.10} />
                )}
                <polyline points={polyPts} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
                {pts.length <= 60 && pts.map((p, i) => (
                  <circle key={i} cx={p.x} cy={p.y} r={4} fill={color} stroke={VIZ.surface} strokeWidth={2} />
                ))}
              </g>
            );
          })}
        </g>
        {showBarLabels && spec.data.map((d, i) => {
          const v = Number(d[series[0].key]);
          if (!Number.isFinite(v)) return null;
          return <text key={`bl${i}`} x={xScale(i)} y={yScale(v) - 6} fontSize={11} fontWeight={600} fill={VIZ.sub} textAnchor="middle">{fmtChartNum(v)}</text>;
        })}
        {/* full-height hover columns: one tooltip per x showing every series */}
        {spec.data.map((d, i) => (
          <rect key={`hov${i}`} x={xScale(i) - colW / 2} y={PAD_T} width={colW} height={innerH} fill="transparent"
            onMouseEnter={(e) => tipForIndex(e, i)} onMouseMove={tip.move} onMouseLeave={tip.hide} />
        ))}
      </svg>
      {series.length > 1 && (
        <div style={CHART_LEGEND}>
          {series.map((s, si) => (
            <span key={si} style={CHART_LEGEND_ITEM}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: colorFor(s, si), display: "inline-block" }} />
              <span>{s.label || s.key}</span>
            </span>
          ))}
        </div>
      )}
    </>
  );
}

function MdChart({ spec }) {
  const tip = useVizTip();
  if (!spec || !Array.isArray(spec.data) || spec.data.length === 0) {
    return <div style={{ padding: 12, color: VIZ.muted, fontSize: 13 }}>(empty chart)</div>;
  }
  const body = spec.type === 'donut' || spec.type === 'pie'
    ? <DonutChart spec={spec} tip={tip} />
    : spec.type === 'hbar'
      ? <HBarChart spec={spec} tip={tip} />
      : <XYChart spec={spec} tip={tip} />;
  return (
    <div style={CHART_WRAP}>
      {spec.title && <div style={CHART_TITLE}>{spec.title}</div>}
      {body}
      {tip.node}
    </div>
  );
}

// KPI row of stat tiles: label · value · optional delta (signed; green when
// good) · optional note. The block form for headline numbers — never a table.
function MdStats({ spec }) {
  const tiles = Array.isArray(spec && spec.tiles) ? spec.tiles.filter(t => t && t.label !== undefined && t.value !== undefined).slice(0, 8) : [];
  if (!tiles.length) return <div style={{ padding: 12, color: VIZ.muted, fontSize: 13 }}>(empty stats)</div>;
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(132px, 1fr))', gap: 10, margin: '12px 0' }}>
      {tiles.map((t, i) => {
        const deltaStr = t.delta !== undefined && t.delta !== null ? String(t.delta) : null;
        const neg = deltaStr ? /^\s*[-−↓]/.test(deltaStr) : false;
        const good = t.delta_good !== undefined ? !!t.delta_good : !neg;
        return (
          <div key={i} style={{ border: '1px solid var(--ds-border)', borderRadius: 12, padding: '12px 14px', background: '#fff', minWidth: 0 }}>
            <div style={{ fontSize: 12, color: VIZ.sub, marginBottom: 4, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{String(t.label)}</div>
            <div style={{ fontSize: 25, fontWeight: 600, color: VIZ.ink, letterSpacing: '-0.02em', lineHeight: 1.15, wordBreak: 'break-word' }}>{String(t.value)}</div>
            {deltaStr && <div style={{ fontSize: 12, fontWeight: 600, marginTop: 4, color: good ? VIZ.good : VIZ.bad }}>{deltaStr}</div>}
            {t.note !== undefined && t.note !== null && <div style={{ fontSize: 11, color: VIZ.muted, marginTop: 4 }}>{String(t.note)}</div>}
          </div>
        );
      })}
    </div>
  );
}

const MD = {
  h1: { fontSize: 20, fontWeight: 650, color: "var(--ds-gray-900)", margin: "18px 0 8px", letterSpacing: "-0.025em", lineHeight: 1.3 },
  h2: { fontSize: 17, fontWeight: 650, color: "var(--ds-gray-900)", margin: "16px 0 6px", letterSpacing: "-0.015em", lineHeight: 1.3 },
  h3: { fontSize: 15, fontWeight: 650, color: "var(--ds-gray-900)", margin: "14px 0 4px", lineHeight: 1.3 },
  h4: { fontSize: 14, fontWeight: 600, color: "var(--ds-gray-900)", margin: "12px 0 4px", lineHeight: 1.3 },
  p: { margin: "6px 0", lineHeight: 1.6 },
  ul: { margin: "6px 0 6px 4px", padding: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 2 },
  ol: { margin: "6px 0 6px 4px", padding: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 2 },
  liRow: { display: "flex", gap: 8, lineHeight: 1.6, alignItems: "flex-start" },
  bullet: { color: "var(--ds-gray-600)", flexShrink: 0, lineHeight: 1.6 },
  num: { color: "var(--ds-gray-700)", fontWeight: 600, fontVariantNumeric: "tabular-nums", flexShrink: 0, minWidth: 18, lineHeight: 1.6 },
  code: { background: "var(--ds-gray-100)", padding: "1px 6px", borderRadius: 4, fontSize: "0.88em", fontFamily: mono, color: "var(--ds-gray-900)", border: "1px solid var(--ds-border)" },
  preWrap: { margin: "10px 0", borderRadius: 8, overflow: "hidden", border: "1px solid var(--ds-gray-800)" },
  preLang: { background: "#292929", color: "#a1a1a1", padding: "6px 14px", fontSize: 11, fontWeight: 600, fontFamily: mono, textTransform: "lowercase", letterSpacing: "0.04em" },
  pre: { background: "#1f1f1f", color: "#ededed", padding: "12px 16px", overflow: "auto", fontSize: 12.5, lineHeight: 1.55, fontFamily: mono, margin: 0, whiteSpace: "pre" },
  blockquote: { borderLeft: "3px solid var(--ds-border-hover)", padding: "4px 0 4px 14px", margin: "10px 0", color: "var(--ds-gray-700)", fontStyle: "italic" },
  hr: { border: "none", borderTop: "1px solid var(--ds-border)", margin: "16px 0" },
  link: { color: "var(--ds-blue-700)", textDecoration: "underline", textUnderlineOffset: 2 },
  strong: { fontWeight: 650, color: "var(--ds-gray-900)" },
  em: { fontStyle: "italic" },
  strike: { textDecoration: "line-through", color: "var(--ds-gray-600)" },
  // Wrap is a horizontal-scroll container. Tables size to their content
  // (max-content) but stretch to at least the column width — so narrow tables
  // fill the space and wide tables get a horizontal scrollbar instead of
  // crushing their cells.
  tableWrap: { margin: "12px 0", border: "1px solid var(--ds-border)", borderRadius: 8, overflowX: "auto", maxWidth: "100%" },
  table: { borderCollapse: "collapse", minWidth: "100%", width: "max-content", fontSize: 13, background: "var(--ds-background-100)" },
  th: { background: "var(--ds-background-200)", padding: "10px 14px", textAlign: "left", fontWeight: 600, color: "var(--ds-gray-800)", borderBottom: "1px solid var(--ds-border)", fontSize: 12 },
  td: { padding: "10px 14px", borderBottom: "1px solid var(--ds-border)", color: "var(--ds-gray-800)", fontVariantNumeric: "tabular-nums" },
  trEven: { background: "var(--ds-background-200)" },
};

const splitTableRow = (line) => {
  const source = line.trim();
  const cells = [];
  let current = "";
  let inCode = false;

  for (let index = 0; index < source.length; index++) {
    const char = source[index];
    if (char === "`") {
      inCode = !inCode;
      current += char;
      continue;
    }
    if (char === "|" && !inCode) {
      let precedingSlashes = 0;
      for (let cursor = current.length - 1; cursor >= 0 && current[cursor] === "\\"; cursor--) precedingSlashes++;
      if (precedingSlashes % 2 === 1) {
        current = `${current.slice(0, -1)}|`;
      } else {
        cells.push(current.trim());
        current = "";
      }
      continue;
    }
    current += char;
  }
  cells.push(current.trim());
  if (source.startsWith("|") && cells[0] === "") cells.shift();
  if (source.endsWith("|") && cells[cells.length - 1] === "") cells.pop();
  return cells;
};

const normalizeTableCells = (cells, columnCount) => {
  if (cells.length === columnCount) return cells;
  if (cells.length < columnCount) return [...cells, ...Array(columnCount - cells.length).fill("")];
  // If an AI response forgot to escape pipes inside a descriptive first cell
  // (campaign names are the common case), merge the overflow back into it.
  const firstCellParts = cells.length - columnCount + 1;
  return [cells.slice(0, firstCellParts).join(" | "), ...cells.slice(firstCellParts)];
};
const isTableSeparator = (line) => {
  if (!line || !line.includes('|')) return false;
  const cells = splitTableRow(line);
  return cells.length > 0 && cells.every(c => /^:?-+:?$/.test(c));
};
const isBlockLine = (line) => (
  line.startsWith('```') ||
  /^#{1,4}\s/.test(line) ||
  /^---+\s*$/.test(line) || /^\*\*\*+\s*$/.test(line) ||
  line.startsWith('>') ||
  /^\s*[-*]\s+/.test(line) ||
  /^\s*\d+\.\s+/.test(line)
);

function renderInline(text, baseKey = 0) {
  if (!text) return null;
  const regex = /(`[^`]+`)|(\*\*[^*\n]+\*\*)|(\*[^*\n]+\*)|(_[^_\n]+_)|(~~[^~\n]+~~)|(\[[^\]]+\]\([^)\s]+\))/g;
  const out = [];
  let last = 0;
  let m;
  let k = baseKey;
  while ((m = regex.exec(text)) !== null) {
    if (m.index > last) out.push(<span key={k++}>{text.slice(last, m.index)}</span>);
    const s = m[0];
    if (s.startsWith('`')) {
      out.push(<code key={k++} style={MD.code}>{s.slice(1, -1)}</code>);
    } else if (s.startsWith('**')) {
      out.push(<strong key={k++} style={MD.strong}>{s.slice(2, -2)}</strong>);
    } else if (s.startsWith('~~')) {
      out.push(<span key={k++} style={MD.strike}>{s.slice(2, -2)}</span>);
    } else if (s.startsWith('[')) {
      const lm = s.match(/^\[([^\]]+)\]\(([^)\s]+)\)$/);
      if (lm) out.push(<a key={k++} href={lm[2]} target="_blank" rel="noopener noreferrer" style={MD.link}>{lm[1]}</a>);
      else out.push(<span key={k++}>{s}</span>);
    } else if (s.startsWith('*') || s.startsWith('_')) {
      out.push(<em key={k++} style={MD.em}>{s.slice(1, -1)}</em>);
    }
    last = m.index + s.length;
  }
  if (last < text.length) out.push(<span key={k++}>{text.slice(last)}</span>);
  return out.length > 0 ? out : text;
}

function renderMd(text) {
  if (!text) return text;
  const lines = text.split('\n');
  const out = [];
  let i = 0;
  let key = 0;
  while (i < lines.length) {
    const line = lines[i];

    // Fenced code block
    if (line.startsWith('```')) {
      const lang = line.slice(3).trim();
      const code = [];
      i++;
      while (i < lines.length && !lines[i].startsWith('```')) { code.push(lines[i]); i++; }
      i++;
      // Special-case ```chart / ```stats — parse JSON and render a visual
      // block. Fall back to a code block if the JSON is malformed.
      if (lang === 'chart' || lang === 'stats') {
        try {
          const spec = JSON.parse(code.join('\n'));
          out.push(lang === 'stats' ? <MdStats key={key++} spec={spec} /> : <MdChart key={key++} spec={spec} />);
          continue;
        } catch (e) {
          out.push(
            <div key={key++} style={MD.preWrap}>
              <div style={MD.preLang}>{lang} (invalid JSON)</div>
              <pre style={MD.pre}>{code.join('\n')}</pre>
            </div>
          );
          continue;
        }
      }
      out.push(
        <div key={key++} style={MD.preWrap}>
          {lang && <div style={MD.preLang}>{lang}</div>}
          <pre style={MD.pre}>{code.join('\n')}</pre>
        </div>
      );
      continue;
    }

    // Heading
    const h = line.match(/^(#{1,4})\s+(.*)$/);
    if (h) {
      const lvl = h[1].length;
      const inline = renderInline(h[2], 0);
      const k = key++;
      if (lvl === 1) out.push(<h1 key={k} style={MD.h1}>{inline}</h1>);
      else if (lvl === 2) out.push(<h2 key={k} style={MD.h2}>{inline}</h2>);
      else if (lvl === 3) out.push(<h3 key={k} style={MD.h3}>{inline}</h3>);
      else out.push(<h4 key={k} style={MD.h4}>{inline}</h4>);
      i++; continue;
    }

    // Horizontal rule
    if (/^---+\s*$/.test(line) || /^\*\*\*+\s*$/.test(line)) {
      out.push(<hr key={key++} style={MD.hr} />);
      i++; continue;
    }

    // Table: header | --- | rows
    if (line.includes('|') && i + 1 < lines.length && isTableSeparator(lines[i+1])) {
      const header = splitTableRow(line);
      const separator = normalizeTableCells(splitTableRow(lines[i + 1]), header.length);
      const usesBoundaryPipes = line.trim().startsWith("|") && line.trim().endsWith("|");
      i += 2;
      const rows = [];
      while (i < lines.length && lines[i].includes('|') && lines[i].trim() !== '') {
        const rowLine = lines[i].trim();
        if (isBlockLine(rowLine)) break;
        if (usesBoundaryPipes && !(rowLine.startsWith("|") && rowLine.endsWith("|"))) break;
        rows.push(normalizeTableCells(splitTableRow(lines[i]), header.length)); i++;
      }
      const numericColumns = header.map((_, columnIndex) => {
        const values = rows.map(row => (row[columnIndex] || "").trim()).filter(Boolean);
        return columnIndex > 0 && values.length > 0 && values.every(value => /^[-+]?[$£€]?\s*\d[\d,]*(?:\.\d+)?(?:%|\/[A-Za-z]+)?$/.test(value));
      });
      const alignments = header.map((_, columnIndex) => {
        const marker = separator[columnIndex] || "";
        if (/^:.*:$/.test(marker)) return "center";
        if (/:$/.test(marker) || numericColumns[columnIndex]) return "right";
        return "left";
      });
      out.push(
        <div key={key++} className={`md-table-wrap${header.length >= 5 ? " is-wide" : ""}`} style={MD.tableWrap} role="region" aria-label={`${header[0] || "Data"} table`} tabIndex={0}>
          <table className="md-table" style={MD.table}>
            <thead>
              <tr>{header.map((c, j) => <th key={j} className={`${j === 0 ? "is-primary " : ""}${numericColumns[j] ? "is-numeric" : ""}`} style={{ ...MD.th, textAlign: alignments[j] }}>{renderInline(c, 0)}</th>)}</tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => (
                <tr key={ri} style={ri % 2 === 1 ? MD.trEven : undefined}>
                  {r.map((c, ci) => <td key={ci} data-label={header[ci]} className={`${ci === 0 ? "is-primary " : ""}${numericColumns[ci] ? "is-numeric" : ""}`} style={{ ...MD.td, textAlign: alignments[ci] }}>{renderInline(c, 0)}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
      continue;
    }

    // Blockquote
    if (line.startsWith('>')) {
      const q = [];
      while (i < lines.length && lines[i].startsWith('>')) {
        q.push(lines[i].replace(/^>\s?/, '')); i++;
      }
      out.push(<blockquote key={key++} style={MD.blockquote}>{renderInline(q.join(' '), 0)}</blockquote>);
      continue;
    }

    // Unordered list
    if (/^\s*[-*]\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*[-*]\s+/.test(lines[i])) {
        items.push(lines[i].replace(/^\s*[-*]\s+/, '')); i++;
      }
      out.push(
        <ul key={key++} style={MD.ul}>
          {items.map((it, j) => (
            <li key={j} style={MD.liRow}><span style={MD.bullet}>•</span><span>{renderInline(it, 0)}</span></li>
          ))}
        </ul>
      );
      continue;
    }

    // Ordered list
    if (/^\s*\d+\.\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*\d+\.\s+/.test(lines[i])) {
        items.push(lines[i].replace(/^\s*\d+\.\s+/, '')); i++;
      }
      out.push(
        <ol key={key++} style={MD.ol}>
          {items.map((it, j) => (
            <li key={j} style={MD.liRow}><span style={MD.num}>{j + 1}.</span><span>{renderInline(it, 0)}</span></li>
          ))}
        </ol>
      );
      continue;
    }

    // Blank line
    if (line.trim() === '') { i++; continue; }

    // Paragraph (gather consecutive non-special lines)
    const para = [];
    while (i < lines.length && lines[i].trim() !== '' && !isBlockLine(lines[i])) {
      para.push(lines[i]); i++;
    }
    out.push(<p key={key++} style={MD.p}>{renderInline(para.join(' '), 0)}</p>);
  }
  return out;
}

function QueryBuilder({ flash }) {
  const [table, setTable] = useState("events");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [eventType, setEventType] = useState("");
  const [variant, setVariant] = useState("");
  const [search, setSearch] = useState("");
  const [limit, setLimit] = useState("500");
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [sortBy, setSortBy] = useState("");
  const [sortDir, setSortDir] = useState("desc");

  const runQuery = async (overrideSort, overrideDir) => {
    setLoading(true);
    try {
      const headers = await getAuthHeaders();
      const body = { table, dateFrom: dateFrom || undefined, dateTo: dateTo || undefined, eventType: eventType || undefined, variant: variant || undefined, search: search || undefined, sortBy: overrideSort || sortBy || undefined, sortDir: overrideDir || sortDir, limit: Number(limit) || 500 };
      const res = await fetch(`${API_BASE}/api/admin/query`, { method: "POST", headers, body: JSON.stringify(body) });
      if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Query failed: ${res.status}`); }
      const data = await res.json();
      setResults(data);
    } catch (e) { flash(e.message, "err"); }
    finally { setLoading(false); }
  };

  const handleSort = (col) => {
    const newDir = sortBy === col && sortDir === "desc" ? "asc" : "desc";
    setSortBy(col);
    setSortDir(newDir);
    runQuery(col, newDir);
  };

  const downloadCSV = () => {
    if (!results?.data?.length) return;
    const cols = Object.keys(results.data[0]);
    const csv = [cols.join(","), ...results.data.map(row => cols.map(c => { const v = row[c]; if (v === null || v === undefined) return ""; const s = typeof v === "object" ? JSON.stringify(v) : String(v); return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g, '""')}"` : s; }).join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = `${table}_export_${new Date().toISOString().slice(0, 10)}.csv`; a.click();
    URL.revokeObjectURL(url);
  };

  const eventTypes = ["registrations", "attended", "replays", "viewedcta", "clickedcta", "purchases"];
  const QS = {
    wrap: { width: "100%" },
    header: { marginBottom: 24 },
    badge: { display: "inline-block", padding: "3px 8px", borderRadius: 5, fontSize: 11, fontWeight: 600, letterSpacing: "0.02em", background: "var(--ds-blue-100)", color: "var(--ds-blue-700)", border: "1px solid var(--ds-border)", marginBottom: 10 },
    title: { fontSize: 24, fontWeight: 600, color: "var(--ds-gray-900)", letterSpacing: "-0.03em", margin: 0 },
    subtitle: { fontSize: 13, color: "var(--ds-gray-700)", marginTop: 5 },
    filters: { display: "flex", flexWrap: "wrap", gap: 12, padding: 18, background: "var(--ds-background-100)", borderRadius: 12, border: "1px solid var(--ds-border)", boxShadow: "var(--ds-shadow-small)", marginBottom: 20 },
    filterGroup: { display: "flex", flexDirection: "column", gap: 4 },
    filterLabel: { fontSize: 11, fontWeight: 600, color: "var(--ds-gray-700)", letterSpacing: "0.01em" },
    filterInput: { minHeight: 38, padding: "8px 11px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", background: "var(--ds-background-100)", color: "var(--ds-gray-900)", fontSize: 13, outline: "none", minWidth: 140, boxShadow: "var(--ds-shadow-small)" },
    filterSelect: { minHeight: 38, padding: "8px 11px", borderRadius: 8, border: "1px solid var(--ds-border-hover)", fontSize: 13, outline: "none", background: "var(--ds-background-100)", color: "var(--ds-gray-900)", cursor: "pointer", minWidth: 140, boxShadow: "var(--ds-shadow-small)" },
    actions: { display: "flex", gap: 10, alignItems: "flex-end" },
    resultBar: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 },
    resultCount: { fontSize: 13, color: "var(--ds-gray-700)", fontWeight: 500 },
    tableWrap: { overflowX: "auto", border: "1px solid var(--ds-border)", width: "100%", maxWidth: "100%", borderRadius: 12, background: "var(--ds-background-100)", boxShadow: "var(--ds-shadow-small)" },
    table: { width: "100%", borderCollapse: "collapse", fontSize: 13 },
    th: { padding: "11px 14px", textAlign: "left", fontWeight: 550, color: "var(--ds-gray-700)", borderBottom: "1px solid var(--ds-border)", background: "var(--ds-background-200)", whiteSpace: "normal", maxWidth: 100, lineHeight: 1.3, cursor: "pointer", userSelect: "none" },
    td: { padding: "10px 14px", borderBottom: "1px solid var(--ds-border)", color: "var(--ds-gray-800)", whiteSpace: "nowrap", maxWidth: 250, overflow: "hidden", textOverflow: "ellipsis" },
    sortIcon: { marginLeft: 4, fontSize: 10 },
  };

  return (
    <div className="query-wrap" style={QS.wrap}>
      <div style={QS.header}>
        <div style={QS.badge}>Admin</div>
        <h1 style={QS.title}>Data explorer</h1>
        <p style={QS.subtitle}>Filter, inspect, sort, and export your underlying analytics data.</p>
      </div>

      <div className="query-filters" style={QS.filters}>
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>Table</span>
          <select aria-label="Data table" style={QS.filterSelect} value={table} onChange={e => { setTable(e.target.value); setResults(null); setEventType(""); setVariant(""); setSortBy(""); }}>
            <option value="events">Events (Raw)</option>
            <option value="daily_metrics">Daily Metrics (Raw)</option>
            <option value="dashboard">Dashboard (Processed)</option>
          </select>
        </div>
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>From</span>
          <input aria-label="From date" type="date" style={QS.filterInput} value={dateFrom} onChange={e => setDateFrom(e.target.value)} />
        </div>
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>To</span>
          <input aria-label="To date" type="date" style={QS.filterInput} value={dateTo} onChange={e => setDateTo(e.target.value)} />
        </div>
        {(table === "events" || table === "dashboard") && (
          <>
            <div style={QS.filterGroup}>
              <span style={QS.filterLabel}>Event Type</span>
              <select aria-label="Event type" style={QS.filterSelect} value={eventType} onChange={e => setEventType(e.target.value)}>
                <option value="">All</option>
                {eventTypes.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div style={QS.filterGroup}>
              <span style={QS.filterLabel}>Variant {table === "dashboard" ? "(attributed)" : "(own tag)"}</span>
              <select aria-label="Variant" style={QS.filterSelect} value={variant} onChange={e => setVariant(e.target.value)}>
                <option value="">All</option>
                <option value="A">A</option>
                <option value="B">B</option>
                <option value="undetected">Undetected</option>
              </select>
            </div>
            <div style={QS.filterGroup}>
              <span style={QS.filterLabel}>Search (name/email/phone)</span>
              <input aria-label="Search name, email, or phone" style={QS.filterInput} placeholder="Search…" value={search} onChange={e => setSearch(e.target.value)} />
            </div>
          </>
        )}
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>Limit</span>
          <select aria-label="Result limit" style={QS.filterSelect} value={limit} onChange={e => setLimit(e.target.value)}>
            <option value="100">100</option>
            <option value="500">500</option>
            <option value="1000">1,000</option>
            <option value="5000">5,000</option>
          </select>
        </div>
        <div style={QS.actions}>
          <button style={{ ...S.btnDark, padding: "8px 20px", borderRadius: 8, fontSize: 13 }} onClick={() => runQuery()} disabled={loading}>{loading ? "Querying…" : "Run query"}</button>
        </div>
      </div>

      {results && (
        <>
          <div style={QS.resultBar}>
            <span style={QS.resultCount}>{results.count.toLocaleString()} result{results.count !== 1 ? "s" : ""}</span>
            <button style={{ ...S.btnLight, padding: "6px 14px", fontSize: 12 }} onClick={downloadCSV}><I d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3" size={14} stroke="var(--ds-gray-800)" /> Export CSV</button>
          </div>
          <div style={QS.tableWrap}>
            <table style={QS.table}>
              <thead><tr>
                {results.data.length > 0 && Object.keys(results.data[0]).map(col => (
                  <th key={col} style={QS.th} aria-sort={sortBy === col ? (sortDir === "asc" ? "ascending" : "descending") : "none"}>
                    <button type="button" onClick={() => handleSort(col)} style={{ display: "inline-flex", alignItems: "center", gap: 4, width: "100%", padding: 0, border: 0, background: "transparent", color: "inherit", fontWeight: "inherit", textAlign: "left" }}>
                      {col}{sortBy === col && <span aria-hidden="true" style={QS.sortIcon}>{sortDir === "asc" ? "▲" : "▼"}</span>}
                    </button>
                  </th>
                ))}
              </tr></thead>
              <tbody>
                {results.data.length === 0 ? (
                  <tr><td colSpan={99} style={{ ...QS.td, textAlign: "center", color: "var(--ds-gray-600)", padding: 32 }}>No results found</td></tr>
                ) : results.data.map((row, i) => (
                  <tr key={i} onMouseEnter={e => e.currentTarget.style.background = "var(--ds-background-200)"} onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                    {Object.values(row).map((v, j) => (
                      <td key={j} style={QS.td} title={v !== null && v !== undefined ? String(typeof v === "object" ? JSON.stringify(v) : v) : ""}>
                        {v === null ? <span style={{ color: "var(--ds-gray-600)" }}>—</span> : typeof v === "object" ? JSON.stringify(v) : String(v)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

const TYPING_STATUSES = [
  "Pulling up your metrics",
  "Crunching the numbers",
  "Spotting trends",
  "Comparing periods",
  "Running the math",
  "Querying the data",
  "Charting the results",
  "Putting it together",
  "Almost there",
];

function TypingStatus({ IC }) {
  const [idx, setIdx] = useState(0);
  useEffect(() => {
    const t = setInterval(() => setIdx(i => (i + 1) % TYPING_STATUSES.length), 2400);
    return () => clearInterval(t);
  }, []);
  return (
    <div role="status" aria-live="polite" aria-label="AI is preparing a response" style={IC.typing}>
      <ThinkingOrb state="composing" size={20} speed={0.75} theme="light" aria-hidden="true" style={{ flex: "0 0 auto" }} />
      <span aria-hidden="true" key={idx} className="fi" style={IC.typingText}>{TYPING_STATUSES[idx]}…</span>
    </div>
  );
}

function InsightsChat({ flash, isMobile, activeFunnel, userId, mode = "insights", compact = false, active = true, onClose }) {
  // mode="worker" turns this into the admin-only AI Worker: same shell, but it
  // talks to /api/worker/chat (write tools) and keeps its own history slice —
  // both modes share the conversations table, namespaced by id prefix.
  const isWorker = mode === "worker";
  const historyIsOverlay = isMobile || compact;
  const idPrefix = isWorker ? "work-" : "chat-";
  const brandLabel = isWorker ? "Worker AI" : "Funnel AI";
  // Popup presentation state is intentionally separate from the full AI page:
  // a first-time popup must open blank, while its own last chat and draft persist.
  const chatUiStorageKey = `shumard-chat-ui:v1:${userId || "user"}:${activeFunnel}:${mode}${compact ? ":popup" : ""}`;
  const initialChatUiRef = useRef(null);
  if (initialChatUiRef.current === null) initialChatUiRef.current = readChatUiState(chatUiStorageKey);
  const [history, setHistory] = useState([]);
  const [activeId, setActiveId] = useState(() => initialChatUiRef.current.activeId);
  const [chatMsgs, setChatMsgs] = useState([]);
  const [chatInput, setChatInput] = useState(() => initialChatUiRef.current.draft);
  const [chatLoading, setChatLoading] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(!historyIsOverlay);
  const [historyLoading, setHistoryLoading] = useState(true);
  const [historyLoadedSuccessfully, setHistoryLoadedSuccessfully] = useState(false);
  const chatEndRef = useRef(null);
  // Cache of full conversation messages, keyed by id and stamped with the
  // server's updated_at so we can detect remote edits (other tabs/devices).
  // Shape: { [id]: { msgs, updatedAt } }
  const msgsCache = useRef({});
  const inputRef = useRef(null);
  const chatSidebarRef = useRef(null);
  const historyToggleRef = useRef(null);
  const activeIdRef = useRef(activeId);
  const saveQueuesRef = useRef({});
  const deletedChatIdsRef = useRef(new Set());
  activeIdRef.current = activeId;

  useEffect(() => {
    try { localStorage.setItem(chatUiStorageKey, JSON.stringify({ activeId, draft: chatInput })); } catch {}
  }, [activeId, chatInput, chatUiStorageKey]);

  useEffect(() => {
    if (compact && !active) { setSidebarOpen(false); return; }
    setSidebarOpen(!historyIsOverlay);
  }, [active, compact, historyIsOverlay]);
  useEffect(() => {
    if (!active || !historyIsOverlay || !sidebarOpen) return;
    const panel = chatSidebarRef.current;
    const getFocusable = () => panel ? [...panel.querySelectorAll('button:not(:disabled), [tabindex]:not([tabindex="-1"])')] : [];
    const focusFrame = requestAnimationFrame(() => panel?.querySelector("button")?.focus());
    const handleKey = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setSidebarOpen(false);
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = getFocusable();
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", handleKey);
    return () => {
      cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", handleKey);
      requestAnimationFrame(() => historyToggleRef.current?.focus());
    };
  }, [active, historyIsOverlay, sidebarOpen]);

  // Auto-grow the composer with its content (ChatGPT-style); snaps back when
  // cleared after send. Capped so long drafts scroll instead of eating the page.
  useEffect(() => {
    const el = inputRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 220) + "px";
  }, [chatInput]);

  const loadHistory = useCallback(async () => {
    try {
      const { data } = await api.getConversations(activeFunnel);
      setHistory((data || []).filter(c => isWorker === String(c.id).startsWith("work-") && !deletedChatIdsRef.current.has(c.id)));
      setHistoryLoadedSuccessfully(true);
    } catch (e) {
      console.error('Failed to load conversations:', e.message);
    } finally {
      setHistoryLoading(false);
    }
  }, [isWorker, activeFunnel]);

  // Load conversation list from Supabase on mount
  useEffect(() => { loadHistory(); }, [loadHistory]);

  // Validate popup-local restoration without auto-opening an unrelated chat
  // from the full AI page or from the server history list.
  useEffect(() => {
    if (!compact || historyLoading || !historyLoadedSuccessfully || !activeIdRef.current) return;
    const hasSavedChat = history.some(chat => chat.id === activeIdRef.current);
    const hasOptimisticChat = Boolean(msgsCache.current[activeIdRef.current]);
    if (!hasSavedChat && !hasOptimisticChat) {
      activeIdRef.current = null;
      setActiveId(null);
    }
  }, [compact, history, historyLoadedSuccessfully, historyLoading]);

  // Refresh history when the tab regains focus — picks up edits made in
  // other tabs/devices so msgsCache (keyed by updated_at) can invalidate.
  useEffect(() => {
    const onFocus = () => { loadHistory(); };
    window.addEventListener('focus', onFocus);
    return () => window.removeEventListener('focus', onFocus);
  }, [loadHistory]);

  // When selecting a chat, fetch full messages if cache is missing or stale
  useEffect(() => {
    if (!activeId) { setChatMsgs([]); return; }
    let cancelled = false;
    const requestedId = activeId;
    const remote = history.find(c => c.id === activeId);
    const cached = msgsCache.current[activeId];
    if (cached && (!remote || cached.updatedAt === remote.updated_at)) {
      setChatMsgs(cached.msgs);
      return () => { cancelled = true; };
    }
    // Cache miss or stale (remote updated_at moved). Fetch and re-stamp.
    (async () => {
      try {
        const headers = await getAuthHeaders(activeFunnel);
        const res = await fetch(`${API_BASE}/api/insights/conversations/${requestedId}`, { headers });
        if (res.ok) {
          const body = await res.json();
          const msgs = body.data?.messages || [];
          msgsCache.current[requestedId] = { msgs, updatedAt: body.data?.updated_at || remote?.updated_at };
          if (!cancelled && activeIdRef.current === requestedId) setChatMsgs(msgs);
        }
      } catch { /* fallback: empty */ }
    })();
    return () => { cancelled = true; };
  }, [activeId, history]);

  // Auto-scroll
  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [chatMsgs, chatLoading]);

  const saveChat = (id, msgs) => {
    if (deletedChatIdsRef.current.has(id)) return;
    const title = msgs.find(m => m.role === "user")?.content?.slice(0, 50) || "New chat";
    const stamp = new Date().toISOString();
    // Stamp the cache and the optimistic history row with the same updated_at
    // so the selection effect sees them as in sync.
    msgsCache.current[id] = { msgs, updatedAt: stamp };
    setHistory(prev => {
      const existing = prev.find(c => c.id === id);
      if (existing) {
        return prev.map(c => c.id === id ? { ...c, title, updated_at: stamp } : c);
      }
      return [{ id, title, updated_at: stamp }, ...prev];
    });
    // Serialize writes per conversation so a slower user-only save cannot
    // overwrite the later response-complete version.
    const previousSave = saveQueuesRef.current[id] || Promise.resolve();
    saveQueuesRef.current[id] = previousSave
      .catch(() => {})
      .then(() => deletedChatIdsRef.current.has(id) ? undefined : api.saveConversation(id, title, msgs, activeFunnel))
      .catch(e => console.error('Save chat error:', e.message));
  };

  const newChat = () => {
    if (chatLoading) return;
    activeIdRef.current = null;
    setActiveId(null);
    setChatMsgs([]);
    setChatInput("");
    if (historyIsOverlay) setSidebarOpen(false);
    requestAnimationFrame(() => requestAnimationFrame(() => inputRef.current?.focus()));
  };

  const deleteChat = (id) => {
    deletedChatIdsRef.current.add(id);
    setHistory(prev => prev.filter(c => c.id !== id));
    delete msgsCache.current[id];
    if (activeId === id) { activeIdRef.current = null; setActiveId(null); setChatMsgs([]); }
    const previousSave = saveQueuesRef.current[id] || Promise.resolve();
    saveQueuesRef.current[id] = previousSave
      .catch(() => {})
      .then(() => api.deleteConversation(id, activeFunnel))
      .catch(e => {
        deletedChatIdsRef.current.delete(id);
        loadHistory();
        flash(e.message, "err");
      });
  };

  const starters = [
    "What trends do you see this week?",
    "How can we improve CTA conversion?",
    "Summarize today's performance",
    "What's our cost per acquisition trend?",
    "Compare this week vs last week",
  ];

  const sendMessage = async (text) => {
    const msg = text || chatInput.trim();
    if (!msg || chatLoading) return;
    if (historyIsOverlay) setSidebarOpen(false);

    const chatId = activeId || `${idPrefix}${Date.now()}`;
    if (!activeId) { activeIdRef.current = chatId; setActiveId(chatId); }

    const newMsgs = [...chatMsgs, { role: "user", content: msg }];
    setChatMsgs(newMsgs);
    setChatInput("");
    setChatLoading(true);
    saveChat(chatId, newMsgs);

    try {
      const { reply } = await (isWorker ? api.workerChat(newMsgs, activeFunnel) : api.chat(newMsgs, activeFunnel));
      const fullMsgs = [...newMsgs, { role: "assistant", content: reply }];
      if (deletedChatIdsRef.current.has(chatId)) return;
      if (activeIdRef.current === chatId) setChatMsgs(fullMsgs);
      saveChat(chatId, fullMsgs);
    } catch (e) {
      flash(e.message, "err");
      // Leave the failure in the thread — a toast alone reads as "no response".
      const errMsgs = [...newMsgs, { role: "assistant", content: `⚠️ ${e.message}` }];
      if (deletedChatIdsRef.current.has(chatId)) return;
      if (activeIdRef.current === chatId) setChatMsgs(errMsgs);
      saveChat(chatId, errMsgs);
    } finally {
      setChatLoading(false);
    }
  };

  const fmtTime = (ts) => {
    const d = new Date(ts);
    const now = new Date();
    const diff = now - d;
    if (diff < 60000) return "Just now";
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    if (diff < 604800000) return d.toLocaleDateString("en-US", { weekday: "short" });
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  };

  const quickPrompts = isWorker ? [
    { letter: "S", color: "#0A7F43", label: "Add a Sale", prompt: "I need to add a sale that the webhooks missed. Ask me for the buyer's email, the source, and when it happened — then record it and verify it landed on the right day." },
    { letter: "F", color: "#C2410C", label: "Fix a Day", prompt: "The numbers for a specific day look wrong. Ask me which day and what's off, check the underlying data, and fix it." },
    { letter: "A", color: "#6D28D9", label: "Recent Actions", prompt: "Show me the most recent worker actions from the audit log." },
  ] : [
    { letter: "W", color: "#1D4ED8", label: "Weekly Recap", prompt: "Summarize this week's performance and call out anything unusual." },
    { letter: "F", color: "#C2410C", label: "Funnel Health", prompt: "Walk me through the funnel step by step and flag where we're losing the most people." },
    { letter: "T", color: "#6D28D9", label: "Trend Analyst", prompt: "What trends do you see in the last 14 days of metrics?" },
  ];

  const grouped = (() => {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const yesterday = new Date(today); yesterday.setDate(yesterday.getDate() - 1);
    const buckets = { Today: [], Yesterday: [], Earlier: [] };
    for (const c of [...history].sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))) {
      const d = new Date(c.updated_at); d.setHours(0, 0, 0, 0);
      if (d.getTime() === today.getTime()) buckets.Today.push(c);
      else if (d.getTime() === yesterday.getTime()) buckets.Yesterday.push(c);
      else buckets.Earlier.push(c);
    }
    return buckets;
  })();

  const shareChat = async () => {
    if (chatMsgs.length === 0) { flash("Start a chat first", "err"); return; }
    const text = chatMsgs.map(m => `**${m.role === "user" ? "You" : brandLabel}:** ${m.content}`).join("\n\n");
    try { await navigator.clipboard.writeText(text); flash("Chat copied to clipboard", "ok"); }
    catch { flash("Copy failed", "err"); }
  };

  const sourceLabel = activeFunnel ? activeFunnel.charAt(0).toUpperCase() + activeFunnel.slice(1) : "Select Source";

  const IC = {
    outer: { display: "flex", height: compact ? "100%" : "calc(100dvh - 64px)", gap: 0, position: "relative", background: "var(--ds-background-100)", border: "none", borderRadius: 0, overflow: "hidden" },

    sidebar: { width: sidebarOpen ? (historyIsOverlay ? "100%" : 280) : 0, minWidth: sidebarOpen ? (historyIsOverlay ? "100%" : 280) : 0, background: "var(--ds-background-200)", borderRight: sidebarOpen && !historyIsOverlay ? "1px solid var(--ds-border)" : "none", display: "flex", flexDirection: "column", overflow: "hidden", transition: "width 0.2s, min-width 0.2s", ...(historyIsOverlay && sidebarOpen ? { position: "absolute", inset: 0, zIndex: 10 } : {}) },
    sidebarHeader: { padding: compact ? "14px 16px 8px" : "18px 20px 10px", display: "flex", justifyContent: "space-between", alignItems: "center" },
    sidebarTitle: { fontSize: 15, fontWeight: 600, color: "var(--ds-gray-900)", letterSpacing: "-0.01em" },
    iconOnlyBtn: { width: 32, height: 32, background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", cursor: "pointer", padding: 0, color: "var(--ds-gray-700)", display: "inline-flex", alignItems: "center", justifyContent: "center", borderRadius: 7 },
    newChatBtn: { margin: "0 16px 14px", padding: "9px 14px", background: "var(--ds-gray-900)", color: "#fff", border: "1px solid var(--ds-gray-900)", borderRadius: 8, fontSize: 13, fontWeight: 550, cursor: "pointer", fontFamily: fn, display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 6 },

    groupHeader: { padding: "14px 20px 6px", display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 12, fontWeight: 500, color: "var(--ds-gray-600)" },
    groupHeaderClickable: { cursor: "pointer", userSelect: "none" },

    savedItem: { width: "calc(100% - 16px)", padding: "8px 10px", margin: "0 8px", display: "flex", alignItems: "center", gap: 10, cursor: "pointer", border: "1px solid transparent", background: "transparent", textAlign: "left", borderRadius: 8, transition: "background 0.1s" },
    savedLabel: { fontSize: 13, color: "var(--ds-gray-900)", fontWeight: 500, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
    savedMore: { fontSize: 16, color: "var(--ds-border-hover)" },

    chatList: { flex: 1, overflowY: "auto", paddingBottom: 16 },
    histItem: { padding: "8px 20px", display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer", fontSize: 13, color: "var(--ds-gray-700)", transition: "background 0.1s", position: "relative" },
    histItemActive: { color: "var(--ds-gray-900)", fontWeight: 500, background: "var(--ds-gray-100)" },
    histDot: { position: "absolute", left: 0, top: "50%", transform: "translateY(-50%)", width: 3, height: 18, background: "var(--ds-gray-900)", borderRadius: "0 3px 3px 0" },
    histTitle: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 },
    histDel: { width: 28, height: 28, display: "grid", placeItems: "center", background: "transparent", border: "none", borderRadius: 6, cursor: "pointer", color: "var(--ds-gray-600)", fontSize: 16, padding: 0, lineHeight: 1, flexShrink: 0 },

    main: { flex: 1, display: "flex", flexDirection: "column", minWidth: 0, background: "var(--ds-background-100)" },
    topbar: { display: "flex", justifyContent: "space-between", alignItems: "center", padding: compact ? "10px 12px" : "12px 20px", borderBottom: "1px solid var(--ds-border)", gap: 10, flexWrap: "wrap" },
    brand: { display: "flex", alignItems: "center", gap: 8 },
    brandName: { fontSize: 15, fontWeight: 600, color: "var(--ds-gray-900)", letterSpacing: "-0.01em" },
    brandPill: { padding: "2px 8px", background: "var(--ds-gray-100)", color: "var(--ds-gray-700)", fontSize: 11, fontWeight: 600, borderRadius: 6 },
    topActions: { display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" },
    pillBtn: { minHeight: 34, padding: "7px 12px", background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", borderRadius: 8, fontSize: 13, fontWeight: 500, color: "var(--ds-gray-800)", cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 6, fontFamily: fn },
    pillBtnDark: { minHeight: 34, padding: "7px 14px", background: "var(--ds-gray-900)", color: "#fff", border: "1px solid var(--ds-gray-900)", borderRadius: 8, fontSize: 13, fontWeight: 550, cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 6, fontFamily: fn },

    body: { flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", minHeight: 0 },
    // Scroll container is full-width so backgrounds extend edge-to-edge.
    // The column constrains text/markdown to a comfortable reading width.
    msgScroll: { flex: 1, overflowY: "auto", padding: compact ? "18px 14px 6px" : "32px 24px 8px", minHeight: 0 },
    msgColumn: { maxWidth: 760, margin: "0 auto", display: "flex", flexDirection: "column", gap: compact ? 16 : 24 },

    // User: tight rounded bubble, right-aligned. Uniform 16px radius (no tail).
    userBub: { alignSelf: "flex-end", maxWidth: compact ? "88%" : "80%", padding: compact ? "8px 11px" : "10px 14px", background: "var(--ds-gray-900)", color: "#fff", borderRadius: 10, fontSize: compact ? 13 : 15, lineHeight: 1.5, whiteSpace: "pre-wrap", wordBreak: "break-word" },
    // Assistant: NO bubble. Markdown flows naturally up to the column width.
    aiMessage: { color: "var(--ds-gray-900)", fontSize: compact ? 13 : 15, lineHeight: 1.65, wordBreak: "break-word" },

    typing: { alignSelf: "flex-start", padding: "10px 16px", background: "transparent", display: "flex", gap: 9, alignItems: "center" },
    typingText: { fontSize: 13, color: "var(--ds-gray-700)", fontWeight: 500, fontVariantNumeric: "tabular-nums" },

    // Bottom area hosts the input column. Sticky-feeling via the body's flex layout.
    bottomWrap: { padding: compact ? "8px 12px 12px" : "12px 24px 18px", background: "linear-gradient(180deg, rgba(255,255,255,0) 0%, #fff 30%)" },
    bottomColumn: { maxWidth: 760, margin: "0 auto" },

    // Single calm input container. No internal divider, no sparkle, just space.
    inputCard: { width: "100%", background: "var(--ds-background-100)", border: "1px solid var(--ds-border-hover)", borderRadius: 12, padding: compact ? "9px 9px 7px 11px" : "12px 12px 10px 14px", boxShadow: "var(--ds-shadow-small)", transition: "border-color 0.15s, box-shadow 0.15s", boxSizing: "border-box" },
    inputCardFocus: { borderColor: "var(--ds-gray-500)", boxShadow: "0 0 0 2px rgba(0,0,0,.035)" },
    // Multi-line composer: auto-grows with content (see the chatInput effect),
    // scrolls past maxHeight. resize handle off — growth is automatic.
    textInput: { width: "100%", display: "block", border: "none", outline: "none", background: "transparent", fontSize: compact ? 13 : 15, color: "var(--ds-gray-900)", fontFamily: fn, padding: "4px 2px", lineHeight: 1.5, resize: "none", minHeight: 24, maxHeight: compact ? 120 : 220, overflowY: "auto", boxSizing: "border-box" },

    // Footer row inside the input card. No background, no border — just spacing.
    inputFooter: { display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 4, gap: 8 },
    sourceBtn: { display: "inline-flex", alignItems: "center", gap: 6, padding: "4px 8px", background: "transparent", border: "none", fontSize: 12, fontWeight: 500, color: "var(--ds-gray-700)", cursor: "default", fontFamily: fn, borderRadius: 6 },
    // Send is an icon-only circular button. Subtle when disabled, prominent when ready.
    sendBtn: { width: 34, height: 34, display: "inline-flex", alignItems: "center", justifyContent: "center", background: "var(--ds-gray-900)", color: "#fff", border: "1px solid var(--ds-gray-900)", borderRadius: 8, cursor: "pointer", padding: 0, flexShrink: 0, transition: "opacity 0.15s, background 0.15s" },

    disclaimer: { fontSize: compact ? 9 : 11, color: "var(--ds-gray-600)", textAlign: "center", marginTop: compact ? 6 : 10, lineHeight: 1.45 },
    disclaimerLink: { color: "var(--ds-gray-700)", textDecoration: "underline" },

    // Empty-state greeting shown above the input column when no messages exist.
    emptyGreet: { padding: compact ? "44px 10px 18px" : "80px 16px 24px", textAlign: "center" },
    emptyGreetTitle: { fontSize: compact ? 18 : 24, fontWeight: 600, color: "var(--ds-gray-900)", letterSpacing: "-0.02em", marginBottom: 8 },
    emptyGreetSub: { fontSize: compact ? 12 : 14, color: "var(--ds-gray-700)", lineHeight: 1.55, maxWidth: 480, margin: "0 auto" },
  };

  const avatarStyle = (color) => ({ width: 28, height: 28, borderRadius: "50%", background: color, color: "#fff", display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 700, flexShrink: 0 });

  const InputCard = (
    <div
      className="insights-input-card"
      style={IC.inputCard}
      onFocus={e => { e.currentTarget.style.borderColor = IC.inputCardFocus.borderColor; e.currentTarget.style.boxShadow = IC.inputCardFocus.boxShadow; }}
      onBlur={e => { e.currentTarget.style.borderColor = IC.inputCard.border.split(' ').slice(-1)[0]; e.currentTarget.style.boxShadow = IC.inputCard.boxShadow; }}
    >
      <textarea
        aria-label={isWorker ? "Message Worker AI" : "Message Funnel AI"}
        ref={inputRef}
        rows={1}
        style={IC.textInput}
        value={chatInput}
        onChange={e => setChatInput(e.target.value)}
        onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMessage(); } }}
        placeholder={isWorker ? "Tell me what needs doing… (Shift+Enter for a new line)" : "Ask anything about your funnel… (Shift+Enter for a new line)"}
        autoFocus={!compact}
      />
      <div style={IC.inputFooter}>
        <div style={IC.sourceBtn} aria-label={`Active funnel: ${sourceLabel}`}>
          <I d="M3 6h18 M6 12h12 M10 18h4" size={12} stroke="var(--ds-gray-700)" /> {sourceLabel}
        </div>
        <button
          style={{ ...IC.sendBtn, opacity: chatLoading || !chatInput.trim() ? 0.4 : 1, cursor: chatLoading || !chatInput.trim() ? "default" : "pointer" }}
          onClick={() => sendMessage()}
          disabled={chatLoading || !chatInput.trim()}
          title="Send"
          aria-label="Send message"
        >
          <I d="M12 19V5 M5 12l7-7 7 7" size={14} stroke="#fff" sw={2.2} />
        </button>
      </div>
    </div>
  );

  const Disclaimer = (
    <div style={IC.disclaimer}>
      {isWorker
        ? <>Worker AI makes real changes to your data — it confirms before anything destructive, and every action is logged. Double-check what it reports.</>
        : <>Funnel AI may display inaccurate info, so please double check the response. <span style={IC.disclaimerLink}>Your Privacy &amp; Funnel AI</span></>}
    </div>
  );

  return (
    <div className={`insights-outer${compact ? " floating-insights-chat" : ""}`} style={IC.outer}>
      {/* Sidebar */}
      {sidebarOpen && (
        <div ref={chatSidebarRef} role="complementary" aria-label="Conversation history" style={IC.sidebar}>
          <div style={IC.sidebarHeader}>
            <span style={IC.sidebarTitle}>{isWorker ? "Worker" : "Chat"}</span>
            <button style={IC.iconOnlyBtn} aria-label="Close conversation history" title="Close history" onClick={() => setSidebarOpen(false)}><I d="M6 18L18 6M6 6l12 12" size={15} /></button>
          </div>
          <button style={IC.newChatBtn} onClick={newChat} disabled={chatLoading}>
            <I d="M12 5v14 M5 12h14" size={14} stroke="#fff" /> New chat
            <I d="M12 3l1.9 5.8 5.8 1.9-5.8 1.9L12 18.4l-1.9-5.8L4.3 10.7l5.8-1.9z" size={12} stroke="#fff" />
          </button>

          <div style={IC.groupHeader}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
              <I d="M12 2l2.39 7.36H22l-6.18 4.49L18.21 22 12 17.27 5.79 22l2.39-8.15L2 9.36h7.61z" size={12} stroke="var(--ds-gray-600)" /> Saved
            </span>
          </div>
          <div>
            {quickPrompts.map(q => (
              <button
                type="button"
                key={q.label}
                style={IC.savedItem}
                disabled={chatLoading}
                onClick={() => sendMessage(q.prompt)}
                onMouseEnter={e => { e.currentTarget.style.background = "var(--ds-gray-100)"; }}
                onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}
              >
                <div aria-hidden="true" style={avatarStyle(q.color)}>{q.letter}</div>
                <span style={IC.savedLabel}>{q.label}</span>
                <I d="M9 18l6-6-6-6" size={13} stroke="var(--ds-gray-600)" />
              </button>
            ))}
          </div>

          <div style={IC.chatList}>
            {historyLoading ? (
              <div style={{ padding: 16, textAlign: "center", color: "var(--ds-gray-600)", fontSize: 13 }}>Loading…</div>
            ) : history.length === 0 ? (
              <div style={{ padding: 16, textAlign: "center", color: "var(--ds-gray-600)", fontSize: 13 }}>No conversations yet</div>
            ) : (
              Object.entries(grouped).map(([group, items]) => (
                items.length > 0 && (
                  <div key={group}>
                    <div style={IC.groupHeader}>
                      <span>{group}</span>
                      <span style={{ fontSize: 10, color: "var(--ds-border-hover)" }}>▾</span>
                    </div>
                    {items.map(chat => (
                      <div
                        key={chat.id}
                        style={{ ...IC.histItem, ...(activeId === chat.id ? IC.histItemActive : {}) }}
                        onMouseEnter={e => { if (activeId !== chat.id) e.currentTarget.style.background = "var(--ds-gray-100)"; }}
                        onMouseLeave={e => { if (activeId !== chat.id) e.currentTarget.style.background = "transparent"; }}
                      >
                        {activeId === chat.id && <span style={IC.histDot} />}
                        <button
                          type="button"
                          aria-current={activeId === chat.id ? "true" : undefined}
                          disabled={chatLoading}
                          style={{ ...IC.histTitle, flex: 1, minWidth: 0, border: 0, padding: 0, background: "transparent", textAlign: "left", color: "inherit" }}
                          onClick={() => { activeIdRef.current = chat.id; setActiveId(chat.id); if (historyIsOverlay) setSidebarOpen(false); }}
                        >
                          {chat.title}
                        </button>
                        <button className="chat-history-delete" aria-label={`Delete ${chat.title}`} style={IC.histDel} onClick={() => deleteChat(chat.id)} title="Delete" disabled={chatLoading}>×</button>
                      </div>
                    ))}
                  </div>
                )
              ))
            )}
          </div>
        </div>
      )}

      {/* Main chat area */}
      <div aria-hidden={historyIsOverlay && sidebarOpen ? "true" : undefined} inert={historyIsOverlay && sidebarOpen ? "" : undefined} style={IC.main}>
        <div style={IC.topbar}>
          <div style={IC.brand}>
            {!sidebarOpen && <button ref={historyToggleRef} style={IC.iconOnlyBtn} aria-label="Open conversation history" title="Conversation history" onClick={() => setSidebarOpen(true)}><I d="M4 6h16M4 12h16M4 18h16" size={15} /></button>}
            <span style={IC.brandName}>{brandLabel}</span>
            <span style={isWorker ? { ...IC.brandPill, background: "#FEF3C7", color: "#92400E" } : IC.brandPill}>{isWorker ? "Admin" : "Beta"}</span>
          </div>
          <div style={IC.topActions}>
            {compact ? (
              <>
                <button style={IC.iconOnlyBtn} onClick={newChat} title="New chat" aria-label="Start a new chat" disabled={chatLoading}><I d="M12 5v14M5 12h14" size={15} /></button>
                <button style={IC.iconOnlyBtn} onClick={onClose} title="Close AI insights" aria-label="Close AI insights"><I d="M6 18L18 6M6 6l12 12" size={15} /></button>
              </>
            ) : (
              <>
                <button style={IC.pillBtn} onClick={shareChat} title="Copy conversation">
                  <I d="M4 12v7a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7 M16 6l-4-4-4 4 M12 2v13" size={13} stroke="var(--ds-gray-700)" />
                  Copy chat
                </button>
                <button style={IC.pillBtnDark} onClick={newChat} disabled={chatLoading}>
                  New chat
                  <I d="M12 3l1.9 5.8 5.8 1.9-5.8 1.9L12 18.4l-1.9-5.8L4.3 10.7l5.8-1.9z" size={12} stroke="#fff" />
                </button>
              </>
            )}
          </div>
        </div>

        <div style={IC.body}>
          <div style={IC.msgScroll}>
            <div role="log" aria-live="polite" aria-relevant="additions" aria-label="Conversation" style={IC.msgColumn}>
              {chatMsgs.length === 0 && !chatLoading ? (
                <div style={IC.emptyGreet}>
                  <div style={IC.emptyGreetTitle}>{isWorker ? "What needs doing?" : "What would you like to explore?"}</div>
                  <div style={IC.emptyGreetSub}>{isWorker ? "I can record missed sales and events, fix a day's numbers, link duplicate contacts, and repair data — then verify everything I changed." : "Ask about trends, compare periods, dig into a specific day, or pull a chart of any metric over time."}</div>
                </div>
              ) : (
                <>
                  {chatMsgs.map((m, i) => (
                    m.role === "user"
                      ? <div key={i} style={IC.userBub}>{m.content}</div>
                      : <div key={i} style={IC.aiMessage}>{renderMd(m.content)}</div>
                  ))}
                  {chatLoading && <TypingStatus IC={IC} />}
                </>
              )}
              <div ref={chatEndRef} />
            </div>
          </div>
          <div style={IC.bottomWrap}>
            <div style={IC.bottomColumn}>
              {InputCard}
              {Disclaimer}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function FloatingInsights({ flash, activeFunnel, userId }) {
  const [open, setOpen] = useState(false);
  const launcherRef = useRef(null);
  const panelRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    const focusFrame = requestAnimationFrame(() => panelRef.current?.querySelector("textarea")?.focus());
    return () => cancelAnimationFrame(focusFrame);
  }, [open]);

  const closePanel = () => {
    setOpen(false);
    requestAnimationFrame(() => launcherRef.current?.focus());
  };

  return (
    <div className={`floating-insights${open ? " is-open" : ""}`}>
      <div
        ref={panelRef}
        id="floating-insights-panel"
        className="floating-insights-panel"
        role="dialog"
        aria-label="AI insights"
        aria-hidden={!open ? "true" : undefined}
        inert={!open ? "" : undefined}
        hidden={!open}
        onKeyDown={event => {
          if (event.key !== "Escape") return;
          if (panelRef.current?.querySelector('[aria-label="Conversation history"]')) return;
          event.preventDefault();
          closePanel();
        }}
      >
        <InsightsChat
          key={`${userId || "user"}:${activeFunnel}:floating-v2`}
          flash={flash}
          isMobile={false}
          activeFunnel={activeFunnel}
          userId={userId}
          compact
          active={open}
          onClose={closePanel}
        />
      </div>
      <button
        ref={launcherRef}
        type="button"
        className="floating-insights-launcher"
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-controls="floating-insights-panel"
        aria-label={open ? "Close AI insights" : "Open AI insights"}
        onClick={() => open ? closePanel() : setOpen(true)}
      >
        <span className="floating-insights-launcher-icon"><I d="M9.7 17h4.6M12 3v1m6.4 1.6l-.7.7M21 12h-1M4 12H3m3.3-5.7l-.7-.7m2.8 9.9a5 5 0 117.1 0l-.6.6A3.4 3.4 0 0014 18.5V19a2 2 0 11-4 0v-.5c0-.9-.4-1.8-1-2.4l-.6-.6z" size={17} sw={1.9} /></span>
        <span className="floating-insights-launcher-label">AI insights</span>
      </button>
    </div>
  );
}

const CSS = `
@font-face{font-family:'Geist';src:url('/fonts/Geist-Variable.woff2') format('woff2');font-style:normal;font-weight:100 900;font-display:swap}
@font-face{font-family:'Geist Mono';src:url('/fonts/GeistMono-Variable.woff2') format('woff2');font-style:normal;font-weight:100 900;font-display:swap}
:root{
  color-scheme:light;
  --ds-background-100:#fff;
  --ds-background-200:#fafafa;
  --ds-gray-100:#f2f2f2;
  --ds-gray-200:#ebebeb;
  --ds-gray-300:#e6e6e6;
  --ds-gray-400:#d4d4d4;
  --ds-gray-500:#b5b5b5;
  --ds-gray-600:#666;
  --ds-gray-700:#4d4d4d;
  --ds-gray-800:#333;
  --ds-gray-900:#171717;
  --ds-blue-100:#eef6ff;
  --ds-blue-200:#e1f0ff;
  --ds-blue-600:#0070f3;
  --ds-blue-700:#0060d1;
  --ds-green-100:#eefbf3;
  --ds-green-600:#0a7f43;
  --ds-red-100:#fff2f2;
  --ds-red-600:#b42318;
  --ds-amber-100:#fff7e6;
  --ds-amber-700:#a35200;
  --ds-border:#eaeaea;
  --ds-border-hover:#cfcfcf;
  --ds-focus:#171717;
  --ds-shadow-small:0 1px 2px rgba(0,0,0,.04);
  --ds-shadow-menu:0 12px 24px rgba(0,0,0,.10);
  --ds-shadow-dialog:0 24px 48px rgba(0,0,0,.16);
  --ds-radius-small:6px;
  --ds-radius:8px;
  --ds-radius-large:12px;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{background:var(--ds-background-200)}
body{-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;background:var(--ds-background-200);color:var(--ds-gray-900);font-family:'Geist',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
button,input,select,textarea{font:inherit}
button{transition:background-color 150ms ease,border-color 150ms ease,color 150ms ease,box-shadow 150ms ease,transform 150ms ease,filter 150ms ease}
button:not(:disabled){cursor:pointer}
button:disabled{cursor:not-allowed;opacity:.48}
button:not(:disabled):hover{filter:brightness(.97)}
button:not(:disabled):active{transform:translateY(1px)}
input,select,textarea{accent-color:var(--ds-gray-900)}
button:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible,[tabindex]:focus-visible{outline:2px solid var(--ds-focus)!important;outline-offset:2px!important;box-shadow:0 0 0 2px var(--ds-background-100)!important}
a:focus-visible{outline:2px solid var(--ds-focus);outline-offset:3px;border-radius:2px}
::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--ds-gray-400);border-radius:3px}::-webkit-scrollbar-thumb:hover{background:var(--ds-gray-500)}
@keyframes fu{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.fi{animation:fu 300ms cubic-bezier(0.16, 1, 0.3, 1)}
.trow:hover{background:var(--ds-background-200)!important}
.trow:hover .rowBtn{opacity:1}
.trow:nth-child(even){background:var(--ds-background-200)}
.trow.selected, .trow.selected:hover, .trow.selected:nth-child(even){background:var(--ds-blue-100)!important;box-shadow:none}
.trow.selected .sticky-col, .trow.selected:hover .sticky-col, .trow.selected:nth-child(even) .sticky-col{background:var(--ds-blue-100)!important}
.rowBtn{opacity:0;transition:opacity 150ms ease}
.chat-history-delete:hover,.chat-history-delete:focus-visible{color:var(--ds-red-600)!important;background:var(--ds-red-100)!important}

/* Sticky Day/Date columns */
.sticky-col { position: sticky; z-index: 2; background: inherit; }
.sticky-col-1 { left: 0; }
.sticky-col-2 { left: 64px; }
thead .sticky-col { background: var(--ds-background-200); z-index: 3; }
.trow .sticky-col { background: #fff; }
.trow:nth-child(even) .sticky-col { background: var(--ds-background-200); }
.trow:hover .sticky-col { background: var(--ds-background-200) !important; }
.sticky-col-2::after {
  content: ''; position: absolute; top: 0; right: -6px; bottom: 0; width: 6px;
  background: linear-gradient(90deg, rgba(0,0,0,0.04), transparent);
  pointer-events: none;
}

input:focus,select:focus,textarea:focus{border-color:var(--ds-border-hover)!important;box-shadow:0 0 0 3px rgba(0,0,0,.06)!important}
::selection{background:rgba(0,112,243,.16)}

/* ─── Mobile Responsive (≤768px) ──────────────────────────────── */
@media (max-width: 768px) {
  /* Main */
  .main-content {
    padding: 16px 12px 40px !important;
  }

  /* Title row */
  .title-row {
    flex-direction: column !important; align-items: stretch !important; gap: 12px !important;
  }
  .title-row h1 { font-size: 20px !important; }

  /* Summary strip — 2-column grid on mobile (parent gap handles dividers) */
  .summary-strip {
    grid-template-columns: repeat(2, 1fr) !important;
  }
  .summary-strip > div {
    padding: 12px 14px !important;
    gap: 4px !important;
  }
  .summary-strip .strip-val-row > span:first-child {
    font-size: 19px !important;
  }
  .summary-strip > div > div:first-child {
    font-size: 11px !important;
  }

  /* Stack value + badge vertically on mobile */
  .strip-val-row {
    flex-direction: column !important;
    align-items: flex-start !important;
    gap: 6px !important;
  }

  /* Toolbar */
  .toolbar-row {
    flex-direction: column !important;
    align-items: stretch !important;
    gap: 10px !important;
  }
  .toolbar-controls {
    flex-wrap: wrap !important;
  }
  /* Hide List/Board toggle on mobile — board is auto-forced */
  .list-board-toggle {
    display: none !important;
  }
  /* Search bar full-width on mobile */
  .search-wrap {
    width: 100% !important;
  }

  /* Board grid on mobile */
  .boardGrid {
    grid-template-columns: 1fr !important;
  }

  /* Forms */
  .form-container {
    padding: 0 4px !important;
  }
  .form-card {
    padding: 20px !important;
  }
  .form-grid {
    grid-template-columns: 1fr !important;
  }

  /* Events header */
  .events-header {
    flex-direction: column !important;
    align-items: stretch !important;
    gap: 12px !important;
  }

  /* Query builder */
  .query-filters {
    flex-direction: column !important;
  }
  .query-filters > div { width: 100% !important; }
  .query-filters input, .query-filters select { width: 100% !important; min-width: 0 !important; }

  /* Insights chat */
  .insights-outer {
    height: calc(100vh - 56px) !important;
  }

  /* Modal */
  .modal-inner {
    padding: 24px 20px !important;
  }

  /* Toast */
  .toast-el {
    max-width: calc(100vw - 32px) !important;
    left: 16px !important; right: 16px !important;
    transform: none !important;
  }

  /* Login form */
  .login-form {
    padding: 32px 20px !important;
  }

  /* table action buttons always visible on mobile (no hover) */
  .rowBtn { opacity: 1 !important; }
}

/* Tablet (769px - 1024px) minor tweaks */
@media (min-width: 769px) and (max-width: 1024px) {
  .main-content { padding: 24px 20px 48px !important; }
  .summary-strip > div { padding: 18px !important; }
}

/* ─── Geist application shell ─────────────────────────────────── */
.skip-link{
  position:fixed;top:8px;left:8px;z-index:1000;
  transform:translateY(-150%);padding:8px 12px;border-radius:var(--ds-radius);
  background:var(--ds-gray-900);color:#fff;font-size:13px;font-weight:600;text-decoration:none
}
.skip-link:focus{transform:translateY(0)}
.geist-select{position:relative;min-width:0}
.geist-select-trigger{
  min-width:150px;height:36px;padding:0 10px;display:flex;align-items:center;gap:8px;
  border:1px solid var(--ds-border);border-radius:var(--ds-radius);background:var(--ds-background-100);
  color:var(--ds-gray-800);font-size:13px;font-weight:500;box-shadow:var(--ds-shadow-small);text-align:left
}
.geist-select-trigger:hover,.geist-select.is-open .geist-select-trigger{border-color:var(--ds-border-hover);background:var(--ds-gray-100)}
.geist-select-value{min-width:0;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.geist-select-trigger>svg:last-child{flex:0 0 auto;color:var(--ds-gray-600);transition:transform 150ms ease}
.geist-select.is-open .geist-select-trigger>svg:last-child{transform:rotate(180deg)}
.geist-select-menu{
  position:absolute;top:calc(100% + 6px);left:0;z-index:200;width:max-content;min-width:100%;max-width:calc(100vw - 32px);
  padding:6px;background:var(--ds-background-100);border:1px solid var(--ds-border);border-radius:var(--ds-radius-large);
  max-height:min(420px,calc(100dvh - 24px));overflow-y:auto;box-shadow:var(--ds-shadow-menu);animation:fu 150ms cubic-bezier(.16,1,.3,1)
}
.geist-select-option{
  width:100%;min-height:36px;padding:0 8px;display:flex;align-items:center;justify-content:space-between;gap:18px;
  border:0;border-radius:6px;background:transparent;color:var(--ds-gray-700);font-size:12px;font-weight:500;text-align:left;white-space:nowrap
}
.geist-select-option:hover,.geist-select-option:focus-visible{background:var(--ds-gray-100);color:var(--ds-gray-900)}
.geist-select-option.is-selected{color:var(--ds-gray-900);font-weight:600}
.app-layout{min-height:100dvh;display:flex;flex-direction:column;background:var(--ds-background-200)}
.app-navbar{
  height:64px;position:sticky;top:0;z-index:120;flex:0 0 64px;
  border-bottom:1px solid var(--ds-border)
}
.navbar-main{
  position:relative;width:100%;height:64px;padding:0 24px;display:flex;align-items:center;gap:18px;min-width:0;
  background:rgba(255,255,255,.94);-webkit-backdrop-filter:saturate(180%) blur(14px);backdrop-filter:saturate(180%) blur(14px)
}
.navbar-left{display:flex;align-items:center;gap:16px;min-width:0;flex:0 0 auto}
.navbar-brand{display:flex;align-items:center;gap:9px;min-width:0}
.navbar-brand-mark{
  width:30px;height:30px;border-radius:8px;display:grid;place-items:center;flex:0 0 auto;
  background:var(--ds-gray-900);color:#fff;font-size:14px;font-weight:700;letter-spacing:-.03em
}
.navbar-brand-copy{display:flex;flex-direction:column;line-height:1.05;white-space:nowrap}
.navbar-brand-copy strong{font-size:13px;font-weight:650;color:var(--ds-gray-900);letter-spacing:-.015em}
.navbar-brand-copy span{margin-top:3px;font-size:10px;font-weight:500;color:var(--ds-gray-600)}
.navbar-workspace{
  min-width:134px;padding-left:16px;border-left:1px solid var(--ds-border);
  display:flex;flex-direction:column;align-items:flex-start;gap:2px
}
.navbar-workspace-label{font-size:9px;line-height:11px;font-weight:650;letter-spacing:.055em;text-transform:uppercase;color:var(--ds-gray-600)}
.workspace-geist-select{width:154px;height:20px}
.workspace-geist-select .geist-select-trigger{
  width:100%;min-width:0;height:20px;padding:0;border:0;border-radius:4px;background:transparent;
  color:var(--ds-gray-900);font-size:12px;font-weight:550;box-shadow:none
}
.workspace-geist-select .geist-select-trigger:hover,.workspace-geist-select.is-open .geist-select-trigger{background:transparent}
.workspace-geist-select .geist-select-menu{top:35px;min-width:190px}
.overview-date-controls{display:flex;align-items:center;justify-content:flex-end;gap:8px}
.geist-date-picker{position:relative}
.geist-date-picker-trigger{
  min-width:172px;height:36px;padding:0 10px;display:flex;align-items:center;gap:8px;
  border:1px solid var(--ds-border);border-radius:var(--ds-radius);background:var(--ds-background-100);
  color:var(--ds-gray-800);font-size:13px;font-weight:500;box-shadow:var(--ds-shadow-small)
}
.geist-date-picker-trigger>span{min-width:0;flex:1;text-align:left;white-space:nowrap}
.geist-date-picker-trigger>svg:last-child{color:var(--ds-gray-600);transition:transform 150ms ease}
.geist-date-picker-trigger:hover,.geist-date-picker.is-open .geist-date-picker-trigger{border-color:var(--ds-border-hover);background:var(--ds-gray-100)}
.geist-date-picker.is-open .geist-date-picker-trigger>svg:last-child{transform:rotate(180deg)}
.geist-date-picker-panel{
  position:absolute;top:calc(100% + 8px);right:0;z-index:160;width:min(340px,calc(100vw - 28px));
  max-height:min(620px,calc(100dvh - 96px));overflow-y:auto;padding:8px;
  background:var(--ds-background-100);border:1px solid var(--ds-border);border-radius:var(--ds-radius-large);
  box-shadow:var(--ds-shadow-menu);animation:fu 150ms cubic-bezier(.16,1,.3,1)
}
.geist-date-presets{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:4px}
.geist-date-presets>button{
  min-height:36px;padding:0 8px;display:flex;align-items:center;justify-content:space-between;gap:8px;
  border:0;border-radius:6px;background:transparent;color:var(--ds-gray-700);font-size:12px;font-weight:500;text-align:left
}
.geist-date-presets>button:hover{background:var(--ds-gray-100);color:var(--ds-gray-900)}
.geist-date-presets>button.is-selected{background:var(--ds-gray-100);color:var(--ds-gray-900);font-weight:600}
.geist-date-calendar{margin-top:8px;padding-top:8px;border-top:1px solid var(--ds-border)}
.geist-date-calendar .react-datepicker{width:100%;border:0!important;border-radius:0!important;box-shadow:none!important}
.geist-date-calendar .react-datepicker__month-container{float:none;width:100%}
.geist-date-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;padding:8px 2px 0}
.geist-date-actions button{min-height:34px;padding:0 11px;border-radius:var(--ds-radius);font-size:12px;font-weight:550}
.geist-date-cancel{border:1px solid var(--ds-border);background:var(--ds-background-100);color:var(--ds-gray-700)}
.geist-date-apply{border:1px solid var(--ds-gray-900);background:var(--ds-gray-900);color:#fff}
.split-test-settings{position:relative}
.split-test-trigger{
  height:34px;padding:0 10px;display:flex;align-items:center;gap:7px;border:1px solid var(--ds-border);
  border-radius:var(--ds-radius);background:var(--ds-background-100);color:var(--ds-gray-700);
  font-size:12px;font-weight:550;box-shadow:var(--ds-shadow-small)
}
.split-test-trigger:hover,.split-test-settings.is-open .split-test-trigger{border-color:var(--ds-border-hover);background:var(--ds-gray-100);color:var(--ds-gray-900)}
.split-test-label-mobile{display:none}
.split-test-trigger>svg:last-child{transition:transform 150ms ease}
.split-test-settings.is-open .split-test-trigger>svg:last-child{transform:rotate(180deg)}
.split-test-popover{
  position:absolute;top:calc(100% + 8px);left:50%;z-index:160;width:min(300px,calc(100vw - 28px));transform:translateX(-50%);
  padding:8px;background:var(--ds-background-100);border:1px solid var(--ds-border);border-radius:var(--ds-radius-large);
  box-shadow:var(--ds-shadow-menu);animation:centered-popover-in 150ms cubic-bezier(.16,1,.3,1)
}
.split-test-popover-header{padding:8px;display:flex;flex-direction:column;gap:4px}
.split-test-popover-header strong{font-size:13px;font-weight:650;color:var(--ds-gray-900)}
.split-test-popover-header span{font-size:11px;line-height:1.45;color:var(--ds-gray-600)}
.split-test-current{margin:4px 2px 8px;padding:10px;display:flex;align-items:center;justify-content:space-between;gap:12px;border:1px solid var(--ds-border);border-radius:var(--ds-radius);background:var(--ds-background-200)}
.split-test-current span{font-size:11px;color:var(--ds-gray-600)}
.split-test-current strong{font-size:12px;font-weight:600;color:var(--ds-gray-900);white-space:nowrap}
.split-test-popover-actions{display:grid;grid-template-columns:repeat(auto-fit,minmax(100px,1fr));gap:6px}
.split-test-popover-actions button{min-height:34px;border:1px solid var(--ds-border);border-radius:var(--ds-radius);background:var(--ds-background-100);color:var(--ds-gray-800);font-size:12px;font-weight:550}
.split-test-popover-divider{height:1px;margin:8px 2px;background:var(--ds-border)}
.split-test-reg-pages{width:100%;min-height:48px;padding:7px 8px;display:flex;align-items:center;justify-content:space-between;gap:12px;border:0;border-radius:6px;background:transparent;color:var(--ds-gray-700);text-align:left}
.split-test-reg-pages:hover{background:var(--ds-gray-100);color:var(--ds-gray-900)}
.split-test-reg-pages>span{display:flex;flex-direction:column;gap:2px}
.split-test-reg-pages strong{font-size:12px;font-weight:600}
.split-test-reg-pages small{font-size:10px;color:var(--ds-gray-600)}
.lens-menu-root{position:relative}
.lens-menu-popover{
  position:absolute;top:calc(100% + 6px);left:50%;z-index:160;width:min(260px,calc(100vw - 28px));
  max-height:min(420px,calc(100dvh - 96px));overflow-y:auto;transform:translateX(-50%);padding:8px 0;
  background:var(--ds-background-100);border:1px solid var(--ds-border);border-radius:var(--ds-radius-large);
  box-shadow:var(--ds-shadow-menu);animation:centered-popover-in 150ms cubic-bezier(.16,1,.3,1)
}
@keyframes centered-popover-in{
  from{opacity:0;transform:translateX(-50%) translateY(6px)}
  to{opacity:1;transform:translateX(-50%) translateY(0)}
}
.navbar-current{display:none;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:13px;font-weight:600;color:var(--ds-gray-800)}
.navbar-routes{position:absolute;left:50%;transform:translateX(-50%);height:64px;display:flex;align-items:stretch;gap:8px;min-width:0}
.navbar-route{
  position:relative;height:64px;padding:0 12px;border:0;background:transparent;
  color:var(--ds-gray-700);font-size:13px;font-weight:500;white-space:nowrap
}
.navbar-route:hover{color:var(--ds-gray-900);background:var(--ds-gray-100)}
.navbar-route.is-active{color:var(--ds-gray-900);font-weight:600}
.navbar-route.is-active::after{content:'';position:absolute;left:12px;right:12px;bottom:-1px;height:2px;border-radius:2px 2px 0 0;background:var(--ds-gray-900)}
.navbar-actions{margin-left:auto;display:flex;align-items:center;gap:8px;min-width:max-content}
.icon-button,.app-navbar .mobile-menu-toggle{
  width:34px;height:34px;display:inline-flex;align-items:center;justify-content:center;padding:0;
  border:1px solid var(--ds-border);border-radius:var(--ds-radius);background:var(--ds-background-100);
  color:var(--ds-gray-700);box-shadow:var(--ds-shadow-small)
}
.icon-button:hover,.app-navbar .mobile-menu-toggle:hover{border-color:var(--ds-border-hover);background:var(--ds-gray-100);color:var(--ds-gray-900)}
.app-navbar .mobile-menu-toggle{display:none!important}
.navbar-dropdown{position:relative;height:34px}
.navbar-dropdown>summary{list-style:none}
.navbar-dropdown>summary::-webkit-details-marker{display:none}
.navbar-dropdown-trigger{
  height:34px;padding:0 10px;display:inline-flex;align-items:center;gap:5px;
  border:1px solid var(--ds-border);border-radius:var(--ds-radius);background:var(--ds-background-100);
  color:var(--ds-gray-700);font-size:12px;font-weight:550;box-shadow:var(--ds-shadow-small);cursor:pointer
}
.navbar-dropdown-trigger:hover,.navbar-dropdown[open] .navbar-dropdown-trigger{border-color:var(--ds-border-hover);background:var(--ds-gray-100);color:var(--ds-gray-900)}
.navbar-dropdown.has-active .navbar-dropdown-trigger{color:var(--ds-gray-900);border-color:var(--ds-gray-400);font-weight:650}
.navbar-dropdown[open] .navbar-dropdown-trigger svg{transform:rotate(180deg)}
.navbar-popover{
  position:absolute;top:49px;right:0;z-index:150;width:220px;padding:6px;
  background:var(--ds-background-100);border:1px solid var(--ds-border);border-radius:var(--ds-radius-large);
  box-shadow:var(--ds-shadow-menu);animation:fu 150ms cubic-bezier(.16,1,.3,1)
}
.navbar-popover-label{padding:7px 8px 6px;font-size:10px;font-weight:650;letter-spacing:.05em;text-transform:uppercase;color:var(--ds-gray-600)}
.navbar-popover button{
  width:100%;min-height:36px;padding:8px;display:flex;align-items:center;gap:9px;
  border:0;border-radius:6px;background:transparent;color:var(--ds-gray-700);font-size:12px;font-weight:500;text-align:left
}
.navbar-popover button:hover{background:var(--ds-gray-100);color:var(--ds-gray-900)}
.navbar-popover button.is-active{background:var(--ds-gray-900);color:#fff}
.navbar-popover-divider{height:1px;margin:5px 2px;background:var(--ds-border)}
.account-trigger{width:34px;height:34px;display:grid;place-items:center;cursor:pointer;border-radius:50%}
.account-trigger:hover .account-avatar,.account-dropdown[open] .account-avatar{border-color:var(--ds-border-hover);background:var(--ds-gray-200)}
.account-avatar{
  width:30px;height:30px;border-radius:50%;display:grid;place-items:center;flex:0 0 auto;
  background:var(--ds-gray-100);color:var(--ds-gray-800);font-size:12px;font-weight:650;border:1px solid var(--ds-border)
}
.account-popover{width:248px}
.account-popover-identity{padding:10px 9px 8px;display:flex;flex-direction:column;min-width:0}
.account-popover-identity strong{font-size:13px;font-weight:600;color:var(--ds-gray-900)}
.account-popover-identity span{margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;color:var(--ds-gray-600)}
.account-popover-identity small{align-self:flex-start;margin-top:8px;padding:2px 6px;border-radius:5px;background:var(--ds-gray-100);font-size:10px;font-weight:600;color:var(--ds-gray-700)}
.workspace-shell{width:100%;min-width:0;flex:1;display:flex;flex-direction:column}
.nav-menu-scrim{position:fixed;inset:64px 0 0;z-index:0;border:0;background:rgba(0,0,0,.26);-webkit-backdrop-filter:blur(2px);backdrop-filter:blur(2px)}
.nav-menu-panel{
  position:fixed;top:64px;left:0;right:0;z-index:1;max-height:calc(100dvh - 64px);overflow-y:auto;
  padding:20px 24px 24px;background:var(--ds-background-100);border-bottom:1px solid var(--ds-border);
  box-shadow:var(--ds-shadow-dialog);animation:nav-menu-in 180ms cubic-bezier(.16,1,.3,1)
}
@keyframes nav-menu-in{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:none}}
.nav-menu-header{display:flex;align-items:center;justify-content:space-between;gap:16px;padding-bottom:16px;border-bottom:1px solid var(--ds-border)}
.nav-menu-header>div{display:flex;flex-direction:column;min-width:0}
.nav-menu-header strong{font-size:15px;font-weight:650;color:var(--ds-gray-900)}
.nav-menu-header span{margin-top:3px;font-size:11px;color:var(--ds-gray-600)}
.nav-menu-close{width:34px;height:34px;display:grid;place-items:center;border:1px solid var(--ds-border);border-radius:var(--ds-radius);background:var(--ds-background-100);color:var(--ds-gray-700)}
.menu-workspace{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:16px 0;border-bottom:1px solid var(--ds-border)}
.menu-workspace>span{font-size:12px;font-weight:600;color:var(--ds-gray-700)}
.menu-workspace-geist-select{min-width:180px}
.menu-workspace-geist-select .geist-select-trigger{width:100%;min-width:180px}
.menu-workspace-geist-select .geist-select-menu{right:0;left:auto;min-width:200px}
.nav-menu-groups{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:24px;padding:20px 0}
.nav-menu-group{display:flex;flex-direction:column;gap:4px}
.nav-menu-label{padding:0 10px 5px;font-size:10px;font-weight:650;letter-spacing:.055em;text-transform:uppercase;color:var(--ds-gray-600)}
.nav-menu-group button,.nav-menu-utilities button{
  width:100%;min-height:40px;padding:0 10px;display:flex;align-items:center;gap:10px;
  border:1px solid transparent;border-radius:var(--ds-radius);background:transparent;color:var(--ds-gray-700);font-size:13px;font-weight:500;text-align:left
}
.nav-menu-group button:hover,.nav-menu-utilities button:hover{background:var(--ds-gray-100);color:var(--ds-gray-900)}
.nav-menu-group button.is-active{background:var(--ds-gray-900);border-color:var(--ds-gray-900);color:#fff}
.nav-menu-footer{padding-top:16px;border-top:1px solid var(--ds-border)}
.nav-menu-utilities{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-bottom:16px}
.nav-menu-utilities button{border-color:var(--ds-border);background:var(--ds-background-100);box-shadow:var(--ds-shadow-small)}
.nav-menu-account{display:flex;align-items:center;gap:10px;padding:12px;border-radius:var(--ds-radius-large);background:var(--ds-background-200);border:1px solid var(--ds-border)}
.nav-menu-account>div{min-width:0;flex:1;display:flex;flex-direction:column}
.nav-menu-account strong{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;font-weight:600;color:var(--ds-gray-900)}
.nav-menu-account>div span{margin-top:3px;font-size:11px;color:var(--ds-gray-600)}
.nav-menu-account>button{width:34px;height:34px;display:grid;place-items:center;border:0;border-radius:6px;background:transparent;color:var(--ds-gray-600)}
.nav-menu-account>button:hover{background:var(--ds-red-100);color:var(--ds-red-600)}

/* ─── Core surfaces and controls ──────────────────────────────── */
.main-content{width:100%}
.main-content.chat-main{padding:0!important;max-width:none!important;margin:0!important;overflow:hidden}
.fi{animation-duration:220ms}
.title-row h1{font-feature-settings:'ss01' 1,'ss06' 1}
.summary-strip{isolation:isolate}
.summary-strip>div{transition:background-color 150ms ease}
.summary-strip>div:not(.strip-pad):hover{background:var(--ds-background-200)!important}
.summary-strip .strip-pad{display:none!important}
.summary-card{position:relative;min-height:168px;overflow:hidden}
.summary-sparkline{
  position:relative;width:calc(100% + 40px);height:72px;margin:8px -20px -20px;
  overflow:hidden;outline:none;touch-action:pan-y;background:linear-gradient(180deg,transparent,rgba(0,0,0,.012))
}
.summary-sparkline svg{display:block;width:100%;height:100%;overflow:visible}
.summary-sparkline-baseline{stroke:var(--ds-border);stroke-width:1;vector-effect:non-scaling-stroke}
.summary-sparkline-area{pointer-events:none}
.summary-sparkline-line{fill:none;stroke:var(--summary-chart-accent);stroke-width:2;vector-effect:non-scaling-stroke;pointer-events:none}
.summary-sparkline-single-dot{fill:var(--summary-chart-accent);stroke:var(--ds-background-100);stroke-width:2;vector-effect:non-scaling-stroke;pointer-events:none}
.summary-sparkline-rule{stroke:var(--ds-gray-400);stroke-width:1;stroke-dasharray:2 3;vector-effect:non-scaling-stroke;pointer-events:none}
.summary-sparkline-dot{fill:var(--ds-background-100);stroke:var(--summary-chart-accent);stroke-width:2;vector-effect:non-scaling-stroke;pointer-events:none}
.summary-sparkline-readout{
  position:absolute;top:4px;z-index:2;min-width:max-content;padding:4px 6px;display:flex;align-items:center;gap:5px;
  border:1px solid var(--ds-border);border-radius:5px;background:rgba(255,255,255,.96);box-shadow:var(--ds-shadow-small);
  color:var(--ds-gray-600);font-size:10px;line-height:1.2;pointer-events:none
}
.summary-sparkline-readout strong{color:var(--ds-gray-900);font-weight:650;font-variant-numeric:tabular-nums}
.summary-sparkline:focus-visible{outline:2px solid var(--ds-focus);outline-offset:-3px}
.summary-sparkline.is-empty{display:flex;align-items:center;justify-content:center;color:var(--ds-gray-500);font-size:11px}
.toolbar-row{padding:12px;background:var(--ds-background-100);border:1px solid var(--ds-border);border-radius:var(--ds-radius-large)}
.overview-toolbar{justify-content:center!important;align-items:center!important}
.overview-toolbar .toolbar-controls{justify-content:center!important}
.overview-utility-actions{display:contents}
.list-board-toggle,.variant-toggle{min-height:34px}
.list-board-toggle button,.variant-toggle button{border:0;margin:0;min-height:28px}
.search-wrap:focus-within{border-color:var(--ds-border-hover)!important;box-shadow:0 0 0 3px rgba(0,0,0,.06)!important}
.table-wrap{scrollbar-gutter:stable}
.overview-table-wrap{
  width:calc(100% + 48px)!important;max-width:none!important;margin-inline:-24px;
  border-left:0!important;border-right:0!important;border-radius:0!important
}
@media (min-width:769px) and (max-width:1024px){
  .overview-table-wrap{width:calc(100% + 40px)!important;margin-inline:-20px}
}
table{font-variant-numeric:tabular-nums}
thead th{position:sticky;top:0;z-index:1}
tbody tr{transition:background-color 120ms ease}
tbody tr:hover{background:var(--ds-background-200)}
.trow:nth-child(even){background:var(--ds-background-100)}
.trow:hover{background:var(--ds-background-200)!important}
.trow.selected,.trow.selected:hover,.trow.selected:nth-child(even){
  background:var(--ds-blue-100)!important;box-shadow:none
}
.trow.selected .sticky-col,.trow.selected:hover .sticky-col,.trow.selected:nth-child(even) .sticky-col{background:var(--ds-blue-100)!important}
.trow .sticky-col{background:var(--ds-background-100)}
.trow:nth-child(even) .sticky-col{background:var(--ds-background-100)}
.trow:hover .sticky-col{background:var(--ds-background-200)!important}
.rowBtn:focus-visible{opacity:1}
code,pre{font-family:'Geist Mono',ui-monospace,SFMono-Regular,Menlo,monospace}
.form-card{border-radius:var(--ds-radius-large)!important}
.form-grid input{font-variant-numeric:tabular-nums}
.form-section{border:0;padding:22px 0;margin:0;border-top:1px solid var(--ds-border)}
.form-section legend{float:left;width:100%;font-size:14px;font-weight:600;color:var(--ds-gray-900);letter-spacing:-.01em}
.form-section>p{clear:both;padding-top:4px;margin-bottom:14px;font-size:12px;line-height:1.4;color:var(--ds-gray-600)}
.form-section-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}
.modal-backdrop{animation:backdrop-in 150ms ease-out}
.modal-inner{animation:dialog-in 180ms cubic-bezier(.16,1,.3,1)}
@keyframes backdrop-in{from{opacity:0}to{opacity:1}}
@keyframes dialog-in{from{opacity:0;transform:translateY(8px) scale(.985)}to{opacity:1;transform:none}}
.floating-insights{
  position:fixed;right:24px;bottom:20px;z-index:110;display:flex;flex-direction:column;align-items:flex-end;gap:10px
}
.floating-insights-panel{
  position:absolute;right:0;bottom:58px;width:min(430px,calc(100vw - 32px));height:min(650px,calc(100dvh - 112px));
  overflow:hidden;border:1px solid var(--ds-border);border-radius:14px;background:var(--ds-background-100);
  box-shadow:0 20px 48px rgba(0,0,0,.16),0 2px 8px rgba(0,0,0,.08);animation:dialog-in 180ms cubic-bezier(.16,1,.3,1)
}
.floating-insights-chat{height:100%!important}
.floating-insights-launcher{
  min-height:46px;padding:0 14px 0 8px;display:inline-flex;align-items:center;gap:9px;border:1px solid var(--ds-gray-900);
  border-radius:24px;background:var(--ds-gray-900);color:#fff;box-shadow:0 10px 28px rgba(0,0,0,.2);font-size:13px;font-weight:600
}
.floating-insights-launcher-icon{width:30px;height:30px;display:grid;place-items:center;border-radius:50%;background:rgba(255,255,255,.12)}
.floating-insights-launcher-label{white-space:nowrap}
.floating-insights-chat pre{max-width:100%;overflow-x:auto}
.md-table-wrap{
  -webkit-overflow-scrolling:touch;overscroll-behavior-inline:contain;scrollbar-width:thin;
  scrollbar-color:var(--ds-gray-400) transparent
}
.md-table{font-variant-numeric:tabular-nums}
.md-table :is(th,td){white-space:nowrap;vertical-align:middle}
.md-table :is(th,td).is-numeric{text-align:right!important;white-space:nowrap}
.md-table :is(th,td).is-primary{
  position:sticky;left:0;z-index:1;min-width:160px;max-width:280px;white-space:normal;
  overflow-wrap:anywhere;background:var(--ds-background-100);box-shadow:1px 0 0 var(--ds-border)
}
.md-table thead th.is-primary{z-index:3;background:var(--ds-background-200)}
.md-table tbody tr:nth-child(even) td.is-primary{background:var(--ds-background-200)}
.md-table tbody tr:hover td.is-primary{background:var(--ds-background-200)}
.md-table-wrap:focus-visible{outline-offset:2px!important}
.floating-insights-chat .md-table-wrap{margin:10px 0!important;border-radius:7px!important}
.floating-insights-chat .md-table{font-size:11.5px!important;line-height:1.35}
.floating-insights-chat .md-table th{
  padding:6px 7px!important;font-size:10.5px!important;line-height:1.25;white-space:normal;vertical-align:bottom
}
.floating-insights-chat .md-table td{padding:7px!important;vertical-align:top}
.floating-insights-chat .md-table :is(th,td).is-primary{min-width:112px;max-width:138px}
.floating-insights-chat .md-table-wrap.is-wide .md-table{font-size:11px!important}
.floating-insights-chat .md-table-wrap.is-wide .md-table :is(th,td){padding-inline:6px!important}
.floating-insights-chat h1{font-size:17px!important;margin:12px 0 6px!important}
.floating-insights-chat h2{font-size:15px!important;margin:12px 0 5px!important}
.floating-insights-chat h3{font-size:14px!important;margin:10px 0 4px!important}
.insights-input-card textarea:focus,.insights-input-card textarea:focus-visible{outline:none!important;box-shadow:none!important}
.toast-el{right:24px!important;left:auto!important;transform:none!important;bottom:82px!important}
.auth-shell{
  background-color:var(--ds-background-200);
  background-image:linear-gradient(var(--ds-border) 1px,transparent 1px),linear-gradient(90deg,var(--ds-border) 1px,transparent 1px);
  background-size:32px 32px;background-position:center center
}
.auth-shell::before{content:'';position:fixed;inset:0;pointer-events:none;background:radial-gradient(circle at center,rgba(250,250,250,.25) 0%,var(--ds-background-200) 72%)}
.login-form{position:relative;border-radius:var(--ds-radius-large)!important;border-color:var(--ds-border)!important;box-shadow:0 12px 32px rgba(0,0,0,.06)!important}

/* Geist date picker */
.react-datepicker{font-family:'Geist',sans-serif!important;border:1px solid var(--ds-border)!important;border-radius:var(--ds-radius-large)!important;box-shadow:var(--ds-shadow-menu)!important;overflow:hidden}
.react-datepicker__header{background:var(--ds-background-200)!important;border-bottom:1px solid var(--ds-border)!important}
.react-datepicker__day--selected,.react-datepicker__day--in-range{background:var(--ds-gray-900)!important;color:#fff!important}
.react-datepicker__day--keyboard-selected{background:var(--ds-gray-200)!important;color:var(--ds-gray-900)!important}
.react-datepicker__day:hover{border-radius:6px!important;background:var(--ds-gray-100)!important}

@media (max-width:1180px){
  .navbar-routes,.navbar-workspace,.desktop-only{display:none!important}
  .app-navbar .mobile-menu-toggle{display:inline-flex!important}
  .navbar-current{display:block}
  .navbar-left{flex:1}
}
@media (max-width:768px){
  .app-navbar{height:58px;flex-basis:58px}
  .navbar-main{height:58px;padding:0 14px;gap:10px}
  .navbar-left{gap:10px}
  .navbar-brand-copy span{display:none}
  .navbar-current{padding-left:10px;border-left:1px solid var(--ds-border)}
  .nav-menu-scrim{inset:58px 0 0}
  .nav-menu-panel{top:58px;max-height:calc(100dvh - 58px);padding:18px 14px 20px}
  .nav-menu-groups{grid-template-columns:1fr;gap:20px}
  .menu-workspace{align-items:stretch;flex-direction:column;gap:7px}
  .menu-workspace-geist-select,.menu-workspace-geist-select .geist-select-trigger{width:100%;min-width:0}
  .menu-workspace-geist-select .geist-select-menu{position:static;width:100%;max-width:none;margin-top:6px}
  .overview-date-controls{width:100%}
  .geist-date-picker,.geist-date-picker-trigger{width:100%}
  .main-content{padding:20px 14px 48px!important}
  .summary-strip{grid-template-columns:repeat(2,minmax(0,1fr))!important}
  .summary-card{min-height:138px}
  .summary-sparkline{width:calc(100% + 28px);height:58px;margin:6px -14px -12px}
  .toolbar-row{padding:10px!important}
  .overview-toolbar{position:relative;padding:6px!important}
  .overview-toolbar .toolbar-controls{display:flex!important;flex-wrap:nowrap!important;justify-content:center!important;align-items:center!important;gap:6px!important;grid-template-columns:none!important}
  .overview-toolbar .overview-search{display:none!important}
  .overview-utility-actions{position:static;display:flex;flex:0 0 auto;align-items:center;justify-content:center;gap:4px}
  .overview-utility-actions .column-editor-trigger{order:1}
  .overview-utility-actions .split-test-settings{position:static;order:2;flex:0 0 auto;display:block}
  .overview-utility-actions .lens-menu-root{position:static;order:3;flex:0 0 auto;display:block}
  .overview-utility-actions .column-editor-trigger,.overview-utility-actions .lens-menu-trigger{
    width:34px;min-width:34px;height:34px;min-height:34px;padding:0!important;justify-content:center;gap:0
  }
  .overview-utility-actions .toolbar-action-label,.overview-utility-actions .toolbar-action-caret{display:none}
  .overview-utility-actions .split-test-trigger{padding:0 7px;gap:5px}
  .overview-utility-actions .split-test-label-desktop{display:none}
  .overview-utility-actions .split-test-label-mobile{display:inline}
  .overview-utility-actions .split-test-trigger>svg:last-child{display:none}
  .overview-utility-actions .split-test-popover,.overview-utility-actions .lens-menu-popover{
    left:50%;right:auto;max-width:calc(100vw - 48px);transform:translateX(-50%)
  }
  .overview-table-wrap{width:calc(100% + 28px)!important;margin-inline:-14px}
  .variant-toggle{display:flex!important;flex:0 1 auto;width:auto!important;min-width:0;overflow:visible;padding:2px!important;gap:1px!important}
  .variant-toggle button{flex:0 0 auto!important;height:28px;padding:0 6px!important;justify-content:center;white-space:nowrap;font-size:11.5px!important}
  .variant-toggle button:nth-child(2),.variant-toggle button:nth-child(3){width:28px;padding-inline:0!important}
  .boardGrid{grid-template-columns:1fr!important}
  .form-section-grid{grid-template-columns:1fr}
  .repeater-row{flex-wrap:wrap}
  .repeater-row input,.repeater-row select{width:100%!important;flex:1 1 100%!important}
  .repeater-row button{margin-left:auto}
  .dialog-tabs{max-width:100%;overflow-x:auto;scrollbar-width:none}
  .dialog-tabs::-webkit-scrollbar{display:none}
  .dialog-tabs button{flex:0 0 auto}
  .modal-backdrop{align-items:flex-end!important}
  .modal-inner{
    width:100%!important;max-width:none!important;max-height:92dvh!important;margin:0!important;
    border-radius:14px 14px 0 0!important;padding:24px 18px!important
  }
  .modal-inner.structured-dialog{padding:0!important}
  .main-content.chat-main{padding:0!important}
  .floating-insights{right:14px;bottom:14px}
  .floating-insights-panel{
    position:fixed;left:10px;right:10px;bottom:70px;width:auto;height:min(700px,calc(100dvh - 82px));
    border-radius:14px
  }
  .toast-el{left:14px!important;right:14px!important;bottom:76px!important;max-width:none!important}
  .insights-outer{height:calc(100dvh - 58px)!important}
  .floating-insights-chat{height:100%!important}
}
@media (max-width:480px){
  .overview-toolbar .variant-toggle{display:flex!important}
  .overview-toolbar .variant-toggle button{width:auto}
  .overview-toolbar .variant-toggle button:nth-child(2),.overview-toolbar .variant-toggle button:nth-child(3){width:28px}
}
@media (max-width:360px){
  .overview-toolbar .toolbar-controls{gap:4px!important}
  .overview-utility-actions{gap:3px}
  .overview-utility-actions .column-editor-trigger,.overview-utility-actions .lens-menu-trigger,.overview-utility-actions .split-test-trigger{
    width:32px;min-width:32px;height:32px;min-height:32px;padding:0!important
  }
  .overview-utility-actions .split-test-label-mobile{display:none}
  .overview-toolbar .variant-toggle button{height:27px;padding-inline:5px!important;font-size:11px!important}
  .overview-toolbar .variant-toggle button:nth-child(2),.overview-toolbar .variant-toggle button:nth-child(3){width:27px;padding-inline:0!important}
}
@media (max-width:380px){
  .summary-strip{grid-template-columns:1fr!important}
  .navbar-brand-copy{display:none}
  .topbar-text-action{width:34px!important;padding:0!important}
  .topbar-text-action span{display:none}
}
@media (prefers-reduced-motion:reduce){
  *,*::before,*::after{animation-duration:.01ms!important;animation-iteration-count:1!important;scroll-behavior:auto!important;transition-duration:.01ms!important}
}
`;

const S = {
  app: { minHeight: "100dvh", background: "var(--ds-background-200)", color: "var(--ds-gray-900)", fontFamily: fn },
  hdr: { padding: "16px 32px", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid var(--ds-border)", background: "#FFFFFF", position: "sticky", top: 0, zIndex: 100 },
  logoMark: { width: 32, height: 32, background: "#F97316", borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center" },
  logoT: { fontSize: 15, fontWeight: 700, color: "var(--ds-gray-900)", letterSpacing: "-0.01em" },
  logoSub: { fontSize: 11, color: "var(--ds-gray-700)", fontWeight: 500, display: "none" }, // Hidden for cleaner look
  tzPill: { display: "inline-flex", alignItems: "center", gap: 5, padding: "5px 12px", background: "var(--ds-gray-100)", color: "var(--ds-gray-700)", fontSize: 12, fontWeight: 600, borderRadius: 100 },
  main: { width: "100%", maxWidth: "none", margin: 0, padding: "28px 24px 64px" },
  titleRow: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24, gap: 16, flexWrap: "wrap" },
  pageTitle: { fontSize: 24, fontWeight: 600, color: "var(--ds-gray-900)", letterSpacing: "-0.03em", lineHeight: 1.25 },
  pageSub: { fontSize: 13, color: "var(--ds-gray-700)", fontWeight: 400, marginTop: 5, display: "block", lineHeight: 1.45 },
  // Grid (not flex) so columns are always uniform and dividers align across rows.
  // The 1px gap on a colored parent doubles as the divider; cells have white bg.
  strip: { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 1, border: "1px solid var(--ds-border)", borderRadius: 12, background: "var(--ds-border)", marginBottom: 24, overflow: "hidden", boxShadow: "var(--ds-shadow-small)" },
  stripCell: { background: "var(--ds-background-100)", padding: "20px", display: "flex", flexDirection: "column", gap: 8, minWidth: 0 },
  stripLabel: { fontSize: 12, fontWeight: 500, color: "var(--ds-gray-700)" },
  stripValRow: { display: "flex", alignItems: "baseline", gap: 12 },
  stripVal: { fontSize: 28, fontWeight: 600, color: "var(--ds-gray-900)", letterSpacing: "-0.035em", lineHeight: 1, fontVariantNumeric: "tabular-nums" },
  pctBadge: { display: "inline-flex", alignItems: "center", gap: 2, padding: "2px 6px", fontSize: 12, fontWeight: 600, borderRadius: 4 },
  toolbar: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 },
  listBoardToggle: { display: "flex", background: "var(--ds-gray-100)", padding: 3, borderRadius: 8, border: "1px solid var(--ds-border)", gap: 2 },
  listToggleActive: { background: "var(--ds-background-100)", padding: "6px 12px", borderRadius: 6, fontSize: 13, fontWeight: 500, color: "var(--ds-gray-900)", display: "flex", alignItems: "center", gap: 6, boxShadow: "var(--ds-shadow-small)", whiteSpace: "nowrap" },
  listToggleInactive: { background: "transparent", padding: "6px 12px", borderRadius: 6, fontSize: 13, fontWeight: 500, color: "var(--ds-gray-700)", display: "flex", alignItems: "center", gap: 6, cursor: "pointer", whiteSpace: "nowrap" },
  searchWrap: { display: "flex", alignItems: "center", gap: 8, padding: "8px 11px", minHeight: 36, background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", borderRadius: 8, width: 260, boxShadow: "var(--ds-shadow-small)" },
  searchInput: { border: "none", outline: "none", background: "transparent", fontFamily: fn, fontSize: 13, color: "var(--ds-gray-900)", width: "100%", fontWeight: 400 },
  tableWrap: { background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", overflowX: "auto", boxShadow: "var(--ds-shadow-small)", width: "100%", maxWidth: "100%", borderRadius: 12 },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth: 900 },
  th: { padding: "11px 14px", textAlign: "center", verticalAlign: "middle", fontSize: 12, fontWeight: 550, color: "var(--ds-gray-700)", textTransform: "none", borderBottom: "1px solid var(--ds-border)", background: "var(--ds-background-200)", whiteSpace: "normal", lineHeight: 1.35 },
  td: { padding: "11px 14px", borderBottom: "1px solid var(--ds-border)", verticalAlign: "middle", textAlign: "center" },
  tdNum: { padding: "11px 14px", borderBottom: "1px solid var(--ds-border)", fontWeight: 500, fontVariantNumeric: "tabular-nums", color: "var(--ds-gray-900)", fontSize: 13, textAlign: "center" },
  tdMoney: { padding: "11px 14px", borderBottom: "1px solid var(--ds-border)", fontWeight: 500, fontVariantNumeric: "tabular-nums", color: "var(--ds-gray-900)", fontSize: 13, textAlign: "center" },
  dayPill: { display: "inline-block", padding: "3px 8px", background: "var(--ds-gray-100)", color: "var(--ds-gray-700)", fontSize: 11, fontWeight: 600, borderRadius: 6, letterSpacing: "0.02em", textTransform: "uppercase", border: "1px solid var(--ds-border)" },
  todayDot: { display: "inline-block", width: 6, height: 6, borderRadius: "50%", background: "#F97316", marginLeft: 4, verticalAlign: "middle" },
  purchBadge: { display: "inline-block", padding: "2px 7px", background: "var(--ds-green-100)", color: "var(--ds-green-600)", fontWeight: 600, borderRadius: 5, border: "1px solid rgba(10,127,67,.16)" },
  thHighlight: { background: "var(--ds-green-100)", color: "var(--ds-green-600)", borderBottom: "1px solid rgba(10,127,67,.18)" },
  tdHighlight: { background: "rgba(238,251,243,.72)" },
  emptyTd: { padding: 56, textAlign: "center", color: "var(--ds-gray-600)", fontSize: 14, fontWeight: 450 },
  rowAct: { background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", borderRadius: 6, cursor: "pointer", padding: "6px", display: "inline-flex", alignItems: "center", boxShadow: "var(--ds-shadow-small)" },
  btnDark: { minHeight: 34, display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 6, padding: "7px 14px", fontSize: 13, fontWeight: 550, fontFamily: fn, background: "var(--ds-gray-900)", color: "#fff", border: "1px solid var(--ds-gray-900)", borderRadius: 8, cursor: "pointer", boxShadow: "var(--ds-shadow-small)", whiteSpace: "nowrap" },
  btnLight: { minHeight: 34, display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 6, padding: "7px 12px", fontSize: 13, fontWeight: 500, fontFamily: fn, background: "var(--ds-background-100)", color: "var(--ds-gray-900)", border: "1px solid var(--ds-border)", borderRadius: 8, cursor: "pointer", boxShadow: "var(--ds-shadow-small)", whiteSpace: "nowrap" },
  btnGhost: { minHeight: 34, display: "inline-flex", alignItems: "center", justifyContent: "center", padding: "7px 9px", background: "transparent", color: "var(--ds-gray-700)", border: "1px solid transparent", borderRadius: 8, cursor: "pointer" },
  overlay: { position: "fixed", inset: 0, background: "rgba(0,0,0,.38)", display: "flex", alignItems: "center", justifyContent: "center", padding: 20, zIndex: 200, backdropFilter: "blur(4px)" },
  modal: { background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", borderRadius: 12, padding: 28, textAlign: "center", maxWidth: 400, width: "90%", boxShadow: "var(--ds-shadow-dialog)" },
  toast: { position: "fixed", bottom: 24, right: 24, background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", borderRadius: 10, padding: "12px 16px", display: "flex", alignItems: "center", gap: 10, boxShadow: "var(--ds-shadow-menu)", fontSize: 13, fontWeight: 500, color: "var(--ds-gray-900)", zIndex: 300, maxWidth: 420 },
  fc: { maxWidth: 820, margin: "0 auto" },
  fh: { textAlign: "center", marginBottom: 32 },
  formBadge: { display: "none" },
  ft: { fontSize: 24, fontWeight: 600, color: "var(--ds-gray-900)", letterSpacing: "-0.03em", marginBottom: 8 },
  fs: { fontSize: 14, color: "var(--ds-gray-700)", maxWidth: 440, margin: "0 auto", lineHeight: 1.5 },
  fcard: { background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", borderRadius: 12, padding: 28, boxShadow: "var(--ds-shadow-small)" },
  fgrid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 },
  fl: { display: "block", fontSize: 12, fontWeight: 550, color: "var(--ds-gray-800)", marginBottom: 6 },
  hint: { fontSize: 12, color: "var(--ds-gray-600)", marginTop: 6, lineHeight: 1.45 },
  inp: { width: "100%", minHeight: 40, padding: "9px 12px", background: "var(--ds-background-100)", border: "1px solid var(--ds-border-hover)", borderRadius: 8, fontFamily: fn, fontSize: 14, fontWeight: 400, color: "var(--ds-gray-900)", transition: "border-color 0.15s ease, box-shadow .15s ease", boxShadow: "var(--ds-shadow-small)" },
  fa: { display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 28, paddingTop: 20, borderTop: "1px solid var(--ds-border)" },
  fmtB: { minHeight: 34, padding: "7px 13px", fontSize: 13, fontWeight: 500, fontFamily: fn, background: "var(--ds-background-100)", color: "var(--ds-gray-700)", border: "1px solid var(--ds-border)", borderRadius: 6, cursor: "pointer" },
  fmtA: { minHeight: 34, padding: "7px 13px", fontSize: 13, fontWeight: 550, fontFamily: fn, background: "var(--ds-gray-900)", color: "#fff", border: "1px solid var(--ds-gray-900)", borderRadius: 6, cursor: "pointer" },
  prev: { padding: 24, background: "var(--ds-background-200)", border: "1px dashed var(--ds-border-hover)", borderRadius: 8, textAlign: "center" },
  boardGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 16 },
  boardCard: { background: "var(--ds-background-100)", border: "1px solid var(--ds-border)", borderRadius: 12, padding: 20, boxShadow: "var(--ds-shadow-small)", display: "flex", flexDirection: "column" },
  boardCardHeader: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14, borderBottom: "1px solid var(--ds-border)", paddingBottom: 14 },
  boardCardBody: { display: "flex", flexDirection: "column", gap: 12, flex: 1 },
  bcItem: { display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 13 },
  bcLabel: { color: "var(--ds-gray-700)", fontWeight: 450 },
  bcVal: { color: "var(--ds-gray-900)", fontWeight: 600, fontVariantNumeric: "tabular-nums" },
  boardCardActions: { display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 16, paddingTop: 14, borderTop: "1px solid var(--ds-border)" },
};
