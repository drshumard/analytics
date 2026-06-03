// ============================================================================
// Dr Shumard Analytics — Production Frontend (Redesigned)
// Connects to Express API. No window.storage.
// ============================================================================

import { useState, useEffect, useCallback, useRef } from "react";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import { parse, startOfDay, endOfDay, subDays } from "date-fns";
import { createClient } from "@supabase/supabase-js";

// ─── Supabase Client ─────────────────────────────────────────────────────────
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

const getAuthHeaders = async () => {
  const { data: { session } } = await supabase.auth.getSession();
  const headers = { "Content-Type": "application/json", "X-Funnel": getActiveFunnel() };
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
  async chat(messages) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/insights/chat`, { method: "POST", headers, body: JSON.stringify({ messages }) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Chat failed: ${res.status}`); }
    return res.json();
  },
  async getConversations() {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/insights/conversations`, { headers });
    if (!res.ok) throw new Error(`Failed to fetch conversations: ${res.status}`);
    return res.json();
  },
  async saveConversation(id, title, messages) {
    const headers = await getAuthHeaders();
    const res = await fetch(`${API_BASE}/api/insights/conversations/${id}`, { method: "PUT", headers, body: JSON.stringify({ title, messages }) });
    if (!res.ok) throw new Error(`Failed to save conversation: ${res.status}`);
    return res.json();
  },
  async deleteConversation(id) {
    const headers = await getAuthHeaders();
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
  async getCrmContacts(search = "", stage = "", limit = 100, offset = 0) {
    const headers = await getAuthHeaders();
    const qs = new URLSearchParams({ limit, offset });
    if (search) qs.set("search", search);
    if (stage) qs.set("stage", stage);
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
const MK = ["fb_spend", "fb_link_clicks", "registrations", "replays", "viewedcta", "clickedcta", "purchases", "purchases_fb", "purchases_native", "purchases_youtube", "purchases_aibot", "purchases_postwebinar", "purchases_cpa", "stayed_45", "stayed_60", "stayed_80", "total_purchases", "attended"];
const COL_LABELS = { fb_spend: "FB Spend", fb_link_clicks: "Total Reg. Page Visited", registrations: "Registra​tions", attended: "Attended", replays: "Replays", viewedcta: "Viewed CTA", clickedcta: "Clicked CTA", purchases_fb: "FB Purchases", purchases_native: "Native Ads", purchases_youtube: "Youtube", purchases_aibot: "AI Chat Bot", purchases_postwebinar: "Post Webinar", purchases_cpa: "CPA Traffic Funnel", stayed_45: "45 min", stayed_60: "60 min", stayed_80: "80 min", total_purchases: "Total Purchases" };
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
  { key: "purchases_youtube", label: "Youtube", defaultFormat: "number" },
  { key: "purchases_aibot", label: "AI Chat Bot", defaultFormat: "number" },
  { key: "purchases_postwebinar", label: "Post Webinar", defaultFormat: "number" },
  { key: "purchases_cpa", label: "CPA Traffic Funnel", defaultFormat: "number" },
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

const I = ({ d, size = 16, stroke = "currentColor", sw = 1.8 }) => (<svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={stroke} strokeWidth={sw} strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>);

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

export default function App() {
  const isMobile = useIsMobile();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [session, setSession] = useState(null);
  const [userRole, setUserRole] = useState(null);
  const [activeFunnel, setActiveFunnelState] = useState(() => getActiveFunnel());
  const [allowedFunnels, setAllowedFunnels] = useState([activeFunnel]);
  const [variantFilter, setVariantFilter] = useState("all"); // 'all' | 'A' | 'B' | 'undetected' — split-test toggle (funnels with variant data)
  const [abTestStart, setAbTestStartState] = useState(null); // ISO string or null — variants only count from here
  const [authLoading, setAuthLoading] = useState(true);
  const [authEmail, setAuthEmail] = useState("");
  const [authPass, setAuthPass] = useState("");
  const [authError, setAuthError] = useState("");
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
  const [events, setEvents] = useState([]);
  const [evFilter, setEvFilter] = useState("");
  // CRM (people spine: tracking_contacts ⋈ events by email)
  const [crmContacts, setCrmContacts] = useState([]);
  const [crmStats, setCrmStats] = useState(null);
  const [crmTotal, setCrmTotal] = useState(0);
  const [crmOffset, setCrmOffset] = useState(0);
  const [crmSearch, setCrmSearch] = useState("");
  const [crmStage, setCrmStage] = useState("");
  const [crmLoading, setCrmLoading] = useState(false);
  const [crmContact, setCrmContact] = useState(null);
  const [crmContactTab, setCrmContactTab] = useState("journey");
  const [emailReport, setEmailReport] = useState(null);
  const [emailReportLoading, setEmailReportLoading] = useState(false);
  const [emailDrill, setEmailDrill] = useState(null);        // { source, ...clicks } or null
  const [emailDrillLoading, setEmailDrillLoading] = useState(false);
  const [lenses, setLenses] = useState([]);
  const [activeLensId, setActiveLensId] = useState("default-all");
  const [lensMenuOpen, setLensMenuOpen] = useState(false);
  const [lensEditing, setLensEditing] = useState(null); // null | { id?, name, metrics }
  const [summaryCards, setSummaryCards] = useState(DEFAULT_SUMMARY_CARDS);
  const [summaryEditorOpen, setSummaryEditorOpen] = useState(false);
  const lensMenuRef = useRef(null);
  const activeLens = (() => {
    const found = lenses.find(l => l.id === activeLensId);
    if (!found) return lenses[0] || { id: "default-all", name: "All Metrics", metrics: MK };
    return found;
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
  useEffect(() => { const h = (e) => { if (lensMenuRef.current && !lensMenuRef.current.contains(e.target)) setLensMenuOpen(false); }; document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h); }, []);
  // Close mobile menu on nav
  useEffect(() => { setMobileMenuOpen(false); }, [view]);
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
      const list = await api.getCrmContacts(crmSearch, crmStage, CRM_PAGE, crmOffset);
      setCrmContacts(list.data || []);
      setCrmTotal(list.total || 0);
    } catch (e) { flash(e.message, "err"); }
    setCrmLoading(false);
  }, [crmSearch, crmStage, crmOffset]);
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
  // Reset to the first page whenever the search/stage filter changes.
  useEffect(() => { setCrmOffset(0); }, [crmSearch, crmStage]);
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
    const { error } = await supabase.auth.signInWithPassword({ email: authEmail, password: authPass });
    if (error) setAuthError(error.message);
  };

  const handleLogout = async () => {
    await supabase.auth.signOut();
    setSession(null);
    setUserRole(null);
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

  // When rows are selected, scope cards/totals/averages to that set only.
  // Otherwise fall back to the date-filtered card view. Selection is sourced
  // from tableRows (which ignores the date filter), so a selected day is
  // honored even if it's outside the current card-scope filter.
  const effectiveRows = selectedDates.size > 0
    ? tableRows.filter(r => selectedDates.has(r.date))
    : displayRows;
  const totals = effectiveRows.reduce((a, r) => { MK.forEach(k => { a[k] = (a[k] || 0) + (Number(r[k]) || 0); }); return a; }, {});
  const averages = {};
  if (effectiveRows.length > 0) { MK.forEach(k => { averages[k] = Math.round(((totals[k] || 0) / effectiveRows.length) * 100) / 100; }); }

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
    // Custom metric keys are prefixed with "cm:"
    if (card.key.startsWith("cm:")) {
      const cmId = card.key.slice(3);
      const cm = customs.find(c => String(c.id) === cmId);
      if (!cm || effectiveRows.length === 0) return formatSummaryVal(0, card.format);
      const vals = effectiveRows.map(row => {
        const ctx = evalAllCustoms(customs, row);
        return ctx[cm.name] ?? 0;
      });
      const sum = vals.reduce((a, v) => a + (Number(v) || 0), 0);
      const raw = card.agg === "avg" ? sum / effectiveRows.length : sum;
      return formatSummaryVal(Math.round(raw * 100) / 100, card.format);
    }
    const raw = card.agg === "avg" ? (averages[card.key] || 0) : (totals[card.key] || 0);
    return formatSummaryVal(raw, card.format);
  };

  // % change compares recent half vs older half. With an active selection
  // it's not meaningful (sample is too small / arbitrary), so we suppress it.
  const half = Math.floor(displayRows.length / 2);
  const recent = displayRows.slice(0, half || 1);
  const older = displayRows.slice(half || 1);
  const pctChange = (key) => {
    const rSum = recent.reduce((a, r) => a + (Number(r[key]) || 0), 0);
    const oSum = older.reduce((a, r) => a + (Number(r[key]) || 0), 0);
    if (oSum === 0) return null;
    return Math.round(((rSum - oSum) / oSum) * 100);
  };

  // Auth loading
  if (authLoading) return (
    <div style={S.app}><style>{CSS}</style>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh" }}>
        <div style={{ textAlign: "center" }}>
          <div className="spin" style={{ width: 32, height: 32, border: "2.5px solid #E8E8E6", borderTopColor: "#1A1A1A", borderRadius: "50%", margin: "0 auto 14px" }} />
          <div style={{ color: "#8A8A88", fontSize: 14, fontWeight: 500 }}>Loading...</div>
        </div>
      </div>
    </div>
  );

  // Login screen
  if (!session) return (
    <div style={S.app}><style>{CSS}</style>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh", padding: "0 16px" }}>
        <form onSubmit={handleLogin} className="login-form" style={{ width: "100%", maxWidth: 380, background: "#fff", borderRadius: 16, padding: "48px 36px", boxShadow: "0 8px 32px rgba(0,0,0,0.08)", border: "1px solid #E8E8E6" }}>
          <div style={{ textAlign: "center", marginBottom: 32 }}>
            <img src="https://portal-drshumard.b-cdn.net/trans_sized.png" alt="Logo" style={{ height: 36, objectFit: "contain", marginBottom: 16 }} />
            <h2 style={{ fontSize: 20, fontWeight: 700, color: "#111827", margin: 0 }}>Analytics Dashboard</h2>
            <p style={{ fontSize: 13, color: "#6B7280", marginTop: 6 }}>Sign in to continue</p>
          </div>
          {authError && <div style={{ background: "#FEF2F2", color: "#DC2626", padding: "10px 14px", borderRadius: 8, fontSize: 13, marginBottom: 16, border: "1px solid #FECACA" }}>{authError}</div>}
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#374151", marginBottom: 6 }}>Email</label>
            <input type="email" value={authEmail} onChange={e => setAuthEmail(e.target.value)} required style={{ width: "100%", padding: "10px 14px", borderRadius: 10, border: "1px solid #D1D5DB", fontSize: 14, outline: "none", boxSizing: "border-box" }} />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#374151", marginBottom: 6 }}>Password</label>
            <input type="password" value={authPass} onChange={e => setAuthPass(e.target.value)} required style={{ width: "100%", padding: "10px 14px", borderRadius: 10, border: "1px solid #D1D5DB", fontSize: 14, outline: "none", boxSizing: "border-box" }} />
          </div>
          <button type="submit" style={{ ...S.btnDark, width: "100%", justifyContent: "center", padding: "11px 0", borderRadius: 10, fontSize: 14, fontWeight: 600 }}>Sign In</button>
          <div style={{ display: "flex", alignItems: "center", gap: 12, margin: "20px 0" }}>
            <div style={{ flex: 1, height: 1, background: "#E5E7EB" }} />
            <span style={{ fontSize: 12, color: "#9CA3AF", fontWeight: 500 }}>or</span>
            <div style={{ flex: 1, height: 1, background: "#E5E7EB" }} />
          </div>
          <button type="button" onClick={() => supabase.auth.signInWithOAuth({ provider: "google", options: { redirectTo: window.location.origin } })} style={{ ...S.btnLight, width: "100%", justifyContent: "center", padding: "11px 0", borderRadius: 10, fontSize: 14, fontWeight: 600, gap: 10 }}>
            <svg width="18" height="18" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z" /><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z" /><path fill="#FBBC05" d="M10.53 28.59a14.5 14.5 0 010-9.18l-7.98-6.19a24.01 24.01 0 000 21.56l7.98-6.19z" /><path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z" /></svg>
            Continue with Google
          </button>
        </form>
      </div>
    </div>
  );

  if (loading) return (
    <div style={S.app}><style>{CSS}</style>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh" }}>
        <div style={{ textAlign: "center" }}>
          <div className="spin" style={{ width: 32, height: 32, border: "2.5px solid #E8E8E6", borderTopColor: "#1A1A1A", borderRadius: "50%", margin: "0 auto 14px" }} />
          <div style={{ color: "#8A8A88", fontSize: 14, fontWeight: 500 }}>Connecting to server...</div>
        </div>
      </div>
    </div>
  );

  const effectiveViewMode = isMobile ? "board" : viewMode;

  return (
    <div style={S.app}><style>{CSS}</style>
      <header className="app-header" style={S.hdr}>
        <div style={{ display: "flex", alignItems: "center" }}>
          <img src="https://portal-drshumard.b-cdn.net/trans_sized.png" alt="Dr Shumard Analytics" style={{ height: 32, objectFit: "contain" }} />
        </div>
        <div className="header-actions">
          {/* Hamburger toggle — visible only on mobile via CSS */}
          <button className="mobile-menu-toggle" style={S.btnGhost} onClick={() => setMobileMenuOpen(p => !p)} aria-label="Menu">
            <I d={mobileMenuOpen ? "M6 18L18 6M6 6l12 12" : "M4 6h16M4 12h16M4 18h16"} size={20} stroke="#374151" sw={2} />
          </button>
          {/* Desktop-visible buttons */}
          <div className="nav-buttons-desktop">
            <button style={S.btnGhost} onClick={refreshWebhook} title="Refresh Spend Data"><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={15} stroke="#8A8A88" /></button>
            <button style={S.btnLight} onClick={clearCache}>Clear Cache</button>
            {isAdmin && <button style={{ ...S.btnLight, opacity: finalizing ? 0.6 : 1 }} disabled={finalizing} onClick={finalizePastDays} title="Freeze deduped values for all past days into the canonical columns">{finalizing ? "Finalizing…" : "Finalize Past Days"}</button>}
            {view === "dash" && (
              <>
                <button style={S.btnLight} onClick={() => setView("crm")}>CRM</button>
                <button style={S.btnLight} onClick={() => setView("emailreport")}>Email Report</button>
                <button style={S.btnLight} onClick={() => setView("insights")}>AI Insights</button>
                <button style={S.btnLight} onClick={async () => { try { const r = await api.getEvents(100, evFilter); setEvents(r.data || []); } catch (e) { flash(e.message, "err"); } setView("events"); }}>Activity Log</button>
                {isAdmin && <button style={S.btnLight} onClick={() => setView("query")}>Query Data</button>}
                {isAdmin && <button style={S.btnLight} onClick={() => { setEditCM(null); setView("custom-list"); }}>Manage Metrics</button>}
              </>
            )}
            {view === "dash" ? (
              isAdmin && <button style={{ ...S.btnDark, padding: "8px 16px", borderRadius: 8 }} onClick={() => { setEditRow(null); setView("entry"); }}>+ New Entry</button>
            ) : (
              <button style={S.btnLight} onClick={() => { setView("dash"); setEditRow(null); setEditCM(null); }}>← Back</button>
            )}
            {allowedFunnels.length > 1 && (
              <select
                value={activeFunnel}
                onChange={(e) => switchFunnel(e.target.value)}
                title="Switch funnel"
                style={{ ...S.btnLight, paddingRight: 30, appearance: "none", WebkitAppearance: "none", MozAppearance: "none", backgroundImage: "url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%236B7280' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><polyline points='6 9 12 15 18 9'/></svg>\")", backgroundRepeat: "no-repeat", backgroundPosition: "right 10px center", lineHeight: "1.2" }}
              >
                {allowedFunnels.map(f => (
                  <option key={f} value={f}>{f === "analytics" ? "Main" : f === "native" ? "Native" : f}</option>
                ))}
              </select>
            )}
            <button style={S.btnGhost} onClick={handleLogout} title="Sign out"><I d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9" size={15} stroke="#8A8A88" /></button>
          </div>
        </div>
      </header>

      {/* Mobile dropdown menu */}
      {mobileMenuOpen && (
        <div className="mobile-nav-dropdown">
          <button className="mobile-nav-item" onClick={refreshWebhook}><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={16} stroke="#6B7280" /> Refresh Data</button>
          {view === "dash" && (
            <>
              <button className="mobile-nav-item" onClick={() => setView("crm")}><I d="M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-2.13a4 4 0 10-4-4 4 4 0 004 4z" size={16} stroke="#6B7280" /> CRM</button>
              <button className="mobile-nav-item" onClick={() => setView("emailreport")}><I d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z M22 6l-10 7L2 6" size={16} stroke="#6B7280" /> Email Report</button>
              <button className="mobile-nav-item" onClick={() => setView("insights")}><I d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" size={16} stroke="#6B7280" /> AI Insights</button>
              <button className="mobile-nav-item" onClick={async () => { try { const r = await api.getEvents(100, evFilter); setEvents(r.data || []); } catch (e) { flash(e.message, "err"); } setView("events"); }}><I d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" size={16} stroke="#6B7280" /> Activity Log</button>
              {isAdmin && <button className="mobile-nav-item" onClick={() => setView("query")}><I d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" size={16} stroke="#6B7280" /> Query Data</button>}
              {isAdmin && <button className="mobile-nav-item" onClick={() => { setEditCM(null); setView("custom-list"); }}><I d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" size={16} stroke="#6B7280" /> Manage Metrics</button>}
              {isAdmin && <button className="mobile-nav-item" disabled={finalizing} onClick={finalizePastDays}><I d="M12 4v16m8-8H4" size={16} stroke="#6B7280" /> {finalizing ? "Finalizing…" : "Finalize Past Days"}</button>}
              {isAdmin && <button className="mobile-nav-item mobile-nav-primary" onClick={() => { setEditRow(null); setView("entry"); }}><I d="M12 4v16m8-8H4" size={16} stroke="#fff" /> New Entry</button>}
            </>
          )}
          {view !== "dash" && (
            <button className="mobile-nav-item" onClick={() => { setView("dash"); setEditRow(null); setEditCM(null); }}><I d="M10 19l-7-7m0 0l7-7m-7 7h18" size={16} stroke="#6B7280" /> Back to Dashboard</button>
          )}
          {allowedFunnels.length > 1 && (
            <>
              <div className="mobile-nav-divider" />
              <div style={{ padding: "8px 16px" }}>
                <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", color: "#6B7280", marginBottom: 6 }}>Funnel</div>
                <select
                  value={activeFunnel}
                  onChange={(e) => { switchFunnel(e.target.value); setMobileMenuOpen(false); }}
                  style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: "1px solid #D1D5DB", background: "#fff", fontSize: 14, fontWeight: 500, color: "#374151" }}
                >
                  {allowedFunnels.map(f => (
                    <option key={f} value={f}>{f === "analytics" ? "Main" : f === "native" ? "Native" : f}</option>
                  ))}
                </select>
              </div>
            </>
          )}
          <div className="mobile-nav-divider" />
          <button className="mobile-nav-item mobile-nav-danger" onClick={handleLogout}><I d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" size={16} stroke="#DC2626" /> Sign Out</button>
        </div>
      )}

      <main className="main-content" style={view === "insights" ? { padding: 0, maxWidth: "none", margin: 0 } : S.main}>
        {view === "dash" && (
          <div className="fi">
            <div className="title-row" style={S.titleRow}>
              <div><h1 style={S.pageTitle}>Dashboard Summary</h1><div style={S.pageSub}>Last {metrics.length} days &middot; auto-refreshes every 30s</div></div>
              <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
                <div style={{ ...S.searchWrap, width: "auto", padding: "6px 12px", boxShadow: "0 1px 2px rgba(0,0,0,0.05)" }}>
                  <I d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" size={14} stroke="#6B7280" />
                  <select style={S.searchInput} value={dateFilter} onChange={e => setDateFilter(e.target.value)}>
                    <option value="today">Today</option>
                    <option value="yesterday">Yesterday</option>
                    <option value="7">Last 7 Days</option>
                    <option value="30">Last 30 Days</option>
                    <option value="all">All Time</option>
                    <option value="custom">Custom</option>
                  </select>
                  {dateFilter === "custom" && (
                    <DatePicker
                      selectsRange={true}
                      startDate={dateRange[0]}
                      endDate={dateRange[1]}
                      onChange={(update) => setDateRange(update)}
                      customInput={<input style={{ ...S.searchInput, width: 180, borderLeft: "1px solid #E5E7EB", paddingLeft: 10, marginLeft: 6 }} placeholder="Select Dates..." />}
                    />
                  )}
                </div>
                <div style={S.livePill}><span style={S.liveDot} /> LIVE</div>
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
                <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 14px", marginBottom: 12, background: "#ECFDF5", border: "1px solid #A7F3D0", borderRadius: 10, fontSize: 13, color: "#065F46" }}>
                  <span style={{ fontWeight: 600 }}>Showing {selectedDates.size} selected row{selectedDates.size === 1 ? "" : "s"}</span>
                  <span style={{ color: "#10B981" }}>•</span>
                  <span>{span}</span>
                  <button onClick={clearSelection} style={{ marginLeft: "auto", background: "transparent", border: "1px solid #A7F3D0", borderRadius: 6, padding: "3px 10px", fontSize: 12, fontWeight: 500, color: "#065F46", cursor: "pointer", fontFamily: "Inter, sans-serif" }}>Clear</button>
                </div>
              );
            })()}

            <div className="summary-strip" style={{ ...S.strip, position: "relative" }}>
              {summaryCards.filter(c => !cardHiddenInVariant(c)).map((c, i) => {
                const pct = selectedDates.size > 0 ? null : pctChange(c.key);
                return (
                  <div key={i} style={S.stripCell}>
                    <div style={S.stripLabel}>
                      {c.label}
                      {c.agg === "avg" && <span style={{ fontSize: 10, color: "#9CA3AF", fontWeight: 400, marginLeft: 4 }}>(avg)</span>}
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
                style={{ position: "absolute", top: 8, right: 8, background: "transparent", border: "1px solid transparent", borderRadius: 6, padding: 4, cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center", justifyContent: "center", transition: "all 0.15s" }}
                onMouseEnter={e => { e.currentTarget.style.background = "#F3F4F6"; e.currentTarget.style.borderColor = "#E5E7EB"; e.currentTarget.style.color = "#6B7280"; }}
                onMouseLeave={e => { e.currentTarget.style.background = "transparent"; e.currentTarget.style.borderColor = "transparent"; e.currentTarget.style.color = "#9CA3AF"; }}
              >
                <I d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" size={14} />
                <I d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" size={14} />
              </button>
            </div>

            <div className="toolbar-row" style={S.toolbar}>
              <div className="toolbar-controls" style={{ display: "flex", gap: "10px", alignItems: "center", flexWrap: "wrap" }}>
                <div className="list-board-toggle" style={S.listBoardToggle}>
                  <div style={viewMode === "list" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("list")}><I d="M4 6h16M4 12h16M4 18h16" size={14} /> List</div>
                  <div style={viewMode === "board" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("board")}><I d="M4 4h4v16H4zM10 4h4v16h-4zM16 4h4v16h-4z" size={14} /> Board</div>
                </div>
                {(activeFunnel === "native" || activeFunnel === "analytics") && (
                  <div className="variant-toggle" style={S.listBoardToggle} title="Split test variant filter">
                    {["all", "A", "B", "undetected"].map(v => (
                      <div
                        key={v}
                        style={variantFilter === v ? S.listToggleActive : S.listToggleInactive}
                        onClick={() => setVariantFilter(v)}
                      >
                        {v === "all" ? "All" : v === "undetected" ? "Undetected" : v}
                      </div>
                    ))}
                  </div>
                )}
                {isAdmin && (activeFunnel === "native" || activeFunnel === "analytics") && (
                  <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#6B7280" }} title="Variants only count from this time — earlier (test) traffic is excluded from the A/B reports. Totals are unaffected.">
                    <span style={{ whiteSpace: "nowrap" }}>A/B from: <strong style={{ color: "#374151", fontWeight: 600 }}>{abTestStart ? new Date(abTestStart).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) : "all time"}</strong></span>
                    <button style={{ ...S.btnLight, padding: "4px 10px", fontSize: 12 }} onClick={startAbTestNow}>Start now</button>
                    {abTestStart && <button style={{ ...S.btnLight, padding: "4px 10px", fontSize: 12 }} onClick={clearAbTest}>Clear</button>}
                  </div>
                )}
                <button style={S.btnLight} onClick={() => setColEditorOpen(true)}><I d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01" size={14} stroke="#6B7280" /> Edit Columns</button>
                <div style={{ position: "relative" }} ref={lensMenuRef}>
                  <button style={S.btnLight} onClick={() => setLensMenuOpen(p => !p)}><I d="M15 12a3 3 0 11-6 0 3 3 0 016 0zM2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" size={14} stroke="#6B7280" /> {activeLens.name} ▾</button>
                  {lensMenuOpen && (
                    <div style={{ position: "absolute", top: "100%", left: 0, marginTop: 6, background: "#fff", border: "1px solid #E5E7EB", borderRadius: 10, padding: "8px 0", boxShadow: "0 4px 12px rgba(0,0,0,0.1)", zIndex: 50, minWidth: 220 }}>
                      <div style={{ padding: "6px 14px", fontSize: 10, fontWeight: 700, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.05em" }}>Lenses</div>
                      {lenses.map(lens => (
                        <div key={lens.id} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 14px", cursor: "pointer", fontSize: 13, color: "#374151", fontWeight: activeLensId === lens.id ? 600 : 500, background: activeLensId === lens.id ? "#F3F4F6" : "transparent" }} onMouseEnter={e => { if (activeLensId !== lens.id) e.currentTarget.style.background = "#F9FAFB"; }} onMouseLeave={e => { if (activeLensId !== lens.id) e.currentTarget.style.background = "transparent"; }}>
                          <span style={{ flex: 1 }} onClick={() => { setActiveLensId(lens.id); setLensMenuOpen(false); }}>{lens.name}</span>
                          {isAdmin && lens.id !== "default-all" && (
                            <>
                              <span title="Set as default" style={{ cursor: "pointer", fontSize: 14 }} onClick={() => { setDefaultLens(lens.id); setActiveLensId(lens.id); }}>⭐</span>
                              <span title="Edit" style={{ cursor: "pointer", fontSize: 12, color: "#6B7280" }} onClick={() => { setLensEditing({ id: lens.id, name: lens.name, metrics: [...lens.metrics] }); setLensMenuOpen(false); }}>✏️</span>
                              <span title="Delete" style={{ cursor: "pointer", fontSize: 12, color: "#DC2626" }} onClick={() => { deleteLens(lens.id); }}>🗑️</span>
                            </>
                          )}
                        </div>
                      ))}
                      {isAdmin && (
                        <>
                          <div style={{ height: 1, background: "#E5E7EB", margin: "4px 0" }} />
                          <div style={{ padding: "7px 14px", cursor: "pointer", fontSize: 13, color: "#3538CD", fontWeight: 600 }} onMouseEnter={e => e.currentTarget.style.background = "#F9FAFB"} onMouseLeave={e => e.currentTarget.style.background = "transparent"} onClick={() => { setLensEditing({ name: "", metrics: MK.filter(k => COL_LABELS[k]) }); setLensMenuOpen(false); }}>+ Create Lens</div>
                        </>
                      )}
                    </div>
                  )}
                </div>
              </div>
              <div className="search-wrap" style={S.searchWrap}><I d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" size={15} stroke="#AEAEA8" /><input style={S.searchInput} placeholder="Search records..." value={search} onChange={e => setSearch(e.target.value)} /></div>
            </div>

            {effectiveViewMode === "list" ? (
              <div className="table-wrap" style={S.tableWrap}>
                <table style={S.table}>
                  <thead><tr>
                    <th className="sticky-col sticky-col-1" style={S.th}>Day</th><th className="sticky-col sticky-col-2" style={S.th}>Date</th>
                    {orderedCols.filter(c => (c.type === "base" ? isColVisible(c.key) : true) && !isHiddenInVariant(c)).map(c => (
                      <th key={c.key} style={{ ...S.th, ...(c.type === "custom" ? { color: "#12864A" } : {}), ...(c.key === "total_purchases" ? S.thHighlight : {}) }}>{c.label}</th>
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
                          style={{
                            cursor: "pointer",
                            userSelect: isSelected ? "none" : undefined,
                            ...(!isSelected && isToday ? { background: "#F8FDF9" } : {}),
                          }}
                          onClick={(e) => toggleRowSelection(row.date, e.shiftKey)}
                        >
                          <td className="sticky-col sticky-col-1" style={S.td}><span style={S.dayPill}>{getLADayShort(row.date)}</span></td>
                          <td className="sticky-col sticky-col-2" style={{ ...S.td, whiteSpace: "nowrap", minWidth: 90 }}><span style={{ color: "#1A1A1A", fontWeight: 500, fontSize: 14 }}>{fmtDateNice(row.date)}</span></td>
                          {orderedCols.filter(c => (c.type === "base" ? isColVisible(c.key) : true) && !isHiddenInVariant(c)).map(c => {
                            if (c.type === "custom") {
                              return <td key={c.key} style={{ ...S.tdNum, color: "#12864A", fontWeight: 600 }}>{fmtVal(ctx[c.cm.name], c.cm.format)}</td>;
                            }
                            if (c.key === "total_purchases") return <td key={c.key} style={S.tdNum}><span style={S.purchBadge}>{(Number(row.total_purchases) || 0).toLocaleString()}</span></td>;
                            if (c.key === "fb_spend") return <td key={c.key} style={S.tdMoney}>{"$" + (Number(row.fb_spend) || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })}</td>;
                            return <td key={c.key} style={S.tdNum}>{(Number(row[c.key]) || 0).toLocaleString()}</td>;
                          })}
                          <td style={S.td} onClick={(e) => e.stopPropagation()}>
                            <div style={{ display: "flex", gap: 2, justifyContent: "center" }}>
                              <button className="rowBtn" style={{ ...S.rowAct, borderColor: "#DBEAFE", background: "#EFF6FF" }} title="Recalc spend from Facebook" onClick={() => recalcSpend(row.date)}><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={14} stroke="#3B82F6" /></button>
                              {isAdmin && <button className="rowBtn" style={{ ...S.rowAct, borderColor: "#D1FAE5", background: "#ECFDF5" }} title="Refinalize this day (recompute deduped values from events)" onClick={() => finalizeDay(row.date)}><I d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" size={14} stroke="#10B981" /></button>}
                              {isAdmin && <button className="rowBtn" style={S.rowAct} onClick={() => { setEditRow(row); setView("entry"); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={14} stroke="#8A8A88" /></button>}
                              {isAdmin && <button className="rowBtn" style={{ ...S.rowAct, borderColor: "#FEE2E2", background: "#FEF2F2" }} onClick={() => setDelConfirm(row.date)}><I d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" size={14} stroke="#EF4444" /></button>}
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div style={S.boardGrid}>
                {tableRows.length === 0 ? (
                  <div style={{ ...S.emptyTd, gridColumn: "1 / -1", border: "1px dashed #E5E7EB", borderRadius: 12 }}>No entries found for this period.</div>
                ) : tableRows.map((row) => {
                  const boardCtx = evalAllCustoms(customs, row);
                  const isSelected = selectedDates.has(row.date);
                  const cardStyle = {
                    ...S.boardCard,
                    cursor: "pointer",
                    ...(isSelected ? { background: "#DCFCE7", borderColor: "#10B981", boxShadow: "0 0 0 2px #A7F3D0" } : {}),
                  };
                  return (
                    <div
                      key={row.date}
                      style={cardStyle}
                      onClick={(e) => toggleRowSelection(row.date, e.shiftKey)}
                    >
                      <div style={S.boardCardHeader}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={S.dayPill}>{getLADayShort(row.date)}</span>
                          <div style={{ fontWeight: 600, color: "#111827", fontSize: 14 }}>{fmtDateNice(row.date)}</div>
                        </div>
                      </div>
                      <div style={S.boardCardBody}>
                        {orderedCols.filter(c => (c.type === "base" ? isColVisible(c.key) : true) && !isHiddenInVariant(c)).map(c => {
                          if (c.type === "custom") {
                            return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={{ ...S.bcVal, color: "#10B981" }}>{fmtVal(boardCtx[c.cm.name], c.cm.format)}</span></div>;
                          }
                          if (c.key === "fb_spend") return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={S.bcVal}>{"$" + (Number(row.fb_spend) || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })}</span></div>;
                          if (c.key === "total_purchases") return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={S.purchBadge}>{(Number(row.total_purchases) || 0).toLocaleString()}</span></div>;
                          return <div key={c.key} style={S.bcItem}><span style={S.bcLabel}>{c.label}</span><span style={S.bcVal}>{(Number(row[c.key]) || 0).toLocaleString()}</span></div>;
                        })}
                      </div>
                      <div style={S.boardCardActions} onClick={(e) => e.stopPropagation()}>
                        <button style={{ ...S.rowAct, borderColor: "#DBEAFE", background: "#EFF6FF" }} title="Recalc spend from Facebook" onClick={() => recalcSpend(row.date)}><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={14} stroke="#3B82F6" /></button>
                        {isAdmin && <button style={{ ...S.rowAct, borderColor: "#D1FAE5", background: "#ECFDF5" }} title="Refinalize this day (recompute deduped values from events)" onClick={() => finalizeDay(row.date)}><I d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" size={14} stroke="#10B981" /></button>}
                        {isAdmin && <button style={S.rowAct} title="Edit row" onClick={() => { setEditRow(row); setView("entry"); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={14} stroke="#6B7280" /></button>}
                        {isAdmin && <button style={{ ...S.rowAct, borderColor: "#FECACA", background: "#FEF2F2" }} title="Delete row" onClick={() => setDelConfirm(row.date)}><I d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" size={14} stroke="#EF4444" /></button>}
                      </div>
                    </div>
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
            <div style={{ ...S.fh, marginBottom: 16 }}><h2 style={S.ft}>Manage Custom Metrics</h2></div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "20px" }}>
              <p style={{ ...S.fs, margin: 0 }}>Define calculated columns for your table.</p>
              <button style={S.btnDark} onClick={() => { setEditCM(null); setView("custom"); }}><I d="M12 5v14M5 12h14" stroke="#fff" /> New Metric</button>
            </div>
            <div style={S.fcard}>
              {customs.length === 0 ? (
                <div style={{ textAlign: "center", padding: "40px 0", color: "#8A8A88" }}>No custom metrics created yet.</div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  {customs.map(cm => (
                    <div key={cm.id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "16px", border: "1px solid #E4E4E5", borderRadius: 12, background: "#FAFAFA" }}>
                      <div>
                        <div style={{ fontWeight: 600, color: "#111827", fontSize: 14 }}>{cm.name}</div>
                        <div style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 12, color: "#6B7280", marginTop: 4 }}>{cm.formula}</div>
                      </div>
                      <div style={{ display: "flex", gap: 6 }}>
                        <button style={S.btnLight} onClick={() => { setEditCM(cm); setView("custom"); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={14} /></button>
                        <button style={{ ...S.btnLight, color: "#DC2626" }} onClick={() => setDelCM(cm.id)}><I d="M18 6L6 18M6 6l12 12" size={14} /></button>
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
              <div><h1 style={S.pageTitle}>Activity Log</h1><div style={{ fontSize: 13, color: "#6B7280", marginTop: 4 }}>Recent events from your webhooks</div></div>
              <div style={{ ...S.searchWrap, width: "auto", padding: "6px 12px" }}>
                <select style={S.searchInput} value={evFilter} onChange={async e => { setEvFilter(e.target.value); try { const r = await api.getEvents(100, e.target.value); setEvents(r.data || []); } catch { } }}>
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
                <div style={{ textAlign: "center", padding: "48px 0", color: "#9CA3AF" }}>No events recorded yet. Events will appear when your webhooks send data.</div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column" }}>
                  {events.map((ev, i) => {
                    const typeColors = { registrations: "#3B82F6", replays: "#8B5CF6", viewedcta: "#F59E0B", clickedcta: "#10B981", purchases: "#EF4444", fb_spend: "#6366F1" };
                    const tc = typeColors[ev.event_type] || "#6B7280";
                    const ago = ((Date.now() - new Date(ev.event_time).getTime()) / 1000);
                    const agoStr = ago < 60 ? "just now" : ago < 3600 ? `${Math.floor(ago / 60)}m ago` : ago < 86400 ? `${Math.floor(ago / 3600)}h ago` : `${Math.floor(ago / 86400)}d ago`;
                    return (
                      <div key={ev.id || i} style={{ display: "flex", alignItems: "flex-start", gap: 16, padding: "16px 0", borderBottom: i < events.length - 1 ? "1px solid #F3F4F6" : "none" }}>
                        <div style={{ width: 8, height: 8, borderRadius: "50%", background: tc, marginTop: 6, flexShrink: 0 }} />
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                            <span style={{ fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em", color: tc, background: `${tc}14`, padding: "2px 8px", borderRadius: 4 }}>{ev.event_type.replace("cta", " CTA")}</span>
                            <span style={{ fontSize: 12, color: "#9CA3AF" }}>{agoStr}</span>
                          </div>
                          <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
                            {ev.name && <span style={{ fontSize: 14, fontWeight: 600, color: "#111827" }}>{ev.name}</span>}
                            {ev.email && <span style={{ fontSize: 13, color: "#6B7280" }}>{ev.email}</span>}
                            {ev.phone && <span style={{ fontSize: 13, color: "#6B7280" }}>{ev.phone}</span>}
                            {!ev.name && !ev.email && !ev.phone && <span style={{ fontSize: 13, color: "#9CA3AF", fontStyle: "italic" }}>No contact info</span>}
                          </div>
                          {ev.metadata && Object.keys(ev.metadata).length > 0 && (
                            <div style={{ fontSize: 12, color: "#9CA3AF", marginTop: 4, fontFamily: "'IBM Plex Mono', monospace" }}>{Object.entries(ev.metadata).map(([k, v]) => `${k}: ${v}`).join(" · ")}</div>
                          )}
                        </div>
                        <div style={{ fontSize: 11, color: "#9CA3AF", whiteSpace: "nowrap", flexShrink: 0 }}>{new Date(ev.event_time).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}</div>
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
              <div><h1 style={S.pageTitle}>CRM</h1><div style={{ fontSize: 13, color: "#6B7280", marginTop: 4 }}>{crmTotal.toLocaleString()} people · click anyone to see their full journey</div></div>
              <div style={{ ...S.searchWrap, width: "auto", minWidth: 240, padding: "6px 12px" }}>
                <I d="M21 21l-4.35-4.35M11 19a8 8 0 100-16 8 8 0 000 16z" size={14} stroke="#6B7280" />
                <input style={S.searchInput} placeholder="Search name, email, phone…" value={crmSearch} onChange={e => setCrmSearch(e.target.value)} />
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
                const color = card.stage ? (CRM_STAGE_META[card.stage] || {}).color : "#111827";
                return (
                  <div key={card.label} onClick={() => setCrmStage(active ? "" : card.key)} style={{ ...S.stripCell, cursor: "pointer", padding: "16px 18px", boxShadow: active ? `inset 0 -3px 0 ${color}` : "none", background: active ? "#FAFAFF" : "#fff" }}>
                    <span style={{ ...S.stripLabel, color }}>{card.label}</span>
                    <span style={{ ...S.stripVal, fontSize: 22 }}>{card.val != null ? card.val.toLocaleString() : "—"}</span>
                  </div>
                );
              })}
            </div>

            <div style={S.fcard}>
              {crmLoading ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "#9CA3AF" }}>Loading…</div>
              ) : crmContacts.length === 0 ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "#9CA3AF" }}>No contacts found.</div>
              ) : (
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                    <thead>
                      <tr>{["Name", "Email", "Phone", "Stage", "Source", "Visits", "Last seen"].map(h => <th key={h} style={{ ...S.th, textAlign: "left" }}>{h}</th>)}</tr>
                    </thead>
                    <tbody>
                      {crmContacts.map((c, i) => (
                        <tr key={c.contact_id || c.email || i} onClick={() => openCrmContact(c)} style={{ cursor: "pointer" }}>
                          <td style={{ ...S.td, textAlign: "left", fontWeight: 600, color: "#111827" }}>{c.name || <span style={{ color: "#9CA3AF", fontStyle: "italic", fontWeight: 400 }}>Unknown</span>}{c.is_shared_ip && <span title="Seen on a shared/NAT IP — identity not auto-merged" style={{ marginLeft: 6, fontSize: 9, fontWeight: 700, letterSpacing: "0.04em", color: "#B91C1C", background: "#FEE2E2", padding: "1px 5px", borderRadius: 4, verticalAlign: "middle" }}>SHARED IP</span>}</td>
                          <td style={{ ...S.td, textAlign: "left", color: "#374151" }}>{c.email}{c.has_linked && <span title="Has linked identities — another email/phone was fused into this person (same browser/IP). Open to see the Linked tab." style={{ marginLeft: 6, fontSize: 9, fontWeight: 700, letterSpacing: "0.04em", color: "#3538CD", background: "#EEF4FF", padding: "1px 5px", borderRadius: 4, verticalAlign: "middle" }}>L</span>}</td>
                          <td style={{ ...S.td, textAlign: "left", color: "#6B7280" }}>{c.phone || "—"}</td>
                          <td style={{ ...S.td, textAlign: "left" }}><CrmStageBadge stage={c.stage} /></td>
                          <td style={{ ...S.td, textAlign: "left", color: "#6B7280" }}>{crmSourceLabel(c.attribution) || "—"}</td>
                          <td style={{ ...S.td, textAlign: "left", color: c.is_tracked ? "#374151" : "#9CA3AF" }}>{c.is_tracked ? c.visit_count : "LEGACY"}</td>
                          <td style={{ ...S.td, textAlign: "left", color: "#9CA3AF", whiteSpace: "nowrap" }}>{c.last_activity ? crmRelTime(c.last_activity) : "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {!crmLoading && crmTotal > CRM_PAGE && (
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", paddingTop: 16, marginTop: 4, borderTop: "1px solid #F3F4F6", fontSize: 13, color: "#6B7280" }}>
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
              <div><h1 style={S.pageTitle}>Email Report</h1><div style={{ fontSize: 13, color: "#6B7280", marginTop: 4 }}>Email-click performance by source · {emailReport?.totals?.window_days ? `buyers within ${emailReport.totals.window_days} days of click` : "buyers = purchased any time after clicking"}</div></div>
              <button style={S.btnLight} onClick={loadEmailReport}>↻ Refresh</button>
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
                <div style={{ textAlign: "center", padding: "48px 0", color: "#9CA3AF" }}>Loading…</div>
              ) : !emailReport?.sources?.length ? (
                <div style={{ textAlign: "center", padding: "48px 0", color: "#9CA3AF" }}>
                  No email clicks tracked yet.
                  <div style={{ fontSize: 13, marginTop: 8 }}>They appear once your emails link with <code style={{ background: "#F3F4F6", padding: "1px 5px", borderRadius: 4 }}>?he=&#123;&#123;contact.email&#125;&#125;&amp;el=&lt;source&gt;&amp;htrafficsource=email</code></div>
                </div>
              ) : (
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                    <thead>
                      <tr>{["Source (el)", "Clicks", "People", "Buyers", "Conversion"].map((h, i) => <th key={h} style={{ ...S.th, textAlign: i === 0 ? "left" : "right" }}>{h}</th>)}</tr>
                    </thead>
                    <tbody>
                      {emailReport.sources.map((r, i) => (
                        <tr key={r.source || i} onClick={() => openEmailDrill(r.source)} style={{ cursor: "pointer" }} title="View individual clicks">
                          <td style={{ ...S.td, textAlign: "left", fontWeight: 600, color: "#111827" }}>
                            <span style={{ color: "#6B7280", marginRight: 6 }}>›</span>{r.source}
                          </td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{r.clicks.toLocaleString()}</td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{r.people.toLocaleString()}</td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{r.buyers.toLocaleString()}</td>
                          <td style={{ ...S.td, textAlign: "right", fontVariantNumeric: "tabular-nums", fontWeight: 600, color: r.conversion > 0 ? "#047857" : "#9CA3AF" }}>{r.conversion}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
        {view === "insights" && <InsightsChat flash={flash} isMobile={isMobile} activeFunnel={activeFunnel} />}
        {view === "query" && <QueryBuilder flash={flash} />}
      </main>

      {delConfirm && <Modal title="Delete Entry" msg={`Remove the entry for ${fmtDateNice(delConfirm)}?`} onCancel={() => setDelConfirm(null)} onConfirm={() => deleteEntry(delConfirm)} />}
      {delCM && <Modal title="Delete Custom Metric" msg="This will remove the column from your table." onCancel={() => setDelCM(null)} onConfirm={() => deleteCM(delCM)} />}
      {lensEditing && <LensEditor lens={lensEditing} onSave={saveLens} onCancel={() => setLensEditing(null)} />}
      {colEditorOpen && <ColumnEditor columns={orderedCols.filter(c => c.type !== "base" || isColVisible(c.key))} isAdmin={isAdmin} onSave={(keys) => { saveColOrder(keys); setColEditorOpen(false); }} onCancel={() => setColEditorOpen(false)} />}
      {crmContact && <CrmContactModal contact={crmContact} tab={crmContactTab} setTab={setCrmContactTab} onClose={() => setCrmContact(null)} />}
      {emailDrill && <EmailDrillModal drill={emailDrill} loading={emailDrillLoading} onClose={() => setEmailDrill(null)} onOpenContact={(idf) => { setEmailDrill(null); openCrmContact({ email: idf }); }} />}
      {summaryEditorOpen && <SummaryEditor cards={summaryCards} customs={customs} onSave={(cards) => { saveSummaryCards(cards); setSummaryEditorOpen(false); flash("Summary cards updated"); }} onCancel={() => setSummaryEditorOpen(false)} />}
      {toast && (<div className="fi" style={{ ...S.toast, borderLeft: `3px solid ${toast.type === "ok" ? "#12864A" : "#D92D20"}` }}><I d={toast.type === "ok" ? "M20 6L9 17l-5-5" : "M12 2a10 10 0 100 20 10 10 0 000-20zM12 8v4M12 16h.01"} size={16} stroke={toast.type === "ok" ? "#12864A" : "#D92D20"} sw={2.2} />{toast.msg}</div>)}
    </div>
  );
}

function SummaryEditor({ cards: initialCards, customs, onSave, onCancel }) {
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
    cardRow: { display: "flex", alignItems: "center", gap: 8, padding: "12px 14px", border: "1px solid #E5E7EB", borderRadius: 10, background: "#FAFAFA", marginBottom: 8 },
    fieldGroup: { display: "flex", flexDirection: "column", gap: 3 },
    miniLabel: { fontSize: 10, fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.04em" },
    miniSelect: { padding: "5px 8px", fontSize: 12, fontWeight: 500, border: "1px solid #D1D5DB", borderRadius: 6, background: "#fff", color: "#374151", outline: "none", cursor: "pointer", fontFamily: "inherit" },
    miniInput: { padding: "5px 8px", fontSize: 12, fontWeight: 500, border: "1px solid #D1D5DB", borderRadius: 6, background: "#fff", color: "#374151", outline: "none", fontFamily: "inherit", width: 120 },
    moveBtn: { background: "transparent", border: "none", cursor: "pointer", padding: 2, color: "#9CA3AF", fontSize: 14, lineHeight: 1, display: "flex", alignItems: "center" },
    removeBtn: { background: "transparent", border: "none", cursor: "pointer", padding: "2px 4px", color: "#DC2626", fontSize: 16, lineHeight: 1, display: "flex", alignItems: "center", borderRadius: 4 },
  };

  return (
    <div style={S.overlay} onClick={onCancel}>
      <div style={{ ...S.modal, maxWidth: 560, textAlign: "left", padding: "28px 32px" }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
          <div style={{ width: 40, height: 40, borderRadius: 10, background: "#EEF4FF", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
            <I d="M4 6h16M4 12h16M4 18h7" size={20} stroke="#3538CD" sw={2} />
          </div>
          <div>
            <div style={{ fontSize: 17, fontWeight: 600, color: "#111827" }}>Edit Summary Cards</div>
            <div style={{ fontSize: 13, color: "#6B7280", marginTop: 2 }}>Choose metrics, labels, and aggregation type</div>
          </div>
        </div>

        <div style={{ maxHeight: 400, overflowY: "auto", marginBottom: 16, paddingRight: 4 }}>
          {cards.length === 0 && (
            <div style={{ textAlign: "center", padding: "32px 0", color: "#9CA3AF", fontSize: 13 }}>No cards configured. Add one below.</div>
          )}
          {cards.map((card, idx) => (
            <div key={idx} style={SE.cardRow}>
              <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
                <button style={SE.moveBtn} onClick={() => moveCard(idx, -1)} disabled={idx === 0} title="Move up">▲</button>
                <button style={SE.moveBtn} onClick={() => moveCard(idx, 1)} disabled={idx === cards.length - 1} title="Move down">▼</button>
              </div>
              <div style={{ flex: 1, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "end" }}>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Label</span>
                  <input style={SE.miniInput} value={card.label} onChange={e => updateCard(idx, "label", e.target.value)} />
                </div>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Metric</span>
                  <select style={SE.miniSelect} value={card.key} onChange={e => {
                    const opt = allOptions.find(o => o.key === e.target.value);
                    updateCard(idx, "key", e.target.value);
                    if (opt && card.label === (allOptions.find(o => o.key === card.key)?.label || "")) updateCard(idx, "label", opt.label);
                  }}>
                    {allOptions.map(o => <option key={o.key} value={o.key}>{o.label}</option>)}
                  </select>
                </div>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Type</span>
                  <select style={SE.miniSelect} value={card.agg} onChange={e => updateCard(idx, "agg", e.target.value)}>
                    <option value="total">Total</option>
                    <option value="avg">Average</option>
                  </select>
                </div>
                <div style={SE.fieldGroup}>
                  <span style={SE.miniLabel}>Format</span>
                  <select style={SE.miniSelect} value={card.format} onChange={e => updateCard(idx, "format", e.target.value)}>
                    <option value="number">Number</option>
                    <option value="currency">Currency ($)</option>
                    <option value="decimal">Decimal</option>
                  </select>
                </div>
              </div>
              <button style={SE.removeBtn} onClick={() => removeCard(idx)} title="Remove card">×</button>
            </div>
          ))}
        </div>

        {!addOpen ? (
          <button
            style={{ ...S.btnLight, width: "100%", justifyContent: "center", marginBottom: 16, padding: "10px 0", borderStyle: "dashed", color: "#6B7280" }}
            onClick={() => setAddOpen(true)}
          >
            + Add Card
          </button>
        ) : (
          <div style={{ display: "flex", gap: 8, marginBottom: 16, alignItems: "center" }}>
            <select
              autoFocus
              style={{ ...SE.miniSelect, flex: 1, padding: "8px 10px", fontSize: 13 }}
              defaultValue=""
              onChange={e => { if (e.target.value) addCard(e.target.value); }}
            >
              <option value="" disabled>Select a metric…</option>
              {allOptions.map(o => <option key={o.key} value={o.key}>{o.label}</option>)}
            </select>
            <button style={{ ...S.btnGhost, padding: "6px 10px", fontSize: 12, color: "#6B7280" }} onClick={() => setAddOpen(false)}>Cancel</button>
          </div>
        )}

        <div style={{ display: "flex", gap: 10, justifyContent: "space-between" }}>
          <button style={{ ...S.btnGhost, fontSize: 13, color: "#6B7280" }} onClick={() => setCards(DEFAULT_SUMMARY_CARDS.map(c => ({ ...c })))}>Reset to Default</button>
          <div style={{ display: "flex", gap: 10 }}>
            <button style={{ ...S.btnLight, justifyContent: "center" }} onClick={onCancel}>Cancel</button>
            <button style={{ ...S.btnDark, justifyContent: "center" }} onClick={() => onSave(cards)} disabled={cards.length === 0}>Save</button>
          </div>
        </div>
      </div>
    </div>
  );
}

function LensEditor({ lens, onSave, onCancel }) {
  const [name, setName] = useState(lens.name || "");
  const [metrics, setMetrics] = useState((lens.metrics || MK).filter(k => COL_LABELS[k]));
  const toggle = (m) => setMetrics(prev => prev.includes(m) ? prev.filter(x => x !== m) : [...prev, m]);
  return (
    <div style={S.overlay} onClick={onCancel}>
      <div style={{ ...S.modal, maxWidth: 420 }} onClick={e => e.stopPropagation()}>
        <div style={{ width: 44, height: 44, borderRadius: 12, background: "#EEF4FF", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}><I d="M15 12a3 3 0 11-6 0 3 3 0 016 0zM2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" size={22} stroke="#3538CD" sw={2} /></div>
        <div style={{ fontSize: 18, fontWeight: 600, color: "#111827", marginBottom: 6, textAlign: "center" }}>{lens.id ? "Edit Lens" : "Create Lens"}</div>
        <div style={{ color: "#6B7280", fontSize: 13, marginBottom: 20, textAlign: "center" }}>Choose a name and the metrics to include</div>
        <div style={{ marginBottom: 16 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: "#374151", display: "block", marginBottom: 6 }}>Lens Name</label>
          <input style={S.inp} value={name} onChange={e => setName(e.target.value)} placeholder="e.g. Funnel Overview" autoFocus />
        </div>
        <div style={{ marginBottom: 20 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: "#374151", display: "block", marginBottom: 8 }}>Metrics</label>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
            {MK.filter(m => COL_LABELS[m]).map(m => (
              <label key={m} style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", borderRadius: 8, border: `1px solid ${metrics.includes(m) ? "#3538CD" : "#E5E7EB"}`, background: metrics.includes(m) ? "#EEF4FF" : "#fff", cursor: "pointer", fontSize: 13, fontWeight: 500, color: metrics.includes(m) ? "#3538CD" : "#6B7280", transition: "all 0.15s" }}>
                <input type="checkbox" checked={metrics.includes(m)} onChange={() => toggle(m)} style={{ accentColor: "#3538CD" }} />
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
    item: (isDragging, isOver) => ({ display: "flex", alignItems: "center", gap: 10, padding: "10px 12px", background: isDragging ? "#EEF2FF" : isOver ? "#F0FDF4" : "#F9FAFB", border: `1px solid ${isDragging ? "#818CF8" : isOver ? "#86EFAC" : "#E5E7EB"}`, borderRadius: 8, cursor: "grab", transition: "all 150ms ease", opacity: isDragging ? 0.6 : 1, transform: isDragging ? "scale(0.98)" : "scale(1)" }),
    grip: { color: "#9CA3AF", fontSize: 14, cursor: "grab", userSelect: "none", display: "flex", flexDirection: "column", lineHeight: 0.5, letterSpacing: 2 },
    label: { flex: 1, fontSize: 14, fontWeight: 500, color: "#111827" },
    badge: { fontSize: 10, fontWeight: 600, padding: "2px 6px", borderRadius: 4, textTransform: "uppercase", letterSpacing: "0.04em" },
  };
  return (
    <div style={S.overlay} onClick={onCancel}>
      <div style={{ ...S.modal, maxWidth: 440, textAlign: "left" }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <div>
            <div style={{ fontSize: 18, fontWeight: 600, color: "#111827" }}>Edit Column Order</div>
            <div style={{ fontSize: 13, color: "#6B7280", marginTop: 4 }}>Drag to reorder. Day & Date are always first.</div>
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
              <span style={{ color: "#9CA3AF", fontSize: 12, fontWeight: 600, width: 20, textAlign: "center" }}>{i + 1}</span>
              <span style={CE.label}>{col.label}</span>
              {col.type === "custom" && <span style={{ ...CE.badge, background: "#ECFDF5", color: "#047857" }}>Custom</span>}
            </div>
          ))}
        </div>
        <div style={{ display: "flex", gap: 10, justifyContent: "space-between", flexWrap: "wrap" }}>
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <button style={{ ...S.btnGhost, fontSize: 13, color: "#6B7280" }} onClick={reset}>Reset to Default</button>
            {isAdmin && (
              <button
                style={{ ...S.btnGhost, fontSize: 12, color: "#3538CD", opacity: propagating ? 0.5 : 1 }}
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
                {propagating ? "Pushing..." : "✨ Set as Default for All"}
              </button>
            )}
            {statusMsg && <span style={{ fontSize: 12, color: "#047857", fontWeight: 500 }}>{statusMsg}</span>}
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <button style={{ ...S.btnLight, justifyContent: "center" }} onClick={onCancel}>Cancel</button>
            <button style={{ ...S.btnDark, justifyContent: "center" }} onClick={() => onSave(items.map(c => c.key))}>Save Order</button>
          </div>
        </div>
      </div>
    </div>
  );
}

function Modal({ title, msg, onCancel, onConfirm }) {
  return (
    <div style={S.overlay} onClick={onCancel}>
      <div style={S.modal} onClick={e => e.stopPropagation()}>
        <div style={{ width: 44, height: 44, borderRadius: 12, background: "#FEF3F2", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}><I d="M12 2a10 10 0 100 20 10 10 0 000-20zM12 8v4M12 16h.01" size={22} stroke="#DC2626" sw={2} /></div>
        <div style={{ fontSize: 18, fontWeight: 600, color: "#111827", marginBottom: 6 }}>{title}</div>
        <div style={{ color: "#4B5563", fontSize: 14, marginBottom: 28, lineHeight: 1.5 }}>{msg}</div>
        <div style={{ display: "flex", gap: 10 }}><button style={{ ...S.btnLight, flex: 1, justifyContent: "center" }} onClick={onCancel}>Cancel</button><button style={{ ...S.btnDark, background: "#DC2626", borderColor: "#DC2626", flex: 1, justifyContent: "center" }} onClick={onConfirm}>Delete</button></div>
      </div>
    </div>
  );
}

// ─── CRM presentational helpers ──────────────────────────────────────────────
const CRM_STAGE_META = {
  lead:         { label: "Lead",        color: "#6B7280" },
  registration: { label: "Registered",  color: "#3B82F6" },
  attended:     { label: "Attended",    color: "#0EA5E9" },
  replay:       { label: "Replay",      color: "#8B5CF6" },
  viewedcta:    { label: "Saw CTA",     color: "#F59E0B" },
  clickedcta:   { label: "Clicked CTA", color: "#10B981" },
  purchase:     { label: "Purchased",   color: "#EF4444" },
};
const crmTimelineColor = (item) => {
  if (item.kind === "pageview") return "#9CA3AF";
  if (item.kind === "tag") return "#14B8A6";
  const ec = { registrations: "#3B82F6", attended: "#0EA5E9", replays: "#8B5CF6", viewedcta: "#F59E0B", clickedcta: "#10B981", purchases: "#EF4444", stayeduntil: "#6366F1" };
  return ec[item.event_type] || "#6B7280";
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
  return <span style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.04em", color: m.color, background: `${m.color}14`, padding: "3px 9px", borderRadius: 5, whiteSpace: "nowrap" }}>{m.label}</span>;
}
function CrmDetailRows({ rows }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {rows.filter(([, v]) => v != null && v !== "").map(([k, v]) => (
        <div key={k} style={{ display: "flex", fontSize: 13, gap: 12 }}>
          <span style={{ color: "#9CA3AF", minWidth: 110, flexShrink: 0 }}>{k}</span>
          <span style={{ color: "#374151", wordBreak: "break-all" }}>{v}</span>
        </div>
      ))}
    </div>
  );
}
function CrmContactModal({ contact, tab, setTab, onClose }) {
  const c = contact.contact || {};
  const timeline = contact.timeline || [];
  const visits = contact.visits || [];
  const events = contact.events || [];
  const linked = contact.linked || [];
  const attr = c.attribution || {};
  const title = c.name || c.email || "Contact";
  return (
    <div style={S.overlay} onClick={onClose}>
      <div style={{ ...S.modal, maxWidth: 720, width: "92%", maxHeight: "85vh", padding: 0, textAlign: "left", display: "flex", flexDirection: "column", overflow: "hidden" }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: "20px 24px", borderBottom: "1px solid #E5E7EB", display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
          <div style={{ minWidth: 0 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4, flexWrap: "wrap" }}>
              <h2 style={{ fontSize: 18, fontWeight: 600, color: "#111827", margin: 0 }}>{title}</h2>
              <CrmStageBadge stage={c.stage} />
              {!c.is_tracked && <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.06em", color: "#9CA3AF", background: "#F3F4F6", padding: "2px 7px", borderRadius: 4 }}>LEGACY</span>}
              {c.is_shared_ip && <span title="Seen on a shared/NAT IP — identity was not auto-merged to avoid fusing strangers" style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.06em", color: "#B91C1C", background: "#FEE2E2", padding: "2px 7px", borderRadius: 4 }}>⚠ SHARED IP</span>}
              {linked.length > 0 && <span title="Other emails/phones fused into this person" style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.06em", color: "#3538CD", background: "#EEF4FF", padding: "2px 7px", borderRadius: 4, cursor: "pointer" }} onClick={() => setTab("linked")}>+{linked.length} LINKED</span>}
            </div>
            <div style={{ fontSize: 13, color: "#6B7280", display: "flex", gap: 12, flexWrap: "wrap" }}>
              {c.email && <span>{c.email}</span>}
              {c.phone && <span>{c.phone}</span>}
            </div>
          </div>
          <button style={S.btnGhost} onClick={onClose}><I d="M6 18L18 6M6 6l12 12" size={18} stroke="#6B7280" /></button>
        </div>
        <div style={{ display: "flex", gap: 4, padding: "0 24px", borderBottom: "1px solid #E5E7EB" }}>
          {[["journey", `Journey (${timeline.length})`], ["clicks", `Clicks (${visits.length})`], ["linked", `Linked (${linked.length})`], ["details", "Details"]].map(([k, label]) => (
            <button key={k} onClick={() => setTab(k)} style={{ padding: "12px 14px", fontSize: 13, fontWeight: 600, background: "transparent", border: "none", borderBottom: tab === k ? "2px solid #111827" : "2px solid transparent", color: tab === k ? "#111827" : "#6B7280", cursor: "pointer", marginBottom: -1 }}>{label}</button>
          ))}
        </div>
        <div style={{ padding: 24, overflowY: "auto", flex: 1 }}>
          {tab === "journey" && (
            timeline.length === 0 ? <div style={{ textAlign: "center", padding: "32px 0", color: "#9CA3AF" }}>No journey events yet.</div> : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                {timeline.map((it, i) => {
                  const color = crmTimelineColor(it);
                  const last = i === timeline.length - 1;
                  return (
                    <div key={i} style={{ display: "flex", gap: 14 }}>
                      <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
                        <div style={{ width: 10, height: 10, borderRadius: "50%", background: color, flexShrink: 0, marginTop: 4 }} />
                        {!last && <div style={{ width: 2, flex: 1, background: "#E5E7EB", marginTop: 2 }} />}
                      </div>
                      <div style={{ flex: 1, minWidth: 0, paddingBottom: last ? 0 : 18 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                          <span style={{ fontSize: 13, fontWeight: 600, color: "#111827" }}>{it.label}</span>
                          {it.source && <span style={{ fontSize: 11, color: "#6B7280", background: "#F3F4F6", padding: "1px 6px", borderRadius: 4 }}>{it.source}</span>}
                          <span style={{ fontSize: 12, color: "#9CA3AF" }}>{crmRelTime(it.ts)}</span>
                        </div>
                        {it.url && <div style={{ fontSize: 12, color: "#6B7280", marginTop: 2, wordBreak: "break-all" }}>{it.url}</div>}
                        <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 2 }}>{new Date(it.ts).toLocaleString("en-US", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" })}</div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )
          )}
          {tab === "clicks" && (
            visits.length === 0 ? <div style={{ textAlign: "center", padding: "32px 0", color: "#9CA3AF" }}>{c.is_tracked ? "No page views tracked yet." : "This contact wasn't tracked by shumard.js, so there's no on-site click activity."}</div> : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                {visits.map((v, i) => (
                  <div key={v.id || i} style={{ padding: "12px 0", borderBottom: i < visits.length - 1 ? "1px solid #F3F4F6" : "none" }}>
                    <div style={{ fontSize: 13, fontWeight: 500, color: "#111827" }}>{v.page_title || "Page view"}</div>
                    <div style={{ fontSize: 12, color: "#6B7280", wordBreak: "break-all", marginTop: 2 }}>{v.current_url}</div>
                    <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 2 }}>{new Date(v.timestamp).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}{v.referrer_url ? ` · from ${v.referrer_url}` : ""}</div>
                  </div>
                ))}
              </div>
            )
          )}
          {tab === "linked" && (
            linked.length === 0 ? <div style={{ textAlign: "center", padding: "32px 0", color: "#9CA3AF" }}>No other identities linked to this person.<div style={{ fontSize: 12, marginTop: 6 }}>Linked identities appear when the same person is seen under another email/phone on the same browser or IP (e.g. a purchase under a different checkout email).</div></div> : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                <div style={{ fontSize: 12, color: "#6B7280", marginBottom: 12 }}>These identities were fused into this person by the tracker (same browser / IP). Their events and clicks are already merged into the Journey above.</div>
                {linked.map((l, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, padding: "12px 0", borderBottom: i < linked.length - 1 ? "1px solid #F3F4F6" : "none" }}>
                    <div style={{ minWidth: 0 }}>
                      <div style={{ fontSize: 13, fontWeight: 500, color: "#111827", wordBreak: "break-all" }}>{l.email || l.phone || l.contact_id}</div>
                      {l.email && l.phone && <div style={{ fontSize: 12, color: "#6B7280", marginTop: 2 }}>{l.phone}</div>}
                    </div>
                    <span style={{ flexShrink: 0, fontSize: 10, fontWeight: 700, letterSpacing: "0.06em", padding: "2px 7px", borderRadius: 4, color: l.tracked ? "#3538CD" : "#9CA3AF", background: l.tracked ? "#EEF4FF" : "#F3F4F6" }}>{l.tracked ? "TRACKED" : "EVENT-ONLY"}</span>
                  </div>
                ))}
              </div>
            )
          )}
          {tab === "details" && (
            <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
              <div>
                <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "#9CA3AF", marginBottom: 8 }}>Identity</div>
                <CrmDetailRows rows={[["Name", c.name], ["Email", c.email], ["Phone", c.phone], ["First name", c.first_name], ["Last name", c.last_name], ["Tracked", c.is_tracked ? "yes" : "no"], ["Contact ID", c.contact_id]]} />
              </div>
              {Object.keys(attr).filter(k => k !== "extra").length > 0 && (
                <div>
                  <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "#9CA3AF", marginBottom: 8 }}>Attribution</div>
                  <CrmDetailRows rows={Object.entries(attr).filter(([k]) => k !== "extra").map(([k, v]) => [k, String(v)])} />
                  {attr.extra && Object.keys(attr.extra).length > 0 && <div style={{ marginTop: 6 }}><CrmDetailRows rows={Object.entries(attr.extra).map(([k, v]) => [`extra.${k}`, String(v)])} /></div>}
                </div>
              )}
              {(c.tags || []).length > 0 && (
                <div>
                  <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "#9CA3AF", marginBottom: 8 }}>Tags</div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>{c.tags.map(t => <span key={t} style={{ fontSize: 11, background: "#F3F4F6", color: "#374151", padding: "3px 8px", borderRadius: 4 }}>{t}</span>)}</div>
                </div>
              )}
              <div>
                <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "#9CA3AF", marginBottom: 8 }}>Funnel events ({events.length})</div>
                {events.length === 0 ? <div style={{ fontSize: 13, color: "#9CA3AF" }}>None</div> : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {events.map((e, i) => (
                      <div key={e.id || i} style={{ display: "flex", justifyContent: "space-between", fontSize: 13, gap: 12 }}>
                        <span style={{ color: "#374151" }}>{e.event_type}{e.metadata && e.metadata.source ? ` · ${e.metadata.source}` : ""}</span>
                        <span style={{ color: "#9CA3AF", whiteSpace: "nowrap" }}>{new Date(e.event_time).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}</span>
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
  const clicks = drill.clicks || [];
  const fmtWhen = (ts) => new Date(ts).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
  const fmtGap = (click, buy) => {
    const h = (new Date(buy).getTime() - new Date(click).getTime()) / 3600000;
    if (h < 1) return `${Math.max(1, Math.round(h * 60))}m later`;
    if (h < 48) return `${Math.round(h)}h later`;
    return `${Math.round(h / 24)}d later`;
  };
  return (
    <div style={S.overlay} onClick={onClose}>
      <div style={{ ...S.modal, maxWidth: 860, width: "94%", maxHeight: "85vh", padding: 0, textAlign: "left", display: "flex", flexDirection: "column", overflow: "hidden" }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: "20px 24px", borderBottom: "1px solid #E5E7EB", display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#6B7280" }}>Email source</div>
            <h2 style={{ fontSize: 18, fontWeight: 600, color: "#111827", margin: "2px 0 6px" }}>{drill.source}</h2>
            <div style={{ fontSize: 13, color: "#6B7280", display: "flex", gap: 14, flexWrap: "wrap" }}>
              <span><b style={{ color: "#111827" }}>{drill.total_clicks ?? clicks.length}</b> clicks</span>
              <span><b style={{ color: "#111827" }}>{drill.people ?? "—"}</b> people</span>
              <span><b style={{ color: "#047857" }}>{drill.buyers ?? 0}</b> buyers</span>
            </div>
          </div>
          <button style={S.btnGhost} onClick={onClose}><I d="M6 18L18 6M6 6l12 12" size={18} stroke="#6B7280" /></button>
        </div>
        {/* Funnel-stage breakdown — of the people who clicked this source, how far they got */}
        {!loading && drill.funnel && drill.funnel.clickers > 0 && (() => {
          const f = drill.funnel;
          const stages = [
            { key: "clickers", label: "Clicked", color: "#14B8A6" },
            { key: "registered", label: "Registered", color: "#3B82F6" },
            { key: "attended", label: "Attended", color: "#0EA5E9" },
            { key: "saw_cta", label: "Saw CTA", color: "#F59E0B" },
            { key: "clicked_cta", label: "Clicked CTA", color: "#10B981" },
            { key: "purchased", label: "Purchased", color: "#EF4444" },
          ];
          const base = f.clickers || 1;
          return (
            <div style={{ padding: "16px 24px", borderBottom: "1px solid #E5E7EB", background: "#FAFAFA" }}>
              <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#6B7280", marginBottom: 12 }}>Funnel reach of these clickers</div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                {stages.map(s => {
                  const v = f[s.key] || 0;
                  const pct = Math.round((v / base) * 100);
                  return (
                    <div key={s.key} style={{ flex: "1 1 110px", minWidth: 100, background: "#fff", border: "1px solid #E5E7EB", borderRadius: 8, padding: "10px 12px" }}>
                      <div style={{ fontSize: 11, fontWeight: 600, color: s.color, marginBottom: 4 }}>{s.label}</div>
                      <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
                        <span style={{ fontSize: 20, fontWeight: 600, color: "#111827", fontVariantNumeric: "tabular-nums" }}>{v.toLocaleString()}</span>
                        <span style={{ fontSize: 12, color: "#9CA3AF" }}>{pct}%</span>
                      </div>
                      <div style={{ height: 3, background: "#F3F4F6", borderRadius: 2, marginTop: 6, overflow: "hidden" }}>
                        <div style={{ width: `${pct}%`, height: "100%", background: s.color }} />
                      </div>
                    </div>
                  );
                })}
              </div>
              <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 10 }}>% of the {base.toLocaleString()} identified people who clicked this email. Post‑webinar emails reach people who already registered/attended — this is their overall funnel position.</div>
            </div>
          );
        })()}
        <div style={{ padding: 0, overflowY: "auto", flex: 1 }}>
          {loading ? (
            <div style={{ textAlign: "center", padding: "48px 0", color: "#9CA3AF" }}>Loading…</div>
          ) : clicks.length === 0 ? (
            <div style={{ textAlign: "center", padding: "48px 0", color: "#9CA3AF" }}>No clicks recorded for this source yet.</div>
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
                    <tr key={i} onClick={() => (c.email || c.contact_id) && onOpenContact(c.email || c.contact_id)} style={{ cursor: (c.email || c.contact_id) ? "pointer" : "default" }} title={c.email ? "Open full journey" : ""}>
                      <td style={{ ...S.td, textAlign: "left" }}>
                        <div style={{ fontWeight: 600, color: "#111827" }}>{who}</div>
                        {c.email && c.name && <div style={{ fontSize: 12, color: "#6B7280" }}>{c.email}</div>}
                      </td>
                      <td style={{ ...S.td, textAlign: "right", color: "#6B7280", whiteSpace: "nowrap" }}>{fmtWhen(c.click_ts)}</td>
                      <td style={{ ...S.td, textAlign: "left", color: "#6B7280", maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={c.current_url}>{path}</td>
                      <td style={{ ...S.td, textAlign: "right", whiteSpace: "nowrap" }}>
                        {c.purchased_at
                          ? <span style={{ color: "#047857", fontWeight: 600 }}>✓ {fmtGap(c.click_ts, c.purchased_at)}</span>
                          : <span style={{ color: "#D1D5DB" }}>—</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
        {clicks.length >= 500 && <div style={{ padding: "10px 24px", borderTop: "1px solid #E5E7EB", fontSize: 12, color: "#9CA3AF" }}>Showing the most recent 500 clicks.</div>}
      </div>
    </div>
  );
}

function EntryForm({ initial, onSubmit, onCancel, isMobile }) {
  const today = getLADate();
  const defaults = initial
    ? { fb_spend: initial.fb_spend ?? 0, fb_link_clicks: initial.fb_link_clicks ?? 0, registrations: initial.registrations ?? 0, replays: initial.replays ?? 0, viewedcta: initial.viewedcta ?? 0, clickedcta: initial.clickedcta ?? 0, purchases_fb: initial.purchases_fb ?? 0, purchases_native: initial.purchases_native ?? 0, purchases_youtube: initial.purchases_youtube ?? 0, purchases_aibot: initial.purchases_aibot ?? 0, purchases_postwebinar: initial.purchases_postwebinar ?? 0, purchases_cpa: initial.purchases_cpa ?? 0, stayed_45: initial.stayed_45 ?? 0, stayed_60: initial.stayed_60 ?? 0, stayed_80: initial.stayed_80 ?? 0, attended: initial.attended ?? 0 }
    : { fb_spend: "", fb_link_clicks: "", registrations: "", replays: "", viewedcta: "", clickedcta: "", purchases_fb: "", purchases_native: "", purchases_youtube: "", purchases_aibot: "", purchases_postwebinar: "", purchases_cpa: "", stayed_45: "", stayed_60: "", stayed_80: "", attended: "" };
  const [f, setF] = useState({ date: initial?.date || today, ...defaults });
  const set = (k, v) => setF(p => ({ ...p, [k]: v }));
  const go = () => onSubmit({ date: f.date, day: getLADay(f.date), fb_spend: parseFloat(f.fb_spend) || 0, fb_link_clicks: parseInt(f.fb_link_clicks) || 0, registrations: parseInt(f.registrations) || 0, replays: parseInt(f.replays) || 0, viewedcta: parseInt(f.viewedcta) || 0, clickedcta: parseInt(f.clickedcta) || 0, purchases_fb: parseInt(f.purchases_fb) || 0, purchases_native: parseInt(f.purchases_native) || 0, purchases_youtube: parseInt(f.purchases_youtube) || 0, purchases_aibot: parseInt(f.purchases_aibot) || 0, purchases_postwebinar: parseInt(f.purchases_postwebinar) || 0, purchases_cpa: parseInt(f.purchases_cpa) || 0, stayed_45: parseInt(f.stayed_45) || 0, stayed_60: parseInt(f.stayed_60) || 0, stayed_80: parseInt(f.stayed_80) || 0, attended: parseInt(f.attended) || 0 });
  const fields = [{ k: "fb_spend", l: "Facebook Spend ($)", step: "0.01", ph: "0.00" }, { k: "fb_link_clicks", l: "Total Reg. Page Visited", ph: "0" }, { k: "registrations", l: "Registrations", ph: "0" }, { k: "replays", l: "Replays", ph: "0" }, { k: "viewedcta", l: "Viewed CTA", ph: "0" }, { k: "clickedcta", l: "Clicked CTA", ph: "0" }, { k: "purchases_fb", l: "FB Purchases", ph: "0" }, { k: "purchases_native", l: "Native Ads", ph: "0" }, { k: "purchases_youtube", l: "Youtube", ph: "0" }, { k: "purchases_aibot", l: "AI Chat Bot", ph: "0" }, { k: "purchases_postwebinar", l: "Post Webinar", ph: "0" }, { k: "purchases_cpa", l: "CPA Traffic Funnel", ph: "0" }, { k: "stayed_45", l: "45 min", ph: "0" }, { k: "stayed_60", l: "60 min", ph: "0" }, { k: "stayed_80", l: "80 min", ph: "0" }, { k: "attended", l: "Attended", ph: "0" }];
  return (
    <div className="fi form-container" style={S.fc}>
      <div style={S.fh}><div style={{ ...S.formBadge, background: initial ? "#EFF8FF" : "#ECFDF3", color: initial ? "#175CD3" : "#12864A" }}>{initial ? "EDIT ENTRY" : "NEW ENTRY"}</div><h2 style={S.ft}>{initial ? `Update ${fmtDateNice(initial.date)}` : "Add Daily Metrics"}</h2><p style={S.fs}>Enter metrics for the day. Fields default to 0 if empty.</p></div>
      <div className="form-card" style={S.fcard}>
        <div style={{ marginBottom: 20 }}><label style={S.fl}>Date (MM/DD/YYYY)</label><input style={S.inp} value={f.date} onChange={e => set("date", e.target.value)} placeholder="03/14/2026" disabled={!!initial} />{!initial && f.date && <div style={S.hint}>{getLADay(f.date)}</div>}</div>
        <div className="form-grid" style={S.fgrid}>{fields.map(fi => (<div key={fi.k}><label style={S.fl}>{fi.l}</label><input style={S.inp} type="number" step={fi.step} placeholder={fi.ph} value={f[fi.k]} onChange={e => set(fi.k, e.target.value)} /></div>))}</div>
        <div style={S.fa}><button style={S.btnLight} onClick={onCancel}>Cancel</button><button style={S.btnDark} onClick={go}>{initial ? "Update Entry" : "Add Entry"}</button></div>
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
      <div style={S.fh}><h2 style={S.ft}>{initial ? `Edit "${initial.name}"` : "Create Custom Metric"}</h2></div>
      <div style={S.fcard}>
        <div style={{ marginBottom: 20 }}><label style={S.fl}>Metric Name</label><input style={S.inp} value={f.name} onChange={e => setF(p => ({ ...p, name: e.target.value }))} placeholder="e.g. CTA Click Rate" /></div>
        <div style={{ marginBottom: 20 }}><label style={S.fl}>Formula</label><input style={{ ...S.inp, fontFamily: "'IBM Plex Mono', monospace" }} value={f.formula} onChange={e => setF(p => ({ ...p, formula: e.target.value }))} placeholder="e.g. clickedcta / viewedcta * 100" /><div style={S.hint}>Available: {allVars.join(", ")}</div></div>
        <div style={{ marginBottom: 20 }}><label style={S.fl}>Display Format</label><div style={{ display: "flex", gap: 8 }}>{[{ v: "number", l: "Number" }, { v: "percent", l: "Percent (%)" }, { v: "currency", l: "Currency ($)" }].map(o => (<button key={o.v} style={f.format === o.v ? S.fmtA : S.fmtB} onClick={() => setF(p => ({ ...p, format: o.v }))}>{o.l}</button>))}</div></div>
        {metrics.length > 0 && f.formula && (<div style={S.prev}><div style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase", color: "#6B7280", marginBottom: 8 }}>Preview ({fmtDateNice(metrics[0].date)})</div><div style={{ fontSize: 28, fontWeight: 600, color: preview !== null ? "#111827" : "#DC2626" }}>{fmtVal(preview, f.format)}</div></div>)}
        <div style={S.fa}><button style={S.btnGhost} onClick={onCancel}>Cancel</button><button style={{ ...S.btnDark, opacity: f.name && f.formula ? 1 : 0.4 }} onClick={() => f.name && f.formula && onSubmit(f)} disabled={!f.name || !f.formula}>{initial ? "Update Metric" : "Create Metric"}</button></div>
      </div>
    </div>
  );
}

// Simple markdown renderer for chat messages
const CHART_COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#8B5CF6', '#EF4444', '#06B6D4'];

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
  if (abs >= 10) return v.toFixed(0);
  if (abs >= 1) return v.toFixed(1);
  if (abs > 0) return v.toFixed(2);
  return '0';
}

function MdChart({ spec }) {
  if (!spec || !Array.isArray(spec.data) || spec.data.length === 0) {
    return <div style={{ padding: 12, color: '#9CA3AF', fontSize: 13 }}>(empty chart)</div>;
  }
  const type = spec.type === 'bar' ? 'bar' : (spec.type === 'area' ? 'area' : 'line');
  const xKey = spec.x || 'x';
  const series = (Array.isArray(spec.series) ? spec.series : []).filter(s => s && s.key);
  if (series.length === 0) return <div style={{ padding: 12, color: '#9CA3AF', fontSize: 13 }}>(chart has no series)</div>;

  const W = 640, H = 280, PAD_L = 48, PAD_R = 16, PAD_T = 12, PAD_B = 44;
  const innerW = W - PAD_L - PAD_R;
  const innerH = H - PAD_T - PAD_B;

  let yMin = 0, yMax = 0;
  spec.data.forEach(d => {
    series.forEach(s => {
      const v = Number(d[s.key]);
      if (Number.isFinite(v)) {
        if (v < yMin) yMin = v;
        if (v > yMax) yMax = v;
      }
    });
  });
  if (yMax === yMin) yMax = yMin + 1;
  // 10% headroom above the tallest value so bars/lines don't kiss the top edge.
  const niceMax = niceCeil(yMax * 1.1);
  const niceMin = yMin >= 0 ? 0 : -niceCeil(-yMin * 1.1);
  const range = niceMax - niceMin || 1;
  const clipId = `chartclip-${Math.random().toString(36).slice(2, 9)}`;

  const yScale = (v) => PAD_T + innerH - ((v - niceMin) / range) * innerH;
  const xCount = spec.data.length;
  const xScale = (i) => PAD_L + (xCount === 1 ? innerW / 2 : (i / (xCount - 1)) * innerW);
  const groupW = innerW / Math.max(xCount, 1);
  const barW = type === 'bar' ? Math.max(2, (groupW * 0.8) / series.length) : 0;

  const yTickCount = 5;
  const yTicks = [];
  for (let t = 0; t <= yTickCount; t++) {
    const v = niceMin + range * (t / yTickCount);
    yTicks.push({ v, y: yScale(v) });
  }

  const xLabelEvery = Math.max(1, Math.ceil(xCount / 8));
  const zeroY = yScale(0);

  const chartWrap = { margin: "12px 0", padding: 14, border: "1px solid #E5E7EB", borderRadius: 12, background: "#fff" };
  const chartTitle = { fontSize: 13, fontWeight: 600, color: "#111827", marginBottom: 8 };
  const chartLegend = { display: "flex", flexWrap: "wrap", gap: 14, marginTop: 8, justifyContent: "center" };
  const chartLegendItem = { display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: "#4B5563" };

  return (
    <div style={chartWrap}>
      {spec.title && <div style={chartTitle}>{spec.title}</div>}
      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto", display: "block" }}>
        <defs>
          <clipPath id={clipId}>
            <rect x={PAD_L} y={PAD_T} width={innerW} height={innerH} />
          </clipPath>
        </defs>
        {yTicks.map((t, i) => (
          <g key={`t${i}`}>
            <line x1={PAD_L} y1={t.y} x2={W - PAD_R} y2={t.y} stroke="#F3F4F6" strokeWidth={1} />
            <text x={PAD_L - 6} y={t.y + 3} fontSize={10} fill="#9CA3AF" textAnchor="end">{fmtChartNum(t.v)}</text>
          </g>
        ))}
        <line x1={PAD_L} y1={H - PAD_B} x2={W - PAD_R} y2={H - PAD_B} stroke="#E5E7EB" />
        {spec.data.map((d, i) => (i % xLabelEvery === 0 || i === xCount - 1) && (
          <text key={`x${i}`} x={xScale(i)} y={H - PAD_B + 14} fontSize={10} fill="#6B7280" textAnchor="middle">
            {(() => { const s = String(d[xKey] ?? ''); return s.length > 10 ? s.slice(0, 10) : s; })()}
          </text>
        ))}
        <g clipPath={`url(#${clipId})`}>
        {series.map((s, si) => {
          const color = s.color || CHART_COLORS[si % CHART_COLORS.length];
          if (type === 'bar') {
            return spec.data.map((d, i) => {
              const v = Number(d[s.key]);
              if (!Number.isFinite(v)) return null;
              // Center the group of bars on each x position. 80% of group
              // width is used for bars; the remaining 20% gives breathing room.
              const x = xScale(i) - (barW * series.length) / 2 + si * barW;
              const y = yScale(v);
              const h = Math.abs(zeroY - y);
              return <rect key={`${si}-${i}`} x={x} y={Math.min(y, zeroY)} width={barW * 0.86} height={Math.max(h, 1)} fill={color} rx={2} />;
            });
          }
          const pts = spec.data
            .map((d, i) => {
              const v = Number(d[s.key]);
              return Number.isFinite(v) ? { x: xScale(i), y: yScale(v) } : null;
            })
            .filter(Boolean);
          if (pts.length === 0) return null;
          const polyPts = pts.map(p => `${p.x},${p.y}`).join(' ');
          if (type === 'area' && pts.length >= 2) {
            const area = `M${pts[0].x},${zeroY} L${pts.map(p => `${p.x},${p.y}`).join(' L')} L${pts[pts.length-1].x},${zeroY} Z`;
            return (
              <g key={`s${si}`}>
                <path d={area} fill={color} opacity={0.14} />
                <polyline points={polyPts} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" />
              </g>
            );
          }
          return (
            <g key={`s${si}`}>
              <polyline points={polyPts} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
              {pts.length <= 60 && pts.map((p, i) => (
                <circle key={i} cx={p.x} cy={p.y} r={2.5} fill={color} />
              ))}
            </g>
          );
        })}
        </g>
      </svg>
      <div style={chartLegend}>
        {series.map((s, si) => (
          <span key={si} style={chartLegendItem}>
            <span style={{ width: 10, height: 10, borderRadius: 2, background: s.color || CHART_COLORS[si % CHART_COLORS.length], display: "inline-block" }} />
            <span>{s.label || s.key}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

const MD = {
  h1: { fontSize: 20, fontWeight: 700, color: "#0F172A", margin: "18px 0 8px", letterSpacing: "-0.02em", lineHeight: 1.3 },
  h2: { fontSize: 17, fontWeight: 700, color: "#0F172A", margin: "16px 0 6px", letterSpacing: "-0.01em", lineHeight: 1.3 },
  h3: { fontSize: 15, fontWeight: 700, color: "#0F172A", margin: "14px 0 4px", lineHeight: 1.3 },
  h4: { fontSize: 14, fontWeight: 600, color: "#0F172A", margin: "12px 0 4px", lineHeight: 1.3 },
  p: { margin: "6px 0", lineHeight: 1.6 },
  ul: { margin: "6px 0 6px 4px", padding: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 2 },
  ol: { margin: "6px 0 6px 4px", padding: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 2 },
  liRow: { display: "flex", gap: 8, lineHeight: 1.6, alignItems: "flex-start" },
  bullet: { color: "#9CA3AF", flexShrink: 0, lineHeight: 1.6 },
  num: { color: "#6B7280", fontWeight: 600, fontVariantNumeric: "tabular-nums", flexShrink: 0, minWidth: 18, lineHeight: 1.6 },
  code: { background: "#F3F4F6", padding: "1px 6px", borderRadius: 4, fontSize: "0.88em", fontFamily: "'IBM Plex Mono', ui-monospace, monospace", color: "#0F172A", border: "1px solid #E5E7EB" },
  preWrap: { margin: "10px 0", borderRadius: 10, overflow: "hidden", border: "1px solid #1F2937" },
  preLang: { background: "#1F2937", color: "#9CA3AF", padding: "6px 14px", fontSize: 11, fontWeight: 600, fontFamily: "'IBM Plex Mono', ui-monospace, monospace", textTransform: "lowercase", letterSpacing: "0.04em" },
  pre: { background: "#0F172A", color: "#E5E7EB", padding: "12px 16px", overflow: "auto", fontSize: 12.5, lineHeight: 1.55, fontFamily: "'IBM Plex Mono', ui-monospace, monospace", margin: 0, whiteSpace: "pre" },
  blockquote: { borderLeft: "3px solid #E5E7EB", padding: "4px 0 4px 14px", margin: "10px 0", color: "#4B5563", fontStyle: "italic" },
  hr: { border: "none", borderTop: "1px solid #E5E7EB", margin: "16px 0" },
  link: { color: "#2563EB", textDecoration: "underline", textUnderlineOffset: 2 },
  strong: { fontWeight: 700, color: "#0F172A" },
  em: { fontStyle: "italic" },
  strike: { textDecoration: "line-through", color: "#9CA3AF" },
  // Wrap is a horizontal-scroll container. Tables size to their content
  // (max-content) but stretch to at least the column width — so narrow tables
  // fill the space and wide tables get a horizontal scrollbar instead of
  // crushing their cells.
  tableWrap: { margin: "12px 0", border: "1px solid #E5E7EB", borderRadius: 10, overflowX: "auto", maxWidth: "100%" },
  table: { borderCollapse: "collapse", minWidth: "100%", width: "max-content", fontSize: 13, background: "#fff" },
  th: { background: "#FAFAFA", padding: "10px 14px", textAlign: "left", fontWeight: 600, color: "#374151", borderBottom: "1px solid #E5E7EB", fontSize: 12, whiteSpace: "nowrap" },
  td: { padding: "10px 14px", borderBottom: "1px solid #F3F4F6", color: "#374151", fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" },
  trEven: { background: "#FAFBFC" },
};

const splitTableRow = (line) => {
  let l = line.trim();
  if (l.startsWith('|')) l = l.slice(1);
  if (l.endsWith('|')) l = l.slice(0, -1);
  return l.split('|').map(c => c.trim());
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
      // Special-case ```chart — parse JSON and render an inline SVG chart.
      // Fall back to a code block if the JSON is malformed.
      if (lang === 'chart') {
        try {
          const spec = JSON.parse(code.join('\n'));
          out.push(<MdChart key={key++} spec={spec} />);
          continue;
        } catch (e) {
          out.push(
            <div key={key++} style={MD.preWrap}>
              <div style={MD.preLang}>chart (invalid JSON)</div>
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
      i += 2;
      const rows = [];
      while (i < lines.length && lines[i].includes('|') && lines[i].trim() !== '') {
        rows.push(splitTableRow(lines[i])); i++;
      }
      out.push(
        <div key={key++} style={MD.tableWrap}>
          <table style={MD.table}>
            <thead>
              <tr>{header.map((c, j) => <th key={j} style={MD.th}>{renderInline(c, 0)}</th>)}</tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => (
                <tr key={ri} style={ri % 2 === 1 ? MD.trEven : undefined}>
                  {r.map((c, ci) => <td key={ci} style={MD.td}>{renderInline(c, 0)}</td>)}
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
    wrap: { padding: "32px 40px", maxWidth: 1400, margin: "0 auto", className: "query-wrap" },
    header: { marginBottom: 24 },
    badge: { display: "inline-block", padding: "4px 10px", borderRadius: 6, fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", background: "#EEF4FF", color: "#3538CD", marginBottom: 10 },
    title: { fontSize: 22, fontWeight: 700, color: "#111827", margin: 0 },
    subtitle: { fontSize: 13, color: "#6B7280", marginTop: 4 },
    filters: { display: "flex", flexWrap: "wrap", gap: 12, padding: 20, background: "#FAFAFA", borderRadius: 12, border: "1px solid #E8E8E6", marginBottom: 20 },
    filterGroup: { display: "flex", flexDirection: "column", gap: 4 },
    filterLabel: { fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.05em" },
    filterInput: { padding: "8px 12px", borderRadius: 8, border: "1px solid #D1D5DB", fontSize: 13, outline: "none", minWidth: 140 },
    filterSelect: { padding: "8px 12px", borderRadius: 8, border: "1px solid #D1D5DB", fontSize: 13, outline: "none", background: "#fff", cursor: "pointer", minWidth: 140 },
    actions: { display: "flex", gap: 10, alignItems: "flex-end" },
    resultBar: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 },
    resultCount: { fontSize: 13, color: "#6B7280", fontWeight: 500 },
    tableWrap: { overflowX: "auto", border: "1px solid #E8E8E6", marginLeft: "calc(-1 * (100vw - 100%) / 2)", marginRight: "calc(-1 * (100vw - 100%) / 2)", width: "100vw", maxWidth: "100vw", borderRadius: 0, borderLeft: "none", borderRight: "none" },
    table: { width: "100%", borderCollapse: "collapse", fontSize: 13 },
    th: { padding: "10px 14px", textAlign: "left", fontWeight: 600, color: "#6B7280", borderBottom: "1px solid #E8E8E6", background: "#FAFAFA", whiteSpace: "normal", maxWidth: 100, lineHeight: 1.3, cursor: "pointer", userSelect: "none" },
    td: { padding: "9px 14px", borderBottom: "1px solid #F3F4F6", color: "#374151", whiteSpace: "nowrap", maxWidth: 250, overflow: "hidden", textOverflow: "ellipsis" },
    sortIcon: { marginLeft: 4, fontSize: 10 },
  };

  return (
    <div className="query-wrap" style={QS.wrap}>
      <div style={QS.header}>
        <div style={QS.badge}>Admin</div>
        <h2 style={QS.title}>Query Data</h2>
        <p style={QS.subtitle}>Filter and export data from the database</p>
      </div>

      <div className="query-filters" style={QS.filters}>
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>Table</span>
          <select style={QS.filterSelect} value={table} onChange={e => { setTable(e.target.value); setResults(null); setEventType(""); setVariant(""); setSortBy(""); }}>
            <option value="events">Events (Raw)</option>
            <option value="daily_metrics">Daily Metrics (Raw)</option>
            <option value="dashboard">Dashboard (Processed)</option>
          </select>
        </div>
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>From</span>
          <input type="date" style={QS.filterInput} value={dateFrom} onChange={e => setDateFrom(e.target.value)} />
        </div>
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>To</span>
          <input type="date" style={QS.filterInput} value={dateTo} onChange={e => setDateTo(e.target.value)} />
        </div>
        {(table === "events" || table === "dashboard") && (
          <>
            <div style={QS.filterGroup}>
              <span style={QS.filterLabel}>Event Type</span>
              <select style={QS.filterSelect} value={eventType} onChange={e => setEventType(e.target.value)}>
                <option value="">All</option>
                {eventTypes.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div style={QS.filterGroup}>
              <span style={QS.filterLabel}>Variant {table === "dashboard" ? "(attributed)" : "(own tag)"}</span>
              <select style={QS.filterSelect} value={variant} onChange={e => setVariant(e.target.value)}>
                <option value="">All</option>
                <option value="A">A</option>
                <option value="B">B</option>
                <option value="undetected">Undetected</option>
              </select>
            </div>
            <div style={QS.filterGroup}>
              <span style={QS.filterLabel}>Search (name/email/phone)</span>
              <input style={QS.filterInput} placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)} />
            </div>
          </>
        )}
        <div style={QS.filterGroup}>
          <span style={QS.filterLabel}>Limit</span>
          <select style={QS.filterSelect} value={limit} onChange={e => setLimit(e.target.value)}>
            <option value="100">100</option>
            <option value="500">500</option>
            <option value="1000">1,000</option>
            <option value="5000">5,000</option>
          </select>
        </div>
        <div style={QS.actions}>
          <button style={{ ...S.btnDark, padding: "8px 20px", borderRadius: 8, fontSize: 13 }} onClick={() => runQuery()} disabled={loading}>{loading ? "Querying…" : "Run Query"}</button>
        </div>
      </div>

      {results && (
        <>
          <div style={QS.resultBar}>
            <span style={QS.resultCount}>{results.count.toLocaleString()} result{results.count !== 1 ? "s" : ""}</span>
            <button style={{ ...S.btnLight, padding: "6px 14px", fontSize: 12 }} onClick={downloadCSV}><I d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3" size={14} stroke="#374151" /> Export CSV</button>
          </div>
          <div style={QS.tableWrap}>
            <table style={QS.table}>
              <thead><tr>
                {results.data.length > 0 && Object.keys(results.data[0]).map(col => (
                  <th key={col} style={QS.th} onClick={() => handleSort(col)}>
                    {col}{sortBy === col && <span style={QS.sortIcon}>{sortDir === "asc" ? " ▲" : " ▼"}</span>}
                  </th>
                ))}
              </tr></thead>
              <tbody>
                {results.data.length === 0 ? (
                  <tr><td colSpan={99} style={{ ...QS.td, textAlign: "center", color: "#9CA3AF", padding: 32 }}>No results found</td></tr>
                ) : results.data.map((row, i) => (
                  <tr key={i} onMouseEnter={e => e.currentTarget.style.background = "#F9FAFB"} onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                    {Object.values(row).map((v, j) => (
                      <td key={j} style={QS.td} title={v !== null && v !== undefined ? String(typeof v === "object" ? JSON.stringify(v) : v) : ""}>
                        {v === null ? <span style={{ color: "#D1D5DB" }}>—</span> : typeof v === "object" ? JSON.stringify(v) : String(v)}
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
    <div style={IC.typing}>
      <div className="blink-1" style={IC.dot} />
      <div className="blink-2" style={IC.dot} />
      <div className="blink-3" style={IC.dot} />
      <span key={idx} className="fi" style={IC.typingText}>{TYPING_STATUSES[idx]}…</span>
    </div>
  );
}

function InsightsChat({ flash, isMobile, activeFunnel }) {
  const [history, setHistory] = useState([]);
  const [activeId, setActiveId] = useState(null);
  const [chatMsgs, setChatMsgs] = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(!isMobile);
  const [historyLoading, setHistoryLoading] = useState(true);
  const chatEndRef = useRef(null);
  // Cache of full conversation messages, keyed by id and stamped with the
  // server's updated_at so we can detect remote edits (other tabs/devices).
  // Shape: { [id]: { msgs, updatedAt } }
  const msgsCache = useRef({});

  const loadHistory = useCallback(async () => {
    try {
      const { data } = await api.getConversations();
      setHistory(data || []);
    } catch (e) {
      console.error('Failed to load conversations:', e.message);
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  // Load conversation list from Supabase on mount
  useEffect(() => { loadHistory(); }, [loadHistory]);

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
    const remote = history.find(c => c.id === activeId);
    const cached = msgsCache.current[activeId];
    if (cached && (!remote || cached.updatedAt === remote.updated_at)) {
      setChatMsgs(cached.msgs);
      return;
    }
    // Cache miss or stale (remote updated_at moved). Fetch and re-stamp.
    (async () => {
      try {
        const headers = await getAuthHeaders();
        const res = await fetch(`${API_BASE}/api/insights/conversations/${activeId}`, { headers });
        if (res.ok) {
          const body = await res.json();
          const msgs = body.data?.messages || [];
          msgsCache.current[activeId] = { msgs, updatedAt: body.data?.updated_at || remote?.updated_at };
          setChatMsgs(msgs);
        }
      } catch { /* fallback: empty */ }
    })();
  }, [activeId, history]);

  // Auto-scroll
  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [chatMsgs, chatLoading]);

  const saveChat = async (id, msgs) => {
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
    // Persist to Supabase (fire-and-forget)
    api.saveConversation(id, title, msgs).catch(e => console.error('Save chat error:', e.message));
  };

  const newChat = () => { setActiveId(null); setChatMsgs([]); setChatInput(""); };

  const deleteChat = async (id) => {
    setHistory(prev => prev.filter(c => c.id !== id));
    delete msgsCache.current[id];
    if (activeId === id) { setActiveId(null); setChatMsgs([]); }
    api.deleteConversation(id).catch(e => flash(e.message, "err"));
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

    const chatId = activeId || `chat-${Date.now()}`;
    if (!activeId) setActiveId(chatId);

    const newMsgs = [...chatMsgs, { role: "user", content: msg }];
    setChatMsgs(newMsgs);
    setChatInput("");
    setChatLoading(true);
    saveChat(chatId, newMsgs);

    try {
      const { reply } = await api.chat(newMsgs);
      const fullMsgs = [...newMsgs, { role: "assistant", content: reply }];
      setChatMsgs(fullMsgs);
      saveChat(chatId, fullMsgs);
    } catch (e) {
      flash(e.message, "err");
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

  const quickPrompts = [
    { letter: "W", color: "#3B82F6", label: "Weekly Recap", prompt: "Summarize this week's performance and call out anything unusual." },
    { letter: "F", color: "#F97316", label: "Funnel Health", prompt: "Walk me through the funnel step by step and flag where we're losing the most people." },
    { letter: "T", color: "#8B5CF6", label: "Trend Analyst", prompt: "What trends do you see in the last 14 days of metrics?" },
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
    const text = chatMsgs.map(m => `**${m.role === "user" ? "You" : "Funnel AI"}:** ${m.content}`).join("\n\n");
    try { await navigator.clipboard.writeText(text); flash("Chat copied to clipboard", "ok"); }
    catch { flash("Copy failed", "err"); }
  };

  const sourceLabel = activeFunnel ? activeFunnel.charAt(0).toUpperCase() + activeFunnel.slice(1) : "Select Source";

  const IC = {
    outer: { display: "flex", height: "calc(100vh - 64px)", gap: 0, position: "relative", background: "#fff", border: "none", borderRadius: 0, overflow: "hidden" },

    sidebar: { width: sidebarOpen ? (isMobile ? "100%" : 280) : 0, minWidth: sidebarOpen ? (isMobile ? "100%" : 280) : 0, background: "#fff", borderRight: sidebarOpen && !isMobile ? "1px solid #F1F3F5" : "none", display: "flex", flexDirection: "column", overflow: "hidden", transition: "width 0.2s, min-width 0.2s", ...(isMobile && sidebarOpen ? { position: "absolute", inset: 0, zIndex: 10 } : {}) },
    sidebarHeader: { padding: "18px 20px 10px", display: "flex", justifyContent: "space-between", alignItems: "center" },
    sidebarTitle: { fontSize: 16, fontWeight: 700, color: "#111827", letterSpacing: "-0.01em" },
    iconOnlyBtn: { background: "none", border: "none", cursor: "pointer", padding: 4, color: "#9CA3AF", display: "inline-flex", alignItems: "center", justifyContent: "center", borderRadius: 6 },
    newChatBtn: { margin: "0 16px 14px", padding: "10px 16px", background: "#0F172A", color: "#fff", border: "none", borderRadius: 999, fontSize: 13, fontWeight: 600, cursor: "pointer", fontFamily: fn, display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 6 },

    groupHeader: { padding: "14px 20px 6px", display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 12, fontWeight: 500, color: "#9CA3AF" },
    groupHeaderClickable: { cursor: "pointer", userSelect: "none" },

    savedItem: { padding: "8px 12px", margin: "0 8px", display: "flex", alignItems: "center", gap: 10, cursor: "pointer", borderRadius: 8, transition: "background 0.1s" },
    savedLabel: { fontSize: 13, color: "#111827", fontWeight: 500, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
    savedMore: { fontSize: 16, color: "#D1D5DB" },

    chatList: { flex: 1, overflowY: "auto", paddingBottom: 16 },
    histItem: { padding: "8px 20px", display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer", fontSize: 13, color: "#4B5563", transition: "background 0.1s", position: "relative" },
    histItemActive: { color: "#111827", fontWeight: 500, background: "#F3F4F6" },
    histDot: { position: "absolute", left: 0, top: "50%", transform: "translateY(-50%)", width: 3, height: 18, background: "#0F172A", borderRadius: "0 3px 3px 0" },
    histTitle: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 },
    histDel: { background: "none", border: "none", cursor: "pointer", color: "#D1D5DB", fontSize: 16, padding: 2, lineHeight: 1, flexShrink: 0 },

    main: { flex: 1, display: "flex", flexDirection: "column", minWidth: 0, background: "#fff" },
    topbar: { display: "flex", justifyContent: "space-between", alignItems: "center", padding: "14px 24px", borderBottom: "1px solid #F1F3F5", gap: 12, flexWrap: "wrap" },
    brand: { display: "flex", alignItems: "center", gap: 8 },
    brandName: { fontSize: 15, fontWeight: 600, color: "#111827", letterSpacing: "-0.01em" },
    brandPill: { padding: "2px 8px", background: "#F3F4F6", color: "#4B5563", fontSize: 11, fontWeight: 600, borderRadius: 6 },
    topActions: { display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" },
    pillBtn: { padding: "7px 14px", background: "#fff", border: "1px solid #E5E7EB", borderRadius: 999, fontSize: 13, fontWeight: 500, color: "#374151", cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 6, fontFamily: fn },
    pillBtnDark: { padding: "7px 16px", background: "#0F172A", color: "#fff", border: "none", borderRadius: 999, fontSize: 13, fontWeight: 600, cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 6, fontFamily: fn },

    body: { flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", minHeight: 0 },
    // Scroll container is full-width so backgrounds extend edge-to-edge.
    // The column constrains text/markdown to a comfortable reading width.
    msgScroll: { flex: 1, overflowY: "auto", padding: "32px 24px 8px", minHeight: 0 },
    msgColumn: { maxWidth: 760, margin: "0 auto", display: "flex", flexDirection: "column", gap: 24 },

    // User: tight rounded bubble, right-aligned. Uniform 16px radius (no tail).
    userBub: { alignSelf: "flex-end", maxWidth: "80%", padding: "10px 16px", background: "#0F172A", color: "#fff", borderRadius: 16, fontSize: 15, lineHeight: 1.5, whiteSpace: "pre-wrap", wordBreak: "break-word" },
    // Assistant: NO bubble. Markdown flows naturally up to the column width.
    aiMessage: { color: "#111827", fontSize: 15, lineHeight: 1.65, wordBreak: "break-word" },

    typing: { alignSelf: "flex-start", padding: "10px 16px", background: "transparent", display: "flex", gap: 6, alignItems: "center" },
    dot: { width: 5, height: 5, borderRadius: "50%", background: "#D1D5DB" },
    typingText: { fontSize: 13, color: "#6B7280", fontWeight: 500, marginLeft: 6, fontVariantNumeric: "tabular-nums" },

    // Bottom area hosts the input column. Sticky-feeling via the body's flex layout.
    bottomWrap: { padding: "12px 24px 18px", background: "linear-gradient(180deg, rgba(255,255,255,0) 0%, #fff 30%)" },
    bottomColumn: { maxWidth: 760, margin: "0 auto" },

    // Single calm input container. No internal divider, no sparkle, just space.
    inputCard: { width: "100%", background: "#fff", border: "1px solid #E5E7EB", borderRadius: 16, padding: "10px 12px 8px 16px", boxShadow: "0 1px 2px rgba(15,23,42,0.04)", transition: "border-color 0.15s, box-shadow 0.15s" },
    inputCardFocus: { borderColor: "#9CA3AF", boxShadow: "0 4px 14px rgba(15,23,42,0.07)" },
    textInput: { width: "100%", border: "none", outline: "none", background: "transparent", fontSize: 15, color: "#111827", fontFamily: fn, padding: "6px 2px", lineHeight: 1.5 },

    // Footer row inside the input card. No background, no border — just spacing.
    inputFooter: { display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 4, gap: 8 },
    sourceBtn: { display: "inline-flex", alignItems: "center", gap: 6, padding: "4px 8px", background: "transparent", border: "none", fontSize: 12, fontWeight: 500, color: "#6B7280", cursor: "default", fontFamily: fn, borderRadius: 6 },
    // Send is an icon-only square button. Subtle when disabled, prominent when ready.
    sendBtn: { width: 32, height: 32, display: "inline-flex", alignItems: "center", justifyContent: "center", background: "#0F172A", color: "#fff", border: "none", borderRadius: 8, cursor: "pointer", padding: 0, transition: "opacity 0.15s, background 0.15s" },

    disclaimer: { fontSize: 11, color: "#9CA3AF", textAlign: "center", marginTop: 10, lineHeight: 1.5 },
    disclaimerLink: { color: "#6B7280", textDecoration: "underline" },

    // Empty-state greeting shown above the input column when no messages exist.
    emptyGreet: { padding: "80px 16px 24px", textAlign: "center" },
    emptyGreetTitle: { fontSize: 24, fontWeight: 600, color: "#111827", letterSpacing: "-0.02em", marginBottom: 8 },
    emptyGreetSub: { fontSize: 14, color: "#6B7280", lineHeight: 1.55, maxWidth: 480, margin: "0 auto" },
  };

  const avatarStyle = (color) => ({ width: 28, height: 28, borderRadius: "50%", background: color, color: "#fff", display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 700, flexShrink: 0 });

  const InputCard = (
    <div
      style={IC.inputCard}
      onFocus={e => { e.currentTarget.style.borderColor = IC.inputCardFocus.borderColor; e.currentTarget.style.boxShadow = IC.inputCardFocus.boxShadow; }}
      onBlur={e => { e.currentTarget.style.borderColor = IC.inputCard.border.split(' ').slice(-1)[0]; e.currentTarget.style.boxShadow = IC.inputCard.boxShadow; }}
    >
      <input
        style={IC.textInput}
        value={chatInput}
        onChange={e => setChatInput(e.target.value)}
        onKeyDown={e => e.key === "Enter" && !e.shiftKey && sendMessage()}
        placeholder="Ask anything about your funnel…"
        disabled={chatLoading}
        autoFocus
      />
      <div style={IC.inputFooter}>
        <button style={IC.sourceBtn} type="button" title="Active funnel">
          <I d="M3 6h18 M6 12h12 M10 18h4" size={12} stroke="#6B7280" /> {sourceLabel}
        </button>
        <button
          style={{ ...IC.sendBtn, opacity: chatLoading || !chatInput.trim() ? 0.4 : 1, cursor: chatLoading || !chatInput.trim() ? "default" : "pointer" }}
          onClick={() => sendMessage()}
          disabled={chatLoading || !chatInput.trim()}
          title="Send"
        >
          <I d="M12 19V5 M5 12l7-7 7 7" size={14} stroke="#fff" sw={2.2} />
        </button>
      </div>
    </div>
  );

  const Disclaimer = (
    <div style={IC.disclaimer}>
      Funnel AI may display inaccurate info, so please double check the response. <span style={IC.disclaimerLink}>Your Privacy &amp; Funnel AI</span>
    </div>
  );

  return (
    <div className="insights-outer" style={IC.outer}>
      {/* Sidebar */}
      {sidebarOpen && (
        <div style={IC.sidebar}>
          <div style={IC.sidebarHeader}>
            <span style={IC.sidebarTitle}>Chat</span>
            <button style={IC.iconOnlyBtn} title="Search"><I d="M21 21l-4.35-4.35 M11 19a8 8 0 1 1 0-16 8 8 0 0 1 0 16z" size={16} stroke="#9CA3AF" /></button>
          </div>
          <button style={IC.newChatBtn} onClick={newChat}>
            <I d="M12 5v14 M5 12h14" size={14} stroke="#fff" /> New Chat
            <I d="M12 3l1.9 5.8 5.8 1.9-5.8 1.9L12 18.4l-1.9-5.8L4.3 10.7l5.8-1.9z" size={12} stroke="#fff" />
          </button>

          <div style={IC.groupHeader}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
              <I d="M12 2l2.39 7.36H22l-6.18 4.49L18.21 22 12 17.27 5.79 22l2.39-8.15L2 9.36h7.61z" size={12} stroke="#9CA3AF" /> Saved
            </span>
          </div>
          <div>
            {quickPrompts.map(q => (
              <div
                key={q.label}
                style={IC.savedItem}
                onClick={() => sendMessage(q.prompt)}
                onMouseEnter={e => { e.currentTarget.style.background = "#F3F4F6"; }}
                onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}
              >
                <div style={avatarStyle(q.color)}>{q.letter}</div>
                <span style={IC.savedLabel}>{q.label}</span>
                <span style={IC.savedMore}>···</span>
              </div>
            ))}
          </div>

          <div style={IC.chatList}>
            {historyLoading ? (
              <div style={{ padding: 16, textAlign: "center", color: "#9CA3AF", fontSize: 13 }}>Loading…</div>
            ) : history.length === 0 ? (
              <div style={{ padding: 16, textAlign: "center", color: "#9CA3AF", fontSize: 13 }}>No conversations yet</div>
            ) : (
              Object.entries(grouped).map(([group, items]) => (
                items.length > 0 && (
                  <div key={group}>
                    <div style={IC.groupHeader}>
                      <span>{group}</span>
                      <span style={{ fontSize: 10, color: "#D1D5DB" }}>▾</span>
                    </div>
                    {items.map(chat => (
                      <div
                        key={chat.id}
                        style={{ ...IC.histItem, ...(activeId === chat.id ? IC.histItemActive : {}) }}
                        onClick={() => setActiveId(chat.id)}
                        onMouseEnter={e => { if (activeId !== chat.id) e.currentTarget.style.background = "#FAFAFA"; }}
                        onMouseLeave={e => { if (activeId !== chat.id) e.currentTarget.style.background = "transparent"; }}
                      >
                        {activeId === chat.id && <span style={IC.histDot} />}
                        <span style={IC.histTitle}>{chat.title}</span>
                        <button style={IC.histDel} onClick={e => { e.stopPropagation(); deleteChat(chat.id); }} title="Delete">×</button>
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
      <div style={IC.main}>
        <div style={IC.topbar}>
          <div style={IC.brand}>
            <span style={IC.brandName}>Funnel AI</span>
            <span style={IC.brandPill}>Beta</span>
          </div>
          <div style={IC.topActions}>
            <button style={IC.pillBtn} onClick={shareChat} title="Copy conversation">
              <I d="M4 12v7a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7 M16 6l-4-4-4 4 M12 2v13" size={13} stroke="#6B7280" />
              Share
            </button>
            <button style={IC.pillBtnDark} onClick={newChat}>
              New Chat
              <I d="M12 3l1.9 5.8 5.8 1.9-5.8 1.9L12 18.4l-1.9-5.8L4.3 10.7l5.8-1.9z" size={12} stroke="#fff" />
            </button>
          </div>
        </div>

        <div style={IC.body}>
          <div style={IC.msgScroll}>
            <div style={IC.msgColumn}>
              {chatMsgs.length === 0 && !chatLoading ? (
                <div style={IC.emptyGreet}>
                  <div style={IC.emptyGreetTitle}>What would you like to explore?</div>
                  <div style={IC.emptyGreetSub}>Ask about trends, compare periods, dig into a specific day, or pull a chart of any metric over time.</div>
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

const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}
::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:#D4D4D0;border-radius:3px}::-webkit-scrollbar-thumb:hover{background:#B0B0AC}
@keyframes fu{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.fi{animation:fu 300ms cubic-bezier(0.16, 1, 0.3, 1)}
@keyframes sp{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
.spin{animation:sp 700ms linear infinite}
.trow:hover{background:#F9FAFB!important}
.trow:hover .rowBtn{opacity:1}
.trow:nth-child(even){background:#FAFBFC}
.trow.selected, .trow.selected:hover, .trow.selected:nth-child(even){background:#DCFCE7!important;box-shadow:inset 3px 0 0 #10B981}
.trow.selected .sticky-col, .trow.selected:hover .sticky-col, .trow.selected:nth-child(even) .sticky-col{background:#DCFCE7!important}
.rowBtn{opacity:0;transition:opacity 150ms ease}

/* Sticky Day/Date columns */
.sticky-col { position: sticky; z-index: 2; background: inherit; }
.sticky-col-1 { left: 0; }
.sticky-col-2 { left: 64px; }
thead .sticky-col { background: #FAFAFA; z-index: 3; }
.trow .sticky-col { background: #fff; }
.trow:nth-child(even) .sticky-col { background: #FAFBFC; }
.trow:hover .sticky-col { background: #F9FAFB !important; }
.sticky-col-2::after {
  content: ''; position: absolute; top: 0; right: -6px; bottom: 0; width: 6px;
  background: linear-gradient(90deg, rgba(0,0,0,0.04), transparent);
  pointer-events: none;
}

input:focus{outline:none;border-color:#D1D5DB!important;box-shadow:0 0 0 3px rgba(243,244,246,1)!important}
::selection{background:rgba(18,134,74,0.12)}
@keyframes blink{0%,100%{opacity:0.3}50%{opacity:1}}
.blink-1{animation:blink 1.2s 0s infinite}.blink-2{animation:blink 1.2s 0.2s infinite}.blink-3{animation:blink 1.2s 0.4s infinite}

/* ─── Desktop nav layout ───────────────────────────────────────── */
.nav-buttons-desktop { display: flex; align-items: center; gap: 10px; }
.header-actions { display: flex; align-items: center; gap: 10px; }

/* ─── Mobile Menu Toggle (hidden on desktop) ───────────────────── */
.mobile-menu-toggle { display: none !important; }

/* ─── Mobile Nav Dropdown ──────────────────────────────────────── */
.mobile-nav-dropdown {
  display: none;
  position: sticky; top: 0; z-index: 99;
  background: #fff; border-bottom: 1px solid #E5E7EB;
  padding: 8px 16px;
  flex-direction: column; gap: 2px;
  animation: fu 200ms cubic-bezier(0.16,1,0.3,1);
  box-shadow: 0 4px 12px rgba(0,0,0,0.06);
}
.mobile-nav-item {
  display: flex; align-items: center; gap: 10px;
  width: 100%; padding: 12px 14px;
  background: none; border: none; border-radius: 10px;
  font-family: 'Inter', -apple-system, sans-serif;
  font-size: 14px; font-weight: 500; color: #374151;
  cursor: pointer; text-align: left;
  transition: background 0.1s;
}
.mobile-nav-item:hover, .mobile-nav-item:active { background: #F3F4F6; }
.mobile-nav-primary {
  background: #111827 !important; color: #fff !important;
  margin-top: 4px; font-weight: 600;
}
.mobile-nav-primary:hover, .mobile-nav-primary:active { background: #1F2937 !important; }
.mobile-nav-danger { color: #DC2626 !important; }
.mobile-nav-divider { height: 1px; background: #E5E7EB; margin: 4px 0; }

/* ─── Mobile Responsive (≤768px) ──────────────────────────────── */
@media (max-width: 768px) {
  /* Header */
  .app-header {
    padding: 12px 16px !important;
  }
  .app-header img { height: 26px !important; }
  .mobile-menu-toggle { display: inline-flex !important; }
  .nav-buttons-desktop { display: none !important; }
  .mobile-nav-dropdown { display: flex !important; }
  .header-actions {
    display: flex !important; align-items: center !important; gap: 6px !important;
  }

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
  .query-wrap {
    padding: 16px !important;
  }
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
`;

const fn = "'Inter',-apple-system,BlinkMacSystemFont,sans-serif";

const S = {
  app: { minHeight: "100vh", background: "#F3F4F6", color: "#111827", fontFamily: fn },
  hdr: { padding: "16px 32px", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #E5E7EB", background: "#FFFFFF", position: "sticky", top: 0, zIndex: 100 },
  logoMark: { width: 32, height: 32, background: "#F97316", borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center" },
  logoT: { fontSize: 15, fontWeight: 700, color: "#111827", letterSpacing: "-0.01em" },
  logoSub: { fontSize: 11, color: "#6B7280", fontWeight: 500, display: "none" }, // Hidden for cleaner look
  tzPill: { display: "inline-flex", alignItems: "center", gap: 5, padding: "5px 12px", background: "#F3F4F6", color: "#4B5563", fontSize: 12, fontWeight: 600, borderRadius: 100 },
  main: { maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" },
  titleRow: { display: "flex", justifyContent: "flex-start", alignItems: "center", marginBottom: 24, gap: 16 },
  pageTitle: { fontSize: 24, fontWeight: 600, color: "#111827", letterSpacing: "-0.02em", lineHeight: 1.1 },
  pageSub: { fontSize: 13, color: "#6B7280", fontWeight: 500, marginTop: 4, display: "none" },
  livePill: { display: "inline-flex", alignItems: "center", gap: 6, padding: "4px 10px", background: "transparent", color: "#6B7280", fontSize: 12, fontWeight: 500, borderRadius: 100 },
  liveDot: { width: 6, height: 6, borderRadius: "50%", background: "#10B981", boxShadow: "0 0 6px rgba(16,185,129,0.4)" },
  // Grid (not flex) so columns are always uniform and dividers align across rows.
  // The 1px gap on a colored parent doubles as the divider; cells have white bg.
  strip: { display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 1, border: "1px solid #E5E7EB", borderRadius: 12, background: "#E8E8E6", marginBottom: 24, overflow: "hidden", boxShadow: "0 1px 2px rgba(0,0,0,0.02)" },
  stripCell: { background: "#fff", padding: "24px", display: "flex", flexDirection: "column", gap: 8, minWidth: 0 },
  stripLabel: { fontSize: 13, fontWeight: 500, color: "#6B7280" },
  stripValRow: { display: "flex", alignItems: "baseline", gap: 12 },
  stripVal: { fontSize: 28, fontWeight: 600, color: "#111827", letterSpacing: "-0.02em", lineHeight: 1 },
  pctBadge: { display: "inline-flex", alignItems: "center", gap: 2, padding: "2px 6px", fontSize: 12, fontWeight: 600, borderRadius: 4 },
  toolbar: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 },
  listBoardToggle: { display: "flex", background: "#F3F4F6", padding: 4, borderRadius: 8, border: "1px solid #E5E7EB", gap: 4 },
  listToggleActive: { background: "#fff", padding: "6px 14px", borderRadius: 6, fontSize: 13, fontWeight: 500, color: "#111827", display: "flex", alignItems: "center", gap: 6, boxShadow: "0 1px 2px rgba(0,0,0,0.05)" },
  listToggleInactive: { padding: "6px 14px", borderRadius: 6, fontSize: 13, fontWeight: 500, color: "#6B7280", display: "flex", alignItems: "center", gap: 6, cursor: "pointer" },
  searchWrap: { display: "flex", alignItems: "center", gap: 8, padding: "8px 12px", background: "#fff", border: "1px solid #E5E7EB", borderRadius: 8, width: 260, boxShadow: "0 1px 2px rgba(0,0,0,0.02)" },
  searchInput: { border: "none", outline: "none", background: "transparent", fontFamily: fn, fontSize: 13, color: "#111827", width: "100%", fontWeight: 400 },
  tableWrap: { background: "#fff", border: "1px solid #E5E7EB", overflowX: "auto", boxShadow: "0 1px 3px rgba(0,0,0,0.04)", marginLeft: "calc(-1 * (100vw - 100%) / 2)", marginRight: "calc(-1 * (100vw - 100%) / 2)", width: "100vw", maxWidth: "100vw", borderRadius: 0, borderLeft: "none", borderRight: "none" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth: 900 },
  th: { padding: "10px 16px", textAlign: "center", verticalAlign: "middle", fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", borderBottom: "2px solid #E5E7EB", background: "#FAFAFA", whiteSpace: "normal", lineHeight: 1.35 },
  td: { padding: "10px 16px", borderBottom: "1px solid #F3F4F6", verticalAlign: "middle", textAlign: "center" },
  tdNum: { padding: "10px 16px", borderBottom: "1px solid #F3F4F6", fontWeight: 500, fontVariantNumeric: "tabular-nums", color: "#111827", fontSize: 13, textAlign: "center" },
  tdMoney: { padding: "10px 16px", borderBottom: "1px solid #F3F4F6", fontWeight: 500, fontVariantNumeric: "tabular-nums", color: "#111827", fontSize: 13, textAlign: "center" },
  dayPill: { display: "inline-block", padding: "4px 10px", background: "#F3F4F6", color: "#4B5563", fontSize: 11, fontWeight: 600, borderRadius: 6, letterSpacing: "0.02em", textTransform: "uppercase" },
  todayDot: { display: "inline-block", width: 6, height: 6, borderRadius: "50%", background: "#F97316", marginLeft: 4, verticalAlign: "middle" },
  purchBadge: { display: "inline-block", padding: "2px 8px", background: "#ECFDF5", color: "#047857", fontWeight: 700, borderRadius: 4, border: "1px solid #A7F3D0" },
  thHighlight: { background: "#F0FDF4", color: "#047857", borderBottom: "2px solid #10B981" },
  tdHighlight: { background: "#F0FDF4" },
  emptyTd: { padding: 48, textAlign: "center", color: "#9CA3AF", fontSize: 14, fontWeight: 500 },
  rowAct: { background: "#fff", border: "1px solid #E5E7EB", borderRadius: 6, cursor: "pointer", padding: "6px", display: "inline-flex", alignItems: "center", boxShadow: "0 1px 2px rgba(0,0,0,0.02)" },
  btnDark: { display: "inline-flex", alignItems: "center", gap: 6, padding: "8px 16px", fontSize: 13, fontWeight: 500, fontFamily: fn, background: "#111827", color: "#F9FAFB", border: "1px solid #111827", borderRadius: 8, cursor: "pointer", boxShadow: "0 1px 2px rgba(0,0,0,0.05)" },
  btnLight: { display: "inline-flex", alignItems: "center", gap: 6, padding: "8px 16px", fontSize: 13, fontWeight: 500, fontFamily: fn, background: "#ffffff", color: "#111827", border: "1px solid #E5E7EB", borderRadius: 8, cursor: "pointer", boxShadow: "0 1px 2px rgba(0,0,0,0.02)" },
  btnGhost: { display: "inline-flex", alignItems: "center", padding: "8px", background: "transparent", color: "#6B7280", border: "1px solid transparent", borderRadius: 8, cursor: "pointer" },
  overlay: { position: "fixed", inset: 0, background: "rgba(17,24,39,0.4)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 200, backdropFilter: "blur(2px)" },
  modal: { background: "#fff", borderRadius: 16, padding: 32, textAlign: "center", maxWidth: 400, width: "90%", boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)" },
  toast: { position: "fixed", bottom: 28, left: "50%", transform: "translateX(-50%)", background: "#111827", border: "none", borderRadius: 8, padding: "12px 20px", display: "flex", alignItems: "center", gap: 10, boxShadow: "0 10px 15px -3px rgba(0, 0, 0, 0.1)", fontSize: 13, fontWeight: 500, color: "#F9FAFB", zIndex: 300 },
  fc: { maxWidth: 640, margin: "0 auto" },
  fh: { textAlign: "center", marginBottom: 32 },
  formBadge: { display: "none" },
  ft: { fontSize: 24, fontWeight: 600, color: "#111827", letterSpacing: "-0.01em", marginBottom: 8 },
  fs: { fontSize: 14, color: "#6B7280", maxWidth: 400, margin: "0 auto", lineHeight: 1.5 },
  fcard: { background: "#fff", border: "1px solid #E5E7EB", borderRadius: 12, padding: 32, boxShadow: "0 4px 6px -1px rgba(0,0,0,0.05)" },
  fgrid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 },
  fl: { display: "block", fontSize: 12, fontWeight: 500, color: "#374151", marginBottom: 6 },
  hint: { fontSize: 12, color: "#9CA3AF", marginTop: 6 },
  inp: { width: "100%", padding: "10px 14px", background: "#fff", border: "1px solid #D1D5DB", borderRadius: 8, fontFamily: fn, fontSize: 14, fontWeight: 400, color: "#111827", transition: "border-color 0.15s ease", boxShadow: "0 1px 2px rgba(0,0,0,0.02)" },
  fa: { display: "flex", justifyContent: "flex-end", gap: 12, marginTop: 32, paddingTop: 24, borderTop: "1px solid #E5E7EB" },
  fmtB: { padding: "8px 16px", fontSize: 13, fontWeight: 500, fontFamily: fn, background: "#fff", color: "#4B5563", border: "1px solid #D1D5DB", borderRadius: 6, cursor: "pointer" },
  fmtA: { padding: "8px 16px", fontSize: 13, fontWeight: 500, fontFamily: fn, background: "#F3F4F6", color: "#111827", border: "1px solid #9CA3AF", borderRadius: 6, cursor: "pointer" },
  prev: { padding: 24, background: "#F9FAFB", border: "1px dashed #D1D5DB", borderRadius: 8, textAlign: "center" },
  boardGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 16 },
  boardCard: { background: "#fff", border: "1px solid #E5E7EB", borderRadius: 12, padding: 24, boxShadow: "0 1px 3px rgba(0,0,0,0.04)", display: "flex", flexDirection: "column" },
  boardCardHeader: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, borderBottom: "1px solid #F3F4F6", paddingBottom: 16 },
  boardCardBody: { display: "flex", flexDirection: "column", gap: 12, flex: 1 },
  bcItem: { display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 13 },
  bcLabel: { color: "#6B7280", fontWeight: 500 },
  bcVal: { color: "#111827", fontWeight: 600, fontVariantNumeric: "tabular-nums" },
  boardCardActions: { display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 16, paddingTop: 16, borderTop: "1px solid #F3F4F6" },
};
