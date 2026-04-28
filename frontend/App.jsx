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

const getAuthHeaders = async () => {
  const { data: { session } } = await supabase.auth.getSession();
  const headers = { "Content-Type": "application/json" };
  if (session?.access_token) headers["Authorization"] = `Bearer ${session.access_token}`;
  return headers;
};

// Parse MM/DD/YYYY → Date at midnight (component-based, no browser TZ dependency)
const parseMDate = (d) => { const [m, dy, y] = d.split('/').map(Number); return new Date(y, m - 1, dy); };

const api = {
  async getMetrics(limit = 90, offset = 0) {
    const res = await fetch(`${API_BASE}/api/metrics?limit=${limit}&offset=${offset}`);
    if (!res.ok) throw new Error(`Failed to fetch metrics: ${res.status}`);
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
    const res = await fetch(`${API_BASE}/api/custom-metrics`);
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
    const url = `${API_BASE}/api/events?limit=${limit}${type ? `&type=${type}` : ""}`;
    const res = await fetch(url);
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
const MK = ["fb_spend", "fb_link_clicks", "registrations", "replays", "viewedcta", "clickedcta", "purchases", "purchases_fb", "purchases_native", "purchases_youtube", "purchases_aibot", "purchases_postwebinar", "total_purchases", "attended"];
const COL_LABELS = { fb_spend: "FB Spend", fb_link_clicks: "Total Reg. Page Visited", registrations: "Registrations", attended: "Attended", replays: "Replays", viewedcta: "Viewed CTA", clickedcta: "Clicked CTA", purchases_fb: "FB Purchases", purchases_native: "Native Ads", purchases_youtube: "Youtube", purchases_aibot: "AI Chat Bot", purchases_postwebinar: "Post Webinar", total_purchases: "Total Purchases" };
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
  { key: "total_purchases", label: "Total Purchases (alt)", defaultFormat: "number" },
];
const evalFormula = (f, row, ctx = {}) => { try { let e = f.trim(); for (const k of MK) e = e.replace(new RegExp(`\\b${k}\\b`, "gi"), String(Number(row[k]) || 0)); for (const [k, v] of Object.entries(ctx)) e = e.replace(new RegExp(`\\b${k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, "gi"), String(Number(v) || 0)); if (/[^0-9+\-*/().%\s]/.test(e)) return null; e = e.replace(/[_%]/g, m => m === '%' ? '/100*' : ''); const r = Function('"use strict"; return (' + e + ")")(); return isFinite(r) ? Math.round(r * 100) / 100 : null; } catch { return null; } };
const fmtVal = (v, fmt) => v === null ? "\u2014" : fmt === "percent" ? `${v}%` : fmt === "currency" ? `$${v.toLocaleString("en-US", { minimumFractionDigits: 2 })}` : v.toLocaleString("en-US", { maximumFractionDigits: 2 });

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
    // "All Metrics" lens always uses the current full MK list
    if (found.id === "default-all") return { ...found, metrics: MK };
    return found;
  })();
  const isColVisible = (col) => activeLens.metrics.includes(col);

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
      const [mRes, cRes] = await Promise.all([api.getMetrics(), api.getCustomMetrics()]);
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
      const res = await fetch(`${API_BASE}/api/me`, { headers: { Authorization: `Bearer ${token}` } });
      const data = await res.json();
      setUserRole(data.role || "viewer");
      if (data.preferences?.default_lens_id) setActiveLensId(data.preferences.default_lens_id);
      if (data.preferences?.col_order) setColOrder(data.preferences.col_order);
      else if (data.default_col_order) setColOrder(data.default_col_order);
      if (data.preferences?.summary_cards) setSummaryCards(data.preferences.summary_cards);
    } catch { setUserRole("viewer"); }
    setAuthLoading(false);
  };

  const isAdmin = userRole === "admin";

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

  const filtered = metrics.filter(m => {
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
            {view === "dash" && (
              <>
                <button style={S.btnLight} onClick={() => setView("insights")}>Insights</button>
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
              <button className="mobile-nav-item" onClick={() => setView("insights")}><I d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" size={16} stroke="#6B7280" /> Insights</button>
              <button className="mobile-nav-item" onClick={async () => { try { const r = await api.getEvents(100, evFilter); setEvents(r.data || []); } catch (e) { flash(e.message, "err"); } setView("events"); }}><I d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" size={16} stroke="#6B7280" /> Activity Log</button>
              {isAdmin && <button className="mobile-nav-item" onClick={() => setView("query")}><I d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" size={16} stroke="#6B7280" /> Query Data</button>}
              {isAdmin && <button className="mobile-nav-item" onClick={() => { setEditCM(null); setView("custom-list"); }}><I d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" size={16} stroke="#6B7280" /> Manage Metrics</button>}
              {isAdmin && <button className="mobile-nav-item mobile-nav-primary" onClick={() => { setEditRow(null); setView("entry"); }}><I d="M12 4v16m8-8H4" size={16} stroke="#fff" /> New Entry</button>}
            </>
          )}
          {view !== "dash" && (
            <button className="mobile-nav-item" onClick={() => { setView("dash"); setEditRow(null); setEditCM(null); }}><I d="M10 19l-7-7m0 0l7-7m-7 7h18" size={16} stroke="#6B7280" /> Back to Dashboard</button>
          )}
          <div className="mobile-nav-divider" />
          <button className="mobile-nav-item mobile-nav-danger" onClick={handleLogout}><I d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" size={16} stroke="#DC2626" /> Sign Out</button>
        </div>
      )}

      <main className="main-content" style={S.main}>
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
              {summaryCards.map((c, i) => {
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
              {Array.from({ length: (4 - (summaryCards.length % 4)) % 4 }).map((_, i) => (
                <div key={`pad-${i}`} className="strip-pad" style={S.stripCell} aria-hidden="true" />
              ))}
              {isAdmin && (
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
              )}
            </div>

            <div className="toolbar-row" style={S.toolbar}>
              <div className="toolbar-controls" style={{ display: "flex", gap: "10px", alignItems: "center", flexWrap: "wrap" }}>
                <div className="list-board-toggle" style={S.listBoardToggle}>
                  <div style={viewMode === "list" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("list")}><I d="M4 6h16M4 12h16M4 18h16" size={14} /> List</div>
                  <div style={viewMode === "board" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("board")}><I d="M4 4h4v16H4zM10 4h4v16h-4zM16 4h4v16h-4z" size={14} /> Board</div>
                </div>
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
                    {orderedCols.filter(c => c.type === "base" ? isColVisible(c.key) : true).map(c => (
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
                          {orderedCols.filter(c => c.type === "base" ? isColVisible(c.key) : true).map(c => {
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
                        {orderedCols.filter(c => c.type === "base" ? isColVisible(c.key) : true).map(c => {
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
        {view === "insights" && <InsightsChat flash={flash} isMobile={isMobile} />}
        {view === "query" && <QueryBuilder flash={flash} />}
      </main>

      {delConfirm && <Modal title="Delete Entry" msg={`Remove the entry for ${fmtDateNice(delConfirm)}?`} onCancel={() => setDelConfirm(null)} onConfirm={() => deleteEntry(delConfirm)} />}
      {delCM && <Modal title="Delete Custom Metric" msg="This will remove the column from your table." onCancel={() => setDelCM(null)} onConfirm={() => deleteCM(delCM)} />}
      {lensEditing && <LensEditor lens={lensEditing} onSave={saveLens} onCancel={() => setLensEditing(null)} />}
      {colEditorOpen && <ColumnEditor columns={orderedCols} isAdmin={isAdmin} onSave={(keys) => { saveColOrder(keys); setColEditorOpen(false); }} onCancel={() => setColEditorOpen(false)} />}
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

  const reset = () => setItems(MK.filter(k => COL_LABELS[k]).map(k => ({ key: k, label: COL_LABELS[k], type: "base" })).concat(columns.filter(c => c.type === "custom")));
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

function EntryForm({ initial, onSubmit, onCancel, isMobile }) {
  const today = getLADate();
  const defaults = initial
    ? { fb_spend: initial.fb_spend ?? 0, fb_link_clicks: initial.fb_link_clicks ?? 0, registrations: initial.registrations ?? 0, replays: initial.replays ?? 0, viewedcta: initial.viewedcta ?? 0, clickedcta: initial.clickedcta ?? 0, purchases_fb: initial.purchases_fb ?? 0, purchases_native: initial.purchases_native ?? 0, purchases_youtube: initial.purchases_youtube ?? 0, purchases_aibot: initial.purchases_aibot ?? 0, purchases_postwebinar: initial.purchases_postwebinar ?? 0, attended: initial.attended ?? 0 }
    : { fb_spend: "", fb_link_clicks: "", registrations: "", replays: "", viewedcta: "", clickedcta: "", purchases_fb: "", purchases_native: "", purchases_youtube: "", purchases_aibot: "", purchases_postwebinar: "", attended: "" };
  const [f, setF] = useState({ date: initial?.date || today, ...defaults });
  const set = (k, v) => setF(p => ({ ...p, [k]: v }));
  const go = () => onSubmit({ date: f.date, day: getLADay(f.date), fb_spend: parseFloat(f.fb_spend) || 0, fb_link_clicks: parseInt(f.fb_link_clicks) || 0, registrations: parseInt(f.registrations) || 0, replays: parseInt(f.replays) || 0, viewedcta: parseInt(f.viewedcta) || 0, clickedcta: parseInt(f.clickedcta) || 0, purchases_fb: parseInt(f.purchases_fb) || 0, purchases_native: parseInt(f.purchases_native) || 0, purchases_youtube: parseInt(f.purchases_youtube) || 0, purchases_aibot: parseInt(f.purchases_aibot) || 0, purchases_postwebinar: parseInt(f.purchases_postwebinar) || 0, attended: parseInt(f.attended) || 0 });
  const fields = [{ k: "fb_spend", l: "Facebook Spend ($)", step: "0.01", ph: "0.00" }, { k: "fb_link_clicks", l: "Total Reg. Page Visited", ph: "0" }, { k: "registrations", l: "Registrations", ph: "0" }, { k: "replays", l: "Replays", ph: "0" }, { k: "viewedcta", l: "Viewed CTA", ph: "0" }, { k: "clickedcta", l: "Clicked CTA", ph: "0" }, { k: "purchases_fb", l: "FB Purchases", ph: "0" }, { k: "purchases_native", l: "Native Ads", ph: "0" }, { k: "purchases_youtube", l: "Youtube", ph: "0" }, { k: "purchases_aibot", l: "AI Chat Bot", ph: "0" }, { k: "purchases_postwebinar", l: "Post Webinar", ph: "0" }, { k: "attended", l: "Attended", ph: "0" }];
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
function renderMd(text) {
  if (!text) return text;
  return text.split('\n').map((line, i) => {
    // Headers
    if (line.startsWith('### ')) return <h4 key={i} style={{ fontSize: 14, fontWeight: 700, color: "#111827", margin: "12px 0 4px" }}>{line.slice(4)}</h4>;
    if (line.startsWith('## ')) return <h3 key={i} style={{ fontSize: 15, fontWeight: 700, color: "#111827", margin: "14px 0 4px" }}>{line.slice(3)}</h3>;
    if (line.startsWith('# ')) return <h2 key={i} style={{ fontSize: 16, fontWeight: 700, color: "#111827", margin: "16px 0 6px" }}>{line.slice(2)}</h2>;
    // Bullet points
    if (line.startsWith('- ') || line.startsWith('* ')) {
      const content = line.slice(2);
      return <div key={i} style={{ paddingLeft: 12, margin: "3px 0", display: "flex", gap: 6 }}><span style={{ color: "#9CA3AF" }}>•</span><span>{renderInline(content)}</span></div>;
    }
    // Empty line
    if (line.trim() === '') return <div key={i} style={{ height: 8 }} />;
    // Regular paragraph
    return <p key={i} style={{ margin: "3px 0", lineHeight: 1.6 }}>{renderInline(line)}</p>;
  });
}

function renderInline(text) {
  // Bold **text**
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, i) => {
    if (part.startsWith('**') && part.endsWith('**')) {
      return <strong key={i} style={{ fontWeight: 700, color: "#111827" }}>{part.slice(2, -2)}</strong>;
    }
    return <span key={i}>{part}</span>;
  });
}

function QueryBuilder({ flash }) {
  const [table, setTable] = useState("events");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [eventType, setEventType] = useState("");
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
      const body = { table, dateFrom: dateFrom || undefined, dateTo: dateTo || undefined, eventType: eventType || undefined, search: search || undefined, sortBy: overrideSort || sortBy || undefined, sortDir: overrideDir || sortDir, limit: Number(limit) || 500 };
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
    wrap: { padding: "32px 40px", maxWidth: 1200, margin: "0 auto", className: "query-wrap" },
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
    tableWrap: { overflowX: "auto", borderRadius: 10, border: "1px solid #E8E8E6" },
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
          <select style={QS.filterSelect} value={table} onChange={e => { setTable(e.target.value); setResults(null); setEventType(""); setSortBy(""); }}>
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

function InsightsChat({ flash, isMobile }) {
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

  const IC = {
    outer: { display: "flex", height: "calc(100vh - 120px)", gap: 0, position: "relative" },
    sidebar: { width: sidebarOpen ? (isMobile ? "100%" : 260) : 0, minWidth: sidebarOpen ? (isMobile ? "100%" : 260) : 0, background: "#F9FAFB", borderRight: sidebarOpen && !isMobile ? "1px solid #E5E7EB" : "none", display: "flex", flexDirection: "column", overflow: "hidden", transition: "width 0.2s, min-width 0.2s", borderRadius: "12px 0 0 12px", ...(isMobile && sidebarOpen ? { position: "absolute", inset: 0, zIndex: 10, borderRadius: 12 } : {}) },
    sidebarHeader: { padding: "16px", borderBottom: "1px solid #E5E7EB", display: "flex", justifyContent: "space-between", alignItems: "center" },
    sidebarTitle: { fontSize: 13, fontWeight: 600, color: "#374151", letterSpacing: "0.02em", textTransform: "uppercase" },
    newBtn: { padding: "5px 10px", background: "#111827", color: "#fff", border: "none", borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "Inter, sans-serif" },
    chatList: { flex: 1, overflowY: "auto", padding: "8px" },
    chatItem: { padding: "10px 12px", borderRadius: 8, cursor: "pointer", marginBottom: 4, display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8, transition: "background 0.1s" },
    chatItemActive: { background: "#fff", border: "1px solid #E5E7EB", boxShadow: "0 1px 2px rgba(0,0,0,0.04)" },
    chatItemInactive: { background: "transparent", border: "1px solid transparent" },
    chatTitle: { fontSize: 13, fontWeight: 500, color: "#111827", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 },
    chatTime: { fontSize: 11, color: "#9CA3AF", whiteSpace: "nowrap", marginTop: 2 },
    delBtn: { background: "none", border: "none", cursor: "pointer", padding: 2, fontSize: 14, color: "#D1D5DB", flexShrink: 0 },
    main: { flex: 1, display: "flex", flexDirection: "column", padding: "0 0 0 16px", minWidth: 0 },
    header: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 },
    headerLeft: {},
    title: { fontSize: 24, fontWeight: 600, color: "#111827", letterSpacing: "-0.02em" },
    sub: { fontSize: 13, color: "#6B7280", marginTop: 4 },
    toggleBtn: { background: "none", border: "1px solid #E5E7EB", borderRadius: 8, cursor: "pointer", padding: "6px 10px", fontSize: 13, color: "#6B7280", fontFamily: "Inter, sans-serif" },
    body: { flex: 1, overflowY: "auto", padding: "0 4px", display: "flex", flexDirection: "column", gap: 12 },
    empty: { flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 20 },
    emptyIcon: { width: 48, height: 48, borderRadius: "50%", background: "linear-gradient(135deg, #EEF2FF, #E0E7FF)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 24 },
    emptyTitle: { fontSize: 18, fontWeight: 600, color: "#111827" },
    emptyDesc: { fontSize: 14, color: "#6B7280", textAlign: "center", maxWidth: 360 },
    chips: { display: "flex", flexWrap: "wrap", gap: 8, justifyContent: "center", marginTop: 8 },
    chip: { padding: "8px 14px", background: "#fff", border: "1px solid #E5E7EB", borderRadius: 20, fontSize: 13, color: "#374151", cursor: "pointer", fontWeight: 500, transition: "all 0.15s" },
    userBub: { alignSelf: "flex-end", maxWidth: "75%", padding: "10px 16px", background: "#111827", color: "#fff", borderRadius: "16px 16px 4px 16px", fontSize: 14, lineHeight: 1.5 },
    aiBub: { alignSelf: "flex-start", maxWidth: "85%", padding: "14px 18px", background: "#F9FAFB", border: "1px solid #E5E7EB", borderRadius: "16px 16px 16px 4px", fontSize: 14, lineHeight: 1.5, color: "#374151" },
    typing: { alignSelf: "flex-start", padding: "12px 18px", background: "#F9FAFB", border: "1px solid #E5E7EB", borderRadius: "16px 16px 16px 4px", display: "flex", gap: 5, alignItems: "center" },
    dot: { width: 6, height: 6, borderRadius: "50%", background: "#9CA3AF" },
    inputWrap: { display: "flex", gap: 8, padding: "12px 0", borderTop: "1px solid #F3F4F6", marginTop: 8 },
    input: { flex: 1, padding: "12px 16px", border: "1px solid #E5E7EB", borderRadius: 12, fontSize: 14, outline: "none", fontFamily: "Inter, sans-serif", background: "#fff" },
    sendBtn: { padding: "10px 20px", background: "#111827", color: "#fff", border: "none", borderRadius: 12, fontSize: 14, fontWeight: 600, cursor: "pointer", fontFamily: "Inter, sans-serif", whiteSpace: "nowrap" },
  };

  return (
    <div className="insights-outer" style={IC.outer}>
      {/* Sidebar */}
      <div style={IC.sidebar}>
        <div style={IC.sidebarHeader}>
          <span style={IC.sidebarTitle}>Chat History</span>
          <button style={IC.newBtn} onClick={newChat}>+ New</button>
        </div>
        <div style={IC.chatList}>
          {historyLoading ? (
            <div style={{ padding: 16, textAlign: "center", color: "#9CA3AF", fontSize: 13 }}>Loading…</div>
          ) : history.length === 0 ? (
            <div style={{ padding: 16, textAlign: "center", color: "#9CA3AF", fontSize: 13 }}>No conversations yet</div>
          ) : (
            [...history].sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at)).map(chat => (
              <div
                key={chat.id}
                style={{ ...IC.chatItem, ...(activeId === chat.id ? IC.chatItemActive : IC.chatItemInactive) }}
                onClick={() => setActiveId(chat.id)}
                onMouseEnter={e => { if (activeId !== chat.id) e.currentTarget.style.background = "#F3F4F6"; }}
                onMouseLeave={e => { if (activeId !== chat.id) e.currentTarget.style.background = "transparent"; }}
              >
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={IC.chatTitle}>{chat.title}</div>
                  <div style={IC.chatTime}>{fmtTime(new Date(chat.updated_at).getTime())}</div>
                </div>
                <button style={IC.delBtn} onClick={e => { e.stopPropagation(); deleteChat(chat.id); }} title="Delete">×</button>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Main chat area */}
      <div style={IC.main}>
        <div style={IC.header}>
          <div style={IC.headerLeft}>
            <h1 style={IC.title}>✨ AI Insights</h1>
            <div style={IC.sub}>Ask questions about your funnel data · Powered by Claude</div>
          </div>
          <button style={IC.toggleBtn} onClick={() => setSidebarOpen(p => !p)}>
            {sidebarOpen ? "◀ Hide" : "▶ History"}
          </button>
        </div>

        <div style={IC.body}>
          {chatMsgs.length === 0 && !chatLoading ? (
            <div style={IC.empty}>
              <div style={IC.emptyIcon}>📊</div>
              <div style={IC.emptyTitle}>Ask me anything about your data</div>
              <div style={IC.emptyDesc}>I can analyze trends, compare periods, identify anomalies, and suggest improvements for your marketing funnel.</div>
              <div style={IC.chips}>
                {starters.map(s => (
                  <button key={s} style={IC.chip} onClick={() => sendMessage(s)} onMouseEnter={e => { e.target.style.background = "#F3F4F6"; e.target.style.borderColor = "#D1D5DB"; }} onMouseLeave={e => { e.target.style.background = "#fff"; e.target.style.borderColor = "#E5E7EB"; }}>
                    {s}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <>
              {chatMsgs.map((m, i) => (
                <div key={i} style={m.role === "user" ? IC.userBub : IC.aiBub}>
                  {m.role === "assistant" ? renderMd(m.content) : m.content}
                </div>
              ))}
              {chatLoading && (
                <div style={IC.typing}>
                  <div className="blink-1" style={IC.dot} />
                  <div className="blink-2" style={IC.dot} />
                  <div className="blink-3" style={IC.dot} />
                </div>
              )}
              <div ref={chatEndRef} />
            </>
          )}
        </div>

        <div style={IC.inputWrap}>
          <input
            style={IC.input}
            value={chatInput}
            onChange={e => setChatInput(e.target.value)}
            onKeyDown={e => e.key === "Enter" && !e.shiftKey && sendMessage()}
            placeholder="Ask about your metrics, trends, conversions..."
            disabled={chatLoading}
          />
          <button style={{ ...IC.sendBtn, opacity: chatLoading || !chatInput.trim() ? 0.5 : 1 }} onClick={() => sendMessage()} disabled={chatLoading || !chatInput.trim()}>
            {chatLoading ? "Thinking..." : "Send"}
          </button>
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
    height: calc(100vh - 80px) !important;
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
  th: { padding: "10px 12px", textAlign: "center", fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", borderBottom: "2px solid #E5E7EB", background: "#FAFAFA", letterSpacing: "0.05em", whiteSpace: "normal", maxWidth: 90, lineHeight: 1.35 },
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
