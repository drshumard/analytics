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
    const res = await fetch(`${API_BASE}/api/metrics`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": getLocalKey() },
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
const MK = ["fb_spend", "registrations", "replays", "unique_replays", "viewedcta", "clickedcta", "purchases", "attended"];
const COL_LABELS = { fb_spend: "FB Spend", registrations: "Registrations", attended: "Attended", replays: "Replays", unique_replays: "Unique Replays", viewedcta: "Viewed CTA", clickedcta: "Clicked CTA", purchases: "Purchases" };
const DEFAULT_HIDDEN = ["unique_replays"];
const evalFormula = (f, row) => { try { let e = f.trim(); for (const k of MK) e = e.replace(new RegExp(k, "gi"), String(Number(row[k]) || 0)); if (/[^0-9+\-*/().%\s_]/.test(e)) return null; e = e.replace(/[_%]/g, m => m === '%' ? '/100*' : ''); const r = Function('"use strict"; return (' + e + ")")(); return isFinite(r) ? Math.round(r * 100) / 100 : null; } catch { return null; } };
const fmtVal = (v, fmt) => v === null ? "\u2014" : fmt === "percent" ? `${v}%` : fmt === "currency" ? `$${v.toLocaleString("en-US", { minimumFractionDigits: 2 })}` : v.toLocaleString("en-US", { maximumFractionDigits: 2 });

const I = ({ d, size = 16, stroke = "currentColor", sw = 1.8 }) => (<svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={stroke} strokeWidth={sw} strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>);

export default function App() {
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
  const [dateFilter, setDateFilter] = useState("30");
  const [dateRange, setDateRange] = useState([null, null]);
  const [toast, setToast] = useState(null);
  const [editCM, setEditCM] = useState(null);
  const [delConfirm, setDelConfirm] = useState(null);
  const [delCM, setDelCM] = useState(null);
  const [editRow, setEditRow] = useState(null);
  const [search, setSearch] = useState("");
  const [events, setEvents] = useState([]);
  const [evFilter, setEvFilter] = useState("");
  const [hiddenCols, setHiddenCols] = useState(DEFAULT_HIDDEN);
  const [colMenuOpen, setColMenuOpen] = useState(false);
  const colMenuRef = useRef(null);
  const savePrefsToServer = async (hidden) => {
    try {
      const headers = await getAuthHeaders();
      await fetch(`${API_BASE}/api/me/preferences`, { method: "PUT", headers, body: JSON.stringify({ preferences: { hidden_cols: hidden } }) });
    } catch { /* silent */ }
  };
  const toggleCol = (col) => { setHiddenCols(prev => { const next = prev.includes(col) ? prev.filter(c => c !== col) : [...prev, col]; savePrefsToServer(next); return next; }); };
  const isColVisible = (col) => !hiddenCols.includes(col);
  useEffect(() => { const h = (e) => { if (colMenuRef.current && !colMenuRef.current.contains(e.target)) setColMenuOpen(false); }; document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h); }, []);
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
      if (data.preferences?.hidden_cols) setHiddenCols(data.preferences.hidden_cols);
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

  useEffect(() => { loadData(); }, [loadData]);
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

  const totalsFiltered = filtered.filter(m => {
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

  const totals = totalsFiltered.reduce((a, r) => { MK.forEach(k => { a[k] = (a[k] || 0) + (Number(r[k]) || 0); }); return a; }, {});

  const half = Math.floor(totalsFiltered.length / 2);
  const recent = totalsFiltered.slice(0, half || 1);
  const older = totalsFiltered.slice(half || 1);
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
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh" }}>
        <form onSubmit={handleLogin} style={{ width: 380, background: "#fff", borderRadius: 16, padding: "48px 36px", boxShadow: "0 8px 32px rgba(0,0,0,0.08)", border: "1px solid #E8E8E6" }}>
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

  return (
    <div style={S.app}><style>{CSS}</style>
      <header style={S.hdr}>
        <div style={{ display: "flex", alignItems: "center" }}>
          <img src="https://portal-drshumard.b-cdn.net/trans_sized.png" alt="Dr Shumard Analytics" style={{ height: 32, objectFit: "contain" }} />
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <button style={S.btnGhost} onClick={refreshWebhook} title="Refresh Spend Data"><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={15} stroke="#8A8A88" /></button>

          {view === "dash" && (
            <>
              <button style={S.btnLight} onClick={() => setView("insights")}>✨ Insights</button>
              <button style={S.btnLight} onClick={async () => { try { const r = await api.getEvents(100, evFilter); setEvents(r.data || []); } catch (e) { flash(e.message, "err"); } setView("events"); }}>Activity Log</button>
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
      </header>

      <main style={S.main}>
        {view === "dash" && (
          <div className="fi">
            <div style={S.titleRow}>
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

            <div style={S.strip}>
              {[
                { label: "Total Spend", val: `$${(totals.fb_spend || 0).toLocaleString("en-US", { maximumFractionDigits: 0 })}`, key: "fb_spend" },
                { label: "Registrations", val: (totals.registrations || 0).toLocaleString(), key: "registrations" },
                { label: "Purchases", val: (totals.purchases || 0).toLocaleString(), key: "purchases" },
                { label: "Total Replays", val: (totals.replays || 0).toLocaleString(), key: "replays" },
              ].map((c, i, arr) => {
                const pct = pctChange(c.key);
                return (
                  <div key={i} style={{ ...S.stripCell, borderRight: i < arr.length - 1 ? "1px solid #E8E8E6" : "none" }}>
                    <div style={S.stripLabel}>{c.label}</div>
                    <div style={S.stripValRow}>
                      <span style={S.stripVal}>{c.val}</span>
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
            </div>

            <div style={S.toolbar}>
              <div style={{ display: "flex", gap: "10px", alignItems: "center" }}>
                <div style={S.listBoardToggle}>
                  <div style={viewMode === "list" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("list")}><I d="M4 6h16M4 12h16M4 18h16" size={14} /> List</div>
                  <div style={viewMode === "board" ? S.listToggleActive : S.listToggleInactive} onClick={() => setViewMode("board")}><I d="M4 4h4v16H4zM10 4h4v16h-4zM16 4h4v16h-4z" size={14} /> Board</div>
                </div>
                <div style={{ position: "relative" }} ref={colMenuRef}>
                  <button style={S.btnLight} onClick={() => setColMenuOpen(p => !p)}><I d="M15 12a3 3 0 11-6 0 3 3 0 016 0zM2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" size={14} stroke="#6B7280" /> Columns</button>
                  {colMenuOpen && (
                    <div style={{ position: "absolute", top: "100%", left: 0, marginTop: 6, background: "#fff", border: "1px solid #E5E7EB", borderRadius: 10, padding: "8px 0", boxShadow: "0 4px 12px rgba(0,0,0,0.1)", zIndex: 50, minWidth: 180 }}>
                      {MK.map(col => (
                        <label key={col} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 14px", cursor: "pointer", fontSize: 13, color: "#374151", fontWeight: 500 }} onMouseEnter={e => e.currentTarget.style.background = "#F9FAFB"} onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                          <input type="checkbox" checked={isColVisible(col)} onChange={() => toggleCol(col)} style={{ accentColor: "#111827" }} />
                          {COL_LABELS[col]}
                        </label>
                      ))}
                    </div>
                  )}
                </div>
              </div>
              <div style={S.searchWrap}><I d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" size={15} stroke="#AEAEA8" /><input style={S.searchInput} placeholder="Search records..." value={search} onChange={e => setSearch(e.target.value)} /></div>
            </div>

            {viewMode === "list" ? (
              <div style={S.tableWrap}>
                <table style={S.table}>
                  <thead><tr>
                    <th style={S.th}>Day</th><th style={S.th}>Date</th>
                    {isColVisible("fb_spend") && <th style={S.th}>FB Spend</th>}
                    {isColVisible("registrations") && <th style={S.th}>Registrations</th>}
                    {isColVisible("attended") && <th style={S.th}>Attended</th>}
                    {isColVisible("replays") && <th style={S.th}>Replays</th>}
                    {isColVisible("unique_replays") && <th style={S.th}>Unique Replays</th>}
                    {isColVisible("viewedcta") && <th style={S.th}>Viewed CTA</th>}
                    {isColVisible("clickedcta") && <th style={S.th}>Clicked CTA</th>}
                    {isColVisible("purchases") && <th style={S.th}>Purchases</th>}
                    {customs.map(cm => <th key={cm.id} style={{ ...S.th, color: "#12864A" }}>{cm.name}</th>)}
                    <th style={{ ...S.th, width: 72 }}>Actions</th>
                  </tr></thead>
                  <tbody>
                    {filtered.length === 0 ? (
                      <tr><td colSpan={2 + MK.filter(k => isColVisible(k)).length + customs.length + 1} style={S.emptyTd}>No entries found for this period.</td></tr>
                    ) : filtered.map((row) => {
                      const isToday = row.date === getLADate();
                      return (
                        <tr key={row.date} className="trow" style={isToday ? { background: "#F8FDF9" } : {}}>
                          <td style={S.td}><span style={S.dayPill}>{getLADayShort(row.date)}</span></td>
                          <td style={{ ...S.td, whiteSpace: "nowrap", minWidth: 90 }}><span style={{ color: "#1A1A1A", fontWeight: 500, fontSize: 14 }}>{fmtDateNice(row.date)}</span></td>
                          {isColVisible("fb_spend") && <td style={S.tdMoney}>${(Number(row.fb_spend) || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })}</td>}
                          {isColVisible("registrations") && <td style={S.tdNum}>{(Number(row.registrations) || 0).toLocaleString()}</td>}
                          {isColVisible("attended") && <td style={S.tdNum}>{(Number(row.attended) || 0).toLocaleString()}</td>}
                          {isColVisible("replays") && <td style={S.tdNum}>{(Number(row.replays) || 0).toLocaleString()}</td>}
                          {isColVisible("unique_replays") && <td style={S.tdNum}>{(Number(row.unique_replays) || 0).toLocaleString()}</td>}
                          {isColVisible("viewedcta") && <td style={S.tdNum}>{(Number(row.viewedcta) || 0).toLocaleString()}</td>}
                          {isColVisible("clickedcta") && <td style={S.tdNum}>{(Number(row.clickedcta) || 0).toLocaleString()}</td>}
                          {isColVisible("purchases") && <td style={S.tdNum}><span style={S.purchBadge}>{(Number(row.purchases) || 0).toLocaleString()}</span></td>}
                          {customs.map(cm => { const v = evalFormula(cm.formula, row); return <td key={cm.id} style={{ ...S.tdNum, color: "#12864A", fontWeight: 600 }}>{fmtVal(v, cm.format)}</td>; })}
                          <td style={S.td}>
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
                {filtered.length === 0 ? (
                  <div style={{ ...S.emptyTd, gridColumn: "1 / -1", border: "1px dashed #E5E7EB", borderRadius: 12 }}>No entries found for this period.</div>
                ) : filtered.map((row) => (
                  <div key={row.date} style={S.boardCard}>
                    <div style={S.boardCardHeader}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={S.dayPill}>{getLADayShort(row.date)}</span>
                        <div style={{ fontWeight: 600, color: "#111827", fontSize: 14 }}>{fmtDateNice(row.date)}</div>
                      </div>
                    </div>
                    <div style={S.boardCardBody}>
                      {isColVisible("fb_spend") && <div style={S.bcItem}><span style={S.bcLabel}>FB Spend</span><span style={S.bcVal}>${(Number(row.fb_spend) || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })}</span></div>}
                      {isColVisible("registrations") && <div style={S.bcItem}><span style={S.bcLabel}>Registrations</span><span style={S.bcVal}>{(Number(row.registrations) || 0).toLocaleString()}</span></div>}
                      {isColVisible("attended") && <div style={S.bcItem}><span style={S.bcLabel}>Attended</span><span style={S.bcVal}>{(Number(row.attended) || 0).toLocaleString()}</span></div>}
                      {isColVisible("replays") && <div style={S.bcItem}><span style={S.bcLabel}>Replays</span><span style={S.bcVal}>{(Number(row.replays) || 0).toLocaleString()}</span></div>}
                      {isColVisible("unique_replays") && <div style={S.bcItem}><span style={S.bcLabel}>Unique Replays</span><span style={S.bcVal}>{(Number(row.unique_replays) || 0).toLocaleString()}</span></div>}
                      {isColVisible("viewedcta") && <div style={S.bcItem}><span style={S.bcLabel}>Viewed CTA</span><span style={S.bcVal}>{(Number(row.viewedcta) || 0).toLocaleString()}</span></div>}
                      {isColVisible("clickedcta") && <div style={S.bcItem}><span style={S.bcLabel}>Clicked CTA</span><span style={S.bcVal}>{(Number(row.clickedcta) || 0).toLocaleString()}</span></div>}
                      {isColVisible("purchases") && <div style={S.bcItem}><span style={S.bcLabel}>Purchases</span><span style={S.purchBadge}>{(Number(row.purchases) || 0).toLocaleString()}</span></div>}
                      {customs.map(cm => { const v = evalFormula(cm.formula, row); return <div key={cm.id} style={S.bcItem}><span style={S.bcLabel}>{cm.name}</span><span style={{ ...S.bcVal, color: "#10B981" }}>{fmtVal(v, cm.format)}</span></div>; })}
                    </div>
                    <div style={S.boardCardActions}>
                      <button style={{ ...S.rowAct, borderColor: "#DBEAFE", background: "#EFF6FF" }} title="Recalc spend from Facebook" onClick={() => recalcSpend(row.date)}><I d="M23 4v6h-6M20.49 15a9 9 0 11-2.12-9.36L23 10" size={14} stroke="#3B82F6" /></button>
                      {isAdmin && <button style={S.rowAct} title="Edit row" onClick={() => { setEditRow(row); setView("entry"); }}><I d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" size={14} stroke="#6B7280" /></button>}
                      {isAdmin && <button style={{ ...S.rowAct, borderColor: "#FECACA", background: "#FEF2F2" }} title="Delete row" onClick={() => setDelConfirm(row.date)}><I d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" size={14} stroke="#EF4444" /></button>}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        {view === "entry" && <EntryForm initial={editRow} onSubmit={submitEntry} onCancel={() => { setView("dash"); setEditRow(null); }} />}
        {view === "custom" && <CMForm initial={editCM} onSubmit={saveCM} onCancel={() => { setView("custom-list"); setEditCM(null); }} metrics={metrics} />}
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
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
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
        {view === "insights" && <InsightsChat flash={flash} />}
      </main>

      {delConfirm && <Modal title="Delete Entry" msg={`Remove the entry for ${fmtDateNice(delConfirm)}?`} onCancel={() => setDelConfirm(null)} onConfirm={() => deleteEntry(delConfirm)} />}
      {delCM && <Modal title="Delete Custom Metric" msg="This will remove the column from your table." onCancel={() => setDelCM(null)} onConfirm={() => deleteCM(delCM)} />}
      {toast && (<div className="fi" style={{ ...S.toast, borderLeft: `3px solid ${toast.type === "ok" ? "#12864A" : "#D92D20"}` }}><I d={toast.type === "ok" ? "M20 6L9 17l-5-5" : "M12 2a10 10 0 100 20 10 10 0 000-20zM12 8v4M12 16h.01"} size={16} stroke={toast.type === "ok" ? "#12864A" : "#D92D20"} sw={2.2} />{toast.msg}</div>)}
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

function EntryForm({ initial, onSubmit, onCancel }) {
  const today = getLADate();
  const [f, setF] = useState({ date: initial?.date || today, fb_spend: initial?.fb_spend ?? "", registrations: initial?.registrations ?? "", replays: initial?.replays ?? "", viewedcta: initial?.viewedcta ?? "", clickedcta: initial?.clickedcta ?? "", purchases: initial?.purchases ?? "", attended: initial?.attended ?? "" });
  const set = (k, v) => setF(p => ({ ...p, [k]: v }));
  const go = () => onSubmit({ date: f.date, day: getLADay(f.date), fb_spend: parseFloat(f.fb_spend) || 0, registrations: parseInt(f.registrations) || 0, replays: parseInt(f.replays) || 0, viewedcta: parseInt(f.viewedcta) || 0, clickedcta: parseInt(f.clickedcta) || 0, purchases: parseInt(f.purchases) || 0, attended: parseInt(f.attended) || 0 });
  const fields = [{ k: "fb_spend", l: "Facebook Spend ($)", step: "0.01", ph: "0.00" }, { k: "registrations", l: "Registrations", ph: "0" }, { k: "replays", l: "Replays", ph: "0" }, { k: "viewedcta", l: "Viewed CTA", ph: "0" }, { k: "clickedcta", l: "Clicked CTA", ph: "0" }, { k: "purchases", l: "Purchases", ph: "0" }, { k: "attended", l: "Attended", ph: "0" }];
  return (
    <div className="fi" style={S.fc}>
      <div style={S.fh}><div style={{ ...S.formBadge, background: initial ? "#EFF8FF" : "#ECFDF3", color: initial ? "#175CD3" : "#12864A" }}>{initial ? "EDIT ENTRY" : "NEW ENTRY"}</div><h2 style={S.ft}>{initial ? `Update ${fmtDateNice(initial.date)}` : "Add Daily Metrics"}</h2><p style={S.fs}>Enter metrics for the day. Fields default to 0 if empty.</p></div>
      <div style={S.fcard}>
        <div style={{ marginBottom: 20 }}><label style={S.fl}>Date (MM/DD/YYYY)</label><input style={S.inp} value={f.date} onChange={e => set("date", e.target.value)} placeholder="03/14/2026" disabled={!!initial} />{!initial && f.date && <div style={S.hint}>{getLADay(f.date)}</div>}</div>
        <div style={S.fgrid}>{fields.map(fi => (<div key={fi.k}><label style={S.fl}>{fi.l}</label><input style={S.inp} type="number" step={fi.step} placeholder={fi.ph} value={f[fi.k]} onChange={e => set(fi.k, e.target.value)} /></div>))}</div>
        <div style={S.fa}><button style={S.btnLight} onClick={onCancel}>Cancel</button><button style={S.btnDark} onClick={go}>{initial ? "Update Entry" : "Add Entry"}</button></div>
      </div>
    </div>
  );
}

function CMForm({ initial, onSubmit, onCancel, metrics }) {
  const [f, setF] = useState({ id: initial?.id || "", name: initial?.name || "", formula: initial?.formula || "", format: initial?.format || "number" });
  const preview = f.formula && metrics.length > 0 ? evalFormula(f.formula, metrics[0]) : null;
  return (
    <div className="fi" style={S.fc}>
      <div style={S.fh}><h2 style={S.ft}>{initial ? `Edit "${initial.name}"` : "Create Custom Metric"}</h2></div>
      <div style={S.fcard}>
        <div style={{ marginBottom: 20 }}><label style={S.fl}>Metric Name</label><input style={S.inp} value={f.name} onChange={e => setF(p => ({ ...p, name: e.target.value }))} placeholder="e.g. CTA Click Rate" /></div>
        <div style={{ marginBottom: 20 }}><label style={S.fl}>Formula</label><input style={{ ...S.inp, fontFamily: "'IBM Plex Mono', monospace" }} value={f.formula} onChange={e => setF(p => ({ ...p, formula: e.target.value }))} placeholder="e.g. clickedcta / viewedcta * 100" /><div style={S.hint}>Available: {MK.join(", ")}</div></div>
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

function InsightsChat({ flash }) {
  const [history, setHistory] = useState([]);
  const [activeId, setActiveId] = useState(null);
  const [chatMsgs, setChatMsgs] = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [historyLoading, setHistoryLoading] = useState(true);
  const chatEndRef = useRef(null);
  // Cache of full conversation messages (sidebar list only has id/title/updated_at)
  const msgsCache = useRef({});

  // Load conversation list from Supabase on mount
  useEffect(() => {
    (async () => {
      try {
        const { data } = await api.getConversations();
        setHistory(data || []);
      } catch (e) {
        console.error('Failed to load conversations:', e.message);
      } finally {
        setHistoryLoading(false);
      }
    })();
  }, []);

  // When selecting a chat, fetch full messages if not cached
  useEffect(() => {
    if (!activeId) { setChatMsgs([]); return; }
    if (msgsCache.current[activeId]) {
      setChatMsgs(msgsCache.current[activeId]);
      return;
    }
    // Messages aren't in cache — for newly created chats they will be;
    // for existing ones loaded from sidebar we need to fetch them
    (async () => {
      try {
        const headers = await getAuthHeaders();
        const res = await fetch(`${API_BASE}/api/insights/conversations/${activeId}`, { headers });
        if (res.ok) {
          const body = await res.json();
          const msgs = body.data?.messages || [];
          msgsCache.current[activeId] = msgs;
          setChatMsgs(msgs);
        }
      } catch { /* fallback: empty */ }
    })();
  }, [activeId]);

  // Auto-scroll
  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [chatMsgs, chatLoading]);

  const saveChat = async (id, msgs) => {
    const title = msgs.find(m => m.role === "user")?.content?.slice(0, 50) || "New chat";
    msgsCache.current[id] = msgs;
    // Optimistic UI update
    setHistory(prev => {
      const existing = prev.find(c => c.id === id);
      if (existing) {
        return prev.map(c => c.id === id ? { ...c, title, updated_at: new Date().toISOString() } : c);
      }
      return [{ id, title, updated_at: new Date().toISOString() }, ...prev];
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
    outer: { display: "flex", height: "calc(100vh - 120px)", gap: 0 },
    sidebar: { width: sidebarOpen ? 260 : 0, minWidth: sidebarOpen ? 260 : 0, background: "#F9FAFB", borderRight: sidebarOpen ? "1px solid #E5E7EB" : "none", display: "flex", flexDirection: "column", overflow: "hidden", transition: "width 0.2s, min-width 0.2s", borderRadius: "12px 0 0 12px" },
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
    <div style={IC.outer}>
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
.rowBtn{opacity:0;transition:opacity 150ms ease}
input:focus{outline:none;border-color:#D1D5DB!important;box-shadow:0 0 0 3px rgba(243,244,246,1)!important}
::selection{background:rgba(18,134,74,0.12)}
@keyframes blink{0%,100%{opacity:0.3}50%{opacity:1}}
.blink-1{animation:blink 1.2s 0s infinite}.blink-2{animation:blink 1.2s 0.2s infinite}.blink-3{animation:blink 1.2s 0.4s infinite}
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
  strip: { display: "flex", border: "1px solid #E5E7EB", borderRadius: 12, background: "#fff", marginBottom: 24, overflow: "hidden", boxShadow: "0 1px 2px rgba(0,0,0,0.02)" },
  stripCell: { flex: 1, padding: "24px", display: "flex", flexDirection: "column", gap: 8 },
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
  tableWrap: { background: "#fff", border: "1px solid #E5E7EB", borderRadius: 12, overflow: "hidden", boxShadow: "0 1px 3px rgba(0,0,0,0.04)" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13 },
  th: { padding: "14px 20px", textAlign: "center", fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", borderBottom: "1px solid #E5E7EB", background: "#FFFFFF", letterSpacing: "0.05em" },
  td: { padding: "14px 20px", borderBottom: "1px solid #F3F4F6", verticalAlign: "middle", textAlign: "center" },
  tdNum: { padding: "14px 20px", borderBottom: "1px solid #F3F4F6", fontWeight: 500, fontVariantNumeric: "tabular-nums", color: "#111827", fontSize: 13, textAlign: "center" },
  tdMoney: { padding: "14px 20px", borderBottom: "1px solid #F3F4F6", fontWeight: 500, fontVariantNumeric: "tabular-nums", color: "#111827", fontSize: 13, textAlign: "center" },
  dayPill: { display: "inline-block", padding: "4px 10px", background: "#F3F4F6", color: "#4B5563", fontSize: 11, fontWeight: 600, borderRadius: 6, letterSpacing: "0.02em", textTransform: "uppercase" },
  todayDot: { display: "inline-block", width: 6, height: 6, borderRadius: "50%", background: "#F97316", marginLeft: 4, verticalAlign: "middle" },
  purchBadge: { display: "inline-block", padding: "2px 8px", background: "#ECFDF5", color: "#047857", fontWeight: 600, borderRadius: 4, border: "1px solid #A7F3D0" },
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
  boardGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 24 },
  boardCard: { background: "#fff", border: "1px solid #E5E7EB", borderRadius: 12, padding: 24, boxShadow: "0 1px 3px rgba(0,0,0,0.04)", display: "flex", flexDirection: "column" },
  boardCardHeader: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, borderBottom: "1px solid #F3F4F6", paddingBottom: 16 },
  boardCardBody: { display: "flex", flexDirection: "column", gap: 12, flex: 1 },
  bcItem: { display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 13 },
  bcLabel: { color: "#6B7280", fontWeight: 500 },
  bcVal: { color: "#111827", fontWeight: 600, fontVariantNumeric: "tabular-nums" },
  boardCardActions: { display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 16, paddingTop: 16, borderTop: "1px solid #F3F4F6" },
};
