/**
 * Shumard - Lead Attribution & Cross-Frame Identity Script
 * Architecture: Hyros-style field capture + postMessage cross-frame stitching
 */
(function () {
  'use strict';

  var BACKEND_URL = '__TRACKING_BACKEND_URL__';
  var API_BASE    = BACKEND_URL + '/api';
  var AUTO_TAG    = '__TRACKING_AUTO_TAG__';   /* injected by server when ?tag=... is in script src */

  /* ─── Central store ─── */
  var store = {
    lead: { email: '', phone: '', firstName: '', lastName: '', name: '' },
    source: {
      utm_source: '', utm_medium: '', utm_campaign: '', utm_term: '', utm_content: '',
      utm_id: '', campaign_id: '', adset_id: '', ad_id: '',
      fbclid: '', gclid: '', ttclid: '', source_link_tag: '', fb_ad_set_id: '', google_campaign_id: '',
      extra: {}
    },
    config: {
      contactId:      '',
      sessionId:      '',
      parentContactId: '',   // set when running inside an iframe and parent sends its ID
      isIframe:       false,
      prevUrl:        document.referrer || '',
      currentUrl:     window.location.href,
      pageTitle:      document.title || ''
    },
    processedData: { emailSent: false, phoneSent: false, pageSent: false }
  };

  /* ─── Detect iframe context ─── */
  try { store.config.isIframe = window.self !== window.top; } catch (e) { store.config.isIframe = true; }

  /* Baseline URL for the SPA change watcher. Module-scoped so captureClickParams()
     can re-sync it after scrubbing email params (prevents a self-induced dup pageview). */
  var _spaLastUrl = window.location.href;

  /* ─── Storage helpers ─── */
  var LS_KEY   = 'st_contact_id';
  var SESS_KEY = 'st_session_id';
  var ATTR_KEY = 'st_attribution';

  function setCookie(name, value, days) {
    try {
      var exp = new Date(Date.now() + days * 864e5).toUTCString();
      document.cookie = name + '=' + encodeURIComponent(value) + '; expires=' + exp + '; path=/; SameSite=None; Secure';
    } catch (e) {}
  }

  function getCookie(name) {
    try {
      var v = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)');
      return v ? decodeURIComponent(v.pop()) : null;
    } catch (e) { return null; }
  }

  function lsGet(key) {
    try { return localStorage.getItem(key) || getCookie(key); } catch (e) { return getCookie(key); }
  }

  function lsSet(key, value) {
    try { localStorage.setItem(key, value); } catch (e) {}
    setCookie(key, value, 365);
  }

  function ssGet(key) {
    try { return sessionStorage.getItem(key); } catch (e) { return null; }
  }

  function ssSet(key, value) {
    try { sessionStorage.setItem(key, value); } catch (e) {}
  }

  /* ─── UUID ─── */
  function genUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      var r = Math.random() * 16 | 0;
      return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
  }

  /* ─── contact_id: persistent per-device per-domain ─── */
  function getContactId() {
    var id = lsGet(LS_KEY);
    if (!id) { id = genUUID(); lsSet(LS_KEY, id); }
    return id;
  }

  /*
   * ─── session_id: shared across parent + iframe within ONE tab ───
   *
   * Strategy:
   *  • Parent creates session_id and stores in sessionStorage
   *  • Parent broadcasts it to all child iframes via postMessage
   *  • iframe receives and uses the SAME session_id
   *  • Backend can stitch contacts sharing the same session_id
   */
  function initSessionId() {
    // If iframe, wait for parent to send us the session_id via postMessage
    // Fallback: generate our own (will be replaced when postMessage arrives)
    var sid = ssGet(SESS_KEY) || lsGet(SESS_KEY);
    if (!sid) { sid = genUUID(); }
    ssSet(SESS_KEY, sid);
    return sid;
  }

  /* ─── URL param parser ─── */
  function getUrlParams(url) {
    try {
      var search = (url || window.location.href).split('?')[1] || '';
      var result = {};
      search.replace(/#.*$/, '').split('&').forEach(function (pair) {
        var kv = pair.split('=');
        if (kv[0]) result[decodeURIComponent(kv[0])] = decodeURIComponent((kv[1] || '').replace(/\+/g, ' '));
      });
      return result;
    } catch (e) { return {}; }
  }

  /* ─── Attribution capture ─── */
  function captureAttribution() {
    var cached = lsGet(ATTR_KEY);
    if (cached) {
      try {
        Object.assign(store.source, JSON.parse(cached));
        // Always refresh fbc/fbp cookies (they may have been set after initial cache)
        var fbc = getCookie('_fbc');
        var fbp = getCookie('_fbp');
        var needsUpdate = false;
        if (fbc && store.source.fbc !== fbc) { store.source.fbc = fbc; needsUpdate = true; }
        if (fbp && store.source.fbp !== fbp) { store.source.fbp = fbp; needsUpdate = true; }
        // Save updated attribution if fbc/fbp changed
        if (needsUpdate) {
          lsSet(ATTR_KEY, JSON.stringify(store.source));
        }
        return;
      } catch (e) {}
    }
    var p = getUrlParams();
    /* Known field map: target_key → list of URL param names that map to it */
    var map = {
      utm_source:         ['utm_source'],
      utm_medium:         ['utm_medium'],
      utm_campaign:       ['utm_campaign'],
      utm_term:           ['utm_term'],
      utm_content:        ['utm_content'],
      utm_id:             ['utm_id'],
      campaign_id:        ['campaign_id'],
      adset_id:           ['adset_id'],
      ad_id:              ['ad_id'],
      fbclid:             ['fbclid', 'fb_cl_id'],
      gclid:              ['gclid', 'g_cl_id'],
      ttclid:             ['ttclid'],
      source_link_tag:    ['sl'],
      fb_ad_set_id:       ['fbc_id', 'h_ad_id', 'fbadid'],
      google_campaign_id: ['gc_id', 'h_campaign_id']
    };

    /* Build a set of ALL URL param names that are already handled by the map */
    var handledParams = {};
    Object.keys(map).forEach(function(key) {
      map[key].forEach(function(param) { handledParams[param] = true; });
    });
    /* el / htrafficsource / he are click-level email params owned by
       captureClickParams() — keep them out of the generic `extra` bucket
       (and never store the raw email from `he` in attribution). */
    handledParams['el'] = handledParams['htrafficsource'] = handledParams['he'] = true;

    var found = false;

    /* Match known params */
    Object.keys(map).forEach(function(key) {
      map[key].forEach(function(param) {
        if (p[param]) { store.source[key] = p[param]; found = true; }
      });
    });

    /* Capture Facebook cookies (_fbc and _fbp) for FB CAPI matching */
    var fbc = getCookie('_fbc');
    var fbp = getCookie('_fbp');
    if (fbc) { store.source.fbc = fbc; found = true; }
    if (fbp) { store.source.fbp = fbp; found = true; }

    /* Capture EVERY remaining unknown param into extra */
    var extra = {};
    Object.keys(p).forEach(function(param) {
      if (!handledParams[param] && p[param]) {
        extra[param] = p[param];
        found = true;
      }
    });
    if (Object.keys(extra).length > 0) {
      store.source.extra = extra;
    }

    if (found) {
      lsSet(ATTR_KEY, JSON.stringify(store.source));
    }
  }

  /*
   * ─── Email link params (click-level) ───
   * Email links carry he=<contact email>, el=<source>, htrafficsource=<traffic source>,
   * e.g. /checkout?he=jane@x.com&el=native_ads&htrafficsource=email
   * Unlike first-touch attribution, these describe THIS click, so we always read them
   * from the current URL and overlay them onto store.source. `he` fires an immediate
   * identify so the click is tied to the known contact even on a device we've never seen.
   */
  function captureClickParams() {
    var p = getUrlParams();
    var changed = false;
    if (p.el && store.source.source !== p.el) { store.source.source = p.el; changed = true; }
    if (p.htrafficsource) {
      var ts = String(p.htrafficsource).toLowerCase();   // normalize so 'Email' == 'email' downstream
      if (store.source.traffic_source !== ts) { store.source.traffic_source = ts; changed = true; }
    }
    if (changed) lsSet(ATTR_KEY, JSON.stringify(store.source));
    /* Read `he` from the raw query string (not getUrlParams, which turns '+' into a
       space) so plus-addressed emails like jane+webinar@gmail.com survive intact. */
    var heRaw = (window.location.search.match(/[?&]he=([^&#]*)/) || [])[1];
    var he = null;
    if (heRaw) { try { he = decodeURIComponent(heRaw); } catch (e) { he = heRaw; } }
    if (he && isEmail(he) && he !== store.lead.email) {
      store.lead.email = he;
      logger('id');
      sendLead({ email: he });
    }
    /* Privacy: once captured, drop he/el/htrafficsource from the visible URL so the
       email isn't left in the address bar / history or leaked via the Referer header. */
    if (p.he || p.el || p.htrafficsource) {
      try {
        var clean = window.location.href
          .replace(/([?&])he=[^&#]*/i, '$1')
          .replace(/([?&])el=[^&#]*/i, '$1')
          .replace(/([?&])htrafficsource=[^&#]*/i, '$1')
          .replace(/([?&])&+/g, '$1')      /* collapse doubled separators */
          .replace(/[?&]+(#|$)/, '$1');    /* drop a dangling ? or & */
        if (clean !== window.location.href && window.history && window.history.replaceState) {
          window.history.replaceState(null, document.title, clean);
          /* Re-baseline the SPA URL watcher so it doesn't read OUR rewrite as a
             navigation and fire a duplicate pageview. */
          _spaLastUrl = window.location.href;
        }
      } catch (e) {}
    }
  }

  /* ─── Network ─── */
  function send(endpoint, payload) {
    var url  = API_BASE + endpoint;
    var body = JSON.stringify(payload);
    var hdrs = { 'Content-Type': 'application/json' };
    if (typeof fetch !== 'undefined') {
      try { fetch(url, { method: 'POST', headers: hdrs, body: body, keepalive: true }).catch(function () { xhrSend(url, body); }); return; }
      catch (e) {}
    }
    xhrSend(url, body);
  }

  function xhrSend(url, body) {
    try { var x = new XMLHttpRequest(); x.open('POST', url, true); x.setRequestHeader('Content-Type', 'application/json'); x.send(body); }
    catch (e) {}
  }

  /* ─── Common payload ─── */
  function buildPayload(extra) {
    return Object.assign({
      contact_id:   store.config.contactId,
      session_id:   store.config.sessionId || null,
      current_url:  window.location.href,
      referrer_url: store.config.prevUrl || null,
      page_title:   document.title || null,
      attribution:  store.source,
      user_agent:   navigator.userAgent || null   /* For FB CAPI: client_user_agent */
    }, extra || {});
  }

  /* ─── Tracking calls ─── */
  function sendPageview() {
    if (store.processedData.pageSent) return;
    store.processedData.pageSent = true;
    send('/sg/pageview', buildPayload());
  }

  function sendLead(fields) {
    /* Refresh fbc/fbp from cookies right before sending (FB Pixel may have set them) */
    var fbc = getCookie('_fbc');
    var fbp = getCookie('_fbp');
    if (fbc && store.source.fbc !== fbc) { store.source.fbc = fbc; lsSet(ATTR_KEY, JSON.stringify(store.source)); }
    if (fbp && store.source.fbp !== fbp) { store.source.fbp = fbp; lsSet(ATTR_KEY, JSON.stringify(store.source)); }
    
    var email = fields && fields.email;
    var phone = fields && fields.phone;
    if (email || phone) {
      var parts = [];
      if (email) parts.push('email: ' + email);
      if (phone) parts.push('phone: ' + phone);
      logger('ev');
    }
    send('/sg/lead', buildPayload(fields));
  }

  function sendRegistration(fields) {
    /* Refresh fbc/fbp from cookies right before sending (FB Pixel may have set them) */
    var fbc = getCookie('_fbc');
    var fbp = getCookie('_fbp');
    if (fbc && store.source.fbc !== fbc) { store.source.fbc = fbc; lsSet(ATTR_KEY, JSON.stringify(store.source)); }
    if (fbp && store.source.fbp !== fbp) { store.source.fbp = fbp; lsSet(ATTR_KEY, JSON.stringify(store.source)); }
    
    var parts = [];
    if (fields && fields.email) parts.push('email: ' + fields.email);
    if (fields && fields.phone) parts.push('phone: ' + fields.phone);
    if (fields && fields.name)  parts.push('name: ' + fields.name);
    if (parts.length) logger('ev');
    send('/sg/registration', buildPayload(fields));
  }

  /* ─── Stitch is now backend-only -- function kept as no-op for public API compat ─── */
  function sendStitch() { /* backend handles all stitching via session_id */ }

  /* Silent by default — nothing is printed to the console. For troubleshooting,
     set localStorage 'st_debug'='1' (or window.__st_debug=true) before load. */
  function logger(msg, data) {
    try {
      var dbg = false;
      try { dbg = window.__st_debug === true || localStorage.getItem('st_debug') === '1'; } catch (e) {}
      if (!dbg) return;
      if (data !== undefined) { console.log('[st] ' + msg, data); } else { console.log('[st] ' + msg); }
    } catch (e) {}
  }

  /* ═══════════════════════════════════════════════════════════════
     CROSS-FRAME IDENTITY STITCHING (postMessage bridge)
     ═══════════════════════════════════════════════════════════════

     HOW IT WORKS:
     1. Parent page broadcasts {type:'st_parent_id', contactId, sessionId} to ALL iframes
     2. iframe receives this, records parentContactId, updates its session_id,
        fires stitch API, and replies with its own contactId
     3. Parent receives child reply and fires stitch API as a double-confirm

     This ensures BOTH ends initiate the stitch even if one message is dropped.
  */

  /* ─── Parent: broadcast identity to all child iframes ─── */
  function broadcastToIframes() {
    if (store.config.isIframe) return; // don't re-broadcast from iframes
    var frames = document.querySelectorAll('iframe');
    if (!frames.length) return;
    var msg = {
      type:      'st_parent_id',
      contactId: store.config.contactId,
      sessionId: store.config.sessionId,
      version:   '2'
    };
    frames.forEach(function (f) {
      try { f.contentWindow.postMessage(msg, '*'); } catch (e) {}
    });
  }

  /* ─── Handle incoming postMessages ─── */
  window.addEventListener('message', function (e) {
    if (!e.data || typeof e.data !== 'object') return;

    /*
     * iframe receives parent session_id -- adopt it so the backend can stitch
     * the two contacts together via _session_auto_stitch when a lead/registration
     * comes in.  No HTTP request is made here; stitching is purely backend-driven.
     */
    if (e.data.type === 'st_parent_id' && store.config.isIframe) {
      var parentSess = e.data.sessionId;
      if (parentSess && parentSess !== store.config.sessionId) {
        store.config.sessionId = parentSess;
        ssSet(SESS_KEY, parentSess);
      }
    }

    /* Capture form data posted by webinar platform iframes */
    if (e.data.type === 'registration' || e.data.type === 'webinar_registration') {
      var d = e.data.data || e.data;
      if (d.email) sendLead({ email: d.email, name: d.name || null, phone: d.phone || null });
    }
  });

  /* ─── Field detection ─── */
  var CLASSES = {
    email:     ['st-email', 'hyros-email'],
    firstName: ['st-first-name', 'hyros-first-name'],
    lastName:  ['st-last-name', 'hyros-last-name'],
    phone:     ['st-phone', 'hyros-phone', 'st-telephone']
  };
  var ATTR_NAMES = {
    email:     ['email', 'Email', 'EMAIL', 'user_email', 'subscriber_email', 'attendee_email', 'email_address', 'emailaddress', 'your-email', 'contact_email'],
    firstName: ['first_name', 'firstname', 'fname', 'first-name', 'FirstName'],
    lastName:  ['last_name', 'lastname', 'lname', 'last-name', 'LastName'],
    name:      ['full_name', 'fullname', 'name', 'Name', 'contact_name', 'your-name', 'attendee_name', 'participant_name'],
    phone:     ['phone', 'Phone', 'PHONE', 'telephone', 'mobile', 'cell', 'phone_number', 'attendee_phone', 'phonenumber', 'your-phone', 'contact_phone', 'mobilephone']
  };

  function hasClass(el, cls) { for (var i = 0; i < cls.length; i++) { if (el.classList && el.classList.contains(cls[i])) return true; } return false; }
  function matchAttr(el, names) {
    var n = (el.name||'').toLowerCase(), id = (el.id||'').toLowerCase(), ph = (el.placeholder||'').toLowerCase(), da = (el.getAttribute('data-field')||'').toLowerCase();
    for (var i = 0; i < names.length; i++) { var nm = names[i].toLowerCase(); if (n===nm||id===nm||ph.indexOf(nm)!==-1||da===nm) return true; }
    return false;
  }
  function classifyInput(el) {
    if (!el || !el.tagName) return null;
    var tag = el.tagName.toUpperCase(), type = (el.type||'').toLowerCase(), im = (el.getAttribute('inputmode')||'').toLowerCase();
    if (tag!=='INPUT'&&tag!=='TEXTAREA'&&tag!=='SELECT') return null;
    if (hasClass(el, CLASSES.email))     return 'email';
    if (hasClass(el, CLASSES.firstName)) return 'firstName';
    if (hasClass(el, CLASSES.lastName))  return 'lastName';
    if (hasClass(el, CLASSES.phone))     return 'phone';
    if (type==='email') return 'email';
    if (type==='tel'||im==='tel'||im==='numeric') return 'phone';
    if (matchAttr(el, ATTR_NAMES.email))     return 'email';
    if (matchAttr(el, ATTR_NAMES.phone))     return 'phone';
    if (matchAttr(el, ATTR_NAMES.firstName)) return 'firstName';
    if (matchAttr(el, ATTR_NAMES.lastName))  return 'lastName';
    if (matchAttr(el, ATTR_NAMES.name))      return 'name';
    return null;
  }

  function isEmail(v) { return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test((v||'').trim()); }
  function isPhone(v) { return /^[+\d][\d\s\-().]{6,19}$/.test((v||'').trim()); }

  /* ─── Field change handler ─── */
  function handleFieldChange(el) {
    var ft = classifyInput(el); if (!ft) return;
    var val = (el.value||'').trim(); if (!val) return;

    /* Helper: build name from whatever is already stored */
    function storedName() {
      return store.lead.name || ((store.lead.firstName+' '+store.lead.lastName).trim()) || null;
    }

    if (ft==='email' && isEmail(val) && val!==store.lead.email) {
      store.lead.email = val;
      var p = { email: val };
      var n = storedName(); if (n) p.name = n;
      if (store.lead.firstName) p.first_name = store.lead.firstName;
      if (store.lead.lastName)  p.last_name  = store.lead.lastName;
      sendLead(p);
    }
    else if (ft==='phone' && isPhone(val) && val!==store.lead.phone) {
      store.lead.phone = val;
      var pp = { phone: val };
      if (store.lead.email) pp.email = store.lead.email;
      var pn = storedName(); if (pn) pp.name = pn;
      sendLead(pp);
    }
    else if (ft==='firstName') {
      store.lead.firstName = val;
      if (store.lead.email || store.lead.phone)
        sendLead({ email: store.lead.email||null, phone: store.lead.phone||null, first_name: val, name: storedName() });
    }
    else if (ft==='lastName') {
      store.lead.lastName = val;
      if (store.lead.email || store.lead.phone)
        sendLead({ email: store.lead.email||null, phone: store.lead.phone||null, last_name: val, name: storedName() });
    }
    else if (ft==='name') {
      store.lead.name = val;
      if (store.lead.email || store.lead.phone)
        sendLead({ email: store.lead.email||null, phone: store.lead.phone||null, name: val });
    }
  }

  /* ─── Form submit ─── */
  function handleFormSubmit(form) {
    form.querySelectorAll('input, textarea, select').forEach(function (el) {
      var ft=classifyInput(el), v=(el.value||'').trim(); if (!ft||!v) return;
      if (ft==='email'&&isEmail(v))     store.lead.email=v;
      if (ft==='phone'&&isPhone(v))     store.lead.phone=v;
      if (ft==='firstName')             store.lead.firstName=v;
      if (ft==='lastName')              store.lead.lastName=v;
      if (ft==='name')                  store.lead.name=v;
    });
    if (!store.lead.email && !store.lead.phone) return;
    var fullName = store.lead.name || ((store.lead.firstName+' '+store.lead.lastName).trim()) || null;
    sendRegistration({ email:store.lead.email||null, phone:store.lead.phone||null, name:fullName, first_name:store.lead.firstName||null, last_name:store.lead.lastName||null });
  }

  /* ─── Form binding ─── */
  function bindInputListeners(form) {
    if (!form||form._st_inputs_bound) return; form._st_inputs_bound=true;
    form.querySelectorAll('input, textarea, select').forEach(function (el) {
      if (el._st_bound) return; el._st_bound=true;
      el.addEventListener('change', function(){handleFieldChange(el);}, true);
      el.addEventListener('blur',   function(){handleFieldChange(el);}, true);
    });
  }
  function bindSubmitListener(form) {
    if (!form||form._st_submit_bound) return; form._st_submit_bound=true;
    form.addEventListener('submit', function(){setTimeout(function(){handleFormSubmit(form);},0);}, true);
  }
  function bindForms() { document.querySelectorAll('form').forEach(function(f){bindInputListeners(f);bindSubmitListener(f);}); }
  function bindLooseInputs() {
    document.querySelectorAll('input, textarea').forEach(function(el){
      if (el.form||el._st_bound) return; el._st_bound=true;
      el.addEventListener('change', function(){handleFieldChange(el);}, true);
      el.addEventListener('blur',   function(){handleFieldChange(el);}, true);
    });
  }

  /* ─── Click capture for SPA submit buttons ─── */
  document.addEventListener('click', function(e) {
    var el=e.target;
    for (var i=0;i<5&&el;i++,el=el.parentElement) {
      var tag=(el.tagName||'').toUpperCase(), type=(el.type||'').toLowerCase();
      if ((tag==='BUTTON'&&(type==='submit'||!el.type||type==='button'))||(tag==='INPUT'&&type==='submit')) {
        var form=el.closest('form');
        if (form) { setTimeout(function(){handleFormSubmit(form);},100); } break;
      }
    }
  }, {capture:true, passive:true});

  /* ─── MutationObserver ─── */
  var _obs=null;
  function watchDOM() {
    if (_obs||!window.MutationObserver) return;
    _obs=new MutationObserver(function(){bindForms();bindLooseInputs();});
    _obs.observe(document.body,{childList:true,subtree:true});
  }

  /* ─── Custom event ─── */
  window.addEventListener('stealthtrack_email', function(e){
    var em=e.detail&&e.detail.email;
    if (em&&isEmail(em)&&em!==store.lead.email){store.lead.email=em;sendLead({email:em});}
  });

  /* ─── SPA URL change detection ─── */
  /* _spaLastUrl is module-scoped so captureClickParams() can re-baseline it after
     scrubbing he/el/htrafficsource (otherwise our own replaceState looks like a nav). */
  function urlNoHash(u){ return (u||'').split('#')[0]; }
  setInterval(function(){
    var cur=window.location.href;
    /* Compare without the hash: a real route change updates the path/query; a bare
       #anchor jump (e.g. #final-cta) is the same page, not a new pageview. */
    if (urlNoHash(cur)!==urlNoHash(_spaLastUrl)){
      _spaLastUrl=cur; store.config.prevUrl=store.config.currentUrl; store.config.currentUrl=cur;
      store.processedData.pageSent=false; captureClickParams(); sendPageview(); bindForms(); bindLooseInputs();
    } else {
      _spaLastUrl=cur;  /* keep baseline current so a later real change is detected once */
    }
  }, 800);

  /* ─── Init ─── */
  function init() {
    store.config.contactId = getContactId();
    store.config.sessionId = initSessionId();
    captureAttribution();
    captureClickParams();
    bindForms();
    bindLooseInputs();
    watchDOM();
    sendPageview();

    /* ─── Delayed fbc/fbp re-capture ─── */
    /* Facebook Pixel often sets _fbc/_fbp cookies AFTER initial page load.
       Re-check cookies after a delay to capture them if FB Pixel was slow. */
    setTimeout(function() {
      var fbc = getCookie('_fbc');
      var fbp = getCookie('_fbp');
      if ((fbc && store.source.fbc !== fbc) || (fbp && store.source.fbp !== fbp)) {
        if (fbc) store.source.fbc = fbc;
        if (fbp) store.source.fbp = fbp;
        lsSet(ATTR_KEY, JSON.stringify(store.source));
        logger('fb');
      }
    }, 2000);  /* 2 second delay for FB Pixel to set cookies */

    /* ─── Auto-tag: fire when script was loaded with ?tag=... ─── */
    if (AUTO_TAG) {
      send('/sg/tag', {
        contact_id: store.config.contactId,
        session_id: store.config.sessionId || null,
        tag:        AUTO_TAG
      });
      logger('ev');
    }

    /* Parent page: broadcast session identity to iframes */
    if (!store.config.isIframe) {
      var broadcastCount = 0;
      var broadcastInterval = setInterval(function () {
        broadcastToIframes();
        if (++broadcastCount >= 30) clearInterval(broadcastInterval);
      }, 1000);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  /* ─── Public API ─── */
  window.Shumard = {
    getContactId:  getContactId,
    getSessionId:  function(){ return store.config.sessionId; },
    identify: function(fields){
      if (fields.email && isEmail(fields.email)){ store.lead.email=fields.email; sendLead(fields); }
    },
    stitch:    sendStitch,
    trackEvent: sendLead,
    store:     store
  };

})();
