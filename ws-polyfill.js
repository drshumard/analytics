// ws-polyfill.js — import this FIRST, before any '@supabase/*' import.
//
// Node < 22 has no native global WebSocket. @supabase/realtime-js (≥2.106), pulled
// in transitively by @supabase/supabase-js's createClient(), probes for a WebSocket
// constructor at client init and THROWS if none is found ("Node.js 20 detected
// without native WebSocket support"). We never use realtime (no .channel/.subscribe
// anywhere), but the probe still runs. Setting the global `ws` constructor satisfies
// it without needing the per-client `transport` option to thread through correctly.
//
// Placed in its own module because ES `import`s are hoisted and evaluated in source
// order: importing this module first guarantees the global is set before the supabase
// module graph is evaluated. (A plain statement between imports would NOT be reliable.)
//
// No-op on Node 22+ (native WebSocket already present).
import ws from 'ws';

if (!globalThis.WebSocket) {
    globalThis.WebSocket = ws;
}
