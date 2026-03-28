// PM2 Ecosystem File — Zero-Downtime Deploy Configuration
module.exports = {
  apps: [{
    name: 'analytics',
    script: 'server.js',
    // Zero-downtime: PM2 waits for process.send('ready') before routing traffic
    wait_ready: true,
    listen_timeout: 10000,  // max 10s to signal ready
    // Graceful shutdown: PM2 sends SIGINT, app closes connections, then exits
    kill_timeout: 5000,
    // Environment
    env: {
      NODE_ENV: 'production',
      TZ: 'America/Los_Angeles',
    },
  }],
};
