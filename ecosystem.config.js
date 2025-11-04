module.exports = {
  apps: [
    {
      name: 'athena-snapshot',
      script: 'index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 5000,
      env: {
        NODE_ENV: 'production',
        RESTART_ON_ERROR: 'true', // 'false' para mantener vivo el scheduler aunque haya errores
        ONE_SHOT: 'false', // 'true' para ejecutar una vez y salir
        TZ: 'America/Costa_Rica'
      }
    }
  ]
};
