module.exports = {
  apps: [
    {
      name: "univ-backend",
      script: "./venv/bin/uvicorn",
      args: "app.main:app --host 0.0.0.0 --port 8000",
      interpreter: "python3",
      env: {
        NODE_ENV: "production",
      },
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
    },
    {
      name: "univ-worker",
      script: "./venv/bin/celery",
      args: "-A app.tasks.celery_app worker --loglevel=info",
      interpreter: "python3",
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
    },
    {
      name: "univ-beat",
      script: "./venv/bin/celery",
      args: "-A app.tasks.celery_app beat --loglevel=info",
      interpreter: "python3",
      autorestart: true,
      watch: false,
    }
  ],
};
