# -----------------------------------------------------------------------------
# CTS Carbon Platform — Gunicorn (Azure VM behind Nginx)
#
# Perf notes (adjust on the VM — do NOT change Flask app code paths):
# • workers: set to ~(2 × CPU cores) for IO-bound dashboards; SQLite local DB
#   stays single-process unless you migrate to Postgres + shared store.
# • threads per worker handles concurrent nginx-proxied requests to one worker.
# • Increase workers ONLY when DB layer no longer SQLite file-locked bottleneck.
# • timeout reflects long-running admin mapping jobs streaming progress.
#
# Example production tweak (PostgreSQL/multi-worker capable):
#   workers = 4
#   threads = 4
# -----------------------------------------------------------------------------
workers = 1
threads = 4
timeout = 300
bind = "127.0.0.1:8000"

