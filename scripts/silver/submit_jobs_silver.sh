#!/bin/bash
set -e

# =========================
# CONFIG
# =========================
PROJECT_ID="${PROJECT_ID:-YOUR_PROJECT_ID}"
REGION="${REGION:-YOUR_REGION}"
BUCKET="${BUCKET:-gs://YOUR_BUCKET_NAME}"

DEPS_BUCKET="${DEPS_BUCKET:-YOUR_DEPS_BUCKET}"

CODE_ZIP_GCS="${BUCKET}/code/steam_bigdata.zip"
MAIN_PY_GCS_BASE="${BUCKET}/code/steam_bigdata/jobs/silver"

# =========================
# HELPER
# =========================
submit_job() {
  local job_file=$1
  echo "=== SUBMIT ${job_file} ==="

  gcloud dataproc batches submit pyspark \
    "${MAIN_PY_GCS_BASE}/${job_file}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --deps-bucket="${DEPS_BUCKET}" \
    --py-files="${CODE_ZIP_GCS}"
}

# =========================
# SILVER JOBS
# =========================
submit_job "01_silver_reviews.py"
submit_job "02_silver_games.py"
submit_job "03_silver_users.py"
submit_job "04_silver_bridges.py"
submit_job "05_silver_game_text.py"

# Nếu cần chạy job cũ thì mở comment bên dưới
# submit_job "90_silver_quality_metrics.py"

# =========================
# QUALITY JOBS
# =========================
submit_job "91_silver_quality_issues.py"
submit_job "92_silver_quality_columns.py"
submit_job "93_silver_quality_outliers.py"
submit_job "94_silver_quality_histograms.py"
submit_job "95_silver_quality_overview.py"

echo "=== DONE SUBMIT ALL SILVER + QUALITY JOBS ==="