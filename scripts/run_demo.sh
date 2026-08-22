#!/usr/bin/env bash
set -euo pipefail
export DATABASE_URL=${DATABASE_URL:-postgresql://platform:platform@localhost:5432/commerce}
python batch/extract_postgres.py
python scripts/build_warehouse.py
pytest -q tests/test_data_quality.py
printf '\nDemo ready. Start API: uvicorn app.main:app --reload\n'
printf 'Then call: curl -H "Authorization: Bearer talent-grid-demo-token" http://localhost:8000/v1/kpis\n'
