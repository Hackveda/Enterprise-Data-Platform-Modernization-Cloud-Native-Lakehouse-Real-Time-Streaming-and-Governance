.PHONY: up down extract warehouse test events stream api demo snowflake snowflake-events snowflake-refresh snowflake-verify
up:
	docker compose up -d postgres redpanda minio

down:
	docker compose down -v

extract:
	python batch/extract_postgres.py

warehouse:
	python scripts/build_warehouse.py

test:
	pytest -q

events:
	python producer/order_events.py

stream:
	python streaming/consumer_to_lake.py

api:
	uvicorn app.main:app --reload

demo:
	bash scripts/run_demo.sh

snowflake:
	bash scripts/run_snowflake_demo.sh

snowflake-events:
	python streaming/consumer_to_snowflake.py

snowflake-refresh:
	dbt run --project-dir warehouse/dbt_snowflake --profiles-dir warehouse/dbt_snowflake
	dbt test --project-dir warehouse/dbt_snowflake --profiles-dir warehouse/dbt_snowflake

snowflake-verify:
	python scripts/verify_snowflake.py
