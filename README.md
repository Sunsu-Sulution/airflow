# SunSu Airflow

Containerised Apache Airflow stack the team uses to orchestrate SunSu data pipelines.  
The default DAG bundle focuses on synchronising Primo membership data from S3 into the internal analytics warehouse, but the stack is ready for additional jobs and plugins.

## Highlights

- Airflow 3.1 with the Celery executor (Postgres metadata DB + Redis broker).
- Custom image that layers Chromium + Chromedriver for Selenium jobs.
- Shared `config/airflow.cfg` baked into the containers for reproducible auth and scheduler settings.
- Local volumes for `dags/`, `logs/`, `plugins/`, and `downloads/` so you can iterate without rebuilding.

## Repository Layout

- `docker-compose.yaml` – multi-service stack (scheduler, triggerer, workers, API server, init job, Redis, Postgres).
- `dockerfile` – Airflow base image with additional system packages and Python deps (`selenium`, `boto3`).
- `config/airflow.cfg` – overrides applied inside every Airflow container.
- `dags/` – project DAGs and utilities.
  - `jobs/process_primo_data.py` – fetches incremental Primo CSVs from S3, normalises column types, writes to Postgres.
  - `util/` – shared helpers (`s3.py`, `chrome.py`).
- `downloads/` – bind-mounted target for Selenium browser downloads.
- `logs/` – scheduler/worker logs from Airflow runs (persisted between container restarts).

## Prerequisites

- Docker Desktop 4.31+ (or Docker Engine 25+).
- Docker Compose plugin v2.
- AWS credentials with read access to `bearhouse-crm-primo` (or whichever bucket you point the DAG at).
- Postgres target reachable from the worker containers.

## Configuration

1. Duplicate your environment template:
   ```bash
   cp .env.example .env  # create one if it does not exist yet
   ```
   The stack reads `${ENV_FILE_PATH:-.env}` from `docker-compose.yaml`.
2. Minimum variables:

   ```bash
   AIRFLOW_UID=50000                 # optional on macOS, required on Linux
   _AIRFLOW_WWW_USER_USERNAME=airflow
   _AIRFLOW_WWW_USER_PASSWORD=airflow

   AWS_ACCESS_KEY_ID=xxxx
   AWS_SECRET_ACCESS_KEY=xxxx
   AWS_DEFAULT_REGION=ap-southeast-2

   DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/dbname
   ```

3. Additional Airflow settings can live in `config/airflow.cfg`; any changes are picked up after restarting the containers.

## Getting Started

1. Build the custom image and initialise Airflow metadata:
   ```bash
   docker compose build
   docker compose up airflow-init
   ```
2. Bring the stack online:
   ```bash
   docker compose up -d
   ```
3. Visit `http://localhost:8080` and log in with the credentials defined in `.env`.
4. Enable the `process_primo_data` DAG from the Airflow UI or trigger it manually:
   ```bash
   docker compose run --rm airflow-cli dags trigger process_primo_data
   ```
5. Logs stream out under `logs/dag_id=process_primo_data/` and via the Airflow UI.

## DAG: `process_primo_data`

- **Schedule**: `9 0 * * *` (runs daily at 00:09 UTC unless overridden).
- **Inputs**: CSV exports stored under `prod/report/<YYYY>/<MM>/<DD>/...` in S3.
- **Outputs**:
  - `primo_memberships`
  - `primo_coupons`
  - `primo_member_tier_movement`
  - `primo_point_on_hand`
- **Behaviour**:
  - Normalises date columns with `dayfirst=True`.
  - Cleans ID-like strings to avoid null/empty values.
  - Appends to the first three tables; replaces the point-on-hand snapshot table.
  - Requires `DATABASE_URL` to be valid inside the worker container (e.g. use a VPC tunnel or exposed test DB).

## Development Tips

- Use `docker compose logs -f airflow-scheduler` (or `airflow-worker`) for real-time debugging.
- To add Python dependencies, update `dockerfile` and rebuild.
- Keep Selenium scripts headless-friendly; downloads land in `/downloads`.
- Add new DAGs under `dags/` and restart the `airflow-scheduler` service (or wait for the scheduler to pick them up automatically).

## Troubleshooting

- **Permissions on mounted volumes**: ensure `AIRFLOW_UID` matches your host UID (Linux). On macOS this default generally works.
- **AWS auth issues**: inject `AWS_*` variables via `.env` or mount an `~/.aws` credentials file using volume overrides.
- **Database SSL/TLS**: append the relevant query params in `DATABASE_URL` (e.g. `?sslmode=require`).
- **Chromium path**: workers expect `/usr/bin/chromium` and `/usr/bin/chromedriver`; adjust `dags/util/chrome.py` if your base image changes.

## Useful Commands

- `docker compose ps` – check container status.
- `docker compose down -v` – reset the stack and wipe volumes (be careful: removes Postgres data).
- `docker compose run --rm airflow-cli tasks test process_primo_data process_primo_member_point_on_hand <execution_date>`

## Next Steps

- Add integration tests that run DAG tasks against a disposable Postgres container.
- Introduce CI linting for DAG style and import errors.
- Define alerts using Airflow connections once deployed to production.
