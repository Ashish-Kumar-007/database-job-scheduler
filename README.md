# Database-Backed Job Scheduler

This repository currently provides validated PostgreSQL connection settings,
SQLAlchemy database utilities, a health check, the initial `Organization` and
`Job` models, and their Alembic migration. Consumers, claim/recovery logic, and
job execution are not implemented yet.

## Requirements

- Python 3.12 or later
- Docker Engine with Docker Compose

## Start PostgreSQL

From the project directory in PowerShell:

```powershell
Copy-Item .env.example .env
docker compose up -d postgres
docker compose ps
```

The Compose service uses PostgreSQL 16. The sample credentials are for local
development only. To select another host port, set `POSTGRES_HOST_PORT` in
`.env`; update the port in `DATABASE_URL` to match.

## Install and check the database

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
$env:DATABASE_URL = (Get-Content .env | Where-Object { $_ -match '^DATABASE_URL=' } | Select-Object -First 1) -replace '^DATABASE_URL=', ''
python -m app
```

The health check executes `SELECT 1`. Configuration requires a SQLAlchemy
PostgreSQL psycopg URL with host, database, username, and password. Credentials
are parsed as a URL rather than interpolated into a connection string.

## Database schema

Apply the initial schema migration after PostgreSQL is healthy:

```powershell
python -m alembic upgrade head
```

The schema includes tenant/job validation constraints and partial indexes for
scheduled jobs, retry eligibility, and expired leases. `attempts` counts
committed claims, including claims later recovered after lease expiry; waiting
jobs must have an attempt remaining. Timestamps are stored as timezone-aware
PostgreSQL timestamps.

Stop PostgreSQL with `docker compose down`. Its named volume preserves data;
`docker compose down --volumes` removes the local database volume.

## Tests

```powershell
python -m pytest
```

The migration can be rendered without a live database for review:

```powershell
python -m alembic upgrade head --sql
```

PostgreSQL integration tests, worker processes, atomic claiming, retries, and
recovery will follow in later implementation phases.
