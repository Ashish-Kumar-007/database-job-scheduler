# Database-Backed Job Scheduler

This repository currently contains the project foundation only: validated
PostgreSQL connection settings, a SQLAlchemy engine/session factory, a database
health check, and a local PostgreSQL Compose service. Domain models and
migrations are intentionally deferred until the foundation is reviewed.

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

Stop PostgreSQL with `docker compose down`. Its named volume preserves data;
`docker compose down --volumes` removes the local database volume.

## Tests

```powershell
python -m pytest
```

There are no database models, migrations, integration tests, worker processes,
or job execution behavior in this foundation phase.
