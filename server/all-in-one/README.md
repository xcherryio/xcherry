# Dex Server All-in-One

Dex Server and the Temporal Server fork run in one long-running process. The image supports PostgreSQL persistence and S3-compatible Dex blob storage.

## Components

| Component | Version |
| --- | --- |
| Dex Server | Same-commit server source |
| Temporal Server fork | `v0.0.2` |
| Temporal SDK fork | `v1.47.1-superdurable.3` |

The Go module builds against the server source in this monorepo.

## Start locally

From the repository root, build the image and start the included Compose topology with PostgreSQL, MinIO, and two all-in-one replicas:

```bash
docker build -f server/all-in-one/Dockerfile -t superdurable/dex-server-all-in-one:latest .
cd server/all-in-one
docker compose up -d
docker compose ps
```

You can also run `make docker-build` from `server/all-in-one`. The final image runs as UID/GID `10001` and includes a gRPC health check.

Dex gRPC is published on `localhost:8801` and `localhost:8802`. Temporal ports are reachable only on the Compose private network.

## Runtime configuration

PostgreSQL configuration follows Temporal's standard environment names:

| Variable | Default | Purpose |
| --- | --- | --- |
| `POSTGRES_SEEDS` | required | PostgreSQL host; exactly one host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` / `POSTGRES_PWD` | empty | PostgreSQL credentials |
| `DBNAME` | `temporal` | Temporal core database |
| `VISIBILITY_DBNAME` | `temporal_visibility` | Temporal visibility database |
| `POSTGRES_ADMIN_DB` | `postgres` | Database used for creation and advisory locks |
| `SKIP_DB_CREATE` | `false` | Skip database creation for managed PostgreSQL |
| `SKIP_SCHEMA_SETUP` | `false` | Skip Temporal schema setup and upgrades |
| `DB_INIT_TIMEOUT` | `2m` | Database initialization deadline |

Only `postgres12` and `postgres12_pgx` are accepted for `DB`. Schema creation and upgrades are serialized across replicas with a PostgreSQL advisory lock. The database user needs `CREATEDB` unless `SKIP_DB_CREATE=true`.

Dex requires S3-compatible storage in the default environment-driven configuration:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DEX_S3_ENDPOINT` | required | S3 API endpoint, including scheme |
| `DEX_S3_BUCKET` | required | Blob bucket |
| `DEX_S3_REGION` | `us-east-1` | S3 region |
| `DEX_S3_ACCESS_KEY` / `DEX_S3_SECRET_KEY` | required | Static S3 credentials |
| `DEX_NAMESPACE` | `default` | Shared Temporal namespace |
| `DEX_HISTORY_RETENTION_DAYS` | `3` | Namespace and blob retention |
| `DEX_API_PORT` | `8801` | Public Dex plaintext gRPC port |

Set `DEX_CONFIG_FILE` to use a full Dex YAML configuration instead. The all-in-one process still forces the Temporal interpreter to its local frontend and uses `DEX_NAMESPACE` as an optional namespace override.

## HA networking

Every replica runs Temporal frontend, history, matching, and worker services. Set:

```text
BIND_ON_IP=0.0.0.0
TEMPORAL_BROADCAST_ADDRESS=<unique pod IP>
TEMPORAL_CLUSTER_FRONTEND_ADDRESS=<private frontend service DNS>:7233
```

If `TEMPORAL_BROADCAST_ADDRESS` is omitted, the process selects the first private IPv4 address. Set it explicitly in Kubernetes with the downward API.

Allow these ports only between Temporal members:

| Service | gRPC | Membership |
| --- | ---: | ---: |
| Frontend | 7233 | 6933 |
| History | 7234 | 6934 |
| Matching | 7235 | 6935 |
| Worker | 7239 | 6939 |

Dex connects to its co-located frontend at `127.0.0.1:7233`. Only Dex port 8801 should be exposed outside the private cluster network.
