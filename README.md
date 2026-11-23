# Agent Hub Work Item Manager (`agent-hub-work-items`)

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.121+-009688.svg)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg)](https://www.postgresql.org)
[![PgBouncer](https://img.shields.io/badge/PgBouncer-Transaction_Pool-brightgreen.svg)](https://www.pgbouncer.org)
[![High Throughput](https://img.shields.io/badge/Throughput-700+_req%2Fs-blueviolet.svg)](#high-throughput-benchmarks)

**`agent-hub-work-items`** is a high-throughput enterprise task queue and workflow coordinator compatible with **Roots Queue**. Designed for modern agentic automation and human-in-the-loop pipelines, it coordinates autonomous bots (RPA bots, AI agents, document extractors) and human verifiers with rigorous concurrency locks and transactional integrity.

---

## Key Features

- **Extreme Throughput & Concurrency**:
  - Benchmarked at **over 700 requests/second** (~2.5 million operations/hour) on standard commodity hardware.
  - Optimized for connection pooling with **PgBouncer** in transaction-pooling mode.
  - Statement and lock timeouts ensure fast failover and zero hung client connections.
- **Strict Concurrency & Safe Leases**:
  - Atomic bot claiming (`POST /items/lock_next`) using row-level locking (`FOR UPDATE SKIP LOCKED`).
  - Lease timeout monitoring (`stale_lock_minutes`): automatically marks abandoned locks for re-queueing without duplicate processing.
  - Automatic retry budgets (`max_attempts`) with dead-letter escalation.
- **Workflow & Exception State Machine**:
  - Full state lifecycle transitions: `queued` &rarr; `in_progress` &rarr; `completed` / `automated` / `business_exception` / `system_exception` / `manual_intervention` / `manually_closed`.
  - Item locking, heartbeats, and safe cooperative unlocks.
- **Advanced Search & Filtering**:
  - Supports both legacy search (`POST /items/searches`) and high-efficiency range pagination (`v2/searches`) using `startRow`/`endRow`.
  - Comprehensive filtering by queue, tenant, priority, date ranges, and custom JSON tags.
- **Lifecycle & Retention Engine**:
  - Built-in data retirement command (`python -m app.retention`) to cleanly sweep finished items older than the retention threshold (`RETENTION_DAYS: 180`).
- **Human-in-the-Loop Integration**:
  - Seamlessly links queue items to document reviews in `agent-hub-annotations`.

---

## State Transition Diagram

```
                 +-------------------+
                 |      QUEUED       |
                 +---------+---------+
                           |
             lock_next / claim (lease lock)
                           v
                 +-------------------+
       +-------->|    IN_PROGRESS    |<--------+
       |         +----+----+----+----+         |
       |              |    |    |              |
 unlock/retry         |    |    |          manual edit
       |              |    |    |              |
       |  +-----------+    |    +-----------+  |
       |  |                |                |  |
       v  v                v                v  v
+--------------+    +--------------+    +----------------------+
|  AUTOMATED / |    |    SYSTEM    |    | MANUAL_INTERVENTION  |
|  COMPLETED   |    |  EXCEPTION   |    | / BUSINESS_EXCEPTION |
+--------------+    +--------------+    +----------------------+
```

---

## API Endpoints Summary

### Work Item Queue Operations
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/items` | Create one or more new work items |
| `POST` | `/items/lock_next` | Claim and lease the next available prioritized item |
| `POST` | `/items/searches` | Search work items (filter by status, queue, date, priority) |
| `GET` | `/items/{id}` | Fetch full work item details and history |
| `PUT` | `/items/{id}/status` | Transition item status (complete, fail, or escalate) |
| `POST` | `/items/{id}/unlock` | Explicitly release lease and return item to queue |

### Queue Management
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/queues` | List all registered automation queues |
| `POST` | `/queues` | Create or update a work queue configuration |
| `GET` | `/queues/{id}/stats` | Real-time queue metrics (pending, active, failed counts) |

### Admin & Retention
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/admin/retire_items` | Trigger retention sweep to prune completed historical items |
| `GET` | `/health` | Health check probe (verifies database readiness) |
| `GET` | `/docs` | Interactive OpenAPI / Swagger documentation |

---

## High-Throughput Benchmarks

Tested on standard 8-core virtual machine against PostgreSQL 16 + PgBouncer:
- **8 worker processes** (`WEB_CONCURRENCY=8`): **~700 req/s (~2,520,000 req/hour)**
- **4 worker processes** (`WEB_CONCURRENCY=4`): **~320 req/s (~1,150,000 req/hour)**
- Latency (p95): `< 12ms` for lock_next and status transitions.

---

## Getting Started

### Prerequisites
- **Python 3.11+**
- (Optional) PostgreSQL 16 and PgBouncer

### 1. Local Setup (SQLite Dev)

```bash
# Clone the repository
git clone https://github.com/madalavt/agent-hub-work-items.git
cd agent-hub-work-items

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Copy environment template
cp .env.example .env

# Start service
uvicorn app.main:app --host 0.0.0.0 --port 8003 --reload
```

Visit API documentation at [http://localhost:8003/docs](http://localhost:8003/docs).

### 2. Docker Deployment

```bash
# Build Docker image
docker build -t agent-hub-work-items .

# Run container
docker run -d \
  --name agent-hub-work-items \
  -p 8003:8003 \
  -e DATABASE_URL="postgresql+psycopg://agent_hub:password@pgbouncer:6432/work_items" \
  -e API_KEY="your-secret-api-key" \
  agent-hub-work-items
```

### 3. Running Data Retention Sweeps

To execute an automated cleanup of archived items older than 180 days:

```bash
python -m app.retention
```

---

## Running Tests

```bash
# Execute pytest suite
pytest

# Code formatting and linting
ruff check .
```

---

## License

This project is licensed under the Apache 2.0 / MIT License.
