# NATS JetStream Payment Events — Hands-On

A small **event-driven architecture** demo built with [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). A Django app publishes payment events; Go workers consume them from a durable JetStream consumer and acknowledge processing.

Use this repo as a learning and installation reference for NATS messaging, streams, consumers, and pub/sub across Python and Go.

---

## What is NATS?

[NATS](https://nats.io/) is a lightweight, high-performance messaging system for connecting services. Think of it as a central message bus: one service publishes a message on a **subject** (like a channel name), and other services subscribe to that subject to receive it.

### Why do you need a message broker?

In a monolith, one function calls another directly. In microservices or event-driven systems, services often need to:

- **Decouple** — the publisher should not know or wait for every downstream service
- **Scale independently** — add more workers without changing the API
- **Survive failures** — if a worker is down, messages should not be lost
- **React to events** — "payment created" can trigger email, analytics, fraud checks, etc.

A message broker sits in the middle and handles delivery, routing, and (with persistence) storage.

### NATS vs Kafka vs RabbitMQ (short comparison)

| | **NATS** | **Apache Kafka** | **RabbitMQ** |
|---|----------|------------------|--------------|
| **Primary model** | Pub/sub subjects; JetStream adds streams | Distributed commit log (topics/partitions) | Queues & exchanges (AMQP) |
| **Complexity** | Low — single binary, minimal setup | High — ZooKeeper/KRaft, brokers, partitions | Medium — Erlang broker, vhosts, exchanges |
| **Latency** | Very low (microseconds) | Higher (ms range, batch-oriented) | Low to medium |
| **Persistence** | JetStream (optional, built-in) | Always (log-based) | Queues with durability flags |
| **Best for** | Real-time signals, IoT, service mesh, lightweight events | High-volume event streaming, analytics pipelines | Task queues, routing patterns, enterprise messaging |
| **Ops footprint** | Small Docker container | Cluster of brokers + coordination | Broker + management plugin |

None replaces the others entirely — the choice depends on volume, latency, team familiarity, and how much infrastructure you want to operate.

### When NATS is a good fit

Choose NATS (especially with JetStream) when you want:

- **Simple setup** — one container, no heavy cluster for learning or small/medium workloads
- **Fast pub/sub** — notifications, live updates, command/control between services
- **Durable events without Kafka-scale ops** — JetStream gives streams, consumers, and ACKs in the same server
- **Polyglot services** — official clients for Go, Python, Node, Java, etc.
- **Cloud-native / edge** — small memory footprint, easy to run everywhere

This project is a typical JetStream use case: Django publishes a business event; a Go worker processes it reliably with ACK-based delivery.

### JetStream in this project

This repo uses **JetStream**, NATS's built-in persistence layer. Unlike plain NATS (fire-and-forget), JetStream:

- **Stores messages** in a **stream** so they are not lost if a consumer is offline
- Supports **durable consumers** that remember where they left off
- Requires **explicit acknowledgements (ACK)** so unprocessed messages can be redelivered

| Concept | In this project |
|---------|-----------------|
| **Subject** | `payment.created` — the event topic |
| **Stream** | `PAYMENT_EVENTS` — stores all messages on `payment.created` |
| **Consumer** | `payment-worker` — delivers stored messages to the Go worker |
| **Publisher** | Django app (`nat_publisher`) |
| **Consumer app** | Go app (`nat_consumer`) |

**Official docs:** [NATS Concepts](https://docs.nats.io/nats-concepts/what-is-nats) · [JetStream](https://docs.nats.io/nats-concepts/jetstream) · [Streams](https://docs.nats.io/nats-concepts/jetstream/streams) · [Consumers](https://docs.nats.io/nats-concepts/jetstream/consumers)

---

## Architecture

```
┌─────────────────────┐   payment.created    ┌────────────────────────────┐
│  nat_publisher      │ ───────────────────► │  NATS JetStream (Docker)   │
│  Django (Python)    │   JetStream publish  │  Stream: PAYMENT_EVENTS    │
│  :8000              │                      │  nats://localhost:14222  │
└─────────────────────┘                      └─────────────┬──────────────┘
                                                         │
                         ┌───────────────────────────────┼───────────────────────────────┐
                         │ pull / deliver                │ browse (Web UI)               │
                         ▼                               ▼                               │
               ┌─────────────────────┐         ┌─────────────────────┐                   │
               │  nat_consumer       │         │  NUI (Docker)       │                   │
               │  Go + Fiber         │         │  :31311               │                   │
               │  payment-worker     │         │  streams · consumers  │                   │
               │  :8080 / :8081      │         │  · messages           │                   │
               └─────────────────────┘         └─────────────────────┘                   │
```

**Message flow**

1. Client sends `POST /payments/` to Django with payment JSON.
2. Django publishes the payload to subject `payment.created` via JetStream.
3. JetStream stores the message in stream `PAYMENT_EVENTS`.
4. The Go consumer reads messages from durable consumer `payment-worker`.
5. The consumer logs the event and sends an ACK (in `main.go`; see below for `main_2.go`).

---

## Project structure

```
.
├── docker-compose.yaml      # NATS server + NUI (web UI)
├── nat_publisher/           # Django publisher (Python)
│   ├── publisher/           # Django project settings & URLs
│   ├── payments/
│   │   ├── views.py         # POST /payments/ endpoint
│   │   └── services/nats.py # NATS JetStream client
│   ├── requirements.txt
│   └── manage.py
└── nat_consumer/            # Go consumer worker
    ├── main.go              # Consumer with ACK on :8080
    ├── main_2.go            # Same consumer, ACK disabled, on :8081
    └── go.mod
```

---

## Prerequisites

| Tool | Version / notes |
|------|-----------------|
| [Docker](https://docs.docker.com/get-docker/) & Docker Compose | Runs NATS + NUI |
| [NATS CLI](https://docs.nats.io/using-nats/nats-tools/nats_cli) | Create streams & consumers |
| [Python](https://www.python.org/) 3.12+ | Django publisher |
| [Go](https://go.dev/) 1.25+ | Consumer worker |

---

## 1. Start NATS (Docker)

From the project root:

```bash
docker compose up -d
```

This starts two services:

| Service | Purpose | URL / port |
|---------|---------|------------|
| **nats** | NATS server with JetStream enabled | Client: `nats://localhost:14222` · Monitoring: `http://localhost:8222` |
| **nui** | Web UI to inspect NATS & JetStream | `http://localhost:31311` |

JetStream is enabled via `-js` and data is persisted in a Docker volume (`nats_data`).

Verify NATS is running:

```bash
curl http://localhost:8222/healthz
# Expected: ok
```

**Docs:** [Running NATS with Docker](https://docs.nats.io/running-a-nats-service/nats_docker)

### NUI — NATS Web UI

[NUI](https://github.com/nats-nui/nui) is included in `docker-compose.yaml` so you can **see** what the CLI configures instead of relying only on terminal output. It is useful while learning: browse streams, consumers, pending messages, and server health in a browser.

**Open NUI:** [http://localhost:31311](http://localhost:31311)

**Add a NATS connection** (first time only):

1. Open NUI in your browser.
2. Add a new connection / context.
3. Use the Docker **internal** service URL (NUI runs in the same network as NATS):

   | Field | Value |
   |-------|-------|
   | Name | `local` (or any label) |
   | Server URL | `nats://nats:4222` |

   > NUI connects from inside Docker, so use hostname `nats` and port `4222` — not `localhost:14222`. Your Django/Go apps run on the host and use `localhost:14222` instead.

**What you can do in NUI:**

| Area | Useful for |
|------|------------|
| **Connections** | Manage one or more NATS servers |
| **Streams** | View `PAYMENT_EVENTS`, message count, subjects, storage |
| **Consumers** | Inspect `payment-worker` — pending msgs, redelivery, ACK state |
| **Messages** | Read payloads published to `payment.created` |
| **Server info** | Confirm JetStream is enabled, see memory/disk usage |

**Typical learning workflow:** create stream/consumer via CLI (steps 3–4) → publish a payment via curl → open NUI → confirm the message appears under `PAYMENT_EVENTS` → watch pending count drop after the Go consumer ACKs.

**Docs:** [NUI on GitHub](https://github.com/nats-nui/nui)

---

## 2. Install & configure NATS CLI

The NATS CLI talks to your server and is used to create streams and consumers before the apps connect. NUI is great for **inspecting** JetStream; the CLI is still the easiest way to **create** streams and consumers from this README.

### Check if NATS CLI is already installed

Before installing, check whether `nats` is on your PATH:

```bash
nats --version
```

If installed, you will see output like `0.1.x` or similar. You can also run:

```bash
which nats
```

If both commands fail (`command not found`), install the CLI using the steps below. If already installed, skip to [Configure a local context](#configure-a-local-context).

### Install

**macOS (Homebrew):**

```bash
brew install nats-io/nats-tools/nats
```

**Other platforms:** [NATS CLI installation guide](https://docs.nats.io/using-nats/nats-tools/nats_cli#installation)

After install, confirm again:

```bash
nats --version
```

### Configure a local context

Point the CLI at the Docker NATS port exposed on your host (`14222`, not the default `4222`):

```bash
nats context add local --server nats://localhost:14222
nats context select local
```

Verify connection:

```bash
nats server check connection
nats account info
```

You should see JetStream enabled in the account info output.

**Docs:** [NATS CLI contexts](https://docs.nats.io/using-nats/nats-tools/nats_cli#nats-contexts)

---

## 3. Create the stream

A **stream** captures and stores messages for one or more subjects. The publisher sends to `payment.created`; the stream must listen on that subject.

```bash
nats stream add PAYMENT_EVENTS \
  --subjects "payment.created" \
  --storage file \
  --retention limits \
  --ack
```

| Flag | Meaning |
|------|---------|
| `--subjects "payment.created"` | Only messages on this subject are stored |
| `--storage file` | Persist messages on disk (matches Docker `-sd /data`) |
| `--retention limits` | Keep messages until limits (count/age/size) are hit |
| `--ack` | Stream expects publishers/consumers to use acknowledgements |

Verify:

```bash
nats stream info PAYMENT_EVENTS
nats stream ls
```

**Docs:** [Stream management (CLI)](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/stream) · [Stream configuration](https://docs.nats.io/nats-concepts/jetstream/streams)

> **Important:** If you skip this step, Django will fail when publishing with a timeout or "no response from stream" error, because JetStream has nowhere to store `payment.created` messages.

---

## 4. Create the consumer

A **consumer** is a view into a stream for a specific worker or service. This project uses a **durable** consumer named `payment-worker` so the Go app can disconnect and resume without losing its place.

```bash
nats consumer add PAYMENT_EVENTS payment-worker \
  --filter "payment.created" \
  --ack explicit \
  --deliver all \
  --max-deliver 5 \
  --ack-wait 30s
```

| Flag | Meaning |
|------|---------|
| `--filter "payment.created"` | Only deliver messages on this subject |
| `--ack explicit` | Consumer must call ACK after successful processing |
| `--deliver all` | Deliver all stored messages (including after restart) |
| `--max-deliver 5` | Redeliver up to 5 times if not ACK'd |
| `--ack-wait 30s` | Wait 30 seconds for ACK before redelivery |

Verify:

```bash
nats consumer info PAYMENT_EVENTS payment-worker
nats consumer ls PAYMENT_EVENTS
```

These settings match the commented `jetstream.ConsumerConfig` in `nat_consumer/main.go`.

**Docs:** [Consumer management (CLI)](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/consumers) · [Consumer configuration](https://docs.nats.io/nats-concepts/jetstream/consumers)

---

## 5. Run the publisher (Django)

```bash
cd nat_publisher

python3 -m venv env
source env/bin/activate        # Windows: env\Scripts\activate

pip install -r requirements.txt
python manage.py migrate
python manage.py runserver 8000
```

NATS URL is configured in `publisher/settings.py`:

```python
NATS_URL = "nats://localhost:14222"
```

### Publish a payment event

```bash
curl -X POST http://localhost:8000/payments/ \
  -H "Content-Type: application/json" \
  -d '{
    "payment_id": "PAY-001",
    "amount": 45000,
    "currency": "USD"
  }'
```

**Expected response:**

```json
{
  "status": "published",
  "payment": {
    "payment_id": "PAY-001",
    "amount": 45000,
    "currency": "USD"
  }
}
```

**Event schema** (JSON published to NATS):

| Field | Type | Example |
|-------|------|---------|
| `payment_id` | string | `"PAY-001"` |
| `amount` | integer | `45000` |
| `currency` | string | `"USD"` |

**Docs:** [nats-py](https://github.com/nats-io/nats.py) · [Django](https://docs.djangoproject.com/)

---

## 6. Run the consumer (Go)

Open a new terminal:

```bash
cd nat_consumer
go mod download
go run main.go
```

The consumer will:

1. Connect to `nats://localhost:14222`
2. Attach to stream `PAYMENT_EVENTS`
3. Use durable consumer `payment-worker`
4. Log each payment and send an ACK
5. Expose a health check at `http://localhost:8080/`

**Expected logs after publishing:**

```
Connected to NATS
Found stream: PAYMENT_EVENTS
Consumer ready: payment-worker
Payment created: id=PAY-001 amount=45000 currency=USD
ACK sent for payment: PAY-001
```

### `main.go` vs `main_2.go`

| File | Port | ACK behaviour | Use case |
|------|------|---------------|----------|
| `main.go` | `:8080` | Calls `msg.Ack()` | Normal processing — message removed from pending queue |
| `main_2.go` | `:8081` | ACK commented out | Experiment with redelivery — messages stay pending and retry after `ack-wait` |

Run the second consumer (optional, for testing redelivery):

```bash
go run main_2.go
```

**Docs:** [nats.go](https://github.com/nats-io/nats.go) · [JetStream Go API](https://github.com/nats-io/nats.go/jetstream)

---

## End-to-end test checklist

Run services in this order:

1. `docker compose up -d`
2. Create stream & consumer with NATS CLI (steps 3–4) — **only needed once**
3. `python manage.py runserver 8000` (publisher)
4. `go run main.go` (consumer)
5. `curl` POST to `/payments/`
6. Open [NUI](http://localhost:31311) — verify the message under stream `PAYMENT_EVENTS` and that consumer `payment-worker` pending count decreases after ACK
7. Confirm logs in the Go terminal

### Useful NATS CLI commands while learning

```bash
# Watch messages on the subject (without consuming permanently)
nats sub payment.created

# Inspect stream stats
nats stream report

# See pending / unacknowledged messages for the consumer
nats consumer info PAYMENT_EVENTS payment-worker

# Manually publish a test message
nats pub payment.created '{"payment_id":"TEST-1","amount":100,"currency":"USD"}'
```

**Docs:** [NATS CLI command reference](https://docs.nats.io/using-nats/nats-tools/nats_cli)

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---------|--------------|-----|
| `nats: timeout` when publishing | Stream `PAYMENT_EVENTS` does not exist or does not capture `payment.created` | Run `nats stream add` (step 3) |
| Consumer fails: `consumer not found` | Consumer `payment-worker` not created | Run `nats consumer add` (step 4) |
| Messages published but not consumed | Consumer not running, or wrong filter subject | Start `go run main.go`; verify `--filter "payment.created"` |
| Same message delivered repeatedly | ACK disabled (`main_2.go`) or ACK failing | Use `main.go` with `msg.Ack()` enabled |
| Connection refused on `:14222` | Docker NATS not running | `docker compose up -d` |
| NUI cannot connect to NATS | Wrong server URL in NUI | Use `nats://nats:4222` (Docker internal), not `localhost:14222` |
| NUI page not loading | NUI container not running | `docker compose ps` · `docker compose up -d nui` |
| Django 500 on first publish after restart | NATS up but stream missing | Recreate stream (data persists in volume if stream already existed) |

---

## Configuration reference

| Setting | Value |
|---------|-------|
| NATS URL | `nats://localhost:14222` |
| Stream name | `PAYMENT_EVENTS` |
| Subject | `payment.created` |
| Consumer name | `payment-worker` |
| Publisher endpoint | `POST http://localhost:8000/payments/` |
| Consumer health | `GET http://localhost:8080/` (main.go) |
| NATS monitoring | `http://localhost:8222` |
| NUI (Web UI) | `http://localhost:31311` |
| NUI → NATS connection URL | `nats://nats:4222` (from inside Docker network) |

---

## Further reading

| Topic | Link |
|-------|------|
| NATS documentation home | https://docs.nats.io/ |
| JetStream overview | https://docs.nats.io/nats-concepts/jetstream |
| Core pub/sub vs JetStream | https://docs.nats.io/nats-concepts/core-nats |
| NATS CLI | https://docs.nats.io/using-nats/nats-tools/nats_cli |
| JetStream admin (streams/consumers) | https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin |
| Python client (nats-py) | https://github.com/nats-io/nats.py |
| Go client (nats.go) | https://github.com/nats-io/nats.go |
| JetStream Go package | https://pkg.go.dev/github.com/nats-io/nats.go/jetstream |
| Django | https://docs.djangoproject.com/ |
| Fiber (Go HTTP) | https://gofiber.io/ |
| NUI (NATS Web UI) | https://github.com/nats-nui/nui |

---

## License

This is a hands-on learning project. Use and modify it freely for practice and experimentation.
