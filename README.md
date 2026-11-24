# TaskFlow

**TaskFlow** is a distributed, fault-tolerant background job processing
system built with Node.js, TypeScript, PostgreSQL, and RabbitMQ.

It separates job creation and durable state management from asynchronous
execution. PostgreSQL stores the authoritative job and execution
history, while RabbitMQ transports work to independent workers. The
system is designed around reliable delivery, controlled retries, worker
health monitoring, lease recovery, and safe handling of infrastructure
failures.

> **Project status:** Core API, dispatcher, worker, database, and
> messaging workflows have been developed and tested incrementally. This
> README documents the functionality and test outcomes verified during
> development so far. It is not a claim that every production-hardening
> concern has been completed.

------------------------------------------------------------------------

## Contents

-   [Highlights](#highlights)
-   [Technology stack](#technology-stack)
-   [Repository structure](#repository-structure)
-   [System architecture](#system-architecture)
-   [Job lifecycle](#job-lifecycle)
-   [Reliability and failure
    handling](#reliability-and-failure-handling)
-   [Database design](#database-design)
-   [RabbitMQ topology](#rabbitmq-topology)
-   [Applications and packages](#applications-and-packages)
-   [Local development setup](#local-development-setup)
-   [Environment configuration](#environment-configuration)
-   [Running TaskFlow](#running-taskflow)
-   [Database migrations](#database-migrations)
-   [Testing and verification](#testing-and-verification)
-   [Operational checks](#operational-checks)
-   [Current scope and future
    improvements](#current-scope-and-future-improvements)

------------------------------------------------------------------------

## Highlights

-   **Distributed workers:** multiple worker processes can execute jobs
    independently.
-   **Persistent job state:** PostgreSQL stores job status, attempts,
    execution history, events, worker records, and outbox events.
-   **Asynchronous messaging:** RabbitMQ transports jobs from the
    dispatcher to workers.
-   **Publisher confirms:** retry publication uses a RabbitMQ confirm
    channel and waits for broker confirmation before acknowledging the
    original delivery.
-   **Delayed retries:** infrastructure retries use a TTL-based retry
    queue and dead-letter routing back to the main queue.
-   **Bounded retries:** infrastructure retry attempts are limited;
    exhausted messages are routed to a dead-letter queue.
-   **Job-level retry scheduling:** failed job executions can be
    rescheduled using exponential backoff, with a configured cap.
-   **Worker heartbeats:** workers periodically update their liveness
    information.
-   **Lease-based execution:** a worker claims a job for a limited
    period rather than holding an indefinite lock.
-   **Lease recovery:** a recovery service checks for jobs whose
    execution lease has expired.
-   **Graceful shutdown:** workers stop receiving new messages, wait for
    in-flight work, stop background services, update their database
    status, and close resources.
-   **Monorepo organization:** applications and reusable packages are
    separated into npm workspaces.

## Technology stack

  -----------------------------------------------------------------------
  Technology              Role in TaskFlow        Why it is used
  ----------------------- ----------------------- -----------------------
  Node.js                 Runtime for the API,    Event-driven I/O and a
                          dispatcher, and workers shared JavaScript
                                                  runtime across services

  TypeScript              Application and package Static typing, clearer
                          language                contracts, and
                                                  maintainable service
                                                  boundaries

  Fastify                 HTTP API framework      Lightweight, performant
                                                  HTTP server with
                                                  schema-oriented
                                                  capabilities

  PostgreSQL              Primary persistent      Transactional job
                          database                state, relational
                                                  integrity, execution
                                                  history, and durable
                                                  records

  RabbitMQ 4              Message broker          Decouples job dispatch
                                                  from execution and
                                                  supports
                                                  acknowledgements,
                                                  confirms, TTL, and
                                                  dead-letter routing

  Docker                  Local infrastructure    Reproducible PostgreSQL
                                                  and RabbitMQ
                                                  development services

  npm workspaces          Monorepo package        Keeps applications and
                          management              shared packages in one
                                                  repository with
                                                  explicit dependencies

  `pg`                    PostgreSQL client       Direct SQL access and
                                                  transaction handling

  `node-pg-migrate`       Database migrations     Versioned, repeatable
                                                  schema changes

  `amqplib`               RabbitMQ client         AMQP connections,
                                                  channels, publishing,
                                                  and consuming

  `tsx`                   TypeScript development  Runs TypeScript entry
                          runner                  points during
                                                  development
  -----------------------------------------------------------------------

### Why this combination?

TaskFlow uses PostgreSQL and RabbitMQ for different responsibilities:

-   PostgreSQL is the durable source of truth for jobs, attempts,
    workers, and event records.
-   RabbitMQ is the asynchronous transport layer that distributes work
    to workers.
-   The API accepts and manages job requests.
-   The dispatcher bridges persisted work/outbox records to RabbitMQ.
-   Workers consume messages and execute registered handlers.

This separation avoids making the message broker the only record of a
job's state and allows execution capacity to scale independently from
the API.

------------------------------------------------------------------------

## Repository structure

The repository is an npm-workspaces monorepo.

``` text
TaskFlow/
├── apps/
│   ├── api/                 # Fastify HTTP API
│   ├── dispatcher/          # Publishes queued/outbox work to RabbitMQ
│   └── worker/              # Consumes and executes jobs
│
├── packages/
│   ├── config/              # Shared configuration helpers
│   ├── database/             # PostgreSQL pool, repositories and migrations
│   ├── messaging/            # RabbitMQ connection, queues, publisher and consumer
│   └── shared/               # Shared types, constants and utilities
│
├── .env                     # Local environment configuration (not committed)
├── package.json             # Workspace configuration and root scripts
└── README.md
```

The exact source files may evolve as the project develops. The structure
above describes the current application and package boundaries.

------------------------------------------------------------------------

## System architecture

``` mermaid
flowchart LR
    Client[Client] --> API[Fastify API]
    API --> DB[(PostgreSQL)]

    DB --> OUTBOX[Outbox events]
    OUTBOX --> Dispatcher[Dispatcher]
    Dispatcher --> Broker[(RabbitMQ)]

    Broker --> Worker1[Worker process 1]
    Broker --> Worker2[Worker process 2]
    Broker --> WorkerN[Worker process N]

    Worker1 --> DB
    Worker2 --> DB
    WorkerN --> DB

    Worker1 --> Handler[Registered job handlers]
    Worker2 --> Handler
    WorkerN --> Handler

    Worker1 -. heartbeat / lease .-> DB
    Worker2 -. heartbeat / lease .-> DB
    WorkerN -. heartbeat / lease .-> DB
```

### Main components

#### 1. API --- `apps/api`

The API is the HTTP entry point to TaskFlow.

Responsibilities include:

-   accepting job-related HTTP requests;
-   exposing a health endpoint;
-   validating request payloads;
-   persisting and managing job state through the database layer;
-   supporting a manual retry workflow for eligible jobs.

A verified health endpoint is:

``` http
GET /health
```

It returns a response in this shape:

``` json
{
  "status": "ok",
  "service": "taskflow-api"
}
```

The root path `/` is not configured as a route in the current API and
therefore returns Fastify's normal 404 response.

#### 2. Dispatcher --- `apps/dispatcher`

The dispatcher connects persisted work to RabbitMQ.

Its verified startup behavior includes:

-   loading configuration;
-   connecting to RabbitMQ;
-   preparing the job queue;
-   logging its dispatcher ID;
-   remaining active as a separate process.

The database includes `outbox_events` so publication intent can be
persisted alongside database changes. This is the foundation for
reliable database-to-broker handoff: the database transaction records
the event, and the dispatcher is responsible for publishing pending
work.

#### 3. Worker --- `apps/worker`

A worker is an independent process that receives job messages and
executes the matching registered handler.

Current verified worker behavior includes:

-   registering the worker in PostgreSQL;
-   registering available job handlers;
-   starting a heartbeat service;
-   starting a lease-recovery service;
-   consuming from the RabbitMQ job queue;
-   claiming and executing jobs;
-   handling execution and infrastructure failures;
-   stopping gracefully when requested or when a fatal messaging failure
    occurs.

The worker has been observed starting with:

-   registered handler: `send_email`;
-   default concurrency: `5`;
-   heartbeat interval: `10,000 ms`;
-   lease-recovery interval: `10,000 ms`.

These are observed development settings; check the current worker
configuration before relying on them as deployment defaults.

#### 4. Shared packages

  -----------------------------------------------------------------------
  Package                             Responsibility
  ----------------------------------- -----------------------------------
  `@taskflow/config`                  Shared environment/configuration
                                      helpers

  `@taskflow/database`                PostgreSQL connection pool,
                                      database access, migrations

  `@taskflow/messaging`               RabbitMQ connection/channel setup,
                                      queue assertions, publishing,
                                      consuming, retry publishing

  `@taskflow/shared`                  Shared types, constants, and
                                      cross-application utilities
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Job lifecycle

A typical job follows this path:

1.  A client submits a job request to the API.
2.  The API validates the request and persists the job in PostgreSQL.
3.  The corresponding publication intent is recorded for dispatcher
    processing.
4.  The dispatcher publishes the job to RabbitMQ.
5.  A worker receives the message.
6.  The worker claims the job in PostgreSQL and creates/updates
    execution state.
7.  The worker invokes the registered handler for the job type.
8.  The result is persisted as a job status and event/attempt history.
9.  The worker acknowledges the RabbitMQ delivery only when the
    message-processing path has completed safely.

``` mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant D as PostgreSQL
    participant X as Dispatcher
    participant R as RabbitMQ
    participant W as Worker
    participant H as Job handler

    C->>A: Submit job
    A->>D: Persist job and publication intent
    X->>D: Read pending outbox work
    X->>R: Publish job
    W->>R: Consume delivery
    W->>D: Claim job and create attempt
    W->>H: Execute handler
    H-->>W: Execution result
    W->>D: Persist result and events
    W->>R: Acknowledge delivery
```

The exact HTTP route names and request/response schemas should be taken
from the current API route files; this README intentionally documents
only the verified `/health` route rather than guessing endpoint paths.

------------------------------------------------------------------------

## Reliability and failure handling

### 1. Database-backed job state

RabbitMQ messages are not treated as the only source of truth.
PostgreSQL stores job state and execution history so the system can
inspect, recover, and manage work independently of an individual
delivery.

### 2. Job attempts and exponential backoff

A job execution can fail and be scheduled for another attempt. The
worker's job repository handles failure state transactionally.

The implemented retry scheduling uses exponential backoff:

``` text
delay = min(base_delay * 2^(attempt - 1), maximum_delay)
```

The backoff was implemented with a cap of **300 seconds**. The observed
schedule for the initial attempts was 1, 2, 4, 8 seconds, and so on, up
to the cap.

When the configured job attempt limit is exhausted, the job is marked
dead/failed rather than being scheduled indefinitely.

### 3. Infrastructure retries in RabbitMQ

Infrastructure failures are treated separately from ordinary handler/job
failures.

The messaging package uses:

-   a dedicated retry exchange/queue;
-   a TTL delay of **5 seconds** in the retry queue;
-   dead-letter routing from the retry queue back to the main job
    exchange/queue;
-   a retry-count message header;
-   a maximum of **5 infrastructure retries**;
-   a dead-letter queue for messages that exhaust the retry limit.

The retry publisher uses a confirm channel. The original message is
acknowledged only after the retry publication is confirmed by RabbitMQ.

### 4. Retry publication failure

A particularly important failure case is when a worker cannot publish a
retry message.

The implemented behavior is:

1.  The worker attempts to publish the retry using the retry publisher
    channel.
2.  If publishing fails, the original delivery is left unacknowledged.
3.  A fatal-error callback tells the worker process to shut down.
4.  The worker stops accepting new deliveries and waits for in-flight
    processing.
5.  It stops lease recovery and heartbeat services.
6.  It marks its worker record as `stopped`.
7.  It closes its consumer channel and RabbitMQ connection.
8.  RabbitMQ requeues the unacknowledged original message.

This prevents the original job from being silently lost merely because
the retry publisher is unavailable.

### 5. Worker heartbeat and lease recovery

Workers periodically update their heartbeat in PostgreSQL. Job execution
uses a lease so that ownership can expire if a worker disappears.

The lease-recovery service checks for expired running leases. The
recovery path is designed to reconcile abandoned attempts and schedule
eligible jobs for another execution, while recording the corresponding
events/outbox work.

### 6. Graceful shutdown

The worker shutdown sequence is designed to:

1.  stop receiving new messages;
2.  wait for in-flight work, subject to a shutdown timeout;
3.  stop lease recovery;
4.  stop heartbeat updates;
5.  mark the worker as stopped;
6.  close the consumer channel;
7.  close the retry publisher channel;
8.  close the RabbitMQ connection;
9.  close the PostgreSQL pool;
10. log completion.

If the retry publisher channel has already been closed by the broker or
failed, attempting to close it again can produce a warning. The worker's
verified shutdown still completed and marked the worker stopped in that
scenario.

------------------------------------------------------------------------

## Database design

TaskFlow uses PostgreSQL as its primary database. The core migration
created the following tables:

  -----------------------------------------------------------------------
  Table                   Purpose                 Main logical connection
  ----------------------- ----------------------- -----------------------
  `workers`               Registered worker       A worker can own
                          processes and their     running jobs through
                          liveness/status         `jobs.locked_by`

  `jobs`                  Authoritative job       Parent record for
                          record, payload,        execution attempts and
                          scheduling, status,     job events
                          attempts, and lease     
                          information             

  `job_attempts`          History of individual   Belongs to a job
                          execution attempts      

  `job_events`            Append-style history of Belongs to a job
                          important job lifecycle 
                          transitions             

  `outbox_events`         Durable records of      Connects database state
                          events/work that must   changes to
                          be published externally dispatcher/broker
                                                  publication
  -----------------------------------------------------------------------

### Logical relationship diagram

``` mermaid
erDiagram
    WORKERS ||--o{ JOBS : "may own / lock"
    JOBS ||--o{ JOB_ATTEMPTS : "has attempts"
    JOBS ||--o{ JOB_EVENTS : "has events"
    JOBS ||--o{ OUTBOX_EVENTS : "may produce publication events"
```

This diagram represents the logical relationships used by the
application. Consult the migration SQL for the exact foreign-key
constraints and full column definitions.

### Important job fields observed during development

The `jobs` table includes fields used for:

-   job identity: `job_id`;
-   handler selection: `job_type`;
-   execution input: payload;
-   lifecycle: `status`;
-   priority: `priority`;
-   scheduled execution: `run_at`;
-   attempt accounting: `attempts`, `total_attempts`, `max_attempts`;
-   idempotency: `idempotency_key`;
-   worker ownership: `locked_by`;
-   lease management: `lease_expires_at`;
-   error tracking: `last_error`;
-   timestamps such as `created_at` and update timestamps.

The `workers` table includes a worker identifier and status; heartbeat
and update information are used by worker liveness/recovery logic.

`job_attempts` records attempt outcomes, including statuses observed
during development such as `running`, `abandoned`, and `failed`.

`outbox_events` stores pending/processing publication work and is used
to decouple database transactions from message publication.

### Indexes created by the core migration

The migration created indexes covering:

-   job status;
-   job `run_at`;
-   job priority;
-   job idempotency key;
-   running-job lease lookup;
-   pending/processing outbox events;
-   worker status and heartbeat lookup.

These indexes support common scheduler, claim, idempotency, recovery,
and dispatcher queries.

### Database migrations

The core migration created:

-   `workers`
-   `jobs`
-   `job_attempts`
-   `job_events`
-   `outbox_events`

The migration was applied successfully using `node-pg-migrate`. The
migration tracking table is named `taskflow_migrations`.

------------------------------------------------------------------------

## RabbitMQ topology

The messaging layer currently uses these queues:

  -----------------------------------------------------------------------
  Queue                               Responsibility
  ----------------------------------- -----------------------------------
  `taskflow.jobs`                     Main queue consumed by workers

  `taskflow.jobs.retry`               Delays infrastructure retries using
                                      message TTL

  `taskflow.jobs.dead`                Receives messages that exhaust the
                                      infrastructure retry limit or are
                                      dead-lettered
  -----------------------------------------------------------------------

The main queue is configured with dead-letter routing to the dead-letter
exchange and queue. The retry queue uses a 5-second TTL and dead-letters
messages back to the main job route after the delay.

The retry publisher sends persistent messages and waits for publisher
confirms before the original delivery is acknowledged.

``` mermaid
flowchart TD
    Main[taskflow.jobs] --> Consumer[Worker consumer]
    Consumer -->|Infrastructure failure| RetryPub[Retry publisher]
    RetryPub --> Retry[taskflow.jobs.retry]
    Retry -->|TTL expires after 5 seconds| Main
    Consumer -->|Retry limit exhausted / rejected| Dead[taskflow.jobs.dead]
```

The exact exchange names and routing keys are defined in the messaging
package's queue/topology declarations. The queue names above were
verified against the running RabbitMQ broker.

------------------------------------------------------------------------

## Applications and packages

### `apps/api`

-   Fastify server.
-   Environment-based configuration.
-   Health route: `GET /health`.
-   Database-backed job management functionality, including a manual
    retry workflow.

### `apps/dispatcher`

-   Independent dispatcher process.
-   Connects to RabbitMQ.
-   Asserts/prepares the job queue.
-   Responsible for delivering persisted publication work to the broker.

### `apps/worker`

-   Independent worker process.
-   Registers job handlers.
-   Registers its worker identity in PostgreSQL.
-   Consumes job messages.
-   Claims jobs and records attempts.
-   Runs heartbeat and lease-recovery services.
-   Handles graceful shutdown and fatal retry-publisher failure.

### Current example handler: `send_email`

The worker currently registers a `send_email` handler. The handler
validates that its input includes string values for:

-   `to`
-   `subject`
-   `body`

The current handler is a development/example implementation that
simulates delivery; it should not be treated as a production email
integration.

------------------------------------------------------------------------

## Local development setup

### Prerequisites

The development environment used for the verified project runs included:

-   Node.js `v24.15.0`
-   npm `11.12.1`
-   Docker
-   Docker Compose
-   TypeScript `6.0.3`

A compatible Node.js/npm/Docker setup should be used when running the
project.

### 1. Install dependencies

From the repository root:

``` powershell
npm install
```

### 2. Configure environment variables

Create a local `.env` file in the repository root. Do not commit
secrets.

The project uses environment configuration for at least:

-   PostgreSQL connection (`DATABASE_URL`);
-   RabbitMQ username (`RABBITMQ_USER`);
-   RabbitMQ password (`RABBITMQ_PASSWORD`);
-   RabbitMQ host/port configuration where configured by the
    application;
-   API/worker/dispatcher-specific settings.

The local PostgreSQL container was configured with:

-   container: `taskflow-postgres`;
-   database: `taskflow`;
-   user: `taskflow`;
-   host port: `5433`;
-   container port: `5432`.

The local RabbitMQ container was configured with:

-   container: `taskflow-rabbitmq`;
-   AMQP port: `5672`;
-   management UI port: `15672`.

Use the actual `.env` values and Docker Compose configuration from the
repository. Do not copy credentials into source control or this README.

### 3. Start infrastructure

If the repository's Compose file defines the PostgreSQL and RabbitMQ
services, start them from the project root:

``` powershell
docker compose up -d
```

Check the containers:

``` powershell
docker ps
```

### 4. Verify PostgreSQL connectivity

The database package includes a connection-check script. From the
repository root, the verified command was:

``` powershell
node --env-file=.env --import tsx packages/database/scripts/check-connection.ts
```

Expected result:

``` text
PostgreSQL connection successful!
Database responded to SELECT 1.
```

### 5. Apply database migrations

From `packages/database`:

``` powershell
node --env-file=../../.env ../../node_modules/node-pg-migrate/bin/node-pg-migrate.js up --migrations-dir migrations --migrations-table taskflow_migrations
```

Use the repository's current migration files and migration status when
setting up a fresh environment.

------------------------------------------------------------------------

## Environment configuration

The exact set of variables is defined by each application's environment
module. The following names are confirmed as used by the project:

  -----------------------------------------------------------------------
  Variable                            Purpose
  ----------------------------------- -----------------------------------
  `DATABASE_URL`                      PostgreSQL connection string

  `RABBITMQ_USER`                     RabbitMQ username

  `RABBITMQ_PASSWORD`                 RabbitMQ password

  `RABBITMQ_HOST`                     RabbitMQ host, with a local default
                                      observed in dispatcher
                                      configuration
  -----------------------------------------------------------------------

Other service-specific variables (such as ports, worker concurrency,
heartbeat intervals, lease intervals, and retry settings) should be
checked in the relevant `apps/*/src/config` files. Keep real credentials
in `.env`, not in source code.

------------------------------------------------------------------------

## Running TaskFlow

Run each long-lived service in its own PowerShell terminal from the
repository root.

### API

``` powershell
npm run dev -w @taskflow/api
```

The API development command loads the root `.env` and starts the Fastify
server.

### Dispatcher

``` powershell
npm run dev -w @taskflow/dispatcher
```

### Worker

``` powershell
npm run dev -w @taskflow/worker
```

For direct worker execution without the workspace watch process, the
following command was also used during integration testing:

``` powershell
node --env-file=.env --import tsx apps/worker/src/index.ts
```

### Build and typecheck

From the repository root:

``` powershell
npm run typecheck
npm run build
```

The root build is configured to build workspaces in dependency order.
Individual workspace checks can be run with:

``` powershell
npm run typecheck -w @taskflow/worker
npm run build -w @taskflow/worker
```

------------------------------------------------------------------------

## Testing and verification

The following tests and checks were run during development. Results are
listed according to what was observed in the development sessions.

### 1. PostgreSQL connection test

Command:

``` powershell
node --env-file=.env --import tsx packages/database/scripts/check-connection.ts
```

Verified:

-   connection to the TaskFlow PostgreSQL database;
-   successful `SELECT 1` response.

**Result: Passed.**

### 2. Core messaging integration test

Command:

``` powershell
node --env-file=.env --import tsx packages/messaging/scripts/check-messaging.ts
```

Verified:

-   RabbitMQ connection;
-   queue setup;
-   message publication;
-   broker-confirmed publication;
-   message consumption;
-   malformed JSON handling and dead-letter behavior in the messaging
    test flow.

The initial execution encountered a dependency-loading issue, which was
resolved; a subsequent execution reported
`Messaging integration test passed.`

**Result: Passed on the subsequent run.**

### 3. Delayed infrastructure retry test

Command:

``` powershell
node --env-file=.env --import tsx packages/messaging/scripts/check-retries.ts
```

Verified:

-   infrastructure retry publication;
-   retry queue TTL delay;
-   return of the message to the main queue.

Two observed retry delays were approximately 5,018 ms and 5,010 ms,
consistent with the configured 5-second delay.

**Result: Passed.**

### 4. Infrastructure retry limit test

Command:

``` powershell
node --env-file=.env --import tsx packages/messaging/scripts/check-retry-limit.ts
```

Verified:

-   repeated infrastructure failure handling;
-   retry-count progression;
-   configured maximum of five infrastructure retries;
-   routing to the dead-letter queue after the retry limit was
    exhausted.

The test observed six handler executions: the original delivery plus
five retries.

**Result: Passed.**

### 5. Retry publisher failure messaging test

Command:

``` powershell
node --env-file=.env --import tsx packages/messaging/scripts/check-retry-publisher-failure.ts
```

Verified:

-   retry publisher channel failure;
-   fatal error callback invocation;
-   original message left unacknowledged;
-   consumer channel closure;
-   original message requeued;
-   original message did not gain a retry-count header when retry
    publication failed.

**Result: Passed.**

### 6. Full worker retry-publisher failure integration test

This test exercised the actual worker process, not only the messaging
consumer in isolation.

A temporary, test-only failure injection caused `claimJob()` to throw
for one known test UUID. A second temporary test hook closed the
worker's retry publisher channel after startup. The test job was then
published to RabbitMQ.

Verified worker output:

-   worker received the test job;
-   simulated infrastructure error occurred;
-   retry publication failed with
    `IllegalOperationError: Channel closed`;
-   worker triggered shutdown with reason `retry publisher failure`;
-   lease recovery stopped;
-   heartbeat stopped;
-   worker database record changed to `stopped`;
-   shutdown completed.

Verified broker state after shutdown:

  Queue                     Ready   Unacknowledged
  ----------------------- ------- ----------------
  `taskflow.jobs`               1                0
  `taskflow.jobs.retry`         0                0
  `taskflow.jobs.dead`          0                0

The original test message was subsequently inspected by its UUID and
acknowledged for cleanup.

Temporary failure-injection code and environment variables were removed
after the test. Worker typecheck and build passed again after cleanup.

**Result: Passed end to end.**

### 7. Job execution and retry-state verification

Development verification also covered:

-   registration of the `send_email` handler;
-   job execution attempts;
-   attempt history persisted in PostgreSQL;
-   retry scheduling and terminal failure state;
-   a job reaching its configured maximum of three job attempts;
-   attempt history showing abandoned/failed outcomes;
-   API manual retry request acceptance after supplying
    `Content-Type: application/json`.

A particular job was observed with `attempts = 3`, `total_attempts = 3`,
`max_attempts = 3`, and an attempt history containing two abandoned
attempts followed by a failed attempt. A manual retry request was
accepted by the API, but the complete post-retry database reset state
was not conclusively captured in the available test output.

**Result: Core execution/failure states observed; full manual-retry
state transition should be reverified.**

### 8. Worker shutdown verification

A normal direct worker process was stopped with Ctrl+C.

Verified:

-   lease recovery service stopped;
-   heartbeat service stopped;
-   worker marked as stopped in PostgreSQL;
-   shutdown completed.

The actual retry-publisher failure integration test subsequently
verified the same stopped-state behavior during a fatal messaging
failure.

**Result: Passed.**

### 9. Typecheck and build verification

The following checks were run successfully at different development
milestones:

``` powershell
npm run typecheck
npm run build
```

And, after the final retry-publisher failure test cleanup:

``` powershell
npm run typecheck -w @taskflow/worker
npm run build -w @taskflow/worker
```

Messaging package TypeScript checks were also run:

``` powershell
npx tsc -p packages/messaging/tsconfig.json --noEmit
npx tsc -p packages/messaging/tsconfig.scripts.json --noEmit
```

The worker and messaging checks passed after their respective fixes. The
full monorepo typecheck and build were also reported passing during
development.

**Result: Passed at the reported checkpoints.**

------------------------------------------------------------------------

## Operational checks

### Check running containers

``` powershell
docker ps
```

### Check RabbitMQ queues

``` powershell
docker exec taskflow-rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

### Check registered workers

``` powershell
docker exec taskflow-postgres psql -U taskflow -d taskflow -c "SELECT worker_id, status FROM workers;"
```

### Inspect jobs

``` powershell
docker exec taskflow-postgres psql -U taskflow -d taskflow -c "SELECT job_id, job_type, status, attempts, total_attempts, max_attempts, run_at, locked_by, lease_expires_at FROM jobs ORDER BY created_at DESC LIMIT 20;"
```

### Inspect attempts

``` powershell
docker exec taskflow-postgres psql -U taskflow -d taskflow -c "SELECT * FROM job_attempts ORDER BY started_at DESC LIMIT 20;"
```

Adjust selected columns if the migration schema changes.

------------------------------------------------------------------------

## Current scope and future improvements

The following are sensible areas to continue developing; they should not
be interpreted as already completed unless implemented and tested in the
codebase:

-   verify and document every API route, request schema, and response
    schema;
-   document the exact database columns and foreign-key constraints
    directly from migration SQL;
-   add automated tests for API validation, job creation, manual retry,
    and authorization if applicable;
-   add repeatable dispatcher/outbox integration tests, including broker
    outage and recovery scenarios;
-   test multiple concurrent workers and contention on the same job;
-   test worker crash/restart recovery with real process termination;
-   add metrics and structured logging for queue depth, job latency,
    retries, failures, and worker health;
-   add authentication/authorization and rate limiting where required by
    the API's intended deployment;
-   add production deployment configuration, secret management, and
    operational runbooks;
-   review dependency audit findings and keep dependencies updated.

------------------------------------------------------------------------

## Reliability principle

TaskFlow is built around one central reliability rule:

> **A job message must not be acknowledged until the system has safely
> completed its processing path or safely arranged its next delivery.**

The retry-publisher failure integration test demonstrated this
principle: when retry publication failed, the worker shut down and
RabbitMQ requeued the original unacknowledged message.

------------------------------------------------------------------------

## License

Right now no license has been specified in the project information available for
this README.
