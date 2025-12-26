-- introduced_version: 0.1.0
-- Absurd SQLite schema (single table set, queue_name scoped).
-- Timestamps are stored as unix epoch milliseconds (DATETIME affinity).

PRAGMA foreign_keys = ON;

create table if not exists absurd_queues (
  queue_name text primary key,
  created_at datetime not null default (cast(strftime('%s','now') as integer) * 1000)
);

create table if not exists absurd_settings (
  key text primary key,
  value text
);

create table if not exists absurd_tasks (
  queue_name text not null,
  task_id text not null,
  task_name text not null,
  params blob not null,
  headers blob,
  retry_strategy blob,
  max_attempts integer,
  cancellation blob,
  enqueue_at datetime not null default (cast(strftime('%s','now') as integer) * 1000),
  first_started_at datetime,
  state text not null check (state in ('pending','running','sleeping','completed','failed','cancelled')),
  attempts integer not null default 0,
  last_attempt_run text,
  completed_payload blob,
  cancelled_at datetime,
  idempotency_key text,
  primary key (queue_name, task_id),
  unique (queue_name, idempotency_key),
  foreign key (queue_name) references absurd_queues(queue_name) on delete cascade,
  foreign key (queue_name, last_attempt_run) references absurd_runs(queue_name, run_id) on delete set null,
  check (json_valid(json(params))),
  check (headers is null or json_valid(json(headers))),
  check (retry_strategy is null or json_valid(json(retry_strategy))),
  check (cancellation is null or json_valid(json(cancellation))),
  check (completed_payload is null or json_valid(json(completed_payload)))
);

create table if not exists absurd_runs (
  queue_name text not null,
  run_id text not null,
  task_id text not null,
  attempt integer not null,
  state text not null check (state in ('pending','running','sleeping','completed','failed','cancelled')),
  claimed_by text,
  claim_expires_at datetime,
  available_at datetime not null,
  wake_event text,
  event_payload blob,
  started_at datetime,
  completed_at datetime,
  failed_at datetime,
  result blob,
  failure_reason blob,
  created_at datetime not null default (cast(strftime('%s','now') as integer) * 1000),
  primary key (queue_name, run_id),
  foreign key (queue_name, task_id) references absurd_tasks(queue_name, task_id) on delete cascade,
  check (event_payload is null or json_valid(json(event_payload))),
  check (result is null or json_valid(json(result))),
  check (failure_reason is null or json_valid(json(failure_reason)))
);

create table if not exists absurd_checkpoints (
  queue_name text not null,
  task_id text not null,
  checkpoint_name text not null,
  state blob,
  status text not null default 'committed',
  owner_run_id text,
  updated_at datetime not null default (cast(strftime('%s','now') as integer) * 1000),
  primary key (queue_name, task_id, checkpoint_name),
  foreign key (queue_name, task_id) references absurd_tasks(queue_name, task_id) on delete cascade,
  foreign key (queue_name, owner_run_id) references absurd_runs(queue_name, run_id) on delete set null,
  check (state is null or json_valid(json(state)))
);

create table if not exists absurd_events (
  queue_name text not null,
  event_name text not null,
  payload blob,
  emitted_at datetime not null default (cast(strftime('%s','now') as integer) * 1000),
  primary key (queue_name, event_name),
  foreign key (queue_name) references absurd_queues(queue_name) on delete cascade,
  check (payload is null or json_valid(json(payload)))
);

create table if not exists absurd_waits (
  queue_name text not null,
  task_id text not null,
  run_id text not null,
  step_name text not null,
  event_name text not null,
  timeout_at datetime,
  created_at datetime not null default (cast(strftime('%s','now') as integer) * 1000),
  primary key (queue_name, run_id, step_name),
  foreign key (queue_name, task_id) references absurd_tasks(queue_name, task_id) on delete cascade,
  foreign key (queue_name, run_id) references absurd_runs(queue_name, run_id) on delete cascade,
  foreign key (queue_name, event_name) references absurd_events(queue_name, event_name) on delete cascade
);

create index if not exists absurd_runs_state_available_idx
  on absurd_runs (queue_name, state, available_at);

create index if not exists absurd_runs_task_idx
  on absurd_runs (queue_name, task_id);

create index if not exists absurd_waits_event_idx
  on absurd_waits (queue_name, event_name);

create index if not exists absurd_tasks_state_idx
  on absurd_tasks (queue_name, state);
