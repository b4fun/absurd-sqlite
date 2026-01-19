import { afterAll } from "vitest";
import sqlite from "better-sqlite3";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  Absurd,
  Temporal,
  type AbsurdHooks,
  type JsonValue,
  type SQLiteDatabase,
} from "../src/index";

import { SQLiteConnection } from "../src/sqlite-connection";

// Database row types matching the SQLite schema
export interface TaskRow {
  task_id: string;
  task_name: string;
  params: JsonValue;
  headers: JsonValue | null;
  retry_strategy: JsonValue | null;
  max_attempts: number | null;
  cancellation: JsonValue | null;
  enqueue_at: Temporal.Instant;
  first_started_at: Temporal.Instant | null;
  state:
    | "pending"
    | "running"
    | "sleeping"
    | "completed"
    | "failed"
    | "cancelled";
  attempts: number;
  last_attempt_run: string | null;
  completed_payload: JsonValue | null;
  cancelled_at: Temporal.Instant | null;
}

export interface RunRow {
  run_id: string;
  task_id: string;
  attempt: number;
  state:
    | "pending"
    | "running"
    | "sleeping"
    | "completed"
    | "failed"
    | "cancelled";
  claimed_by: string | null;
  claim_expires_at: Temporal.Instant | null;
  available_at: Temporal.Instant;
  wake_event: string | null;
  event_payload: JsonValue | null;
  started_at: Temporal.Instant | null;
  completed_at: Temporal.Instant | null;
  failed_at: Temporal.Instant | null;
  result: JsonValue | null;
  failure_reason: JsonValue | null;
  created_at: Temporal.Instant;
}

interface SqliteFixture {
  db: sqlite.Database;
  conn: SQLiteConnection;
  dbPath: string;
  cleanup: () => void;
}

const fixtures: SqliteFixture[] = [];

afterAll(() => {
  for (const fixture of fixtures) {
    fixture.db.close();
    fixture.cleanup();
  }
  fixtures.length = 0;
});

export interface TestContext {
  absurd: Absurd;
  pool: SQLiteConnection;
  queueName: string;
  dbPath: string;
  cleanupTasks(): Promise<void>;
  getQueueStorageState(
    queueName: string
  ): Promise<{ exists: boolean; tables: string[] }>;
  getTask(taskID: string): Promise<TaskRow | null>;
  getRun(runID: string): Promise<RunRow | null>;
  getRuns(taskID: string): Promise<RunRow[]>;
  setFakeNow(ts: Date | null): Promise<void>;
  sleep(ms: number): Promise<void>;
  getRemainingTasksCount(): Promise<number>;
  getRemainingEventsCount(): Promise<number>;
  getWaitsCount(): Promise<number>;
  getCheckpoint(
    taskID: string,
    checkpointName: string
  ): Promise<{
    checkpoint_name: string;
    state: JsonValue;
    owner_run_id: string;
  } | null>;
  scheduleRun(runID: string, wakeAt: Date): Promise<void>;
  completeRun(runID: string, payload: JsonValue): Promise<void>;
  cleanupTasksByTTL(ttlSeconds: number, limit: number): Promise<number>;
  cleanupEventsByTTL(ttlSeconds: number, limit: number): Promise<number>;
  setTaskCheckpointState(
    taskID: string,
    stepName: string,
    state: JsonValue,
    runID: string,
    extendClaimBySeconds: number | null
  ): Promise<void>;
  awaitEventInternal(
    taskID: string,
    runID: string,
    stepName: string,
    eventName: string,
    timeoutSeconds: number | null
  ): Promise<void>;
  extendClaim(runID: string, extendBySeconds: number): Promise<void>;
  expectCancelledError(promise: Promise<unknown>): Promise<void>;
  createClient(options?: { queueName?: string; hooks?: AbsurdHooks }): Absurd;
}

export function randomName(prefix = "test"): string {
  return `${prefix}_${Math.random().toString(36).substring(7)}`;
}

const testDir = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(testDir, "../../..");
const extensionBase = join(repoRoot, "target/release/libabsurd");

function resolveExtensionPath(base: string): string {
  const platformExt =
    process.platform === "win32"
      ? ".dll"
      : process.platform === "darwin"
      ? ".dylib"
      : ".so";
  const candidates = [base, `${base}${platformExt}`];
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  throw new Error(
    `SQLite extension not found at ${base} (expected ${platformExt})`
  );
}

const extensionPath = resolveExtensionPath(extensionBase);

function createFixture(): SqliteFixture {
  const tempDir = mkdtempSync(join(tmpdir(), "absurd-sqlite-"));
  const dbPath = join(tempDir, "absurd.db");
  const db = new sqlite(dbPath);
  db.loadExtension(extensionPath);
  db.prepare("select absurd_apply_migrations()").get();
  const conn = new SQLiteConnection(db as SQLiteDatabase);

  const cleanup = () => {
    rmSync(tempDir, { recursive: true, force: true });
  };

  const fixture = { db, conn, dbPath, cleanup };
  fixtures.push(fixture);
  return fixture;
}

export async function createTestAbsurd(
  queueName: string = "default"
): Promise<TestContext> {
  const fixture = createFixture();
  const absurd = new Absurd(fixture.conn, { queueName });

  await absurd.createQueue(queueName);

  return {
    absurd,
    pool: fixture.conn,
    queueName,
    dbPath: fixture.dbPath,
    cleanupTasks: () => cleanupTasks(fixture.conn, queueName),
    getQueueStorageState: (targetQueueName: string) =>
      getQueueStorageState(fixture.conn, targetQueueName),
    getTask: (taskID: string) => getTask(fixture.conn, taskID, queueName),
    getRun: (runID: string) => getRun(fixture.conn, runID, queueName),
    getRuns: (taskID: string) => getRuns(fixture.conn, taskID, queueName),
    setFakeNow: (ts: Date | null) => setFakeNow(fixture.conn, ts),
    sleep: (ms: number) => new Promise((resolve) => setTimeout(resolve, ms)),
    getRemainingTasksCount: () =>
      getRemainingTasksCount(fixture.conn, queueName),
    getRemainingEventsCount: () =>
      getRemainingEventsCount(fixture.conn, queueName),
    getWaitsCount: () => getWaitsCount(fixture.conn, queueName),
    getCheckpoint: (taskID: string, checkpointName: string) =>
      getCheckpoint(fixture.conn, taskID, checkpointName, queueName),
    scheduleRun: (runID: string, wakeAt: Date) =>
      scheduleRun(fixture.conn, runID, wakeAt, queueName),
    completeRun: (runID: string, payload: JsonValue) =>
      completeRun(fixture.conn, runID, payload, queueName),
    cleanupTasksByTTL: (ttlSeconds: number, limit: number) =>
      cleanupTasksByTTL(fixture.conn, ttlSeconds, limit, queueName),
    cleanupEventsByTTL: (ttlSeconds: number, limit: number) =>
      cleanupEventsByTTL(fixture.conn, ttlSeconds, limit, queueName),
    setTaskCheckpointState: (
      taskID: string,
      stepName: string,
      state: JsonValue,
      runID: string,
      extendClaimBySeconds: number | null
    ) =>
      setTaskCheckpointState(
        fixture.conn,
        taskID,
        stepName,
        state,
        runID,
        extendClaimBySeconds,
        queueName
      ),
    awaitEventInternal: (
      taskID: string,
      runID: string,
      stepName: string,
      eventName: string,
      timeoutSeconds: number | null
    ) =>
      awaitEventInternal(
        fixture.conn,
        taskID,
        runID,
        stepName,
        eventName,
        timeoutSeconds,
        queueName
      ),
    extendClaim: (runID: string, extendBySeconds: number) =>
      extendClaim(fixture.conn, runID, extendBySeconds, queueName),
    expectCancelledError: (promise: Promise<unknown>) =>
      expectCancelledError(promise),
    createClient: (options) =>
      new Absurd(fixture.conn, {
        queueName: options?.queueName ?? queueName,
        hooks: options?.hooks,
      }),
  };
}

async function setFakeNow(
  conn: SQLiteConnection,
  ts: Date | null
): Promise<void> {
  if (ts === null) {
    await conn.exec("select absurd.set_fake_now(null)");
    return;
  }
  await conn.exec("select absurd.set_fake_now($1)", [ts.getTime()]);
}

async function cleanupTasks(
  conn: SQLiteConnection,
  queue: string
): Promise<void> {
  const tables = [
    "absurd_tasks",
    "absurd_runs",
    "absurd_events",
    "absurd_waits",
    "absurd_checkpoints",
  ];
  for (const table of tables) {
    await conn.exec(`DELETE FROM ${table} WHERE queue_name = $1`, [queue]);
  }
}

async function getQueueStorageState(
  conn: SQLiteConnection,
  queue: string
): Promise<{ exists: boolean; tables: string[] }> {
  const { rows } = await conn.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM absurd_queues WHERE queue_name = $1`,
    [queue]
  );
  const tableRows = await conn.query<{ name: string }>(
    `SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'absurd_%'`
  );
  return {
    exists: rows[0]?.count > 0,
    tables: tableRows.rows.map((row) => row.name),
  };
}

async function getTask(
  conn: SQLiteConnection,
  taskID: string,
  queue: string
): Promise<TaskRow | null> {
  const { rows } = await conn.query<TaskRow>(
    `SELECT task_id,
            task_name,
            json(params) as params,
            json(headers) as headers,
            json(retry_strategy) as retry_strategy,
            max_attempts,
            json(cancellation) as cancellation,
            enqueue_at,
            first_started_at,
            state,
            attempts,
            last_attempt_run,
            json(completed_payload) as completed_payload,
            cancelled_at
       FROM absurd_tasks
      WHERE task_id = $1 AND queue_name = $2`,
    [taskID, queue]
  );
  return rows.length > 0 ? rows[0] : null;
}

async function getRun(
  conn: SQLiteConnection,
  runID: string,
  queue: string
): Promise<RunRow | null> {
  const { rows } = await conn.query<RunRow>(
    `SELECT run_id,
            task_id,
            attempt,
            state,
            claimed_by,
            claim_expires_at,
            available_at,
            wake_event,
            json(event_payload) as event_payload,
            started_at,
            completed_at,
            failed_at,
            json(result) as result,
            json(failure_reason) as failure_reason,
            created_at
       FROM absurd_runs
      WHERE run_id = $1 AND queue_name = $2`,
    [runID, queue]
  );
  return rows.length > 0 ? rows[0] : null;
}

async function getRuns(
  conn: SQLiteConnection,
  taskID: string,
  queue: string
): Promise<RunRow[]> {
  const { rows } = await conn.query<RunRow>(
    `SELECT run_id,
            task_id,
            attempt,
            state,
            claimed_by,
            claim_expires_at,
            available_at,
            wake_event,
            json(event_payload) as event_payload,
            started_at,
            completed_at,
            failed_at,
            json(result) as result,
            json(failure_reason) as failure_reason,
            created_at
       FROM absurd_runs
      WHERE task_id = $1 AND queue_name = $2
      ORDER BY attempt`,
    [taskID, queue]
  );
  return rows;
}

async function getRemainingTasksCount(
  conn: SQLiteConnection,
  queue: string
): Promise<number> {
  const { rows } = await conn.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM absurd_tasks WHERE queue_name = $1`,
    [queue]
  );
  return Number(rows[0]?.count ?? 0);
}

async function getRemainingEventsCount(
  conn: SQLiteConnection,
  queue: string
): Promise<number> {
  const { rows } = await conn.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM absurd_events WHERE queue_name = $1`,
    [queue]
  );
  return Number(rows[0]?.count ?? 0);
}

async function getWaitsCount(
  conn: SQLiteConnection,
  queue: string
): Promise<number> {
  const { rows } = await conn.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM absurd_waits WHERE queue_name = $1`,
    [queue]
  );
  return Number(rows[0]?.count ?? 0);
}

async function getCheckpoint(
  conn: SQLiteConnection,
  taskID: string,
  checkpointName: string,
  queue: string
): Promise<{
  checkpoint_name: string;
  state: JsonValue;
  owner_run_id: string;
} | null> {
  const { rows } = await conn.query<{
    checkpoint_name: string;
    state: JsonValue;
    owner_run_id: string;
  }>(
    `SELECT checkpoint_name,
            json(state) as state,
            owner_run_id
       FROM absurd_checkpoints
      WHERE task_id = $1
        AND checkpoint_name = $2
        AND queue_name = $3`,
    [taskID, checkpointName, queue]
  );
  return rows.length > 0 ? rows[0] : null;
}

async function scheduleRun(
  conn: SQLiteConnection,
  runID: string,
  wakeAt: Date,
  queue: string
): Promise<void> {
  await conn.exec(`SELECT absurd.schedule_run($1, $2, $3)`, [
    queue,
    runID,
    wakeAt,
  ]);
}

async function completeRun(
  conn: SQLiteConnection,
  runID: string,
  payload: JsonValue,
  queue: string
): Promise<void> {
  await conn.exec(`SELECT absurd.complete_run($1, $2, $3)`, [
    queue,
    runID,
    JSON.stringify(payload),
  ]);
}

async function cleanupTasksByTTL(
  conn: SQLiteConnection,
  ttlSeconds: number,
  limit: number,
  queue: string
): Promise<number> {
  const { rows } = await conn.query<{ count: number }>(
    `SELECT absurd.cleanup_tasks($1, $2, $3) AS count`,
    [queue, ttlSeconds, limit]
  );
  return Number(rows[0]?.count ?? 0);
}

async function cleanupEventsByTTL(
  conn: SQLiteConnection,
  ttlSeconds: number,
  limit: number,
  queue: string
): Promise<number> {
  const { rows } = await conn.query<{ count: number }>(
    `SELECT absurd.cleanup_events($1, $2, $3) AS count`,
    [queue, ttlSeconds, limit]
  );
  return Number(rows[0]?.count ?? 0);
}

async function setTaskCheckpointState(
  conn: SQLiteConnection,
  taskID: string,
  stepName: string,
  state: JsonValue,
  runID: string,
  extendClaimBySeconds: number | null,
  queue: string
): Promise<void> {
  await conn.exec(
    `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
    [
      queue,
      taskID,
      stepName,
      JSON.stringify(state),
      runID,
      extendClaimBySeconds,
    ]
  );
}

async function awaitEventInternal(
  conn: SQLiteConnection,
  taskID: string,
  runID: string,
  stepName: string,
  eventName: string,
  timeoutSeconds: number | null,
  queue: string
): Promise<void> {
  await conn.query(
    `SELECT should_suspend, json(payload) as payload
       FROM absurd.await_event($1, $2, $3, $4, $5, $6)`,
    [queue, taskID, runID, stepName, eventName, timeoutSeconds]
  );
}

async function extendClaim(
  conn: SQLiteConnection,
  runID: string,
  extendBySeconds: number,
  queue: string
): Promise<void> {
  await conn.exec(`SELECT absurd.extend_claim($1, $2, $3)`, [
    queue,
    runID,
    extendBySeconds,
  ]);
}

async function expectCancelledError(promise: Promise<unknown>): Promise<void> {
  try {
    await promise;
  } catch (err: any) {
    const message = String(err?.message ?? "");
    if (message.toLowerCase().includes("cancelled")) {
      return;
    }
    throw err;
  }
  throw new Error("Expected cancellation error");
}
