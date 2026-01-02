export type SQLiteBindValue = number | string | Buffer | bigint | Date | null;

export type SQLiteBindParams =
  | SQLiteBindValue[]
  | Record<string, SQLiteBindValue>;

export type SQLiteRestBindParams = SQLiteBindValue[] | [SQLiteBindParams];

/**
 * Minimal query interface compatible with Absurd's database operations.
 */
export interface Queryable {
  /**
   * Execute a parameterized SQL query and return rows.
   * @param sql SQL text with parameter placeholders.
   * @param params Optional positional parameters.
   */
  query<R extends object = Record<string, any>>(
    sql: string,
    params?: SQLiteRestBindParams
  ): Promise<{ rows: R[] }>;
}

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | {
      [key: string]: JsonValue;
    };

export type JsonObject = {
  [key: string]: JsonValue;
};

export interface RetryStrategy {
  kind: "fixed" | "exponential" | "none";
  baseSeconds?: number;
  factor?: number;
  maxSeconds?: number;
}

export interface CancellationPolicy {
  maxDuration?: number;
  maxDelay?: number;
}

export interface SpawnOptions {
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
  headers?: JsonObject;
  queue?: string;
  cancellation?: CancellationPolicy;
  idempotencyKey?: string;
}

export interface SpawnResult {
  taskID: string;
  runID: string;
  attempt: number;
  created: boolean;
}

export interface ClaimedTask {
  run_id: string;
  task_id: string;
  task_name: string;
  attempt: number;
  params: JsonValue;
  retry_strategy: JsonValue;
  max_attempts: number | null;
  headers: JsonObject | null;
  wake_event: string | null;
  event_payload: JsonValue | null;
}

export interface WorkerOptions {
  workerId?: string;
  claimTimeout?: number;
  batchSize?: number;
  concurrency?: number;
  pollInterval?: number;
  onError?: (error: Error) => void;
  fatalOnLeaseTimeout?: boolean;
}

export interface TaskRegistrationOptions {
  name: string;
  queue?: string;
  defaultMaxAttempts?: number;
  defaultCancellation?: CancellationPolicy;
}

interface Log {
  log(...args: any[]): void;
  info(...args: any[]): void;
  warn(...args: any[]): void;
  error(...args: any[]): void;
}

/**
 * Hooks for customizing Absurd behavior.
 *
 * These hooks allow integration with tracing systems, correlation ID propagation,
 * and other cross-cutting concerns.
 */
export interface AbsurdHooks {
  /**
   * Called before spawning a task. Can modify spawn options (including headers).
   * Use this to inject trace IDs, correlation IDs, or other context from
   * AsyncLocalStorage into the task.
   */
  beforeSpawn?: (
    taskName: string,
    params: JsonValue,
    options: SpawnOptions
  ) => SpawnOptions | Promise<SpawnOptions>;
  /**
   * Wraps task execution. Must call and return the result of execute().
   * Use this to restore context (e.g., into AsyncLocalStorage) before the
   * task handler runs, ensuring all code within the task has access to it.
   */
  wrapTaskExecution?: <T>(
    ctx: TaskContext,
    execute: () => Promise<T>
  ) => Promise<T>;
}

export interface AbsurdOptions {
  db: Queryable;
  queueName?: string;
  defaultMaxAttempts?: number;
  log?: Log;
  hooks?: AbsurdHooks;
}

export declare class TaskContext {
  readonly taskID: string;
  /**
   * Returns all headers attached to this task.
   */
  get headers(): Readonly<JsonObject>;
  static create(args: {
    log: Log;
    taskID: string;
    con: Queryable;
    queueName: string;
    task: ClaimedTask;
    claimTimeout: number;
  }): Promise<TaskContext>;
  /**
   * Runs an idempotent step identified by name; caches and reuses its result across retries.
   * @param name Unique checkpoint name for this step.
   * @param fn Async function computing the step result (must be JSON-serializable).
   */
  step<T>(name: string, fn: () => Promise<T>): Promise<T>;
  /**
   * Suspends the task until the given duration (seconds) elapses.
   * @param stepName Checkpoint name for this wait.
   * @param duration Duration to wait in seconds.
   */
  sleepFor(stepName: string, duration: number): Promise<void>;
  /**
   * Suspends the task until the specified time.
   * @param stepName Checkpoint name for this wait.
   * @param wakeAt Absolute time when the task should resume.
   */
  sleepUntil(stepName: string, wakeAt: Date): Promise<void>;
  /**
   * Waits for an event by name and returns its payload; optionally sets a custom step name and timeout (seconds).
   * @param eventName Event identifier to wait for.
   * @param options.stepName Optional checkpoint name (defaults to $awaitEvent:<eventName>).
   * @param options.timeout Optional timeout in seconds.
   * @throws TimeoutError If the event is not received before the timeout.
   */
  awaitEvent(
    eventName: string,
    options?: {
      stepName?: string;
      timeout?: number;
    }
  ): Promise<JsonValue>;
  /**
   * Extends the current run's lease by the given seconds (defaults to the original claim timeout).
   * @param seconds Lease extension in seconds.
   */
  heartbeat(seconds?: number): Promise<void>;
  /**
   * Emits an event to this task's queue with an optional payload.
   * @param eventName Non-empty event name.
   * @param payload Optional JSON-serializable payload.
   */
  emitEvent(eventName: string, payload?: JsonValue): Promise<void>;
}

export type TaskHandler<P = any, R = any> = (
  params: P,
  ctx: TaskContext
) => Promise<R>;

/**
 * Background worker handle returned by startWorker().
 */
export interface Worker {
  /**
   * Stop the worker loop and wait for in-flight tasks to settle.
   */
  close(): Promise<void>;
}

/**
 * Absurd client interface.
 */
export interface AbsurdClient {
  /**
   * Create a new client bound to the provided connection.
   * @param con Connection to use for queries.
   * @param owned If true, close the connection when close() is called.
   */
  // bindToConnection(con: Queryable, owned?: boolean): this;

  /**
   * Register a task handler.
   * @param options Task registration options.
   * @param handler Async task handler.
   */
  registerTask<P = any, R = any>(
    options: TaskRegistrationOptions,
    handler: TaskHandler<P, R>
  ): void;

  /**
   * Create a queue.
   * @param queueName Optional queue name (defaults to client queue).
   */
  createQueue(queueName?: string): Promise<void>;
  /**
   * Drop a queue.
   * @param queueName Optional queue name (defaults to client queue).
   */
  dropQueue(queueName?: string): Promise<void>;
  /**
   * List available queues.
   */
  listQueues(): Promise<Array<string>>;

  /**
   * Spawn a task execution.
   * @param taskName Task name.
   * @param params Task parameters.
   * @param options Spawn options including queue and retry behavior.
   */
  spawn<P = any>(
    taskName: string,
    params: P,
    options?: SpawnOptions
  ): Promise<SpawnResult>;

  /**
   * Emit an event on a queue.
   * @param eventName Non-empty event name.
   * @param payload Optional JSON payload.
   * @param queueName Optional queue name (defaults to client queue).
   */
  emitEvent(
    eventName: string,
    payload?: JsonValue,
    queueName?: string
  ): Promise<void>;

  /**
   * Cancel a task by ID.
   * @param taskID Task identifier.
   * @param queueName Optional queue name (defaults to client queue).
   */
  cancelTask(taskID: string, queueName?: string): Promise<void>;

  /**
   * Claim tasks for processing.
   * @param options Claiming options.
   */
  claimTasks(options?: {
    batchSize?: number;
    claimTimeout?: number;
    workerId?: string;
  }): Promise<ClaimedTask[]>;

  /**
   * Claim and process a batch of tasks sequentially.
   * @param workerId Worker identifier.
   * @param claimTimeout Lease duration in seconds.
   * @param batchSize Max tasks to process.
   */
  workBatch(
    workerId?: string,
    claimTimeout?: number,
    batchSize?: number
  ): Promise<void>;

  /**
   * Start a background worker that polls and executes tasks.
   * @param options Worker behavior options.
   */
  startWorker(options?: WorkerOptions): Promise<Worker>;

  /**
   * Close the client and any owned resources.
   */
  close(): Promise<void>;

  /**
   * Execute a claimed task (used by workers).
   * @param task Claimed task record.
   * @param claimTimeout Lease duration in seconds.
   * @param options Execution options.
   */
  executeTask(
    task: ClaimedTask,
    claimTimeout: number,
    options?: { fatalOnLeaseTimeout?: boolean }
  ): Promise<void>;
}
