import type {
  ClaimedTask,
  JsonValue,
  SpawnOptions,
  SpawnResult,
  TaskHandler,
  TaskRegistrationOptions,
  WorkerOptions,
} from "absurd-sdk";

import type { SQLiteRestBindParams } from "./sqlite-types.ts";

/**
 * Minimal query interface compatible with Absurd's database operations.
 */
export interface Queryable {
  /**
   * Execute a parameterized SQL query and return rows.
   * @param sql SQL text with parameter placeholders.
   * @param params Optional positional parameters.
   */
  query<R extends object = Record<string, unknown>>(
    sql: string,
    params?: SQLiteRestBindParams,
  ): Promise<{ rows: R[] }>;
}

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
   * Register a task handler.
   * @param options Task registration options.
   * @param handler Async task handler.
   */
  registerTask<P = unknown, R = unknown>(
    options: TaskRegistrationOptions,
    handler: TaskHandler<P, R>,
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
  spawn<P = unknown>(
    taskName: string,
    params: P,
    options?: SpawnOptions,
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
    queueName?: string,
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
    batchSize?: number,
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
    options?: { fatalOnLeaseTimeout?: boolean },
  ): Promise<void>;
}
