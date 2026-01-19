import {
  AbsurdImpl,
  type AbsurdOptions as AbsurdImplOptions,
  type ClaimedTask,
  type JsonValue,
  type Queryable,
  type SpawnOptions,
  type SpawnResult,
  type TaskHandler,
  type TaskRegistrationOptions,
  type WorkerOptions,
} from "./absurd";
import { SQLiteConnection } from "./sqlite-connection";

export type { Queryable } from "./absurd";
export {
  CancelledTask,
  SuspendTask,
  TimeoutError,
  TaskContext,
  type AbsurdHooks,
  type CancellationPolicy,
  type ClaimedTask,
  type JsonObject,
  type JsonValue,
  type RetryStrategy,
  type SpawnOptions,
  type SpawnResult,
  type TaskHandler,
  type TaskRegistrationOptions,
  type WorkerOptions,
} from "./absurd";
export {
  downloadExtension,
  resolveExtensionPath,
  type DownloadExtensionOptions,
} from "./extension-downloader";
export type {
  SQLiteBindParams,
  SQLiteBindValue,
  SQLiteColumnDefinition,
  SQLiteDatabase,
  SQLiteRestBindParams,
  SQLiteStatement,
  SQLiteVerboseLog,
} from "./sqlite-types";
export { SQLiteConnection } from "./sqlite-connection";
export type { SQLiteConnectionOptions, SQLiteValueCodec } from "./sqlite-connection";

/**
 * SQLite-specific Absurd client that loads the extension and owns the database handle.
 */
export type AbsurdOptions = Omit<AbsurdImplOptions, "db" | "ownedConnection">;

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

export class Absurd extends AbsurdImpl implements AbsurdClient {
  constructor(connection: SQLiteConnection, options?: AbsurdOptions) {
    super({
      db: connection,
      queueName: options?.queueName,
      defaultMaxAttempts: options?.defaultMaxAttempts,
      log: options?.log,
      hooks: options?.hooks,
      ownedConnection: false,
    });
  }
}
