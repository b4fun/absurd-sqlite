import type { Instant } from "./temporal-types";

/**
 * Values that can be bound to SQLite prepared statements.
 * Supports both JavaScript Date and Temporal.Instant for datetime values.
 */
export type SQLiteBindValue =
  | number
  | string
  | Buffer
  | bigint
  | Date
  | Instant
  | null;

export type SQLiteBindParams =
  | SQLiteBindValue[]
  | Record<string, SQLiteBindValue>;

export type SQLiteRestBindParams = SQLiteBindValue[] | [SQLiteBindParams];

export interface SQLiteColumnDefinition {
  name: string;
  column: string | null;
  table: string | null;
  database: string | null;
  type: string | null;
}

export interface SQLiteStatement<Result extends object = Record<string, any>> {
  readonly: boolean;
  columns(): SQLiteColumnDefinition[];
  all(...args: SQLiteRestBindParams): Result[];
  run(...args: SQLiteRestBindParams): unknown;
}

export interface SQLiteDatabase {
  prepare<Result extends object = Record<string, any>>(
    sql: string
  ): SQLiteStatement<Result>;
  close(): void;
  loadExtension(path: string): void;
}

export type SQLiteVerboseLog = (
  message?: any,
  ...optionalParams: any[]
) => void;
