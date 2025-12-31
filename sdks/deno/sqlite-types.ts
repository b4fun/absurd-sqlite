import type { Buffer } from "node:buffer";

export type SQLiteBindValue =
  | number
  | string
  | Uint8Array
  | Buffer
  | bigint
  | boolean
  | symbol
  | Date
  | null
  | undefined
  | SQLiteBindValue[]
  | { [key: string]: SQLiteBindValue };

export type SQLiteBindParams =
  | SQLiteBindValue[]
  | Record<string, SQLiteBindValue>;

export type SQLiteRestBindParams = SQLiteBindParams;

export interface SQLiteColumnDefinition {
  name: string;
  column: string | null;
  table: string | null;
  database: string | null;
  type: string | null;
}

export interface SQLiteStatement<
  Result extends object = Record<string, unknown>,
> {
  readonly: boolean;
  columns(): SQLiteColumnDefinition[];
  all(params?: SQLiteBindParams): Result[];
  run(params?: SQLiteBindParams): number;
}

export interface SQLiteDatabase {
  prepare<Result extends object = Record<string, unknown>>(
    sql: string,
  ): SQLiteStatement<Result>;
  close(): void;
  loadExtension(path: string): void;
}

export type SQLiteVerboseLog = (
  message?: unknown,
  ...optionalParams: unknown[]
) => void;
