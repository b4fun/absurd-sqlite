import type {
  SQLiteBindParams,
  SQLiteBindValue,
  SQLiteRestBindParams,
} from "@absurd-sqlite/sdk-types";

export type { SQLiteBindParams, SQLiteBindValue, SQLiteRestBindParams };

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
  run(...args: SQLiteRestBindParams): number;
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
