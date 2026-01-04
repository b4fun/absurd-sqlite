import { afterEach, describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import "./setup";
import { waitFor } from "./wait-for";

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

let tempDir: string | null = null;

function createDatabaseWithMigrations(): string {
  tempDir = mkdtempSync(join(tmpdir(), "absurd-sqlite-"));
  const dbPath = join(tempDir, "absurd.db");
  const db = new Database(dbPath);
  (db as unknown as { loadExtension(path: string): void }).loadExtension(
    extensionPath
  );
  db.query("select absurd_apply_migrations()").get();
  db.close();
  return dbPath;
}

afterEach(() => {
  if (tempDir) {
    rmSync(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
  delete process.env.ABSURD_DATABASE_PATH;
  delete process.env.ABSURD_DATABASE_EXTENSION_PATH;
});

describe("run", () => {
  it("requires ABSURD_DATABASE_PATH", async () => {
    process.env.ABSURD_DATABASE_PATH = "";
    process.env.ABSURD_DATABASE_EXTENSION_PATH = extensionPath;
    const { default: run } = await import("../src/index");

    await expect(run(() => {})).rejects.toThrow(
      "ABSURD_DATABASE_PATH is required"
    );
  });

  it("requires ABSURD_DATABASE_EXTENSION_PATH", async () => {
    process.env.ABSURD_DATABASE_PATH = "/tmp/absurd.db";
    const { default: run } = await import("../src/index");

    await expect(run(() => {})).rejects.toThrow(
      "ABSURD_DATABASE_EXTENSION_PATH is required"
    );
  });

  it("runs setup, processes tasks, and shuts down on SIGINT", async () => {
    const dbPath = createDatabaseWithMigrations();
    process.env.ABSURD_DATABASE_PATH = dbPath;
    process.env.ABSURD_DATABASE_EXTENSION_PATH = extensionPath;

    const { default: run } = await import("../src/index");

    await run(async (absurd) => {
      await absurd.createQueue("default");
      absurd.registerTask({ name: "ping" }, async () => ({ ok: true }));
      await absurd.spawn("ping", {});
    });

    const verifier = new Database(dbPath);
    await waitFor(() => {
      const row = verifier
        .query("select state from absurd_tasks where task_name = 'ping'")
        .get() as { state: string } | null;
      expect(row?.state).toBe("completed");
    });

    process.emit("SIGINT");
    await new Promise((resolve) => setTimeout(resolve, 0));

    verifier.close();
  });
});
