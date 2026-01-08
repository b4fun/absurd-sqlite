import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { fileURLToPath } from "node:url";

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

describe("Worker configuration", () => {
  let tempDir: string;
  let dbPath: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "absurd-cli-test-"));
    dbPath = join(tempDir, "test.db");

    // Initialize database with migrations
    const db = new Database(dbPath);
    (db as unknown as { loadExtension(path: string): void }).loadExtension(
      extensionPath
    );
    db.query("select absurd_apply_migrations()").get();
    db.close();

    process.env.ABSURD_DATABASE_PATH = dbPath;
    process.env.ABSURD_DATABASE_EXTENSION_PATH = extensionPath;
  });

  afterEach(() => {
    if (tempDir) {
      rmSync(tempDir, { recursive: true, force: true });
    }
    delete process.env.ABSURD_DATABASE_PATH;
    delete process.env.ABSURD_DATABASE_EXTENSION_PATH;
  });

  it("works with default options when none provided", async () => {
    const { default: run } = await import("../src/index");

    let workerStarted = false;

    const promise = run(async (absurd) => {
      await absurd.createQueue("default");
      absurd.registerTask({ name: "test" }, async () => {
        return { ok: true };
      });
      workerStarted = true;
    });

    // Give the worker time to start
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(workerStarted).toBe(true);

    // Clean up by sending SIGINT
    process.emit("SIGINT");
    await new Promise((resolve) => setTimeout(resolve, 100));
  });
});

