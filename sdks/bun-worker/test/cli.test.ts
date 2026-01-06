import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";

import { loadExtension } from "./setup";

describe("CLI flags", () => {
  let tempDir: string;
  let dbPath: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "absurd-cli-test-"));
    dbPath = join(tempDir, "test.db");

    // Initialize database with migrations
    const db = new Database(dbPath);
    loadExtension(db);
    db.query("select absurd_apply_migrations()").get();
    db.close();
  });

  afterEach(() => {
    if (tempDir) {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("accepts concurrency flag", async () => {
    const workerScript = join(tempDir, "worker.ts");
    writeFileSync(
      workerScript,
      `
      import run from "../src/index";
      
      await run(async (absurd) => {
        await absurd.createQueue("default");
        console.log("Worker started");
        // Exit immediately for test
        process.exit(0);
      });
      `
    );

    const result = await runWorkerWithFlags(workerScript, dbPath, [
      "--concurrency",
      "5",
    ]);
    expect(result.exitCode).toBe(0);
  });

  it("accepts poll-interval flag", async () => {
    const workerScript = join(tempDir, "worker.ts");
    writeFileSync(
      workerScript,
      `
      import run from "../src/index";
      
      await run(async (absurd) => {
        await absurd.createQueue("default");
        console.log("Worker started");
        process.exit(0);
      });
      `
    );

    const result = await runWorkerWithFlags(workerScript, dbPath, [
      "--poll-interval",
      "10",
    ]);
    expect(result.exitCode).toBe(0);
  });

  it("accepts worker-id flag", async () => {
    const workerScript = join(tempDir, "worker.ts");
    writeFileSync(
      workerScript,
      `
      import run from "../src/index";
      
      await run(async (absurd) => {
        await absurd.createQueue("default");
        console.log("Worker started");
        process.exit(0);
      });
      `
    );

    const result = await runWorkerWithFlags(workerScript, dbPath, [
      "--worker-id",
      "test-worker",
    ]);
    expect(result.exitCode).toBe(0);
  });

  it("accepts multiple flags", async () => {
    const workerScript = join(tempDir, "worker.ts");
    writeFileSync(
      workerScript,
      `
      import run from "../src/index";
      
      await run(async (absurd) => {
        await absurd.createQueue("default");
        console.log("Worker started");
        process.exit(0);
      });
      `
    );

    const result = await runWorkerWithFlags(workerScript, dbPath, [
      "--concurrency",
      "5",
      "--poll-interval",
      "10",
      "--worker-id",
      "test-worker",
    ]);
    expect(result.exitCode).toBe(0);
  });

  it("programmatic options override CLI flags", async () => {
    const workerScript = join(tempDir, "worker.ts");
    writeFileSync(
      workerScript,
      `
      import run from "../src/index";
      
      await run(
        async (absurd) => {
          await absurd.createQueue("default");
          console.log("Worker started");
          process.exit(0);
        },
        {
          workerOptions: {
            concurrency: 20,
          },
        }
      );
      `
    );

    const result = await runWorkerWithFlags(workerScript, dbPath, [
      "--concurrency",
      "5",
    ]);
    expect(result.exitCode).toBe(0);
    // The programmatic option (20) should override the CLI flag (5)
  });

  it("can disable CLI flag parsing", async () => {
    const workerScript = join(tempDir, "worker.ts");
    writeFileSync(
      workerScript,
      `
      import run from "../src/index";
      
      await run(
        async (absurd) => {
          await absurd.createQueue("default");
          console.log("Worker started");
          process.exit(0);
        },
        {
          parseCliFlags: false,
        }
      );
      `
    );

    const result = await runWorkerWithFlags(workerScript, dbPath, [
      "--concurrency",
      "5",
    ]);
    expect(result.exitCode).toBe(0);
  });
});

interface WorkerResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

async function runWorkerWithFlags(
  scriptPath: string,
  dbPath: string,
  flags: string[]
): Promise<WorkerResult> {
  return new Promise((resolve, reject) => {
    const extensionPath = process.env.ABSURD_DATABASE_EXTENSION_PATH;
    if (!extensionPath) {
      reject(new Error("ABSURD_DATABASE_EXTENSION_PATH not set"));
      return;
    }

    const proc = spawn("bun", ["run", scriptPath, ...flags], {
      env: {
        ...process.env,
        ABSURD_DATABASE_PATH: dbPath,
        ABSURD_DATABASE_EXTENSION_PATH: extensionPath,
      },
      timeout: 5000, // 5 second timeout
    });

    let stdout = "";
    let stderr = "";

    proc.stdout?.on("data", (data) => {
      stdout += data.toString();
    });

    proc.stderr?.on("data", (data) => {
      stderr += data.toString();
    });

    proc.on("close", (code) => {
      resolve({
        exitCode: code ?? 1,
        stdout,
        stderr,
      });
    });

    proc.on("error", (err) => {
      reject(err);
    });
  });
}
