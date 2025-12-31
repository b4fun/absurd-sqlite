import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";

import { withTestContext } from "./setup.ts";
import { createSpy, restoreAllSpies, waitFor } from "./utils.ts";

Deno.test("worker: respects concurrency limit and skips claims when at capacity", async () => {
  await withTestContext("worker", async (ctx) => {
    const absurd = ctx.absurd;
    const gate = new EventEmitter();
    const atGate = new Set<number>();

    gate.on("arrived", (id: number) => atGate.add(id));
    gate.on("left", (id: number) => atGate.delete(id));

    absurd.registerTask<{ id: number }, void>(
      { name: "gated-task" },
      async (params) => {
        gate.emit("arrived", params.id);
        await once(gate, "release");
        gate.emit("left", params.id);
      },
    );

    for (let i = 1; i <= 5; i++) {
      await absurd.spawn("gated-task", { id: i });
    }

    const claimSpy = createSpy(absurd, "claimTasks");

    const concurrency = 2;
    const worker = await absurd.startWorker({
      concurrency,
      pollInterval: 0.01,
      workerId: "worker-concurrency",
    });

    await waitFor(() => {
      assert.equal(atGate.size, concurrency);
    }, { timeoutMs: 100 });

    assert.equal(claimSpy.calls.length, 1);
    const firstCall = claimSpy.calls[0]?.[0] as
      | { batchSize?: number }
      | undefined;
    assert.equal(firstCall?.batchSize, concurrency);

    await ctx.sleep(50);

    assert.equal(atGate.size, concurrency);
    assert.equal(claimSpy.calls.length, 1);

    gate.emit("release");
    await worker.close();
    assert.equal(atGate.size, 0);

    restoreAllSpies();
  });
});

Deno.test("worker: polls again immediately when capacity frees up", async () => {
  await withTestContext("worker", async (ctx) => {
    const absurd = ctx.absurd;
    const gate = new EventEmitter();
    const atGate = new Set<number>();

    gate.on("arrived", (id: number) => atGate.add(id));
    gate.on("left", (id: number) => atGate.delete(id));

    absurd.registerTask<{ id: number }, void>(
      { name: "responsive-task" },
      async (params) => {
        gate.emit("arrived", params.id);
        await once(gate, "release");
        gate.emit("left", params.id);
      },
    );

    for (let i = 1; i <= 3; i++) {
      await absurd.spawn("responsive-task", { id: i });
    }

    const claimSpy = createSpy(absurd, "claimTasks");

    const worker = await absurd.startWorker({
      concurrency: 2,
      pollInterval: 3600,
      workerId: "worker-responsive",
    });

    await waitFor(() => {
      assert.equal(atGate.size, 2);
    }, { timeoutMs: 100 });

    assert.equal(claimSpy.calls.length, 1);
    const initialCallCount = claimSpy.calls.length;

    gate.emit("release");

    await waitFor(() => {
      assert(claimSpy.calls.length > initialCallCount);
    });

    gate.emit("release");
    await worker.close();

    restoreAllSpies();
  });
});

Deno.test("worker: shuts down gracefully", async () => {
  await withTestContext("worker", async (ctx) => {
    const absurd = ctx.absurd;
    const gate = new EventEmitter();
    const started: number[] = [];

    gate.on("started", (id: number) => started.push(id));

    absurd.registerTask<{ id: number }, void>(
      { name: "shutdown-task" },
      async (params) => {
        gate.emit("started", params.id);
        await once(gate, "release");
      },
    );

    await absurd.spawn("shutdown-task", { id: 1 });
    await absurd.spawn("shutdown-task", { id: 2 });

    const worker = await absurd.startWorker({
      concurrency: 1,
      workerId: "worker-shutdown",
    });
    await once(gate, "started");

    const closePromise = worker.close();
    const claimSpy = createSpy(absurd, "claimTasks");

    let resolved = false;
    closePromise.then(() => {
      resolved = true;
    });

    await ctx.sleep(50);
    assert.equal(resolved, false);

    gate.emit("release");

    await waitFor(() => {
      assert.equal(resolved, true);
    }, { timeoutMs: 200 });

    assert.deepEqual(started, [1]);
    assert.equal(claimSpy.calls.length, 0);

    restoreAllSpies();
  });
});

Deno.test("worker: calls onError for unexpected failures", async () => {
  await withTestContext("worker", async (ctx) => {
    const absurd = ctx.absurd;
    const errors: string[] = [];

    absurd.registerTask<void, void>({ name: "test-task" }, async () => {});
    await absurd.spawn("test-task", undefined);

    const executeSpy = createSpy(absurd, "executeTask");
    executeSpy.mockRejectedValueOnce(new Error("Execute failed, PG error"));

    const worker = await absurd.startWorker({
      pollInterval: 0.01,
      workerId: "worker-errors",
      onError: (err) => errors.push(err.message),
    });

    await waitFor(() => {
      assert(errors.includes("Execute failed, PG error"));
    }, { timeoutMs: 200 });

    const claimSpy = createSpy(absurd, "claimTasks");
    claimSpy.mockRejectedValueOnce(new Error("Claim failed"));

    await waitFor(() => {
      assert(errors.includes("Claim failed"));
    }, { timeoutMs: 200 });

    await worker.close();

    assert(errors.includes("Execute failed, PG error"));
    assert(errors.includes("Claim failed"));

    restoreAllSpies();
  });
});
