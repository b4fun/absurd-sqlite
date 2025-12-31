import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";

import { randomName, withTestContext } from "./setup.ts";
import {
  assertArrayContains,
  assertMatchObject,
  rejected,
  resolved,
  waitFor,
} from "./utils.ts";

Deno.test("basic: queue management create/list/drop", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const queueName = randomName("test_queue");
    await absurd.createQueue(queueName);

    let queues = await absurd.listQueues();
    assert(queues.includes(queueName));

    const storage = await ctx.getQueueStorageState(queueName);
    assert.equal(storage.exists, true);
    assertArrayContains(storage.tables, [
      "absurd_tasks",
      "absurd_runs",
      "absurd_events",
      "absurd_waits",
      "absurd_checkpoints",
      "absurd_queues",
    ]);

    await absurd.dropQueue(queueName);

    queues = await absurd.listQueues();
    assert.equal(queues.includes(queueName), false);

    const storageAfterDrop = await ctx.getQueueStorageState(queueName);
    assert.equal(storageAfterDrop.exists, false);
  });
});

Deno.test("basic: spawn with maxAttempts override", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    absurd.registerTask<{ shouldFail: boolean }>(
      { name: "test-max-attempts", defaultMaxAttempts: 5 },
      () => {
        return rejected(new Error("Always fails"));
      },
    );

    const { taskID } = await absurd.spawn("test-max-attempts", undefined, {
      maxAttempts: 2,
    });

    await absurd.workBatch("test-worker-attempts", 60, 1);
    await absurd.workBatch("test-worker-attempts", 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "failed",
      attempts: 2,
    });
  });
});

Deno.test("basic: rejects spawning unregistered task without queue override", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    await assert.rejects(
      () => absurd.spawn("unregistered-task", { value: 1 }),
      (err: unknown) =>
        String((err as { message?: string })?.message ?? "").includes(
          'Task "unregistered-task" is not registered',
        ),
    );
  });
});

Deno.test("basic: rejects spawning registered task on mismatched queue", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const taskName = "registered-queue-task";
    const otherQueue = randomName("other_queue");

    absurd.registerTask(
      { name: taskName, queue: ctx.queueName },
      () => resolved({ success: true }),
    );

    await assert.rejects(
      () => absurd.spawn(taskName, undefined, { queue: otherQueue }),
      (err: unknown) =>
        String((err as { message?: string })?.message ?? "").includes(
          `Task "${taskName}" is registered for queue "${ctx.queueName}"`,
        ),
    );
  });
});

Deno.test("basic: claim tasks with various batch sizes", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    await ctx.cleanupTasks();

    absurd.registerTask<{ id: number }>(
      { name: "test-claim" },
      (params) => {
        return resolved(params);
      },
    );

    const spawned = await Promise.all(
      [1, 2, 3].map((id) => absurd.spawn("test-claim", { id })),
    );

    const claimed = await absurd.claimTasks({
      batchSize: 3,
      claimTimeout: 60,
      workerId: "test-worker",
    });

    assert.equal(claimed.length, 3);
    assert.deepEqual(
      claimed.map((c) => c.task_id).sort(),
      spawned.map((s) => s.taskID).sort(),
    );

    assert.equal((await ctx.getTask(spawned[0].taskID))?.state, "running");

    assert.deepEqual(
      await absurd.claimTasks({
        batchSize: 10,
        claimTimeout: 60,
        workerId: "test-worker-empty",
      }),
      [],
    );
  });
});

Deno.test("basic: scheduleRun moves run between running and sleeping", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    await ctx.cleanupTasks();
    const baseTime = new Date("2024-04-01T10:00:00Z");
    await ctx.setFakeNow(baseTime);

    absurd.registerTask<{ step: string }>(
      { name: "schedule-task" },
      () => {
        return resolved({ done: true });
      },
    );

    const { runID } = await absurd.spawn("schedule-task", { step: "start" });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 120,
    });
    assert.equal(claim.run_id, runID);

    const wakeAt = new Date(baseTime.getTime() + 5 * 60 * 1000);
    await ctx.scheduleRun(runID, wakeAt);

    assertMatchObject(await ctx.getRun(runID), {
      state: "sleeping",
      available_at: wakeAt,
      wake_event: null,
    });

    assert.equal((await ctx.getTask(claim.task_id))?.state, "sleeping");

    await ctx.setFakeNow(wakeAt);
    const [resumed] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 120,
    });
    assert.equal(resumed.run_id, runID);
    assert.equal(resumed.attempt, 1);

    assertMatchObject(await ctx.getRun(runID), {
      state: "running",
      started_at: wakeAt,
    });
  });
});

Deno.test("basic: claim timeout releases run to a new worker", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    await ctx.cleanupTasks();
    const baseTime = new Date("2024-04-02T09:00:00Z");
    await ctx.setFakeNow(baseTime);

    absurd.registerTask<{ step: string }>(
      { name: "lease-task" },
      () => {
        return resolved({ done: true });
      },
    );

    const { taskID } = await absurd.spawn("lease-task", { step: "attempt" });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    assert.equal(claim.task_id, taskID);
    assert.equal((await ctx.getTask(taskID))?.state, "running");

    await ctx.setFakeNow(new Date(baseTime.getTime() + 61 * 1000));
    const [reclaim] = await absurd.claimTasks({
      workerId: "worker-2",
      claimTimeout: 60,
    });

    assert.equal(reclaim.task_id, taskID);

    assertMatchObject(await ctx.getRun(reclaim.run_id), {
      claimed_by: "worker-2",
    });

    const taskRow = await ctx.getTask(taskID);
    assertMatchObject(taskRow, {
      state: "running",
      attempts: 2,
    });
  });
});

Deno.test("basic: cleanup tasks and events respect TTLs", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    await ctx.cleanupTasks();
    const base = new Date("2024-03-01T08:00:00Z");
    await ctx.setFakeNow(base);

    absurd.registerTask<{ step: string }>({ name: "cleanup" }, () => {
      return resolved({ status: "done" });
    });

    const { runID } = await absurd.spawn("cleanup", { step: "start" });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-clean",
      claimTimeout: 60,
    });
    assert.equal(claim.run_id, runID);

    const finishTime = new Date(base.getTime() + 10 * 60 * 1000);
    await ctx.setFakeNow(finishTime);
    await ctx.completeRun(runID, { status: "done" });

    await absurd.emitEvent("cleanup-event", { kind: "notify" });

    assertMatchObject(await ctx.getRun(runID), {
      claimed_by: "worker-clean",
      claim_expires_at: new Date(base.getTime() + 60 * 1000),
    });

    const beforeTTL = new Date(finishTime.getTime() + 30 * 60 * 1000);
    await ctx.setFakeNow(beforeTTL);
    const beforeTasks = await ctx.cleanupTasksByTTL(3600, 10);
    assert.equal(beforeTasks, 0);
    const beforeEvents = await ctx.cleanupEventsByTTL(3600, 10);
    assert.equal(beforeEvents, 0);

    const later = new Date(finishTime.getTime() + 26 * 60 * 60 * 1000);
    await ctx.setFakeNow(later);
    const deletedTasks = await ctx.cleanupTasksByTTL(3600, 10);
    assert.equal(deletedTasks, 1);
    const deletedEvents = await ctx.cleanupEventsByTTL(3600, 10);
    assert.equal(deletedEvents, 1);

    assert.equal(await ctx.getRemainingTasksCount(), 0);
    assert.equal(await ctx.getRemainingEventsCount(), 0);
  });
});

Deno.test("basic: task transitions pending -> running -> completed", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask<{ value: number }>(
      { name: "test-task-complete" },
      async (params, taskCtx) => {
        const doubled = await taskCtx.step("double", () => {
          return resolved(params.value * 2);
        });
        return { doubled };
      },
    );

    const { taskID } = await absurd.spawn("test-task-complete", {
      value: 21,
    });
    assert.equal((await ctx.getTask(taskID))?.state, "pending");

    await absurd.workBatch("test-worker-complete", 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      attempts: 1,
      completed_payload: { doubled: 42 },
    });
  });
});

Deno.test("basic: task transitions to sleeping when suspended", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const eventName = randomName("suspend_event");

    absurd.registerTask(
      { name: "test-task-suspend" },
      async (_params, taskCtx) => {
        return { received: await taskCtx.awaitEvent(eventName) };
      },
    );

    const { taskID } = await absurd.spawn("test-task-suspend", undefined);
    await absurd.workBatch("test-worker-suspend", 60, 1);
    assert.equal((await ctx.getTask(taskID))?.state, "sleeping");

    await absurd.emitEvent(eventName, { data: "wakeup" });
    await absurd.workBatch("test-worker-suspend", 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { received: { data: "wakeup" } },
    });
  });
});

Deno.test("basic: task transitions to failed after retries exhausted", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    absurd.registerTask(
      { name: "test-task-fail", defaultMaxAttempts: 2 },
      () => {
        return rejected(new Error("Task intentionally failed"));
      },
    );

    const { taskID, runID: firstRunID } = await absurd.spawn(
      "test-task-fail",
      undefined,
    );

    await absurd.workBatch("test-worker-fail", 60, 1);
    assert.equal((await ctx.getRun(firstRunID))?.state, "failed");
    assert.equal((await ctx.getTask(taskID))?.state, "pending");

    await absurd.workBatch("test-worker-fail", 60, 1);
    assert.equal((await ctx.getTask(taskID))?.state, "failed");

    const run = await ctx.getRun(firstRunID);
    assertMatchObject(run, {
      state: "failed",
      attempt: 1,
    });
    const failure = run?.failure_reason as
      | Record<string, unknown>
      | null
      | undefined;
    assert(
      String(failure?.message ?? "").includes("Task intentionally failed"),
    );
  });
});

Deno.test("basic: task receives event emitted before spawn", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    absurd.registerTask<{ eventName: string }, { received: unknown }>(
      { name: "test-cached-event" },
      async (params, taskCtx) => {
        const payload = await taskCtx.awaitEvent(params.eventName);
        return { received: payload };
      },
    );

    const eventName = randomName("test_event");
    await absurd.emitEvent(eventName, { data: "cached-payload" });

    const { taskID } = await absurd.spawn("test-cached-event", { eventName });

    await absurd.workBatch("test-worker-cached", 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { received: { data: "cached-payload" } },
    });
  });
});

Deno.test("basic: workBatch processes multiple tasks", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    absurd.registerTask<{ id: number }>(
      { name: "test-work-batch" },
      (params) => {
        return resolved({ result: `task-${params.id}` });
      },
    );

    const tasks = await Promise.all(
      [1, 2, 3].map((id) => absurd.spawn("test-work-batch", { id })),
    );

    await absurd.workBatch("test-worker-batch", 60, 5);

    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i];
      assertMatchObject(await ctx.getTask(task.taskID), {
        state: "completed",
        completed_payload: { result: `task-${i + 1}` },
      });
    }
  });
});

Deno.test("basic: workBatch handles mixed success and failure", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    absurd.registerTask<{ fail: boolean }>(
      { name: "mixed", defaultMaxAttempts: 1 },
      (params) => {
        if (params.fail) {
          return rejected(new Error("Task failed in batch"));
        }
        return resolved({ result: "success" });
      },
    );

    const bad = await absurd.spawn("mixed", { fail: true });
    const ok = await absurd.spawn("mixed", { fail: false });

    await absurd.workBatch("mixed", 60, 2);

    assert.equal((await ctx.getTask(bad.taskID))?.state, "failed");
    assert.equal((await ctx.getTask(ok.taskID))?.state, "completed");
  });
});

Deno.test("basic: heartbeat extends claim timeout", async () => {
  await withTestContext("test_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const gate = new EventEmitter();
    const baseTime = new Date("2025-01-01T00:00:00Z");
    await ctx.setFakeNow(baseTime);

    const claimTimeout = 60;
    const extension = 120;

    absurd.registerTask(
      { name: "heartbeat-extends" },
      async (params: { extension: number }, taskCtx) => {
        gate.emit("task-started");
        await once(gate, "heartbeat");
        await taskCtx.heartbeat(params.extension);
      },
    );

    const { runID } = await absurd.spawn("heartbeat-extends", { extension });

    const getExpiresAt = async (id: string) => {
      const run = await ctx.getRun(id);
      return run?.claim_expires_at ? run.claim_expires_at.getTime() : 0;
    };

    void absurd.workBatch("test-worker", claimTimeout);

    await once(gate, "task-started");
    await waitFor(async () => {
      assert.equal(
        await getExpiresAt(runID),
        baseTime.getTime() + claimTimeout * 1000,
      );
    });

    gate.emit("heartbeat");

    await waitFor(async () => {
      assert.equal(
        await getExpiresAt(runID),
        baseTime.getTime() + extension * 1000,
      );
    });
  });
});
