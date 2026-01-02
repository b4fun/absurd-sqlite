import assert from "node:assert/strict";

import { TimeoutError } from "../mod.ts";

import { randomName, withTestContext } from "./setup.ts";
import { assertMatchObject } from "./utils.ts";

Deno.test("events: await and emit event flow", async () => {
  await withTestContext("event_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const eventName = randomName("test_event");

    absurd.registerTask({ name: "waiter" }, async (_params, taskCtx) => {
      const payload = await taskCtx.awaitEvent(eventName, { timeout: 60 });
      return { received: payload };
    });

    const { taskID, runID } = await absurd.spawn("waiter", { step: 1 });

    await absurd.workBatch("worker1", 60, 1);

    assertMatchObject(await ctx.getRun(runID), {
      state: "sleeping",
      wake_event: eventName,
    });

    const payload = { value: 42 };
    await absurd.emitEvent(eventName, payload);

    assert.equal((await ctx.getRun(runID))?.state, "pending");

    await absurd.workBatch("worker1", 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { received: payload },
    });
  });
});

Deno.test("events: cached event emitted before await", async () => {
  await withTestContext("event_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const eventName = randomName("cached_event");
    const payload = { data: "pre-emitted" };

    await absurd.emitEvent(eventName, payload);

    absurd.registerTask({ name: "late-waiter" }, async (_params, taskCtx) => {
      const received = await taskCtx.awaitEvent(eventName);
      return { received };
    });

    const { taskID } = await absurd.spawn("late-waiter", undefined);

    await absurd.workBatch("worker1", 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { received: payload },
    });
  });
});

Deno.test("events: awaitEvent with timeout expires and wakes task", async () => {
  await withTestContext("event_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const eventName = randomName("timeout_event");
    const baseTime = new Date("2024-05-01T10:00:00Z");
    const timeoutSeconds = 600;

    await ctx.setFakeNow(baseTime);

    absurd.registerTask(
      { name: "timeout-waiter" },
      async (_params, taskCtx) => {
        try {
          const payload = await taskCtx.awaitEvent(eventName, {
            timeout: timeoutSeconds,
          });
          return { timedOut: false, result: payload };
        } catch (err) {
          if (err instanceof TimeoutError) {
            return { timedOut: true, result: null };
          }
          throw err;
        }
      },
    );

    const { taskID, runID } = await absurd.spawn("timeout-waiter", undefined);
    await absurd.workBatch("worker1", 120, 1);

    assert.equal(await ctx.getWaitsCount(), 1);

    const sleepingRun = await ctx.getRun(runID);
    assertMatchObject(sleepingRun, {
      state: "sleeping",
      wake_event: eventName,
    });

    const expectedWake = new Date(baseTime.getTime() + timeoutSeconds * 1000);
    assert.equal(sleepingRun?.available_at?.getTime(), expectedWake.getTime());

    await ctx.setFakeNow(new Date(expectedWake.getTime() + 1000));
    await absurd.workBatch("worker1", 120, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { timedOut: true, result: null },
    });

    assert.equal(await ctx.getWaitsCount(), 0);
  });
});

Deno.test("events: multiple tasks can await same event", async () => {
  await withTestContext("event_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const eventName = randomName("broadcast_event");

    absurd.registerTask<{ taskNum: number }>(
      { name: "multi-waiter" },
      async (params, taskCtx) => {
        const payload = await taskCtx.awaitEvent(eventName);
        return { taskNum: params.taskNum, received: payload };
      },
    );

    const tasks = await Promise.all([
      absurd.spawn("multi-waiter", { taskNum: 1 }),
      absurd.spawn("multi-waiter", { taskNum: 2 }),
      absurd.spawn("multi-waiter", { taskNum: 3 }),
    ]);

    await absurd.workBatch("worker1", 60, 10);

    for (const task of tasks) {
      assert.equal((await ctx.getTask(task.taskID))?.state, "sleeping");
    }

    const payload = { data: "broadcast" };
    await absurd.emitEvent(eventName, payload);

    await absurd.workBatch("worker1", 60, 10);

    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i];
      assertMatchObject(await ctx.getTask(task.taskID), {
        state: "completed",
        completed_payload: { taskNum: i + 1, received: payload },
      });
    }
  });
});

Deno.test("events: awaitEvent timeout does not recreate wait on resume", async () => {
  await withTestContext("event_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const eventName = randomName("timeout_no_loop");
    const baseTime = new Date("2024-05-02T11:00:00Z");
    await ctx.setFakeNow(baseTime);

    absurd.registerTask(
      { name: "timeout-no-loop" },
      async (_params, taskCtx) => {
        try {
          await taskCtx.awaitEvent(eventName, {
            stepName: "wait",
            timeout: 10,
          });
          return { stage: "unexpected" };
        } catch (err) {
          if (err instanceof TimeoutError) {
            const payload = await taskCtx.awaitEvent(eventName, {
              stepName: "wait",
              timeout: 10,
            });
            return { stage: "resumed", payload };
          }
          throw err;
        }
      },
    );

    const { taskID, runID } = await absurd.spawn("timeout-no-loop", undefined);
    await absurd.workBatch("worker-timeout", 60, 1);

    assert.equal(await ctx.getWaitsCount(), 1);

    await ctx.setFakeNow(new Date(baseTime.getTime() + 15 * 1000));
    await absurd.workBatch("worker-timeout", 60, 1);

    assert.equal(await ctx.getWaitsCount(), 0);

    assert.equal((await ctx.getRun(runID))?.state, "completed");
    assert.equal((await ctx.getTask(taskID))?.state, "completed");
  });
});
