<script lang="ts">
  import { page } from "$app/state";
  import { onMount } from "svelte";
  import Button from "$lib/components/Button.svelte";
  import JsonBlock from "$lib/components/JsonBlock.svelte";
  import {
    getAbsurdProvider,
    type TaskCheckpoint,
    type TaskInfo,
    type TaskRun,
  } from "$lib/providers/absurdData";

  const provider = getAbsurdProvider();
  const taskId = $derived(page.params.taskId ?? "");
  let runs = $state<TaskRun[]>([]);
  let taskInfo = $state<TaskInfo | null>(null);
  let taskCheckpoints = $state<TaskCheckpoint[]>([]);
  let isReady = $state(false);
  const sortedRuns = $derived([...runs].sort((a, b) => b.attemptNumber - a.attemptNumber));
  const taskName = $derived(runs[0]?.name ?? taskInfo?.name ?? "Unknown");
  const queueName = $derived(runs[0]?.queue ?? taskInfo?.queue ?? "default");
  const latestUpdatedAgo = $derived(sortedRuns[0]?.updatedAgo ?? "—");
  const formatDuration = (durationMs: number) => {
    const totalSeconds = Math.floor(durationMs / 1000);
    const days = Math.floor(totalSeconds / 86_400);
    const hours = Math.floor((totalSeconds % 86_400) / 3_600);
    const minutes = Math.floor((totalSeconds % 3_600) / 60);
    const seconds = totalSeconds % 60;
    const parts: string[] = [];

    if (days > 0) parts.push(`${days}d`);
    if (hours > 0 || parts.length > 0) parts.push(`${hours}h`);
    if (minutes > 0 || parts.length > 0) parts.push(`${minutes}m`);
    parts.push(`${seconds}s`);

    return parts.join(" ");
  };
  const durationLabel = $derived(
    (() => {
      if (runs.length === 0) return "—";
      const createdValues = runs.map((run) => run.createdAtMs).filter(Number.isFinite);
      const updatedValues = runs.map((run) => run.updatedAtMs).filter(Number.isFinite);
      if (createdValues.length === 0 || updatedValues.length === 0) return "—";
      const minCreated = Math.min(...createdValues);
      const maxUpdated = Math.max(...updatedValues);
      if (!Number.isFinite(minCreated) || !Number.isFinite(maxUpdated)) return "—";
      return formatDuration(Math.max(0, maxUpdated - minCreated));
    })()
  );
  const completionLabel = $derived(
    runs.some((run) => run.status === "completed") ? "Completed" : "Not completed"
  );
  const checkpointCountLabel = $derived(
    taskInfo ? taskInfo.checkpointCount.toString() : "—"
  );
  const checkpointsByRunId = $derived(
    taskCheckpoints.reduce<Map<string, TaskCheckpoint[]>>((map, checkpoint) => {
      const runId = checkpoint.ownerRunId ?? "unowned";
      const entry = map.get(runId) ?? [];
      entry.push(checkpoint);
      map.set(runId, entry);
      return map;
    }, new Map())
  );

  const statusStyles: Record<TaskRun["status"], string> = {
    running: "border-emerald-200 bg-emerald-50 text-emerald-700",
    failed: "border-rose-200 bg-rose-50 text-rose-700",
    completed: "border-slate-200 bg-slate-100 text-slate-700",
    sleeping: "border-amber-200 bg-amber-50 text-amber-700",
    pending: "border-sky-200 bg-sky-50 text-sky-700",
    cancelled: "border-zinc-200 bg-zinc-50 text-zinc-600",
  };
  const refreshRuns = async () => {
    if (!taskId) {
      runs = [];
      taskInfo = null;
      taskCheckpoints = [];
      return;
    }
    runs = await provider.getTaskHistory(taskId);
    taskInfo = await provider.getTaskInfo(taskId);
    taskCheckpoints = await provider.getTaskCheckpoints(taskId);
  };

  $effect(() => {
    if (!isReady) return;
    void refreshRuns();
  });

  onMount(() => {
    isReady = true;
  });
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Task &quot;{taskName}&quot;</h1>
    <p class="mt-1 text-sm text-slate-600">
      <span class="text-slate-500">Task ID:</span>
      <a class="ml-2 font-mono text-xs text-blue-600 hover:underline" href={`/tasks/${taskId}`}>
        {taskId}
      </a>
    </p>
    <p class="mt-1 text-sm text-slate-600">
      <span class="text-slate-500">Queues:</span> {queueName}
    </p>
  </div>
  <div class="flex items-center gap-3 text-sm text-slate-600">
    <a href="/tasks" class="hover:text-slate-900">← Back to runs</a>
    <Button
      type="button"
      class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700"
      onclick={() => void refreshRuns()}
    >
      Refresh
    </Button>
  </div>
</section>

<section class="mt-6 rounded-lg border border-black/10 bg-white px-4 py-3 text-sm text-slate-600">
  <div class="flex flex-wrap items-center gap-4">
    <div class="flex items-center gap-2">
      <span class="text-slate-500">Runs</span>
      <span class="font-medium text-slate-900">{runs.length}</span>
    </div>
    <div class="flex flex-wrap items-center gap-2">
      <span class="text-slate-500">Statuses</span>
      {#each sortedRuns as run}
        <span
          class={`rounded-full border px-2 py-0.5 text-xs font-medium ${statusStyles[run.status]}`}
        >
          {run.status}
        </span>
      {/each}
    </div>
    <div class="flex items-center gap-2">
      <span class="text-slate-500">Duration</span>
      <span class="font-medium text-slate-900">{durationLabel}</span>
    </div>
    <div class="flex items-center gap-2">
      <span class="text-slate-500">Completion</span>
      <span class="font-medium text-slate-900">{completionLabel}</span>
    </div>
    <div class="flex items-center gap-2">
      <span class="text-slate-500">Queue</span>
      <span class="font-medium text-slate-900">{queueName}</span>
    </div>
    <div class="flex items-center gap-2">
      <span class="text-slate-500">Checkpoints</span>
      <span class="font-medium text-slate-900">{checkpointCountLabel}</span>
    </div>
    <div class="flex items-center gap-2">
      <span class="text-slate-500">Updated</span>
      <span class="font-medium text-slate-900">{latestUpdatedAgo}</span>
    </div>
  </div>
</section>

<section class="mt-4 space-y-4">
  {#if sortedRuns.length === 0}
    <div class="rounded-lg border border-black/10 bg-white p-6 text-sm text-slate-500">
      No history found for this task.
    </div>
  {:else}
    {#each sortedRuns as run}
      <article class="rounded-lg border border-black/10 bg-white p-4 shadow-sm">
        <div class="flex flex-wrap items-center gap-2 text-sm text-slate-700">
          <span class="text-slate-500">Run ID:</span>
          <a class="font-mono text-xs text-blue-600 hover:underline" href={`/tasks/${run.id}`}>
            {run.runId}
          </a>
          <span
            class={`rounded-full border px-2 py-0.5 text-xs font-medium ${statusStyles[run.status]}`}
          >
            {run.status}
          </span>
        </div>
        <div class="mt-2 flex flex-wrap gap-4 text-xs text-slate-500">
          <div><span class="text-slate-400">Queue:</span> {run.queue}</div>
          <div><span class="text-slate-400">Attempt:</span> {run.attemptNumber}</div>
          <div><span class="text-slate-400">Created:</span> {run.createdAgo}</div>
          <div><span class="text-slate-400">Updated:</span> {run.updatedAgo}</div>
        </div>
        <div class="mt-2 flex flex-wrap items-center gap-2 text-xs text-slate-500">
          <span class="text-slate-400">Checkpoints:</span>
          {#if checkpointsByRunId.get(run.runId)?.length}
            {#each checkpointsByRunId.get(run.runId) ?? [] as checkpoint}
              <span
                class={`rounded-full border px-2 py-0.5 text-xs font-medium ${
                  checkpoint.status === "committed"
                    ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                    : "border-slate-200 bg-slate-50 text-slate-700"
                }`}
              >
                {checkpoint.name}
              </span>
            {/each}
          {:else}
            <span class="text-slate-400">—</span>
          {/if}
        </div>

        <div class="mt-4">
          <JsonBlock title="Parameters" value={run.paramsJson} emptyText={"{}"} />
        </div>

        <div class="mt-3">
          <JsonBlock
            title="Final State"
            value={run.finalStateJson}
            emptyText="No final state yet."
          />
        </div>
      </article>
    {/each}
  {/if}
</section>
