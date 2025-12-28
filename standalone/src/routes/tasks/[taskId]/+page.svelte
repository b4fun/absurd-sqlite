<script lang="ts">
  import { page } from "$app/state";
  import { mockAbsurdProvider, type TaskRun } from "$lib/providers/absurdData";

  const taskId = $derived(page.params.taskId ?? "");
  const runs = $derived(mockAbsurdProvider.getTaskHistory(taskId));
  const sortedRuns = $derived([...runs].sort((a, b) => b.attemptNumber - a.attemptNumber));
  const taskName = $derived(runs[0]?.name ?? "Unknown");
  const queueName = $derived(runs[0]?.queue ?? "default");
  const latestUpdatedAgo = $derived(sortedRuns[0]?.updatedAgo ?? "—");
  const durationLabel = "8m 2s";
  const completionLabel = $derived(
    runs.some((run) => run.status === "completed") ? "Completed" : "Not completed"
  );

  const statusStyles: Record<TaskRun["status"], string> = {
    running: "border-emerald-200 bg-emerald-50 text-emerald-700",
    failed: "border-rose-200 bg-rose-50 text-rose-700",
    completed: "border-slate-200 bg-slate-100 text-slate-700",
    sleeping: "border-amber-200 bg-amber-50 text-amber-700",
    pending: "border-sky-200 bg-sky-50 text-sky-700",
  };
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
    <button
      type="button"
      class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:cursor-pointer"
      onclick={() => window.location.reload()}
    >
      Refresh
    </button>
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
      <span class="font-medium text-slate-900">0</span>
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

        <div class="mt-4 rounded-md border border-black/10">
          <div class="flex items-center justify-between border-b border-black/10 bg-slate-50 px-3 py-2 text-xs text-slate-500">
            <span>Parameters</span>
            <button type="button" class="hover:text-slate-700 hover:cursor-pointer">Copy</button>
          </div>
          <pre class="whitespace-pre-wrap bg-white px-3 py-3 font-mono text-xs text-slate-700">{run.paramsJson || "{}"}</pre>
        </div>

        {#if run.finalStateJson}
          <div class="mt-3 rounded-md border border-black/10">
            <div class="flex items-center justify-between border-b border-black/10 bg-slate-50 px-3 py-2 text-xs text-slate-500">
              <span>Final State</span>
              <button type="button" class="hover:text-slate-700 hover:cursor-pointer">Copy</button>
            </div>
            <pre class="whitespace-pre-wrap bg-white px-3 py-3 font-mono text-xs text-slate-700">{run.finalStateJson}</pre>
          </div>
        {/if}
      </article>
    {/each}
  {/if}
</section>
