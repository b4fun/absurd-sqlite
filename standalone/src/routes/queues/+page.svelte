<script lang="ts">
  import { onMount } from "svelte";
  import Button from "$lib/components/Button.svelte";
  import { getAbsurdProvider, type QueueSummary } from "$lib/providers/absurdData";

  const provider = getAbsurdProvider();
  let queueSummaries = $state<QueueSummary[]>([]);
  let isCreating = $state(false);
  let isCreateOpen = $state(false);
  let newQueueName = $state("");
  let createError = $state<string | null>(null);
  const refreshData = async () => {
    queueSummaries = await provider.getQueueSummaries();
  };

  const handleRefresh = () => {
    void refreshData();
  };

  const openCreateQueue = () => {
    newQueueName = "";
    createError = null;
    isCreateOpen = true;
  };

  const closeCreateQueue = () => {
    if (isCreating) return;
    isCreateOpen = false;
  };

  const handleCreateQueue = async (event?: SubmitEvent) => {
    event?.preventDefault();
    if (isCreating) return;
    const trimmedName = newQueueName.trim();
    if (!trimmedName) {
      createError = "Queue name is required.";
      return;
    }

    isCreating = true;
    createError = null;
    try {
      await provider.createQueue(trimmedName);
      await refreshData();
      isCreateOpen = false;
    } catch (error) {
      console.error("Failed to create queue", error);
      createError = "Failed to create queue.";
    } finally {
      isCreating = false;
    }
  };

  onMount(() => {
    void refreshData();
  });
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Queues</h1>
    <p class="mt-1 text-sm text-slate-600">
      Inspect queue health, backlog contents, and recent events.
    </p>
  </div>
  <div class="flex items-center gap-3">
    <Button
      type="button"
      class="rounded-md border border-black/10 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
      onclick={openCreateQueue}
      disabled={isCreating}
    >
      New queue
    </Button>
    <Button
      type="button"
      class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700"
      onclick={handleRefresh}
    >
      Refresh
    </Button>
  </div>
</section>

{#if isCreateOpen}
  <div
    class="fixed inset-0 z-10 flex items-center justify-center bg-slate-900/30 p-4"
    role="dialog"
    aria-modal="true"
    aria-label="Create queue"
  >
    <form class="w-full max-w-md rounded-xl bg-white p-6 shadow-xl" onsubmit={handleCreateQueue}>
      <h2 class="text-lg font-semibold text-slate-900">Create new queue</h2>
      <p class="mt-1 text-sm text-slate-500">Queues are created immediately and show up below.</p>
      <label class="mt-4 flex flex-col gap-2 text-sm font-medium text-slate-700">
        Queue name
        <input
          type="text"
          bind:value={newQueueName}
          placeholder="e.g. billing-webhooks"
          autocorrect="off"
          autocomplete="off"
          autocapitalize="off"
          class="rounded-md border border-black/10 bg-white px-3 py-2 text-sm text-slate-700"
          autofocus
        />
      </label>
      {#if createError}
        <p class="mt-2 text-sm text-rose-600">{createError}</p>
      {/if}
      <div class="mt-6 flex items-center justify-end gap-3">
        <Button
          type="button"
          class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm text-slate-700"
          onclick={closeCreateQueue}
          disabled={isCreating}
        >
          Cancel
        </Button>
        <Button
          type="submit"
          class="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
          disabled={isCreating}
        >
          {isCreating ? "Creating..." : "Create queue"}
        </Button>
      </div>
    </form>
  </div>
{/if}

<section class="mt-8 space-y-6">
  {#each queueSummaries as queue}
    <article class="rounded-lg border border-black/10 bg-white p-6">
      <div class="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div class="flex items-center gap-3">
            <h2 class="text-2xl font-semibold text-slate-900">{queue.name}</h2>
            <span class="text-sm text-slate-500">{queue.age}</span>
          </div>
          <p class="mt-1 text-sm text-slate-500">{queue.createdAt}</p>
        </div>
        <div class="flex items-center gap-3">
          <a
            href={`/tasks?queue=${queue.name}`}
            class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm text-slate-700"
          >
            Tasks →
          </a>
          <a
            href={`/events?queue=${queue.name}`}
            class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm text-slate-700"
          >
            Events →
          </a>
        </div>
      </div>

      <div class="mt-6 grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
        {#each queue.stats as stat}
          <div class="rounded-lg border border-black/10 bg-white p-4 text-center">
            <p class="text-sm text-slate-500">{stat.label}</p>
            <p class="mt-2 text-2xl font-semibold text-slate-900">{stat.value}</p>
          </div>
        {/each}
      </div>
    </article>
  {/each}
</section>
