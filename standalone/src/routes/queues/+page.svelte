<script lang="ts">
  import { mockAbsurdProvider } from "$lib/providers/absurdData";

  const queueSummaries = mockAbsurdProvider.getQueueSummaries();
  const handleRefresh = () => {
    window.location.reload();
  };
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Queues</h1>
    <p class="mt-1 text-sm text-slate-600">
      Inspect queue health, backlog contents, and recent events.
    </p>
  </div>
  <button
    type="button"
    class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:cursor-pointer"
    onclick={handleRefresh}
  >
    Refresh
  </button>
</section>

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
