<script lang="ts">
  import { goto } from "$app/navigation";
  import { page } from "$app/state";
  import SelectField from "$lib/components/SelectField.svelte";
  import { mockAbsurdProvider } from "$lib/providers/absurdData";

  const allQueuesLabel = "All queues";
  const urlQueue = $derived(page.url.searchParams.get("queue") ?? allQueuesLabel);
  const urlEventName = $derived(page.url.searchParams.get("eventName") ?? "");
  let selectedQueue = $state(urlQueue);
  let eventNameQuery = $state(urlEventName);
  let lastUrlQueue = $state(urlQueue);
  let lastUrlEventName = $state(urlEventName);
  const filters = $derived(mockAbsurdProvider.getEventFilterDefaults(selectedQueue));
  const filteredEvents = $derived(
    mockAbsurdProvider.getFilteredEvents({
      queueName: selectedQueue,
      eventName: eventNameQuery,
    })
  );

  const updateQuery = (updates: Record<string, string | null>) => {
    const url = new URL(page.url);
    Object.entries(updates).forEach(([key, value]) => {
      if (!value) {
        url.searchParams.delete(key);
      } else {
        url.searchParams.set(key, value);
      }
    });
    const nextUrl = `${url.pathname}${url.searchParams.toString() ? `?${url.searchParams}` : ""}`;
    if (nextUrl !== `${page.url.pathname}${page.url.search}`) {
      goto(nextUrl, { replaceState: true, keepFocus: true, noScroll: true });
    }
  };

  $effect(() => {
    if (urlQueue !== lastUrlQueue) {
      lastUrlQueue = urlQueue;
      if (selectedQueue !== urlQueue) {
        selectedQueue = urlQueue;
      }
    }
    if (urlEventName !== lastUrlEventName) {
      lastUrlEventName = urlEventName;
      if (eventNameQuery !== urlEventName) {
        eventNameQuery = urlEventName;
      }
    }
  });

  $effect(() => {
    updateQuery({
      queue: selectedQueue === allQueuesLabel ? null : selectedQueue,
      eventName: eventNameQuery.trim() === "" ? null : eventNameQuery.trim(),
    });
  });
  const handleRefresh = () => {
    window.location.reload();
  };
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Event log</h1>
    <p class="mt-1 text-sm text-slate-600">
      Inspect emitted events across queues with filterable payloads.
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

<section class="mt-8 grid gap-6 lg:grid-cols-[1.1fr_2fr]">
  <div class="rounded-lg border border-black/10 bg-white p-6">
    <div>
      <h2 class="text-2xl font-semibold text-slate-900">Filters</h2>
      <p class="mt-1 text-sm text-slate-500">Narrow the event list by queue and event name.</p>
    </div>

    <div class="mt-6 space-y-4">
      <label class="flex flex-col gap-2 text-sm font-medium text-slate-600">
        Event name
        <input
          type="text"
          placeholder={filters.eventNamePlaceholder}
          class="rounded-md border border-black/10 bg-white px-3 py-2 text-sm text-slate-700"
          bind:value={eventNameQuery}
        />
      </label>

      <SelectField label="Queue" bind:value={selectedQueue}>
        {#each filters.queueOptions as option}
          <option value={option}>{option}</option>
        {/each}
      </SelectField>
    </div>
  </div>

  <div class="rounded-lg border border-black/10 bg-white p-6">
    <div>
      <h2 class="text-2xl font-semibold text-slate-900">Event timeline</h2>
      <p class="mt-1 text-sm text-slate-500">
        Showing the most recent events matching your filters.
      </p>
    </div>

    {#if filteredEvents.length === 0}
      <div class="mt-6 rounded-lg border border-dashed border-black/20 bg-white px-6 py-10 text-center">
        <p class="text-sm text-slate-500">No events matched the selected filters.</p>
      </div>
    {:else}
      <div class="mt-6 space-y-3">
        {#each filteredEvents as event}
          <div class="rounded-lg border border-black/10 bg-white px-4 py-3">
            <div class="flex flex-wrap items-center justify-between gap-2 text-sm">
              <div class="flex items-center gap-2">
                <span class="font-medium text-slate-800">{event.name}</span>
                <span class="text-xs text-slate-500">•</span>
                <span class="text-xs text-slate-500">{event.queue}</span>
              </div>
              <span class="text-xs text-slate-500">{event.createdAt}</span>
            </div>
            <pre class="mt-2 whitespace-pre-wrap rounded-md border border-black/5 bg-slate-50 px-3 py-2 font-mono text-xs text-slate-600">
{event.payloadPreview}
            </pre>
          </div>
        {/each}
      </div>
    {/if}
  </div>
</section>
