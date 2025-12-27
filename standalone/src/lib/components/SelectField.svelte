<script lang="ts">
  export let label: string;
  export let id: string = "";
  export let value: string = "";

  $: selectId = id || label.toLowerCase().replace(/\s+/g, "-");
</script>

<label class="flex flex-col gap-2 text-sm font-medium text-slate-600" for={selectId}>
  {label}
  <div class="relative">
    <select
      id={selectId}
      bind:value
      class="w-full appearance-none rounded-md border border-black/10 bg-white px-3 py-2 pr-8 text-sm text-slate-700"
      {...$$restProps}
    >
      <slot />
    </select>
    <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-slate-400">▾</span>
  </div>
</label>
