<script lang="ts">
  import Button from "$lib/components/Button.svelte";

  type CopyStatus = "idle" | "copied" | "error";

  const {
    title,
    value,
    emptyText = "No data yet.",
  }: {
    title: string;
    value: string | null | undefined;
    emptyText?: string;
  } = $props();

  let copyStatus = $state<CopyStatus>("idle");

  const formattedValue = $derived.by(() => {
    if (!value) return "";
    const trimmed = value.trim();
    if (!trimmed) return "";
    try {
      const parsed = JSON.parse(trimmed);
      return JSON.stringify(parsed, null, 2);
    } catch {
      return trimmed;
    }
  });

  const handleCopy = async () => {
    const text = formattedValue || "{}";
    try {
      await navigator.clipboard.writeText(text);
      copyStatus = "copied";
    } catch {
      copyStatus = "error";
    }
    setTimeout(() => {
      copyStatus = "idle";
    }, 2000);
  };
</script>

<div class="rounded-md border border-black/10 overflow-hidden">
  <div class="flex items-center justify-between border-b border-black/10 bg-slate-50 px-3 py-2 text-xs text-slate-500">
    <span>{title}</span>
    <Button type="button" class="hover:text-slate-700" onclick={handleCopy}>
      {copyStatus === "copied" ? "Copied" : copyStatus === "error" ? "Copy failed" : "Copy"}
    </Button>
  </div>
  {#if formattedValue}
    <pre class="inline-block max-w-full whitespace-pre-wrap bg-white px-3 py-3 font-mono text-xs text-slate-700">
{formattedValue}
    </pre>
  {:else}
    <div class="bg-white px-3 py-3 text-xs text-slate-500">{emptyText}</div>
  {/if}
</div>
