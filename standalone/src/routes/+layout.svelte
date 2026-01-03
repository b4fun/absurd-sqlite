<script lang="ts">
  import "../app.css";

  // Tauri doesn't have a Node.js server to do proper SSR
  // so we use adapter-static with a fallback to index.html to put the site in SPA mode
  // See: https://svelte.dev/docs/kit/single-page-apps
  // See: https://v2.tauri.app/start/frontend/sveltekit/ for more info
  let { children } = $props();

  import { onMount } from "svelte";
  import { page } from "$app/state";

  const navItems = [
    { label: "Overview", href: "/", dotClass: "bg-emerald-500" },
    { label: "Tasks", href: "/tasks", dotClass: "bg-blue-500" },
    { label: "Events", href: "/events", dotClass: "bg-purple-500" },
    { label: "Queues", href: "/queues", dotClass: "bg-orange-400" },
    { label: "Settings", href: "/settings", dotClass: "bg-rose-500" },
  ];

  const pathname = $derived(page.url.pathname);
  let isCompact = $state(false);

  onMount(() => {
    const updateHeader = () => {
      isCompact = window.scrollY > 12;
    };
    updateHeader();
    window.addEventListener("scroll", updateHeader, { passive: true });
    return () => {
      window.removeEventListener("scroll", updateHeader);
    };
  });
</script>

<div class="min-h-screen text-slate-900">
  <header class="sticky top-0 z-20 border-b border-black/10 bg-white transition-all duration-200" class:header-compact={isCompact}>
    <div class="mx-auto flex w-full max-w-screen-2xl items-center justify-between px-8 py-4 transition-all duration-200 header-inner">
      <div class="flex items-center gap-2 text-base font-semibold text-slate-900 header-logo select-none">
        <img src="/logo.png" alt="Absurd SQLite logo" class="h-8 w-8 header-logo-icon" />
        <span class="header-logo-text">Absurd SQLite</span>
      </div>
      <nav class="flex items-center gap-3 text-sm text-slate-600">
        {#each navItems as item}
          <a
            href={item.href}
            class={`flex items-center gap-2 rounded-lg px-3 py-2 ${
              pathname === item.href
                ? "border border-black/10 bg-white text-slate-900 shadow-sm"
                : "text-slate-600 hover:text-slate-900"
            }`}
            class:font-semibold={pathname === item.href}
            aria-current={pathname === item.href ? "page" : undefined}
          >
            <span class={`h-2 w-2 rounded-full ${item.dotClass}`}></span>
            {item.label}
          </a>
        {/each}
      </nav>
    </div>
  </header>

  <main class="mx-auto w-full max-w-screen-2xl px-8 py-8 pb-20 content-body">
    {@render children?.()}
  </main>
</div>

<style>
  header.header-compact .header-logo-text {
    opacity: 0;
    visibility: hidden;
    pointer-events: none;
  }

  header.header-compact .header-logo-icon {
    height: 1.5rem;
    width: 1.5rem;
  }

  header.header-compact .header-inner {
    padding-top: 0.25rem;
    padding-bottom: 0.25rem;
  }

  .header-logo {
    transition: none;
  }

  .content-body {
    padding-bottom: var(--app-header-height, 4rem);
  }

  :global(:root) {
    --app-header-height: 4rem;
  }
</style>
