<script lang="ts">
  import "../app.css";

  // Tauri doesn't have a Node.js server to do proper SSR
  // so we use adapter-static with a fallback to index.html to put the site in SPA mode
  // See: https://svelte.dev/docs/kit/single-page-apps
  // See: https://v2.tauri.app/start/frontend/sveltekit/ for more info
  export const ssr = false;

  import { page } from "$app/state";

  const navItems = [
    { label: "Overview", href: "/", dotClass: "bg-emerald-500" },
    { label: "Tasks", href: "/tasks", dotClass: "bg-blue-500" },
    { label: "Events", href: "/events", dotClass: "bg-purple-500" },
    { label: "Queues", href: "/queues", dotClass: "bg-orange-400" },
  ];

  const pathname = $derived(page.url.pathname);
</script>

<div class="min-h-screen text-slate-900">
  <header class="border-b border-black/10 bg-white">
    <div class="mx-auto flex w-full max-w-screen-2xl items-center justify-between px-8 py-4">
      <p class="text-base font-semibold text-slate-900">Absurd Habitat</p>
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

  <main class="mx-auto w-full max-w-screen-2xl px-8 py-8">
    <slot />
  </main>
</div>
