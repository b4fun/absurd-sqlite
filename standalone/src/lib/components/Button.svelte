<script lang="ts">
  let {
    children,
    class: className = "",
    type = "button",
    disabled = false,
    ...rest
  } = $props<{
    children?: () => unknown;
    class?: string;
    type?: "button" | "submit" | "reset";
    disabled?: boolean;
  }>();

  const classes = $derived(["cursor-pointer", className].filter(Boolean).join(" "));
</script>

<button type={type} class={classes} disabled={disabled} {...rest}>
  {@render children?.()}
</button>
