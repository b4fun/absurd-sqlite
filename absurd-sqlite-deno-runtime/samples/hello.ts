import { Absurd } from "../../sdks/deno/mod.ts";

export default function setup(absurd: Absurd) {
  absurd.registerTask(
    {
      name: "hello",
    },
    async (params, ctx) => {
      await ctx.step("init", async () => {
        console.log("init step");
        ctx.emitEvent("progress", { message: "Initialization complete" });
      });

      await ctx.sleepFor("back off 15s", 15);

      await ctx.step("process", async () => {
        console.log("process step");
        ctx.emitEvent("progress", { message: "Processing complete" });
      });

      const name = params.name || "world";

      console.log(`Saying hello to ${name}`);
      return { greeting: `Hello, ${name}!` };
    },
  );
}
