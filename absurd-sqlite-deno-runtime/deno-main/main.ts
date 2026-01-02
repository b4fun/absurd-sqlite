import { Database } from "@db/sqlite";
import { Absurd, openDenoDatabase } from "@absurd-sqlite/sdk";

async function main() {
  console.log(Database);

  const dbPath = mustResolveDatabasePath();
  const extensionPath = mustResolveExtensionPath();
  const userModulePaths = mustResolveUserModulePaths();

  const db = openDenoDatabase(dbPath);
  const absurd = new Absurd(db, extensionPath);

  for (const userModulePath of userModulePaths) {
    const setupFunction = await tryImportUserModule(userModulePath);
    setupFunction(absurd);
  }

  await absurd.startWorker({
    concurrency: 2,
  });
}

function mustResolveDatabasePath(): string {
  const rv = Deno.env.get("ABSURD_DATABASE_PATH");
  if (!rv) {
    throw new Error("ABSURD_DATABASE_PATH environment variable is not set");
  }
  return rv;
}

function mustResolveExtensionPath(): string {
  const rv = Deno.env.get("ABSURD_EXTENSION_PATH");
  if (!rv) {
    throw new Error("ABSURD_EXTENSION_PATH environment variable is not set");
  }
  return rv;
}

function mustResolveUserModulePaths(): string[] {
  const rv = Deno.env.get("ABSURD_USER_MODULE_PATHS");
  if (!rv) {
    return [];
  }
  return rv
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

type UserModuleSetupFunction = (absurd: Absurd) => void;

async function tryImportUserModule(
  userModulePath: string
): Promise<UserModuleSetupFunction> {
  const userModule = await import(userModulePath);
  if (typeof userModule !== "function") {
    throw new Error(
      `User module at ${userModulePath} does not export a 'setup' function`
    );
  }

  return userModule as UserModuleSetupFunction;
}

main().catch((err) => {
  console.error("Fatal error:", err);
  Deno.exit(1);
});
