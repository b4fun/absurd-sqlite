import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, describe, expect, it } from "vitest";

const packageRoot = fileURLToPath(new URL("../", import.meta.url));
const nodeModulesDir = join(packageRoot, "node_modules");
const npmBinary = process.platform === "win32" ? "npm.cmd" : "npm";
const tempDirs: string[] = [];

beforeAll(() => {
  execFileSync(npmBinary, ["run", "build", "--silent"], {
    cwd: packageRoot,
    stdio: "inherit",
  });
});

afterEach(() => {
  for (const tempDir of tempDirs.splice(0)) {
    rmSync(tempDir, { recursive: true, force: true });
  }
});

describe("package entrypoints", () => {
  it("includes the packaged license text", () => {
    const packOutput = execFileSync(
      npmBinary,
      ["pack", "--json", "--dry-run", "--ignore-scripts", "--silent"],
      {
        cwd: packageRoot,
        encoding: "utf8",
      }
    );

    const [{ files }] = JSON.parse(packOutput) as Array<{
      files: Array<{ path: string }>;
    }>;

    expect(files.map((file) => file.path)).toContain("LICENSE");
  });

  it("supports both Node ESM import and CommonJS require", () => {
    const tempDir = mkdtempSync(join(tmpdir(), "absurd-sdk-entrypoints-"));
    tempDirs.push(tempDir);

    const tempNodeModulesDir = join(tempDir, "node_modules");
    mkdirSync(join(tempNodeModulesDir, "@absurd-sqlite"), { recursive: true });

    const linkType = process.platform === "win32" ? "junction" : "dir";
    symlinkSync(
      packageRoot,
      join(tempNodeModulesDir, "@absurd-sqlite", "sdk"),
      linkType
    );
    symlinkSync(
      join(nodeModulesDir, "temporal-polyfill"),
      join(tempNodeModulesDir, "temporal-polyfill"),
      linkType
    );

    const esmOutput = execFileSync(
      process.execPath,
      [
        "-e",
        [
          'import("@absurd-sqlite/sdk").then((mod) => {',
          '  if (typeof mod.Absurd !== "function") {',
          '    throw new Error("missing Absurd export");',
          "  }",
          '  console.log("esm-ok");',
          "});",
        ].join("\n"),
      ],
      {
        cwd: tempDir,
        encoding: "utf8",
      }
    );

    const cjsOutput = execFileSync(
      process.execPath,
      [
        "-e",
        [
          'const mod = require("@absurd-sqlite/sdk");',
          'if (typeof mod.Absurd !== "function") {',
          '  throw new Error("missing Absurd export");',
          "}",
          'console.log("cjs-ok");',
        ].join("\n"),
      ],
      {
        cwd: tempDir,
        encoding: "utf8",
      }
    );

    expect(esmOutput).toContain("esm-ok");
    expect(cjsOutput).toContain("cjs-ok");
  });
});
