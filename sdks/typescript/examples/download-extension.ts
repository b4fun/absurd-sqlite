/**
 * Example demonstrating automatic extension download
 * This script can be run without manually downloading the extension
 */
import Database from "better-sqlite3";
import { Absurd } from "../src/index";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

async function main() {
  // Create a temporary database
  const tempDir = join(tmpdir(), `absurd-example-${Date.now()}`);
  mkdirSync(tempDir, { recursive: true });
  const dbPath = join(tempDir, "test.db");

  console.log("Creating database at:", dbPath);
  const db = new Database(dbPath) as any;

  try {
    console.log("Creating Absurd instance with automatic extension download...");
    
    // This will automatically download the extension if needed
    const absurd = await Absurd.create(db, {
      downloadOptions: { version: "v0.1.0-alpha.3" }
    });

    console.log("Extension loaded successfully!");

    // Apply migrations
    console.log("Applying migrations...");
    db.prepare("select absurd_apply_migrations()").get();

    // Create a queue
    console.log("Creating queue 'example'...");
    await absurd.createQueue("example");

    // List queues
    const queues = await absurd.listQueues();
    console.log("Available queues:", queues);

    // Close connection
    await absurd.close();
    console.log("Closed successfully!");

  } catch (error) {
    console.error("Error:", error);
    process.exit(1);
  } finally {
    // Cleanup
    if (existsSync(dbPath)) {
      unlinkSync(dbPath);
    }
  }

  console.log("\n✅ Example completed successfully!");
}

main().catch(console.error);
