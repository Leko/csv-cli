import { createReadStream } from "fs";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { createHash } from "crypto";
import Database from "better-sqlite3";
import _debug from "debug";
import { stdev } from "./aggregation/stdev";
import { mode } from "./aggregation/mode";
import { createCSVParseStream } from "./csv";

const debug = _debug("csv:sqlite");
const CACHE_DIR = path.join(os.tmpdir(), "csv");

export async function clear() {
  await fs.rm(CACHE_DIR, { recursive: true });
}

function escapeTableName(tableName: string): string {
  return tableName.replaceAll(".", "_");
}

export async function cacheAndLoad(
  csvPath: string,
  { useFirstRowAsHeader }: { useFirstRowAsHeader: boolean }
): Promise<{ tableName: string; database: Database.Database }> {
  const md5sum = await md5(csvPath, { useFirstRowAsHeader });
  const cached = path.join(CACHE_DIR, `${md5sum}.db`);
  const t = escapeTableName(path.basename(csvPath, ".csv"));
  try {
    await fs.access(cached);
    debug(`cache hit: ${csvPath} (${md5sum})`);
  } catch {
    debug(`cache miss: ${csvPath} (${md5sum})`);
    await fs.mkdir(path.dirname(cached), { recursive: true });
    const db = new Database(cached);

    // For performance reason
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = normal");

    try {
      const src = await createCSVParseStream(csvPath);
      await new Promise((resolve, reject) => {
        db.transaction(() => {
          let stmt: Database.Statement;
          src
            .on("data", (row: string[]) => {
              if (!stmt) {
                db.exec(
                  `CREATE TABLE "${t}" (${row
                    .map((c, i) =>
                      useFirstRowAsHeader ? `"${c}"` : `column_${i}`
                    )
                    .join(",")})`
                );
                stmt = db.prepare(
                  `INSERT INTO "${t}" VALUES (${row.map(() => "?").join(",")})`
                );
              } else {
                stmt.run(row);
              }
            })
            .on("end", (rowCount: number) => {
              debug(`inserted ${rowCount - 1} rows`);
              resolve(null);
            })
            .on("error", reject);
        })();
      });
    } catch (e) {
      debug(`failed to cache ${csvPath} (${md5sum}): ${e}`);
      await fs.rm(cached, { force: true });
      throw e;
    }
  }
  const database = new Database(cached, { readonly: true });
  database.aggregate("my_stdev", stdev);
  database.aggregate("my_mode", mode);
  return {
    tableName: t,
    database,
  };
}

async function md5(
  filePath: string,
  { useFirstRowAsHeader }: { useFirstRowAsHeader: boolean }
) {
  const hash = createHash("md5");
  hash.push(useFirstRowAsHeader ? "0" : "1");
  return new Promise<string>((resolve, reject) => {
    let read = false;
    createReadStream(filePath)
      .pipe(hash)
      .setEncoding("hex")
      .on("data", (c) => {
        if (read) {
          resolve(c);
        }
        // なぜかイベントが2回トリガーするので、2回目のみを使う
        read = true;
      })
      .on("error", reject);
  });
}
