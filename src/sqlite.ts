import { createReadStream } from "fs";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { createHash } from "crypto";
import Database from "better-sqlite3";
import { parse } from "@fast-csv/parse";
import _debug from "debug";

const debug = _debug("csv:sqlite");
const CACHE_DIR = path.join(os.tmpdir(), "csv");

export async function clear() {
  await fs.rm(CACHE_DIR, { recursive: true });
}

export async function cacheAndLoad(
  csvPath: string
): Promise<{ tableName: string; database: Database.Database }> {
  const md5sum = await md5(csvPath);
  const cached = path.join(CACHE_DIR, `${md5sum}.db`);
  const t = path.basename(csvPath, ".csv");
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

    await new Promise((resolve, reject) => {
      db.transaction(() => {
        let stmt: Database.Statement;
        createReadStream(csvPath)
          .pipe(parse({ ignoreEmpty: true }))
          .on("data", (row: string[]) => {
            if (!stmt) {
              db.exec(
                `CREATE TABLE "${t}" (${row.map((c) => `"${c}"`).join(",")})`
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
  }
  const database = new Database(cached, { readonly: true });
  database.aggregate("my_stdev", stdev);
  database.aggregate("my_mode", mode);
  return {
    tableName: t,
    database,
  };
}

async function md5(filePath: string) {
  const hash = createHash("md5");
  return new Promise<string>((resolve, reject) => {
    createReadStream(filePath)
      .pipe(hash)
      .setEncoding("hex")
      .on("data", resolve)
      .on("error", reject);
  });
}

const stdev: Database.AggregateOptions = {
  start: () => [],
  step: (items, nextValue) => {
    const num = Number(nextValue);
    if (Number.isFinite(num)) {
      items.push(num);
    }
  },
  result: (items: number[]) => {
    if (items.length === 0) {
      return NaN;
    }
    const sum = items.reduce((sum, n) => sum + n, 0);
    const avg = sum / items.length;
    const variance = items.reduce((v, n) => v + (n - avg) ** 2, 0);
    return Math.sqrt(variance / items.length);
  },
};
const mode: Database.AggregateOptions = {
  start: () => new Map(),
  step: (map, nextValue) => {
    map.set(nextValue, (map.get(nextValue) ?? 0) + 1);
  },
  result: (map) => {
    return [...map.entries()].sort(([, a], [, b]) => b - a)[0][0];
  },
};
