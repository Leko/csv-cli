import glob from "glob";
import chunk from "lodash/chunk";
import Table from "cli-table3";
import { cacheAndLoad } from "../sqlite";
import { noBorderOptions, stripe } from "../table";

export async function describe(globPattern: string) {
  let out = "";
  const csvFilePaths = glob.sync(globPattern);
  const csvs = await Promise.all(csvFilePaths.map(cacheAndLoad));
  csvs.forEach(({ tableName: t, database }, i) => {
    const columns = database
      .prepare(`SELECT * FROM "${t}" LIMIT 1`)
      .columns()
      .map((c) => c.column);
    // SQLite only accepts 100 columns per query
    // We need to separate columns into multiple queries
    const selectColumns = columns.flatMap((c) => [
      `SUM("${c}" != '') AS "${c}_not_empty"`,
      `COUNT(DISTINCT "${c}") AS "${c}_uniq"`,
      `ROUND(AVG("${c}"), 5) AS "${c}_mean"`,
      `MIN("${c}") AS "${c}_min"`,
      `MAX("${c}") AS "${c}_max"`,
      `MAX("${c}") AS "${c}_std"`,
      `my_mode("${c}") AS "${c}_mode"`,
      `ROUND(my_stdev("${c}"), 5) AS "${c}_std"`,
    ]);
    const stats = chunk(selectColumns, 100).reduce(
      (acc: Record<string, string | number>, part: string[]) => {
        const stats = database
          .prepare(`SELECT COUNT(*) AS count, ${part.join(",")} FROM "${t}"`)
          .get();
        return { ...acc, ...stats };
      },
      {}
    );
    function missing(value: number, base: number): string {
      return ((1 - value / base) * 100).toFixed(0);
    }
    const table = new Table({
      head: [
        "column",
        "count",
        "missing",
        "unique",
        "mean",
        "min",
        "max",
        "std",
      ],
      wordWrap: false,
      wrapOnWordBoundary: false,
      ...noBorderOptions,
    });
    columns.forEach((c) => {
      const notEmptyCount = stats[`${c}_not_empty`];
      const isNumber =
        Number.isFinite(Number(stats[`${c}_min`])) &&
        Number.isFinite(Number(stats[`${c}_max`]));
      table.push([
        c,
        notEmptyCount,
        missing(notEmptyCount as number, stats.count as number) + "%",
        stats[`${c}_uniq`],
        isNumber ? stats[`${c}_mean`] : "",
        isNumber ? stats[`${c}_min`] : "",
        isNumber ? stats[`${c}_max`] : "",
        isNumber ? stats[`${c}_std`] : "",
      ]);
    });
    out += [t, stripe(table.toString()), "\n"].join("\n");
  });
  csvs.forEach(({ database }) => database.close());
  return out.trim();
}
