import glob from "glob";
import Table from "cli-table3";
import { cacheAndLoad } from "../sqlite";
import { noBorderOptions, stripe } from "../table";

export async function intersections(
  globPattern: string,
  { useFirstRowAsHeader }: { useFirstRowAsHeader: boolean }
) {
  const csvFilePaths = glob.sync(globPattern);
  const csvs = await Promise.all(
    csvFilePaths.map((f) => cacheAndLoad(f, { useFirstRowAsHeader }))
  );
  const intersections = new Map();
  csvs.forEach(({ database, tableName: t }, i) => {
    const columns = database
      .prepare(`SELECT * FROM "${t}" LIMIT 1`)
      .columns()
      .map((c) => c.column);
    for (const col of columns) {
      intersections.set(col, (intersections.get(col) ?? new Set()).add(t));
    }
  });
  const W = process.stdout.columns;
  const tableNames = csvs.map(({ tableName }) => tableName);
  const table = new Table({
    head: ["column", "match"].concat(tableNames),
    colWidths: [
      30,
      7,
      ...tableNames.map(() => Math.floor((W - 37) / tableNames.length)),
    ],
    wordWrap: true,
    wrapOnWordBoundary: false,
    ...noBorderOptions,
  });
  [...intersections.entries()]
    .filter(([, a]) => a.size > 1)
    .sort(([, a], [, b]) => b.size - a.size)
    .forEach(([column, set]) => {
      table.push([
        column,
        (
          (tableNames.filter((t) => set.has(t)).length / tableNames.length) *
          100
        ).toFixed(0) + "%",
        ...tableNames.map((t) => (set.has(t) ? "✅" : "")),
      ]);
    });
  return stripe(table.toString());
}
