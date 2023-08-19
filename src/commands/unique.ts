import glob from "glob";
import Table from "cli-table3";
import { cacheAndLoad } from "../sqlite";
import { noBorderOptions, stripe } from "../table";

export async function unique(
  globPattern: string,
  columnName: string,
  { useFirstRowAsHeader }: { useFirstRowAsHeader: boolean }
) {
  const csvFilePaths = glob.sync(globPattern);
  const csvs = await Promise.all(
    csvFilePaths.map((f) => cacheAndLoad(f, { useFirstRowAsHeader }))
  );
  // FIXME
  const { database, tableName: t } = csvs[0];
  const rows = database
    .prepare(
      `SELECT
        "${columnName}" as value,
        COUNT("${columnName}") AS count,
        COUNT("${columnName}") * 1.0 / (SELECT COUNT(*) FROM "${t}") AS percentage
      FROM "${t}"
      GROUP BY "${columnName}"
      ORDER BY count DESC
    `
    )
    .all();
  const table = new Table({
    head: ["value", "count", "percentage"],
    wordWrap: true,
    wrapOnWordBoundary: false,
    ...noBorderOptions,
  });
  table.push(
    ...rows.map((r) => [
      r.value,
      r.count,
      (r.percentage * 100).toFixed(3) + "%",
    ])
  );
  return stripe(table.toString());
}
