import yargs from "yargs";
import { clear } from "./sqlite";
import { describe } from "./commands/describe";
import { intersections } from "./commands/intersections";
import { unique } from "./commands/unique";

export function run(argv: string[]) {
  const options = yargs(argv)
    .scriptName("csv")
    .usage("$0 <subcommand> [options...]")
    .option("avoid-using-first-row-as-header", {
      type: "boolean",
      alias: "H",
      default: false,
    })
    .command<{
      glob: string;
      avoidUsingFirstRowAsHeader: boolean;
      sampleSize: number;
    }>({
      command: "describe <glob>",
      describe: "Describe statistical features of given CSVs",
      // @ts-expect-error cannot be typed
      builder(yargs) {
        return yargs
          .positional("glob", {
            required: true,
            type: "string",
            describe: "Glob patterns for CSVs",
          })
          .option("sample-size", {
            type: "number",
            alias: "s",
            default: 5,
          });
      },
      async handler(options) {
        console.log(
          await describe(options.glob, {
            useFirstRowAsHeader: !options.avoidUsingFirstRowAsHeader,
            sampleSize: options.sampleSize,
          })
        );
      },
    })
    .command<{
      glob: string;
      column: string;
      avoidUsingFirstRowAsHeader: boolean;
    }>({
      command: "unique <glob> <column>",
      describe: "Describe statistical features of given CSVs",
      // @ts-expect-error cannot be typed
      builder(yargs) {
        return yargs
          .positional("glob", {
            required: true,
            type: "string",
            describe: "Glob patterns for CSVs",
          })
          .positional("column", {
            required: true,
            type: "string",
            describe: "Column name to unique",
          });
      },
      async handler(options) {
        console.log(
          await unique(options.glob, options.column, {
            useFirstRowAsHeader: !options.avoidUsingFirstRowAsHeader,
          })
        );
      },
    })
    .command<{ glob: string; avoidUsingFirstRowAsHeader: boolean }>({
      command: "intersections <glob>",
      describe: "Describe statistical features of given CSVs",
      // @ts-expect-error cannot be typed
      builder(yargs) {
        return yargs.positional("glob", {
          required: true,
          type: "string",
          describe: "Glob patterns for CSVs",
        });
      },
      async handler(options) {
        console.log(
          await intersections(options.glob, {
            useFirstRowAsHeader: !options.avoidUsingFirstRowAsHeader,
          })
        );
      },
    })
    .command({
      command: "clean-cache",
      describe: "Clean the cache",
      async handler() {
        await clear();
      },
    })
    .demandCommand();

  return options.parse();
}
