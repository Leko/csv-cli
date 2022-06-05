import yargs from "yargs";
import { clear } from "./sqlite";
import { describe } from "./commands/describe";
import { intersections } from "./commands/intersections";

export function run(argv: string[]) {
  const options = yargs(argv)
    .scriptName("csv")
    .usage("$0 <subcommand> [options...]")
    .command<{ glob: string }>({
      command: "describe <glob>",
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
        console.log(await describe(options.glob));
      },
    })
    .command<{ glob: string }>({
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
        console.log(await intersections(options.glob));
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
