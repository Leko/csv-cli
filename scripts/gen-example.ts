import { createWriteStream } from "node:fs";
import { join } from "node:path";
// @ts-expect-error
import { finished } from "node:stream/promises";

const EXAMPLES_DIR = join(__dirname, "..", "example");

const seeds = ["apple", "banana", "orange", "grape", "strawberry"];
const dist = createWriteStream(join(EXAMPLES_DIR, "test.csv"));
dist.write("int,str\n");
for (let i = 0; i < Number(process.argv[2] ?? "1000"); i++) {
  const int = ~~(Math.random() * 2147483648);
  const str = seeds[Math.floor(Math.random() * seeds.length)];
  dist.write(`${int},${str}\n`);
}
dist.end();
finished(dist);
