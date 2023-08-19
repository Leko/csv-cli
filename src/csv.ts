import { createReadStream } from "fs";
import fs from "fs/promises";
import { detect } from "jschardet";
import { decodeStream, encodeStream } from "iconv-lite";
import { parse } from "@fast-csv/parse";

export async function createCSVParseStream(filePath: string) {
  const charset = await detectChatset(filePath);
  let src: NodeJS.ReadWriteStream = createReadStream(filePath) as any;
  if (charset !== "UTF-8") {
    src = src.pipe(decodeStream(charset)).pipe(encodeStream("UTF-8"));
  }
  return src.pipe(parse({ ignoreEmpty: true }));
}

async function detectChatset(filePath: string): Promise<string> {
  const chunkSize = 1024;
  const chunk = Buffer.alloc(chunkSize);
  const handle = await fs.open(filePath, "r");
  try {
    await handle.read(chunk, 0, chunkSize);
    const detected = detect(chunk).encoding;
    return detected;
  } finally {
    await handle.close();
  }
}
