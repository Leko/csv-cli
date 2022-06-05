import chalk from "chalk";

export const noBorderOptions = {
  chars: {
    mid: "",
    "mid-mid": "",
    "left-mid": "",
    "right-mid": "",
    bottom: "",
    "bottom-mid": "",
    "bottom-left": "",
    "bottom-right": "",
    top: "",
    "top-mid": "",
    "top-left": "",
    "top-right": "",
    left: "",
    right: "",
    middle: "",
  },
};

export function stripe(lines: string) {
  return lines
    .split("\n")
    .map((line, i) => (i % 2 === 0 ? line : chalk.bgGray(line)))
    .join("\n");
}
