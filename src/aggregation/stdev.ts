import Database from "better-sqlite3";

export const stdev: Database.AggregateOptions = {
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
