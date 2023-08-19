import Database from "better-sqlite3";

export const mode: Database.AggregateOptions = {
  start: () => new Map(),
  step: (map, nextValue) => {
    map.set(nextValue, (map.get(nextValue) ?? 0) + 1);
  },
  result: (map) => {
    return [...map.entries()].sort(([, a], [, b]) => b - a)[0][0];
  },
};
