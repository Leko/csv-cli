#!/usr/bin/env node
import { hideBin } from "yargs/helpers";
import { run } from "./options";

run(hideBin(process.argv));
