// Where playwright-core lives on this machine.
//
// PW (documented) and PW_DIR (what run-lab.sh exports) win. Otherwise the npx
// cache under $HOME: the directory in it carries a machine-specific hash, so it
// is found rather than named — the five viewer probes used to spell out one
// machine's path, which both failed everywhere else and named that machine.
import { existsSync, readdirSync } from 'node:fs';

const npxRoot = `${process.env.HOME ?? ''}/.npm/_npx`;
const npxDirs = existsSync(npxRoot)
  ? readdirSync(npxRoot, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => `${npxRoot}/${d.name}/node_modules/playwright-core`)
  : [];

export const pwDir = [process.env.PW, process.env.PW_DIR, ...npxDirs]
  .find((p) => p && existsSync(p));

if (!pwDir) {
  console.error('playwright-core not found: set PW=<dir> (bash install.sh prints how to get it)');
  process.exit(2);
}
