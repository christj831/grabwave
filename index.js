#!/usr/bin/env node
import 'dotenv/config';
import path from 'node:path';
import { parseSpotifyUrl, runDownload } from './lib/download-core.js';

function parseArgs(argv) {
  let outDir = path.resolve('downloads');
  let url = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '-o' && argv[i + 1]) {
      outDir = path.resolve(argv[++i]);
      continue;
    }
    if (!argv[i].startsWith('-')) {
      url = url || argv[i];
    }
  }
  return { outDir, url };
}

async function main() {
  const { outDir, url: spotifyUrl } = parseArgs(process.argv.slice(2));
  const url = spotifyUrl || process.env.SPOTIFY_URL;

  if (!url) {
    console.error(`Usage:
  node index.js <spotify track|album|playlist url> [-o output-folder]

Or start the web UI:
  node server.js

Environment:
  SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET — Spotify Developer Dashboard (Client Credentials).

Example:
  node index.js "https://open.spotify.com/track/3n3Ppam7vgaVa1iaRUc9Lp" -o ./music`);
    process.exit(1);
  }

  if (!parseSpotifyUrl(url)) {
    console.error(
      'Invalid Spotify URL (expected open.spotify.com track, album, or playlist link).'
    );
    process.exit(1);
  }

  const { ok, fail, files } = await runDownload(url, outDir, {
    log: (s) => console.log(s),
  });

  console.log(`\nDone: ${ok} saved, ${fail} failed. Output: ${outDir}`);
  if (files.length && fail > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
