import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import archiver from 'archiver';
import { parseSpotifyUrl, runDownload } from './lib/download-core.js';
import os from 'os';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = Number(process.env.PORT) || 3000;

app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const jobs = new Map();

async function runDownloadAsync(jobId, url, jobDir, refreshToken) {
  const job = jobs.get(jobId);
  if (!job) return;

  const sendEvent = (data) => {
    if (job.cancelled) return;
    job.events.push(data);
    const msg = `data: ${JSON.stringify(data)}\n\n`;
    for (const client of job.clients) {
      client.write(msg);
    }
  };

  try {
    job.status = 'progress';
    const { ok, fail, files, cancelled } = await runDownload(url, jobDir, {
      spotifyRefreshToken: refreshToken || null,
      signal: job.abortController.signal,
      log: (txt) => {
        sendEvent({ type: 'log', text: txt });
      },
      onProgress: (state) => {
        sendEvent(state);
      }
    });

    if (cancelled || job.cancelled) {
      job.status = 'cancelled';
      sendEvent({ type: 'cancelled' });
      return;
    }

    const doneFiles = files.filter((f) => fs.existsSync(f));
    if (doneFiles.length === 0) {
      throw new Error(fail > 0 ? 'Download failed for all tracks (check server console).' : 'No files produced.');
    }

    job.status = 'done';
    job.result = { ok, fail, doneFiles };
    sendEvent({ type: 'done', ok, fail, resultUrl: `/api/download/${jobId}/result` });

  } catch (err) {
    job.status = 'error';
    job.error = err.message || String(err);
    sendEvent({ type: 'error', error: job.error });
  }
}

app.post('/api/download', (req, res) => {
  const url = req.body?.url?.trim();
  if (!url) {
    return res.status(400).json({ error: 'Missing url' });
  }
  if (!parseSpotifyUrl(url)) {
    return res.status(400).json({
      error:
        'Invalid URL. Provide a track/playlist from Spotify or YouTube Music.',
    });
  }

  const randId = Math.random().toString(36).slice(2, 9);
  const jobId = `job-${Date.now()}-${randId}`;
  const jobDir = path.join(os.tmpdir(), jobId);

  try {
    fs.mkdirSync(jobDir, { recursive: true });
  } catch (err) {
    return res.status(500).json({ error: 'Could not create job directory' });
  }

  const abortController = new AbortController();

  const job = {
    id: jobId,
    url,
    dir: jobDir,
    status: 'starting',
    events: [],
    clients: new Set(),
    result: null,
    abortController,
    cancelled: false,
  };
  jobs.set(jobId, job);

  // Start background process
  runDownloadAsync(jobId, url, jobDir, null).catch(console.error);

  res.json({ jobId });
});

app.get('/api/progress/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Send past events
  for (const ev of job.events) {
    res.write(`data: ${JSON.stringify(ev)}\n\n`);
  }

  job.clients.add(res);

  req.on('close', () => {
    job.clients.delete(res);
  });
});

app.post('/api/download/:jobId/cancel', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  if (job.status === 'done' || job.status === 'error' || job.cancelled) {
    return res.json({ ok: true, alreadyFinished: true });
  }

  job.cancelled = true;
  job.abortController.abort();

  const cancelMsg = { type: 'cancelled' };
  job.events.push(cancelMsg);
  const msg = `data: ${JSON.stringify(cancelMsg)}\n\n`;
  for (const client of job.clients) {
    client.write(msg);
  }

  // Cleanup job files
  setTimeout(() => {
    try { fs.rmSync(job.dir, { recursive: true, force: true }); } catch (_) { }
    jobs.delete(req.params.jobId);
  }, 3000);

  res.json({ ok: true });
});

app.get('/api/download/:jobId/result', async (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job || job.status !== 'done' || !job.result) {
    return res.status(404).json({ error: 'Result not ready or job not found' });
  }

  const { doneFiles } = job.result;

  const cleanup = () => {
    setTimeout(() => {
      try { fs.rmSync(job.dir, { recursive: true, force: true }); } catch (_) { }
      jobs.delete(req.params.jobId);
    }, 5000); // 5 sec buffer to ensure download completes safely
  };

  if (doneFiles.length === 1) {
    const file = doneFiles[0];
    res.download(file, path.basename(file), (err) => {
      cleanup();
      if (err && !res.headersSent) {
        console.error(err);
      }
    });
    return;
  }

  res.setHeader('Content-Type', 'application/zip');
  res.setHeader(
    'Content-Disposition',
    'attachment; filename="spotify-downloads.zip"'
  );

  const archive = archiver('zip', { zlib: { level: 5 } });
  archive.on('error', (err) => {
    cleanup();
    console.error(err);
  });
  archive.pipe(res);

  for (const f of doneFiles) {
    archive.file(f, { name: path.basename(f) });
  }

  await archive.finalize();

  res.on('finish', cleanup);
});

const server = app.listen(PORT, () => {
  console.log(`Web UI:  grabwave.vercel.app`);
});

server.timeout = 0;
server.headersTimeout = 0;
server.requestTimeout = 0;
