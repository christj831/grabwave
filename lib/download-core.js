import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import ffmpegStatic from 'ffmpeg-static';
import ytdlpPkg from 'yt-dlp-wrap';
import NodeID3 from 'node-id3';

const YTDlpWrap = ytdlpPkg.default ?? ytdlpPkg;

export const SPOTIFY_RE =
  /open\.spotify\.com\/(?:intl-[a-z]{2}\/)?(track|album|playlist)\/([a-zA-Z0-9]+)/;

const LIVEISH =
  /\b(live|concert|karaoke|tour|fancam|full album|compilation)\b|\[live\]|\(live\)|\bm\/v\b|\bmv\b|music video|performance|remix\s*mix|cover\s+by/i;

export function parseSpotifyUrl(input) {
  const m = input.trim().match(SPOTIFY_RE);
  if (m) return { source: 'spotify', type: m[1], id: m[2], url: input.trim() };
  
  const ytM = input.trim().match(/(?:music\.|www\.)?youtube\.com\/(watch\?v=|playlist\?list=|shorts\/)([a-zA-Z0-9_-]+)/);
  if (ytM) {
    let type = 'youtube_track';
    if (ytM[1].includes('playlist')) type = 'youtube_playlist';
    return { source: 'youtube', type, id: ytM[2], url: input.trim() };
  }
  const ytuM = input.trim().match(/youtu\.be\/([a-zA-Z0-9_-]+)/);
  if (ytuM) return { source: 'youtube', type: 'youtube_track', id: ytuM[1], url: input.trim() };
  
  return null;
}

export function sanitizeFilename(name) {
  return name.replace(/[<>:"/\\|?*\x00-\x1f]/g, '_').trim() || 'track';
}

function spotifyClientAuthHeader() {
  const id = process.env.SPOTIFY_CLIENT_ID;
  const secret = process.env.SPOTIFY_CLIENT_SECRET;
  if (!id || !secret) {
    throw new Error(
      'Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file.'
    );
  }
  return (
    'Basic ' + Buffer.from(`${id}:${secret}`).toString('base64')
  );
}

/** App-only token — use for public playlists first (avoids 403 quirks with some user tokens). */
export async function spotifyClientCredentialsToken() {
  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: spotifyClientAuthHeader(),
    },
    body: 'grant_type=client_credentials',
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Spotify client-credentials failed (${res.status}): ${t}`);
  }
  const data = await res.json();
  return data.access_token;
}

async function spotifyUserRefreshToken(refreshToken) {
  const refresh = refreshToken?.trim();
  if (!refresh) {
    throw new Error('No refresh token');
  }
  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: spotifyClientAuthHeader(),
    },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: refresh,
    }).toString(),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(
      `Spotify refresh failed (${res.status}). Sign in again in the web app or check your refresh token: ${t}`
    );
  }
  const data = await res.json();
  return data.access_token;
}

/**
 * @param {{ refreshToken?: string | null }} [options]
 * User refresh: session (web) or SPOTIFY_REFRESH_TOKEN (.env / CLI). Else client credentials.
 */
export async function spotifyAccessToken(options = {}) {
  const refresh =
    options.refreshToken?.trim() ||
    process.env.SPOTIFY_REFRESH_TOKEN?.trim();
  if (refresh) {
    return spotifyUserRefreshToken(refresh);
  }
  return spotifyClientCredentialsToken();
}

function spotifyMarketQuery() {
  const raw = process.env.SPOTIFY_MARKET;
  if (raw === '') return '';
  const m = (raw ?? 'US').trim();
  return m ? `market=${encodeURIComponent(m)}` : '';
}

export async function spotifyGet(token, apiPath) {
  const res = await fetch(`https://api.spotify.com/v1${apiPath}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    const t = await res.text();
    if (res.status === 403 && apiPath.includes('/playlists/')) {
      const hint =
        'If this is your private playlist or you recently added the private scope, please click "Sign out" and log back in to refresh your permissions. Otherwise: Try SPOTIFY_MARKET= (empty) or your country code (e.g. PH). Raw: ';
      throw new Error(`Spotify API ${apiPath} → 403 Forbidden. ${hint}${t}`);
    }
    throw new Error(`Spotify API ${apiPath} → ${res.status}: ${t}`);
  }
  return res.json();
}

/** @param {any} track Spotify full track object */
function metaFromApiTrack(track) {
  const album = track.album;
  return {
    name: track.name,
    artists: track.artists,
    durationMs: track.duration_ms,
    albumName: album?.name,
    coverImageUrl: album?.images?.[0]?.url,
    releaseDate: album?.release_date,
    trackNumber: track.track_number,
    discNumber: track.disc_number,
    artistIds: track.artists.map((a) => a.id).filter(Boolean),
    spotifyId: track.id,
    spotifyUri: track.external_urls?.spotify || track.uri,
  };
}

/** @param {any} item album track item */
function metaFromAlbumItem(item, parentAlbum) {
  return {
    name: item.name,
    artists: item.artists,
    durationMs: item.duration_ms,
    trackNumber: item.track_number,
    discNumber: item.disc_number,
    albumName: parentAlbum.name,
    coverImageUrl: parentAlbum.images?.[0]?.url,
    releaseDate: parentAlbum.release_date,
    artistIds: item.artists.map((a) => a.id).filter(Boolean),
    spotifyId: item.id,
    spotifyUri: item.external_urls?.spotify,
  };
}

async function fetchAllPlaylistTrackPages(token, id, mq) {
  const tracks = [];
  const sep = mq ? '&' : '?';
  let url = `/playlists/${id}/tracks?limit=100${mq ? `${sep}${mq}` : ''}`;
  while (url) {
    const page = await spotifyGet(token, url);
    const items = page.items || [];
    for (const item of items) {
      const tr = item.track;
      if (!tr) continue;
      tracks.push(metaFromApiTrack(tr));
    }
    url = page.next
      ? page.next.replace('https://api.spotify.com/v1', '')
      : null;
  }
  return tracks;
}

/** GET /playlists/{id} then follow tracks.next (sometimes works when /tracks alone returns 403). */
async function fetchPlaylistTracksViaPlaylistRoot(token, id, mq) {
  const q = mq ? `?${mq}` : '';
  const pl = await spotifyGet(token, `/playlists/${id}${q}`);
  if (!pl.tracks) {
    throw new Error('PLAYLIST_ROOT_SKIP');
  }
  let page = pl.tracks;
  if (!Array.isArray(page.items)) {
    page = { ...page, items: [] };
  }

  const out = [];
  while (page) {
    for (const item of page.items) {
      const tr = item.track;
      if (!tr) continue;
      out.push(metaFromApiTrack(tr));
    }
    if (!page.next) break;
    const nextPath = page.next.replace('https://api.spotify.com/v1', '');
    page = await spotifyGet(token, nextPath);
  }
  return out;
}

function isSpotify403Or404(err) {
  const msg = err instanceof Error ? err.message : String(err);
  return msg.includes('403') || msg.includes('404');
}

/** Root GET failed or is unusable — try /playlists/.../tracks next. */
function isPlaylistStrategySkip(err) {
  const m = err instanceof Error ? err.message : String(err);
  return m.includes('PLAYLIST_ROOT_SKIP');
}

function uniqueMarkets(arr) {
  return [...new Set(arr.filter((x) => x !== undefined))];
}

function decodeHtmlEntities(s) {
  return s
    .replace(/&#x27;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
}

/**
 * When the Web API returns 403 (common for dev-mode apps), parse the public embed
 * HTML (only the first ~10–12 tracks shown in the widget).
 */
function parseEmbedPlaylistTrackRows(html) {
  const items = [];
  const liRe =
    /<li[^>]*class="[^"]*TracklistRow_trackListRow[^"]*"[^>]*>([\s\S]*?)<\/li>/gi;
  let m;
  while ((m = liRe.exec(html)) !== null) {
    const block = m[1];
    const tMatch = block.match(/<h3[^>]*>([^<]*)<\/h3>/i);
    const aMatch = block.match(/<h4[^>]*>([\s\S]*?)<\/h4>/i);
    if (!tMatch) continue;
    const name = decodeHtmlEntities(tMatch[1].trim());
    let artistRaw = aMatch ? aMatch[1] : 'Unknown';
    artistRaw = artistRaw.replace(/<[^>]+>/g, ' ');
    artistRaw = artistRaw.replace(/\s+/g, ' ').trim();
    const artistName = decodeHtmlEntities(artistRaw);
    items.push({ name, artistName });
  }
  return items;
}

/**
 * Embed page ships `__NEXT_DATA__` with `entity.trackList` (often more tracks than the visible widget).
 */
function parseEmbedTrackListFromNextData(html) {
  const m = html.match(
    /<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/
  );
  if (!m) return null;
  try {
    const data = JSON.parse(m[1]);
    const tl = data?.props?.pageProps?.state?.data?.entity?.trackList;
    if (!Array.isArray(tl) || !tl.length) return null;
    return tl
      .filter(
        (t) =>
          t?.title &&
          (!t.entityType || t.entityType === 'track')
      )
      .map((t) => {
        const id = t.uri?.replace(/^spotify:track:/, '') || '';
        return {
          name: t.title,
          artistName: (t.subtitle || 'Unknown').replace(/\?\?/g, ', '),
          durationMs:
            typeof t.duration === 'number' ? t.duration : undefined,
          spotifyId: id,
          spotifyUri: id
            ? `https://open.spotify.com/track/${id}`
            : '',
        };
      });
  } catch {
    return null;
  }
}

function embedRowsToMeta(rows) {
  return rows.map((r) => ({
    name: r.name,
    artists: [{ name: r.artistName, id: '' }],
    durationMs: r.durationMs,
    albumName: '',
    coverImageUrl: undefined,
    releaseDate: undefined,
    trackNumber: undefined,
    discNumber: undefined,
    artistIds: [],
    spotifyId: r.spotifyId || '',
    spotifyUri: r.spotifyUri || '',
  }));
}

async function fetchPlaylistTracksFromEmbed(playlistId, log) {
  const url = `https://open.spotify.com/embed/playlist/${playlistId}`;
  const res = await fetch(url, {
    headers: {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
      Accept: 'text/html,application/xhtml+xml',
    },
  });
  if (!res.ok) {
    throw new Error(`Embed HTTP ${res.status}`);
  }
  const html = await res.text();

  const fromNext = parseEmbedTrackListFromNextData(html);
  if (fromNext?.length) {
    return embedRowsToMeta(fromNext);
  }

  const rows = parseEmbedPlaylistTrackRows(html);
  if (!rows.length) {
    throw new Error('EMBED_EMPTY');
  }
  return embedRowsToMeta(
    rows.map((r) => ({
      name: r.name,
      artistName: r.artistName,
      durationMs: undefined,
      spotifyId: '',
      spotifyUri: '',
    }))
  );
}

/**
 * Tries several Spotify quirks: playlist root GET vs /tracks, client vs user token,
 * and market from SPOTIFY_MARKET vs no market.
 * Note: market=from_token on /playlists/.../tracks often returns 403 — not used here.
 */
async function collectPlaylistTracksSmart(id, userRefreshToken, log) {
  const refresh =
    userRefreshToken?.trim() || process.env.SPOTIFY_REFRESH_TOKEN?.trim();

  const defaultMq = spotifyMarketQuery();
  const tokenSteps = [
    ...(refresh
      ? [
        {
          label: 'user',
          get: () => spotifyUserRefreshToken(refresh),
          markets: uniqueMarkets([defaultMq, '']),
        },
      ]
      : []),
    {
      label: 'client',
      get: () => spotifyClientCredentialsToken(),
      markets: uniqueMarkets([defaultMq, '']),
    },
  ];

  const fetchers = [
    (tok, m) => fetchPlaylistTracksViaPlaylistRoot(tok, id, m),
    (tok, m) => fetchAllPlaylistTrackPages(tok, id, m),
  ];

  let lastErr = /** @type {Error | null} */ (null);

  for (const step of tokenSteps) {
    let tok;
    try {
      tok = await step.get();
    } catch (e) {
      lastErr = e instanceof Error ? e : new Error(String(e));
      continue;
    }
    for (const m of step.markets) {
      for (const run of fetchers) {
        try {
          return await run(tok, m);
        } catch (e) {
          lastErr = e instanceof Error ? e : new Error(String(e));
          if (
            !isSpotify403Or404(lastErr) &&
            !isPlaylistStrategySkip(lastErr)
          ) {
            throw lastErr;
          }
        }
      }
    }
  }

  if (lastErr && isSpotify403Or404(lastErr)) {
    try {
      return await fetchPlaylistTracksFromEmbed(id, log);
    } catch {
      /* fall through */
    }
  }

  throw (
    lastErr ||
    new Error(
      'Could not read this playlist. If the Web API returns 403, your Spotify app may be in Development mode — see developer.spotify.com/dashboard → your app → User management / quota.'
    )
  );
}

/**
 * @returns {Promise<ReturnType<typeof metaFromApiTrack>[]>}
 */
export async function collectTracksFromSpotifyApi(
  parsed,
  token,
  playlistRefreshToken,
  log
) {
  const { type, id } = parsed;

  const mq = spotifyMarketQuery();

  if (type === 'track') {
    const q = mq ? `?${mq}` : '';
    const t = await spotifyGet(token, `/tracks/${id}${q}`);
    return [metaFromApiTrack(t)];
  }

  if (type === 'album') {
    const aq = mq ? `?${mq}` : '';
    const parentAlbum = await spotifyGet(token, `/albums/${id}${aq}`);
    const tracks = [];
    let url = `/albums/${id}/tracks?limit=50${mq ? `&${mq}` : ''}`;
    while (url) {
      const page = await spotifyGet(token, url);
      for (const item of page.items) {
        tracks.push(metaFromAlbumItem(item, parentAlbum));
      }
      url = page.next
        ? page.next.replace('https://api.spotify.com/v1', '')
        : null;
    }
    return tracks;
  }

  if (type === 'playlist') {
    return collectPlaylistTracksSmart(id, playlistRefreshToken, log);
  }

  throw new Error(`Unsupported resource type: ${type}`);
}

/**
 * @param {string} token
 * @param {string[]} artistIds
 * @returns {Promise<Map<string, string[]>>}
 */
async function fetchArtistGenresMap(token, artistIds) {
  const map = new Map();
  const unique = [...new Set(artistIds)].filter(Boolean);
  for (const id of unique) {
    try {
      const a = await spotifyGet(token, `/artists/${id}`);
      if (a?.id) map.set(a.id, a.genres || []);
    } catch {
      /* genre optional */
    }
  }
  return map;
}

function genreStringForTrack(track, genreMap) {
  const set = new Set();
  for (const aid of track.artistIds) {
    const g = genreMap.get(aid);
    if (g) for (const x of g) set.add(x);
  }
  return [...set].join(', ');
}

function trackQuery(track) {
  const artists = track.artists.map((a) => a.name).join(', ');
  return `${artists} ${track.name}`.trim();
}

async function findYoutubeVideoUrl(baseQuery, durationMs) {
  const spotifySec =
    durationMs != null && durationMs > 0 ? durationMs / 1000 : null;

  const ytdlp = await getYtDlp();

  const queries = [
    `${baseQuery} official audio`,
    `${baseQuery} audio`,
    baseQuery,
  ];

  /** @type {{ url: string, title: string, durationInSec: number | null }[]} */
  let candidates = [];

  for (const q of queries) {
    try {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), 30000);
      let out;
      try {
        const p = ytdlp.execPromise([
          `ytsearch5:${q}`,
          '--dump-json',
          '--flat-playlist',
          '--no-download',
          '--no-warnings',
          '--cookies', path.join(process.cwd(), 'cookies.txt'),
        ], {}, ac.signal);
        out = await p;
      } finally {
        clearTimeout(timer);
      }

      const lines = out.trim().split('\n').filter(Boolean);
      for (const line of lines) {
        try {
          const info = JSON.parse(line);
          const vUrl = info.url
            ? (info.url.startsWith('http') ? info.url : `https://www.youtube.com/watch?v=${info.url}`)
            : (info.webpage_url || (info.id ? `https://www.youtube.com/watch?v=${info.id}` : null));
          if (vUrl && info.title) {
            if (!candidates.some((c) => c.url === vUrl)) {
              candidates.push({
                url: vUrl,
                title: info.title,
                durationInSec: info.duration ?? null,
              });
            }
          }
        } catch { /* skip malformed line */ }
      }
    } catch { /* try next query */ }
    if (candidates.length >= 3) break;
  }

  if (!candidates.length) {
    throw new Error(`No YouTube results for: ${baseQuery}`);
  }

  function score(v) {
    const title = v.title || '';
    let s = 0;
    if (LIVEISH.test(title)) s += 250;
    if (/\[?\s*live\s*\]?/i.test(title)) s += 80;
    if (spotifySec != null && v.durationInSec != null) {
      s += Math.min(120, Math.abs(v.durationInSec - spotifySec));
    }
    return s;
  }

  candidates.sort((a, b) => score(a) - score(b));
  return candidates[0].url;
}

let ytDlpReady = null;
async function getYtDlp() {
  if (ytDlpReady) return ytDlpReady;
  const cacheDir = path.join(process.cwd(), '.cache');
  const binName = os.platform() === 'win32' ? 'yt-dlp.exe' : 'yt-dlp';
  const binPath = path.join(cacheDir, binName);
  if (!fs.existsSync(binPath)) {
    fs.mkdirSync(cacheDir, { recursive: true });
    await YTDlpWrap.downloadFromGithub(binPath);
  }
  if (!ffmpegStatic) {
    throw new Error('ffmpeg-static binary missing');
  }
  ytDlpReady = new YTDlpWrap(binPath);
  return ytDlpReady;
}

async function downloadYoutubeToMp3(youtubeUrl, outPath, onProgressCallback, signal) {
  const ytdlp = await getYtDlp();
  return new Promise((resolve, reject) => {
    let settled = false;
    
    // Clean, strict array of arguments with no empty slots
    const emitter = ytdlp.exec([
      youtubeUrl,
      '-f', 'bestaudio/best',
      '-x',
      '--audio-format', 'mp3',
      '--audio-quality', '320K',
      '--ffmpeg-location', ffmpegStatic,
      '--no-playlist',
      '--no-warnings',
      '--cookies', path.join(process.cwd(), 'cookies.txt'),
      '-o', outPath.replace(/\.mp3$/, '.%(ext)s')
    ], { windowsHide: true });

    // Handle abort signal
    if (signal) {
      const onAbort = () => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          try { emitter.ytDlpProcess?.kill(); } catch {}
          reject(new Error('Download cancelled'));
        }
      };
      if (signal.aborted) { onAbort(); return; }
      signal.addEventListener('abort', onAbort, { once: true });
    }

    // 5 minute timeout for downloading + converting
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        try { emitter.ytDlpProcess?.kill(); } catch {}
        reject(new Error('yt-dlp download timed out after 5 minutes'));
      }
    }, 5 * 60 * 1000);

    if (onProgressCallback) {
      emitter.on('progress', (progress) => {
        onProgressCallback(progress);
      });
    }

    emitter.on('error', (err) => {
      if (!settled) { settled = true; clearTimeout(timer); reject(err); }
    });
    emitter.on('close', (code) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        if (code === 0) resolve();
        else reject(new Error(`yt-dlp exited with code ${code}`));
      }
    });
  });
}

async function fetchCoverBuffer(url) {
  if (!url) return null;
  const res = await fetch(url);
  if (!res.ok) return null;
  const buf = Buffer.from(await res.arrayBuffer());
  const ct = res.headers.get('content-type') || '';
  let mime = 'image/jpeg';
  if (ct.includes('png')) mime = 'image/png';
  return { buffer: buf, mime };
}

function yearFromRelease(releaseDate) {
  if (!releaseDate || typeof releaseDate !== 'string') return undefined;
  const y = releaseDate.slice(0, 4);
  return /^\d{4}$/.test(y) ? y : undefined;
}

/**
 * @param {string} mp3Path
 * @param {object} track
 * @param {string} genreLine
 */
async function embedId3(mp3Path, track, genreLine) {
  const artistLine = track.artists.map((a) => a.name).join(', ');
  const cover = await fetchCoverBuffer(track.coverImageUrl);

  const tags = {
    title: track.name,
    artist: artistLine,
    album: track.albumName || '',
    year: yearFromRelease(track.releaseDate),
    genre: genreLine || undefined,
    trackNumber: track.trackNumber
      ? String(track.trackNumber)
      : undefined,
    partOfSet: track.discNumber ? String(track.discNumber) : undefined,
  };

  if (cover) {
    tags.image = {
      mime: cover.mime,
      type: { id: 3, name: 'front cover' },
      description: 'Album',
      imageBuffer: cover.buffer,
    };
  }

  if (track.spotifyUri) {
    tags.comment = {
      language: 'eng',
      text: `Spotify: ${track.spotifyUri}`,
    };
  }

  NodeID3.write(tags, mp3Path);
}

/**
 * @param {string} spotifyUrl
 * @param {string} outDir
 * @param {{ log?: (s: string) => void, spotifyRefreshToken?: string | null, onProgress?: (state: any) => void }} [opts]
 */
export async function runDownload(spotifyUrl, outDir, opts = {}) {
  const log = opts.log || (() => { });
  const onProgress = opts.onProgress || (() => { });
  const signal = opts.signal || null;
  const sessionRefresh = opts.spotifyRefreshToken?.trim() || null;

  const parsed = parseSpotifyUrl(spotifyUrl);
  if (!parsed) {
    throw new Error('Invalid URL');
  }

  if (parsed.source === 'youtube') {
    return runYoutubeDownload(parsed, outDir, { ...opts, signal });
  }

  fs.mkdirSync(outDir, { recursive: true });

  log('Authenticating with Spotify…');
  const token = await spotifyAccessToken({
    refreshToken: sessionRefresh,
  });

  log('Fetching tracks and metadata…');
  const tracks = await collectTracksFromSpotifyApi(
    parsed,
    token,
    sessionRefresh,
    log
  );

  const tracksMissingMeta = tracks.filter((t) => !t.albumName && t.spotifyId);
  if (tracksMissingMeta.length > 0) {
    log(`Fetching full metadata for ${tracksMissingMeta.length} embedded tracks via HTML scrape...`);
    for (let i = 0; i < tracksMissingMeta.length; i += 5) {
      const batch = tracksMissingMeta.slice(i, i + 5);
      await Promise.all(batch.map(async (t) => {
        try {
          const res = await fetch(`https://open.spotify.com/track/${t.spotifyId}`, {
            headers: { 'User-Agent': 'bot' }
          });
          const html = await res.text();
          
          const imgMatch = html.match(/<meta property="og:image" content="([^"]+)"/);
          if (imgMatch) t.coverImageUrl = imgMatch[1];
          
          const titleMatch = html.match(/<meta property="og:title" content="([^"]+)"/);
          if (titleMatch) t.name = decodeHtmlEntities(titleMatch[1]);
          
          const descMatch = html.match(/<meta property="og:description" content="([^"]+)"/);
          if (descMatch) {
            const parts = decodeHtmlEntities(descMatch[1]).split('·').map(s => s.trim());
            if (parts.length >= 2) {
              t.albumName = parts[1];
              if (parts.length >= 4) t.releaseDate = parts[3];
            }
          }
        } catch (err) {
          log(`    Failed to fetch HTML metadata for track ${t.spotifyId}: ${err.message}`);
        }
      }));
    }
  }

  const genreMap = await fetchArtistGenresMap(
    token,
    tracks.flatMap((t) => t.artistIds)
  );

  onProgress({ type: 'start', total: tracks.length, ok: 0, fail: 0 });

  const files = [];
  let ok = 0;
  let fail = 0;

  for (let i = 0; i < tracks.length; i++) {
    const t = tracks[i];
    const query = trackQuery(t);
    const base = sanitizeFilename(
      `${t.artists.map((a) => a.name).join(', ')} - ${t.name}`
    );
    let outPath = path.join(outDir, `${base}.mp3`);
    let n = 2;
    while (fs.existsSync(outPath)) {
      outPath = path.join(outDir, `${base} (${n}).mp3`);
      n++;
    }

    const prefix = `[${i + 1}/${tracks.length}]`;
    const trackDetails = { index: i + 1, current: t.name, artists: t.artists.map(a => a.name).join(', '), total: tracks.length };

    let success = false;
    let attempts = 0;
    const maxAttempts = 3;

    while (!success && attempts < maxAttempts) {
      if (signal?.aborted) {
        log('Download cancelled by user.');
        return { ok, fail, files, cancelled: true };
      }
      attempts++;
      try {
        log(`${prefix} ${query}`);
        onProgress({ type: 'startTrack', ...trackDetails, ok, fail });

        const ytUrl = await findYoutubeVideoUrl(query, t.durationMs);
        log(`    → ${ytUrl}`);
        await downloadYoutubeToMp3(ytUrl, outPath, (prog) => {
          onProgress({ type: 'trackProgress', percent: prog.percent, ...trackDetails, ok, fail });
        }, signal);
        const genres = genreStringForTrack(t, genreMap);
        await embedId3(outPath, t, genres);
        log(`    ✓ ${path.basename(outPath)}`);
        files.push(outPath);
        ok++;

        onProgress({ type: 'endTrack', status: 'ok', ...trackDetails, ok, fail });
        success = true;
      } catch (err) {
        const errorDetail = err.stderr ? String(err.stderr) : (err.message || JSON.stringify(err, Object.getOwnPropertyNames(err)));
        log(`    ✗ ${errorDetail}`);
        if (attempts >= maxAttempts) {
          log(`    Skipping track after ${maxAttempts} failed attempts.`);
          fail++;
          onProgress({ type: 'endTrack', status: 'error', error: err.message || String(err), ...trackDetails, ok, fail });
          break;
        } else {
          log(`    Retrying...`);
          // Wait seconds before retrying to prevent aggressive rate limiting
          await new Promise(r => setTimeout(r, 2000));
        }
      }
    }
  }

  return { ok, fail, files };
}

async function runYoutubeDownload(parsed, outDir, opts = {}) {
  const log = opts.log || (() => { });
  const signal = opts.signal || null;
  const onProgress = opts.onProgress || (() => { });

  fs.mkdirSync(outDir, { recursive: true });
  log(`Fetching YouTube metadata for: ${parsed.url}`);
  
  const ytdlp = await getYtDlp();
  
  let tracks = [];
  if (parsed.type === 'youtube_playlist') {
     const out = await ytdlp.execPromise([parsed.url, '--dump-json', '--flat-playlist', '--cookies', path.join(process.cwd(), 'cookies.txt'),]);
     const lines = out.trim().split('\n').filter(Boolean);
     for (const line of lines) {
       try {
         const info = JSON.parse(line);
         if (info.url && info.title) {
            tracks.push({ name: info.title, url: info.url, durationMs: info.duration ? info.duration * 1000 : 0 });
         }
       } catch (e) {}
     }
  } else {
     try {
       const out = await ytdlp.execPromise([parsed.url, '--dump-json', '--no-playlist', '--cookies', path.join(process.cwd(), 'cookies.txt'),]);
       const info = JSON.parse(out);
       tracks.push({ name: info.title || 'YouTube Track', url: parsed.url, durationMs: info.duration ? info.duration * 1000 : 0 });
     } catch (e) {
       tracks.push({ name: 'YouTube Track', url: parsed.url, durationMs: 0 });
     }
  }
  
  onProgress({ type: 'start', total: tracks.length, ok: 0, fail: 0 });

  const files = [];
  let ok = 0;
  let fail = 0;

  for (let i = 0; i < tracks.length; i++) {
    const t = tracks[i];
    const base = sanitizeFilename(t.name || 'Youtube_Track');
    let outPath = path.join(outDir, `${base}.mp3`);
    let n = 2;
    while (fs.existsSync(outPath)) {
      outPath = path.join(outDir, `${base} (${n}).mp3`);
      n++;
    }

    const prefix = `[${i + 1}/${tracks.length}]`;
    const trackDetails = { index: i + 1, current: t.name, artists: 'YouTube', total: tracks.length };

    let success = false;
    let attempts = 0;
    const maxAttempts = 3;

    while (!success && attempts < maxAttempts) {
      if (signal?.aborted) {
        log('Download cancelled by user.');
        return { ok, fail, files, cancelled: true };
      }
      attempts++;
      try {
        log(`${prefix} Downloading: ${t.name}`);
        onProgress({ type: 'startTrack', ...trackDetails, ok, fail });

        log(`    → ${t.url}`);
        await downloadYoutubeToMp3(t.url, outPath, (prog) => {
          onProgress({ type: 'trackProgress', percent: prog.percent, ...trackDetails, ok, fail });
        }, signal);
        log(`    ✓ ${path.basename(outPath)}`);
        files.push(outPath);
        ok++;

        onProgress({ type: 'endTrack', status: 'ok', ...trackDetails, ok, fail });
        success = true;
      } catch (err) {
        const errorDetail = err.stderr ? String(err.stderr) : (err.message || JSON.stringify(err, Object.getOwnPropertyNames(err)));
        log(`    ✗ ${errorDetail}`);
        if (attempts >= maxAttempts) {
          log(`    Skipping track after ${maxAttempts} failed attempts.`);
          fail++;
          onProgress({ type: 'endTrack', status: 'error', error: err.message || String(err), ...trackDetails, ok, fail });
          break;
        } else {
          log(`    Retrying...`);
          await new Promise(r => setTimeout(r, 2000));
        }
      }
    }
  }

  return { ok, fail, files };
}
