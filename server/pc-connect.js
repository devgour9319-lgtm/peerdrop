'use strict';

const crypto = require('crypto');
const fs = require('fs');
const express = require('express');
const os = require('os');
const path = require('path');
const { Transform } = require('stream');
const { pipeline } = require('stream/promises');

const SESSION_DURATION_MS = 8 * 60 * 60 * 1000;
const PIN_WINDOW_MS = 10 * 60 * 1000;
const MAX_PIN_ATTEMPTS = 5;
const DEFAULT_MAX_FILE_SIZE = 20 * 1024 * 1024 * 1024;
const MAX_SESSIONS = 100;

function defaultPin() {
  if (process.env.PEERDROP_PIN !== undefined) {
    if (!/^\d{6}$/.test(process.env.PEERDROP_PIN)) {
      throw new Error('PEERDROP_PIN must contain exactly six digits.');
    }
    return process.env.PEERDROP_PIN;
  }
  return String(crypto.randomInt(0, 1_000_000)).padStart(6, '0');
}

function isLoopback(address = '') {
  return address === '::1' || address === '127.0.0.1' || address.startsWith('127.')
    || address.startsWith('::ffff:127.');
}

function cleanFilename(name) {
  return name
    .replace(/[\\/\u0000-\u001f\u007f<>:"|?*]/g, '_')
    .replace(/[. ]+$/g, '')
    .trim()
    .slice(0, 180) || 'file';
}

function registerPcConnectRoutes(app, options = {}) {
  const inboxDir = options.inboxDir || process.env.PEERDROP_INBOX_DIR
    || path.join(os.homedir(), 'PeerDrop', 'Inbox');
  const pin = options.pin || defaultPin();
  const maxFileSize = options.maxFileSize || DEFAULT_MAX_FILE_SIZE;
  const sessions = new Map();
  const attempts = new Map();
  const cookieName = 'peerdrop_pc_session';

  function isSameOrigin(req) {
    const origin = req.get('origin');
    if (!origin) return true;
    try {
      const parsed = new URL(origin);
      return ['http:', 'https:'].includes(parsed.protocol) && parsed.host === req.get('host');
    } catch {
      return false;
    }
  }

  function secureRequest(req) {
    return req.secure || req.get('origin')?.startsWith('https://');
  }

  function requireSameOrigin(req, res, next) {
    if (!isSameOrigin(req)) return res.status(403).json({ error: 'Cross-origin requests are not allowed.' });
    next();
  }

  function readCookie(req) {
    const cookies = req.headers.cookie || '';
    const match = cookies.match(new RegExp(`(?:^|;\\s*)${cookieName}=([a-f0-9]+)(?:;|$)`, 'i'));
    return match ? match[1] : '';
  }

  function requireSession(req, res, next) {
    const token = readCookie(req);
    const expiresAt = sessions.get(token);
    if (!token || !expiresAt || expiresAt <= Date.now()) {
      if (token) sessions.delete(token);
      return res.status(401).json({ error: 'Enter the host PIN to access this inbox.' });
    }
    next();
  }

  function reportInboxError(res, error, message) {
    console.error(`[PeerDrop] PC Connect inbox error: ${error.message}`);
    if (res.headersSent) res.destroy(error);
    else if (!res.destroyed) res.status(500).json({ error: message });
  }

  function takePinAttempt(ip) {
    const now = Date.now();
    for (const [key, state] of attempts) {
      if (state.resetAt <= now) attempts.delete(key);
    }
    let state = attempts.get(ip);
    if (!state || state.resetAt <= now) {
      state = { count: 0, resetAt: now + PIN_WINDOW_MS };
      attempts.set(ip, state);
    }
    if (state.count >= MAX_PIN_ATTEMPTS) return false;
    state.count++;
    if (attempts.size > 1_000) attempts.delete(attempts.keys().next().value);
    return true;
  }

  async function listFiles() {
    await fs.promises.mkdir(inboxDir, { recursive: true });
    const entries = await fs.promises.readdir(inboxDir, { withFileTypes: true });
    const files = await Promise.all(entries.flatMap(entry => {
      const match = entry.isFile() && entry.name.match(/^([a-f0-9-]{36})--(.+)$/i);
      if (!match) return [];
      const fullPath = path.join(inboxDir, entry.name);
      return [fs.promises.stat(fullPath).then(stat => ({
        id: match[1],
        name: match[2],
        size: stat.size,
        uploadedAt: stat.mtime.toISOString(),
      }))];
    }));
    return files.sort((a, b) => b.uploadedAt.localeCompare(a.uploadedAt));
  }

  app.get('/api/pc-connect/host', (req, res) => {
    if (!isLoopback(req.socket.remoteAddress)) {
      return res.status(403).json({ error: 'The host PIN is only available on the host device.' });
    }
    res.set('Cache-Control', 'no-store').json({ pin });
  });

  app.post('/api/pc-connect/auth', express.json({ limit: '1kb' }), requireSameOrigin, (req, res) => {
    const ip = req.socket.remoteAddress || 'unknown';
    if (!takePinAttempt(ip)) {
      return res.status(429).json({ error: 'Too many PIN attempts. Try again in 10 minutes.' });
    }
    if (!req.body || typeof req.body.pin !== 'string' || !/^\d{6}$/.test(req.body.pin)
      || !crypto.timingSafeEqual(Buffer.from(req.body.pin), Buffer.from(pin))) {
      return res.status(401).json({ error: 'Incorrect PIN.' });
    }
    attempts.delete(ip);
    for (const [session, expiresAt] of sessions) {
      if (expiresAt <= Date.now()) sessions.delete(session);
    }
    if (sessions.size >= MAX_SESSIONS) {
      return res.status(429).json({ error: 'The inbox has reached its active connection limit.' });
    }
    const token = crypto.randomBytes(32).toString('hex');
    sessions.set(token, Date.now() + SESSION_DURATION_MS);
    res.set({
      'Cache-Control': 'no-store',
      'Set-Cookie': `${cookieName}=${token}; HttpOnly; SameSite=Strict; Path=/api/pc-connect; Max-Age=${SESSION_DURATION_MS / 1000}${secureRequest(req) ? '; Secure' : ''}`,
    }).json({ ok: true });
  });

  app.post('/api/pc-connect/logout', requireSameOrigin, (req, res) => {
    sessions.delete(readCookie(req));
    res.set('Set-Cookie', `${cookieName}=; HttpOnly; SameSite=Strict; Path=/api/pc-connect; Max-Age=0${secureRequest(req) ? '; Secure' : ''}`)
      .json({ ok: true });
  });

  app.get('/api/pc-connect/files', requireSession, async (req, res) => {
    try {
      res.set('Cache-Control', 'no-store').json({ files: await listFiles() });
    } catch (error) {
      reportInboxError(res, error, 'Could not read the PeerDrop inbox.');
    }
  });

  app.post('/api/pc-connect/upload', requireSameOrigin, requireSession, async (req, res) => {
    const encodedName = req.get('x-peerdrop-filename') || '';
    if (!encodedName) return res.status(400).json({ error: 'A filename is required.' });
    let name;
    try {
      name = cleanFilename(decodeURIComponent(encodedName));
    } catch {
      return res.status(400).json({ error: 'The filename is invalid.' });
    }
    const declaredSize = req.get('content-length') === undefined ? null : Number(req.get('content-length'));
    if (!name || (declaredSize !== null && (!Number.isSafeInteger(declaredSize) || declaredSize < 0 || declaredSize > maxFileSize))) {
      return res.status(413).json({ error: `Files may be up to ${maxFileSize} bytes.` });
    }

    const id = crypto.randomUUID();
    const fullPath = path.join(inboxDir, `${id}--${name}`);
    let receivedBytes = 0;
    const sizeLimiter = new Transform({
      transform(chunk, encoding, callback) {
        receivedBytes += chunk.length;
        if (receivedBytes > maxFileSize) {
          callback(new Error('The uploaded file exceeds the host limit.'));
          return;
        }
        callback(null, chunk);
      },
    });

    try {
      await fs.promises.mkdir(inboxDir, { recursive: true });
      await pipeline(req, sizeLimiter, fs.createWriteStream(fullPath, { flags: 'wx', mode: 0o600 }));
      if (declaredSize !== null && receivedBytes !== declaredSize) {
        throw new Error('The uploaded file size did not match Content-Length.');
      }
      res.status(201).json({
        file: { id, name, size: receivedBytes, uploadedAt: new Date().toISOString() },
      });
    } catch (error) {
      await fs.promises.unlink(fullPath).catch(unlinkError => {
        if (unlinkError.code !== 'ENOENT') error.cleanupError = unlinkError;
      });
      if (!res.headersSent && !res.destroyed) {
        if (error.message === 'The uploaded file exceeds the host limit.') {
          res.status(413).json({ error: `Files may be up to ${maxFileSize} bytes.` });
        } else if (error.message === 'The uploaded file size did not match Content-Length.') {
          res.status(400).json({ error: error.message });
        } else {
          reportInboxError(res, error, 'The file could not be saved to the PeerDrop inbox.');
        }
      }
    }
  });

  app.get('/api/pc-connect/files/:id', requireSession, async (req, res) => {
    if (!/^[a-f0-9-]{36}$/i.test(req.params.id)) return res.status(404).json({ error: 'File not found.' });
    try {
      const file = (await listFiles()).find(item => item.id === req.params.id);
      if (!file) return res.status(404).json({ error: 'File not found.' });
      const fullPath = path.join(inboxDir, `${file.id}--${file.name}`);
      res.set({
        'Content-Type': 'application/octet-stream',
        'Content-Length': String(file.size),
        'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(file.name).replace(/['()*]/g, char => `%${char.charCodeAt(0).toString(16).toUpperCase()}`)}`,
        'X-Content-Type-Options': 'nosniff',
        'Cache-Control': 'no-store',
      });
      const stream = fs.createReadStream(fullPath);
      stream.on('error', error => {
        reportInboxError(res, error, 'Could not open the requested file.');
      });
      stream.pipe(res);
    } catch (error) {
      reportInboxError(res, error, 'Could not open the requested file.');
    }
  });

  app.delete('/api/pc-connect/files/:id', requireSameOrigin, requireSession, async (req, res) => {
    if (!/^[a-f0-9-]{36}$/i.test(req.params.id)) return res.status(404).json({ error: 'File not found.' });
    try {
      const file = (await listFiles()).find(item => item.id === req.params.id);
      if (!file) return res.status(404).json({ error: 'File not found.' });
      await fs.promises.unlink(path.join(inboxDir, `${file.id}--${file.name}`));
      res.json({ ok: true });
    } catch (error) {
      reportInboxError(res, error, 'Could not remove the requested file.');
    }
  });

  return { pin };
}

module.exports = { registerPcConnectRoutes };
