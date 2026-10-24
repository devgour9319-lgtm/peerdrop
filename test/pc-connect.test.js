'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const { once } = require('node:events');
const test = require('node:test');
const express = require('express');
const { registerPcConnectRoutes } = require('../server/pc-connect');

async function startTestServer(t, options = {}) {
  const inboxDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'peerdrop-pc-connect-'));
  const app = express();
  registerPcConnectRoutes(app, { inboxDir, pin: '123456', maxFileSize: 8, ...options });
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await fs.promises.rm(inboxDir, { recursive: true, force: true });
  });
  return { inboxDir, base: `http://127.0.0.1:${server.address().port}` };
}

async function authenticate(base, pin = '123456') {
  const response = await fetch(`${base}/api/pc-connect/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ pin }),
  });
  const cookie = response.headers.get('set-cookie')?.split(';', 1)[0];
  return { response, cookie };
}

function chunkedUpload(base, cookie, chunks) {
  const url = new URL('/api/pc-connect/upload', base);
  return new Promise((resolve, reject) => {
    const request = http.request({
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: 'POST',
      headers: {
        Cookie: cookie,
        'Content-Type': 'application/octet-stream',
        'X-PeerDrop-Filename': 'chunked.bin',
      },
    }, response => {
      response.resume();
      response.on('end', () => resolve(response.statusCode));
    });
    request.on('error', reject);
    for (const chunk of chunks) request.write(chunk);
    request.end();
  });
}

test('PIN session gates streaming uploads, downloads, and deletes', async t => {
  const { base, inboxDir } = await startTestServer(t);
  const locked = await fetch(`${base}/api/pc-connect/files`);
  assert.equal(locked.status, 401);

  const crossOrigin = await fetch(`${base}/api/pc-connect/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: 'http://attacker.example' },
    body: JSON.stringify({ pin: '123456' }),
  });
  assert.equal(crossOrigin.status, 403);

  const { response: auth, cookie } = await authenticate(base);
  assert.equal(auth.status, 200);
  assert.ok(cookie);

  const payload = Buffer.from('offline!');
  const uploaded = await fetch(`${base}/api/pc-connect/upload`, {
    method: 'POST',
    headers: {
      Cookie: cookie,
      'Content-Type': 'application/octet-stream',
      'X-PeerDrop-Filename': encodeURIComponent('../../unsafe.txt'),
    },
    body: payload,
  });
  assert.equal(uploaded.status, 201);
  const { file } = await uploaded.json();
  assert.equal(file.name, '.._.._unsafe.txt');
  assert.equal(file.size, payload.length);
  const storedNames = await fs.promises.readdir(inboxDir);
  assert.equal(storedNames.length, 1);
  assert.ok(storedNames[0].startsWith(`${file.id}--`));

  const listing = await fetch(`${base}/api/pc-connect/files`, { headers: { Cookie: cookie } });
  assert.deepEqual((await listing.json()).files.map(item => item.id), [file.id]);

  const download = await fetch(`${base}/api/pc-connect/files/${file.id}`, { headers: { Cookie: cookie } });
  assert.equal(download.status, 200);
  assert.deepEqual(Buffer.from(await download.arrayBuffer()), payload);
  assert.match(download.headers.get('content-disposition'), /attachment/);

  const removed = await fetch(`${base}/api/pc-connect/files/${file.id}`, {
    method: 'DELETE',
    headers: { Cookie: cookie },
  });
  assert.equal(removed.status, 200);
  const emptyListing = await fetch(`${base}/api/pc-connect/files`, { headers: { Cookie: cookie } });
  assert.deepEqual((await emptyListing.json()).files, []);

  const emptyUpload = await fetch(`${base}/api/pc-connect/upload`, {
    method: 'POST',
    headers: {
      Cookie: cookie,
      'Content-Type': 'application/octet-stream',
      'X-PeerDrop-Filename': 'empty.bin',
    },
    body: Buffer.alloc(0),
  });
  assert.equal(emptyUpload.status, 201);
  assert.equal((await emptyUpload.json()).file.size, 0);
});

test('host PIN is local-only, invalid PIN attempts are throttled, and oversized uploads are rejected', async t => {
  const { base, inboxDir } = await startTestServer(t);
  const hostPin = await fetch(`${base}/api/pc-connect/host`);
  assert.equal(hostPin.status, 200);
  assert.equal((await hostPin.json()).pin, '123456');

  const { cookie } = await authenticate(base);
  const oversized = await fetch(`${base}/api/pc-connect/upload`, {
    method: 'POST',
    headers: {
      Cookie: cookie,
      'Content-Type': 'application/octet-stream',
      'X-PeerDrop-Filename': 'large.bin',
    },
    body: Buffer.alloc(9),
  });
  assert.equal(oversized.status, 413);
  assert.equal(await chunkedUpload(base, cookie, [Buffer.alloc(5), Buffer.alloc(4)]), 413);
  assert.deepEqual(await fs.promises.readdir(inboxDir), []);

  for (let attempt = 0; attempt < 5; attempt++) {
    const { response } = await authenticate(base, '000000');
    assert.equal(response.status, 401);
  }
  const throttled = await authenticate(base, '123456');
  assert.equal(throttled.response.status, 429);
});
