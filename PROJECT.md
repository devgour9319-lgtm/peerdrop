# ⚡ PeerDrop — Project Documentation

> **Direct browser-to-browser file sharing over WiFi. Zero cloud. Zero internet.**

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [Features](#-features)
3. [Architecture](#️-architecture)
4. [Project Structure](#-project-structure)
5. [Core Modules](#-core-modules)
6. [API Reference](#-api-reference)
7. [Socket.IO Events](#-socketio-events)
8. [How It Works](#-how-it-works)
9. [Security Model](#-security-model)
10. [Configuration & Environment Variables](#-configuration--environment-variables)
11. [Running the App](#-running-the-app)
12. [Testing](#-testing)
13. [Deployment](#-deployment)
14. [Tech Stack](#-tech-stack)
15. [Limitations & Roadmap](#-limitations--roadmap)

---

## 🌐 Project Overview

**PeerDrop** is a self-hosted, LAN-first file sharing and real-time chat web application. It runs a lightweight Node.js server that handles WebRTC signaling and Socket.IO connections — but file data itself flows **peer-to-peer** directly between browsers using WebRTC DataChannels, bypassing the server entirely.

| Property        | Value                                                      |
|-----------------|------------------------------------------------------------|
| **Version**     | v12 (Resilient Connection Server)                          |
| **License**     | MIT                                                        |
| **Runtime**     | Node.js (Express + Socket.IO)                             |
| **Client**      | Vanilla JS SPA — no framework, no bundler                 |
| **Default Port**| `3000`                                                     |
| **PWA**         | Yes — installable, offline-capable via Service Worker      |

---

## ✨ Features

### 🔁 Direct P2P File Transfer
- Files are sent in **64 KB WebRTC DataChannel chunks** directly browser-to-browser
- Read-ahead (6 chunks) + backpressure control for maximum throughput (~10–40 MB/s on LAN)
- Transfer persists across the same session — send multiple files without reconnecting
- Receiver approval prompt shown before each file transfer starts

### 💬 Room Chat (WebRTC Session)
- Group chat and **private (DM) messages** within a transfer session (up to 8 members)
- Host approves new members via join-request popup
- Members list updated in real time

### 🏠 Local Chat Rooms
- Multi-user chat rooms served over LAN via Socket.IO (up to 50 users per room)
- Optional **password protection** (bcrypt-equivalent: `crypto.scryptSync`)
- **File sharing** in chat (up to 10 MB per file, sequential 64 KB chunks)
- Chat history stored in browser **IndexedDB**
- Room discovery: lists all rooms on current PeerDrop host
- Bonjour/mDNS advertisement for multi-host discovery on the LAN

### 💻 Connect to PC Inbox
- PIN-protected HTTP file inbox hosted on the server's disk
- Stream uploads (up to **20 GB** per file) directly to disk — no memory limit
- Browser-downloadable files from the inbox; file deletion supported
- Sessions expire after **8 hours**; PIN rate-limited (5 attempts per 10 min)
- Files stored at `~/PeerDrop/Inbox` by default (configurable via `PEERDROP_INBOX_DIR`)

### 📷 QR Code Pairing
- Receiver generates a QR code; sender scans it to initiate transfer
- Camera-based QR scanner (lazy-loaded `html5-qrcode.min.js`)
- Also works with manually entered 6-character room codes

### 🧠 Local AI File Descriptions
- Optional integration with a local [Ollama](https://ollama.com/) instance
- Supported formats: JPEG, PNG, WebP, and text-based files (up to 5 MB)
- Default model: `llava`; configurable via `OLLAMA_MODEL`
- Description shown in approval prompt before file is accepted

### 🔄 Resilient Connections
- **15-second grace period** on socket disconnect — peers can rejoin without losing the room
- ICE restart on connection drop (`restartIce()`)
- Heartbeat: ping every 8 s, timeout after 25 s silence (Socket.IO server-side)
- Animated reconnect UI with 30-second countdown timer

### 📱 Mobile & PWA
- **Wake Lock API** — keeps screen on during transfers
- Service Worker caches all static assets for offline UI
- Installable to home screen (`manifest.json`, `display: standalone`)
- Responsive layout; works on phones, tablets, and desktops

---

## 🏗️ Architecture

```
                     ┌──────────────────────────────────┐
                     │         server.js (LAN host)      │
                     │  Express + Socket.IO              │
                     │  - Room management                │
                     │  - WebRTC signaling relay         │
                     │  - Local Chat relay               │
                     │  - Bonjour/mDNS discovery         │
                     │  - Ollama AI endpoint             │
                     └─────────┬────────────────┬────────┘
                               │                │
         Socket.IO signaling   │                │  Socket.IO signaling
                               │                │
                    ┌──────────┴──┐          ┌──┴──────────┐
                    │  Sender     │          │  Receiver   │
                    │  (Browser)  │◄────────►│  (Browser)  │
                    └─────────────┘ WebRTC   └─────────────┘
                                   DataChannel
                                 (Bypasses server!)

 ──────────────────────────────────────────────────────────────────────

  Connect to PC inbox (separate HTTP flow — files DO pass through server):
                    ┌─────────────┐  HTTP POST  ┌─────────────────────┐
                    │  Browser    │────────────►│  Host Inbox (disk)  │
                    │  (client)   │◄────────────│  ~/PeerDrop/Inbox/  │
                    └─────────────┘  HTTP GET   └─────────────────────┘
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| No STUN on LAN | When hostname is `localhost`, `127.x`, `10.x`, `192.168.x`, or `172.x`, STUN servers are omitted to reduce latency and external dependency |
| 64 KB WebRTC chunks | Conservative SCTP message size for maximum browser compatibility |
| 15-second grace period | Handles brief WiFi drops without forcing full reconnection |
| Lazy QR scanner load | `html5-qrcode.min.js` is only loaded on demand to keep initial page weight small |
| IndexedDB for chat history | Survives page refreshes and tab closes on the same device |

---

## 📁 Project Structure

```
peerdrop/
├── server.js                   # Main Express + Socket.IO app server
├── server/
│   └── pc-connect.js           # PIN-protected, streamed LAN inbox routes
├── package.json                # Dependencies and npm scripts
├── render.yaml                 # Render.com deployment config
├── .gitignore
├── public/                     # Static assets (served by Express)
│   ├── index.html              # Single-page application shell (529 lines)
│   ├── manifest.json           # PWA manifest (installable, standalone display)
│   ├── sw.js                   # Service worker — offline cache (peerdrop-v16)
│   ├── css/
│   │   └── style.css           # Full UI styles (dark/light, responsive)
│   └── js/
│       ├── app.js              # PeerDrop client: WebRTC, rooms, chat, UI (1515 lines)
│       ├── pc-connect.js       # Connect to PC inbox UI (client-side)
│       ├── local-chat.js       # Local Chat Rooms UI + IndexedDB + file transfers (744 lines)
│       ├── qrcode.min.js       # QR code generator (bundled, offline)
│       └── html5-qrcode.min.js # QR camera scanner (lazy-loaded on demand)
├── test/
│   └── pc-connect.test.js      # Integration tests for PIN auth, upload, download, delete
└── devproject.md               # Development notes and decisions log
```

---

## 🔧 Core Modules

### `server.js` — Main Server (790 lines)

The central orchestrator. Responsibilities:

| Responsibility | Details |
|---|---|
| **Express app** | Serves static assets from `public/`, handles API routes |
| **Socket.IO server** | Manages WebRTC signaling, room membership, local chat relay |
| **Room store** | In-memory `Map` of transfer rooms (max 8 members, 2h expiry) |
| **Local Chat store** | In-memory `Map` of chat rooms (max 50 members) |
| **Bonjour/mDNS** | Publishes `peerdrop-chat` service; discovers other PeerDrop hosts |
| **Ollama proxy** | Forwards file content to local Ollama for AI descriptions |
| **Grace period** | 15s timer delays removal of disconnected peers |

**Key constants:**

```js
const GRACE_MS             = 15_000;   // Grace period on socket disconnect
const MAX_ROOM_MEMBERS     = 8;        // Transfer room cap
const MAX_CHAT_LENGTH      = 2_000;    // Message character limit
const MAX_LOCAL_CHAT_MEMBERS = 50;     // Local chat room cap
const MAX_LOCAL_CHAT_FILE_SIZE = 10 * 1024 * 1024;  // 10 MB
const LOCAL_CHAT_CHUNK_SIZE    = 64 * 1024;          // 64 KB per chunk
```

---

### `server/pc-connect.js` — PC Inbox (261 lines)

Handles the **Connect to PC** HTTP inbox. Registered as middleware on the Express app.

| Route | Method | Purpose |
|---|---|---|
| `/api/pc-connect/host` | GET | Returns the 6-digit PIN (loopback only) |
| `/api/pc-connect/auth` | POST | Authenticates with PIN → sets session cookie |
| `/api/pc-connect/logout` | POST | Clears session cookie |
| `/api/pc-connect/files` | GET | Lists all files in inbox |
| `/api/pc-connect/upload` | POST | Streams a file to disk |
| `/api/pc-connect/files/:id` | GET | Downloads a file from inbox |
| `/api/pc-connect/files/:id` | DELETE | Deletes a file from inbox |

**Security features:**
- PIN is rate-limited: 5 attempts per IP per 10 minutes
- Session cookies are `HttpOnly; SameSite=Strict`; expire in 8 hours
- PIN comparison uses `crypto.timingSafeEqual` (prevents timing attacks)
- Max 100 concurrent sessions
- Upload uses `Transform` stream to enforce file size limit without buffering

---

### `public/js/app.js` — PeerDrop Client (1515 lines)

The main frontend class `PeerDrop` handles:

| Area | Details |
|---|---|
| **Socket.IO** | Connects via WebSocket+polling; handles room events |
| **WebRTC** | Creates `RTCPeerConnection`, `RTCDataChannel`; relays SDP/ICE via socket |
| **File sending** | Queue-based; 64 KB chunks; read-ahead=6; backpressure (8 MB high / 2 MB low) |
| **File receiving** | Collects chunks; triggers browser download on completion |
| **Heartbeat** | Ping every 5 s; declares peer dead if no pong in 12 s |
| **ICE restart** | Detects connection failure; calls `restartIce()` after 3 s |
| **Wake Lock** | Acquires `navigator.wakeLock.request('screen')` during transfers |
| **QR pairing** | Generates QR for room URL; scans QR via lazy-loaded library |
| **AI descriptions** | Posts file to `/api/describe` before sending permission prompt |
| **Disconnect UI** | Animated 30-second countdown modal on peer disconnect |
| **Room approval** | Host sees popup for each join request; can approve or decline |

**Tuning constants:**

```js
const CHUNK   = 64 * 1024;         // 64 KB chunk size
const BUF_HI  = 8 * 1024 * 1024;  // Pause sending at 8 MB buffer
const BUF_LO  = 2 * 1024 * 1024;  // Resume sending at 2 MB buffer
const AHEAD   = 6;                 // Read-ahead chunks
const PING_MS = 5_000;             // Heartbeat interval
const PONG_MS = 12_000;            // Dead-peer timeout
const ICE_RESTART_DELAY = 3_000;   // Wait before restartIce()
```

---

### `public/js/local-chat.js` — Local Chat UI (744 lines)

The `LocalChatRooms` class handles:

| Area | Details |
|---|---|
| **Room creation** | Emits `chat-room:create` with name + optional password |
| **Room joining** | Emits `chat-room:join` via room code, room address, or QR scan |
| **Messaging** | Emits `chat-room:message`; supports clickable URLs |
| **File sharing** | `chat-room:file-start` → sequential `chat-room:file-chunk` → `chat-room:file-complete` |
| **Discovery** | Polls `/api/chat/rooms` and `/api/chat/discovery` for host and Bonjour-discovered servers |
| **IndexedDB** | Persists recent messages per room; restored on rejoin |
| **QR invite** | Generates QR for `ws://IP:PORT/roomId` join URL |
| **Multi-socket** | Connects to external PeerDrop hosts discovered via Bonjour |

---

### `public/js/pc-connect.js` — PC Inbox UI (client)

Handles the **Connect to PC** panel in the home screen:
- Fetches the host PIN on loopback; shows "Available on host device" for guests
- Submits PIN to `/api/pc-connect/auth`; stores session cookie
- Lists inbox files, triggers downloads, handles multi-file uploads with progress
- Shows upload progress per file

---

### `public/sw.js` — Service Worker (54 lines)

Cache name: `peerdrop-v16`. Caches all static assets on install:

```
/, /index.html, /css/style.css, /js/app.js, /js/pc-connect.js,
/js/local-chat.js, /js/qrcode.min.js, /manifest.json, /socket.io/socket.io.js
```

- API routes, Socket.IO live traffic, and `/local-info` are always fetched from network
- On activate, old caches are purged

---

## 📡 API Reference

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/local-info` | GET | None | Returns local IP, port, timestamp |
| `/api/rooms` | GET | None | Lists available transfer rooms (non-full) |
| `/api/chat/rooms` | GET | None | Lists active local chat rooms |
| `/api/chat/discovery` | GET | None | Lists Bonjour-discovered PeerDrop hosts |
| `/api/describe` | POST | None | AI description for image/text files via Ollama |
| `/api/pc-connect/host` | GET | Loopback only | Returns host PIN |
| `/api/pc-connect/auth` | POST | None | PIN login → session cookie |
| `/api/pc-connect/logout` | POST | Session | Clears session cookie |
| `/api/pc-connect/files` | GET | Session | Lists inbox files |
| `/api/pc-connect/upload` | POST | Session | Upload file to inbox (streamed) |
| `/api/pc-connect/files/:id` | GET | Session | Download a file from inbox |
| `/api/pc-connect/files/:id` | DELETE | Session | Delete a file from inbox |

---

## 🔌 Socket.IO Events

### Transfer Room Events

| Event | Direction | Payload | Description |
|---|---|---|---|
| `create-room` | Client → Server | `{ displayName }` | Host creates a new room |
| `room-created` | Server → Client | `{ roomId, token, localIP, port }` | Room created confirmation |
| `join-room` | Client → Server | `{ roomId, token, displayName }` | Join with QR token or request approval |
| `join-request` | Server → Host | `{ peerId, name }` | Host receives join request |
| `join-request-pending` | Server → Client | `{ roomId, hostName }` | Pending approval notification |
| `approve-join` | Host → Server | `{ peerId }` | Host approves member |
| `decline-join` | Host → Server | `{ peerId }` | Host declines member |
| `room-joined` | Server → Client | `{ roomId, hostId, members, canTransfer }` | Successful join |
| `room-members` | Server → Room | `[{ id, name }]` | Updated member list |
| `room-closed` | Server → Room | `{ reason }` | Room was closed |
| `join-error` | Server → Client | `{ message }` | Join failed |
| `leave-room` | Client → Server | — | Explicit leave |
| `peer-temporarily-gone` | Server → Room | `{ peerId, graceMs }` | Peer disconnected (grace period started) |
| `peer-disconnected` | Server → Host | `{ reason }` | Grace expired, peer gone |
| `chat-message` | Client ↔ Server ↔ Client | `{ text, to? }` | Group or private message |
| `offer` | Client → Server → Client | `{ offer, to }` | WebRTC SDP offer relay |
| `answer` | Client → Server → Client | `{ answer, to }` | WebRTC SDP answer relay |
| `ice-candidate` | Client → Server → Client | `{ candidate, to }` | ICE candidate relay |
| `peer-joined` | Server → Host | `{ peerId }` | Transfer peer joined (triggers offer) |
| `room-member-joined` | Server → Room | `{ peerId, name }` | Non-transfer member joined |

### Local Chat Room Events

| Event | Direction | Description |
|---|---|---|
| `chat-room:create` | Client → Server | Create a new local chat room |
| `chat-room:join` | Client → Server | Join by room ID + optional password |
| `chat-room:leave` | Client → Server | Leave a room |
| `chat-room:close` | Host → Server | Close a room (host only) |
| `chat-room:message` | Client ↔ Server | Send/receive text message |
| `chat-room:members` | Server → Room | Updated member list |
| `chat-room:notice` | Server → Room | System notice (join/leave) |
| `chat-room:closed` | Server → Room | Room was closed by host |
| `chat-room:file-start` | Client → Server | Announce file transfer start |
| `chat-room:file-chunk` | Client → Server | Send a 64 KB chunk |
| `chat-room:file-complete` | Server → Room | All chunks received |
| `chat-room:file-error` | Server → Room | File transfer error |

---

## 🔐 Security Model

| Layer | Mechanism |
|---|---|
| **P2P file transfer** | WebRTC DTLS encryption end-to-end |
| **Room token** | 32-byte random hex token in QR code; prevents spoofed joins |
| **Local Chat password** | `crypto.scryptSync` + `crypto.timingSafeEqual` comparison |
| **PC inbox auth** | 6-digit PIN; `timingSafeEqual` comparison; rate-limited per IP |
| **Session cookies** | `HttpOnly; SameSite=Strict; Max-Age=8h` |
| **Upload validation** | Filename sanitized; blocked extensions/MIME types; size enforced via stream |
| **Local Chat files** | Blocked extensions: `exe, bat, sh, ps1, html, js, svg` + others |
| **Cross-origin** | Same-origin check on state-mutating PC inbox routes |
| **PIN visibility** | `/api/pc-connect/host` only responds to loopback addresses |

> [!WARNING]
> Connect to PC uses **plain HTTP by default**. The PIN, session cookie, and file contents are not encrypted in transit. Use only on a trusted LAN or configure HTTPS via a reverse proxy.

---

## ⚙️ Configuration & Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP server port |
| `PEERDROP_PIN` | Random 6-digit | Fixed PIN for the PC inbox |
| `PEERDROP_INBOX_DIR` | `~/PeerDrop/Inbox` | Disk path for uploaded inbox files |
| `OLLAMA_URL` | `http://127.0.0.1:11434` | Base URL for local Ollama instance |
| `OLLAMA_MODEL` | `llava` | Ollama model used for AI file descriptions |

---

## 🚀 Running the App

### Prerequisites

- Node.js v18+ recommended
- npm

### Install and start

```bash
git clone https://github.com/YOUR_USERNAME/peerdrop.git
cd peerdrop
npm install
node server.js
```

On Windows PowerShell where npm scripts may be blocked by execution policy:

```powershell
npm.cmd install
npm.cmd start
```

### Development (with auto-restart)

```bash
npm run dev    # uses nodemon
```

### Startup output

```
======================================================
⚡ PeerDrop v12 — Resilient Connection Server
   Grace period: 15s | Keepalive: 8s/25s
📶 WiFi → http://192.168.x.x:3000
💻 Local → http://localhost:3000
🔐 Connect to PC PIN: 123456
======================================================
```

### Optional: Ollama AI descriptions

```bash
ollama pull llava
node server.js
```

---

## 🧪 Testing

Tests use Node's built-in `node:test` runner. The `pc-connect.test.js` covers:

- PIN authentication (correct, wrong, missing, rate limit)
- Session cookie issuance and expiry enforcement
- File upload (full, chunked, oversized, missing filename)
- File listing and download
- File deletion
- Unauthenticated access rejection

```bash
npm test
# or
node --test
```

---

## ☁️ Deployment

### Render.com

A `render.yaml` is included for one-click deploy:

```yaml
services:
  - type: web
    name: peerdrop
    runtime: node
    buildCommand: npm install
    startCommand: node server.js
    plan: free
```

> [!NOTE]
> When deployed to a public host (Render, Railway, etc.), WebRTC will require STUN. `app.js` automatically includes `stun:stun.l.google.com:19302` when the hostname is not a private/local address.

### Cloudflare Tunnel

A `cloudflared.exe` binary is included in the project for exposing the local server via a Cloudflare Tunnel without port forwarding.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Backend** | Node.js, Express 4, Socket.IO 4 |
| **Compression** | `compression` middleware (gzip/deflate for HTTP responses) |
| **LAN discovery** | `bonjour-service` (mDNS/Bonjour) |
| **Frontend** | Vanilla JavaScript ES2022 (no framework, no bundler) |
| **Real-time** | WebRTC DataChannels, Socket.IO WebSocket |
| **PWA** | Web App Manifest + Service Worker Cache API |
| **Persistence** | IndexedDB (chat history), localStorage (name preference) |
| **Screen Lock** | Wake Lock API |
| **QR Code** | `qrcode.min.js` (generator), `html5-qrcode.min.js` (scanner, lazy-loaded) |
| **AI** | Ollama (local LLM inference, optional) |
| **Tests** | Node.js `node:test` built-in runner |
| **Deployment** | Render.com / Cloudflare Tunnel |

---

## ⚠️ Limitations & Roadmap

### Current Limitations

| Limitation | Notes |
|---|---|
| File transfer memory | WebRTC receiver assembles file in **browser memory** before download — large files may exceed RAM. Use Connect to PC for >500 MB files. |
| WebRTC mesh | Only **host ↔ first guest** get a direct DataChannel; other members are chat-only |
| Local Chat encryption | Traffic is relayed by host (not end-to-end encrypted) — use a trusted LAN |
| Bluetooth | Not included — browser BLE support is too limited (no iOS Safari, small payloads) |
| Mobile hotspot + screen lock | Some phones kill the hotspot when the screen locks or battery saver activates |
| mDNS on guest Wi-Fi | Client isolation on many routers blocks multicast DNS; use manual IP/code instead |
| Camera QR on HTTP LAN | Browsers require HTTPS for camera access (except `localhost`) |

### Suggested Next Steps (from devproject.md)

- Extract Local Chat server handlers into `server/local-chat.js` for better modularity
- Add HTTPS support via a built-in self-signed cert or Let's Encrypt integration
- WebRTC mesh for multi-peer direct transfers
- Persistent rooms (survives server restart)

---

## 📅 Recent Changes (Oct 2026)

| Date | Change |
|---|---|
| Oct 8, 2026 | Added `server/pc-connect.js` — dedicated PIN-protected inbox module |
| Oct 8, 2026 | Added `test/pc-connect.test.js` — integration tests for PC inbox routes |
| Oct 8, 2026 | Added `public/sw.js` updates — cache bumped to `peerdrop-v16` |
| Oct 8, 2026 | Added `public/manifest.json` — PWA installability with lightning bolt icon |
| Oct 8, 2026 | Added `public/js/local-chat.js` — Local Chat Rooms UI (IndexedDB, QR, file chunks) |
| Oct 8, 2026 | Added compression middleware (`compression` npm package) |
| Oct 8, 2026 | Server now reports Connect to PC PIN on startup |

---

*Generated from source — `C:\Users\shivi\.gemini\antigravity\scratch\peerdrop`*
