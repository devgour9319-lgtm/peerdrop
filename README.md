# ⚡ PeerDrop

**Direct browser-to-browser file transfer over WiFi. Zero cloud. Zero internet.**

PeerDrop lets two devices on the same WiFi network share files instantly through WebRTC DataChannels — your files never leave your local network.

![Architecture](https://img.shields.io/badge/Architecture-Sender_→_WiFi_→_Receiver-00d9aa?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)

## ✨ Features

- 📶 **Direct WiFi P2P** — Files transfer directly between browsers, no cloud relay
- 🔒 **Encrypted file channel** — WebRTC DTLS encrypts direct file transfers; pairing links include an unguessable token
- 💬 **Room and private chat** — Group messages and one-to-one direct messages for up to 8 room members
- 🔎 **LAN room discovery** — Browse active rooms on the same PeerDrop server; hosts approve new members
- 📷 **QR Code Pairing** — Scan to connect, no typing needed
- 🚀 **Fast** — 128KB chunks, read-ahead pipeline, backpressure tuning
- 🔄 **Resilient** — ICE restart on disconnect, heartbeat dead-connection detection, 15s server grace period
- 📱 **Wake Lock** — Keeps screen on during transfer so phone doesn't kill the connection
- 🔁 **Persistent Connection** — Send multiple files without reconnecting
- 🧠 **Local AI descriptions** — Ollama describes supported images and text files; no third-party AI service
- 📡 **Disconnect Popup** — Animated reconnect UI with 30s countdown + auto-retry
- 🌐 **PWA Ready** — Install to home screen, works offline after first load

## 🚀 Quick Start

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/peerdrop.git
cd peerdrop

# Install
npm install

# Run
node server.js
```

Open `http://localhost:3000` on the server device, then open the server's WiFi address (shown in the header) on the other devices. Devices must use the same PeerDrop server instance to discover rooms and chat.

### How to Use

1. Choose a display name. One device clicks **Receive Files** to create a room.
2. Other devices can select the room under **Nearby rooms** (the host approves each request), scan the QR, or enter the room code.
3. Room members can use group chat or select a member for a private message.
4. The host and first guest establish the direct WebRTC file-transfer channel. Additional members can chat but do not get a direct file-transfer channel in this MVP.

Room discovery is provided by the PeerDrop signaling server's active-room list; it is not UDP broadcast or mDNS and does not discover separate PeerDrop server instances. Chat messages are relayed by that server, so use a trusted local server/network. They are not end-to-end encrypted on plain HTTP; use HTTPS when deploying beyond a trusted LAN. File bytes continue to travel directly over the WebRTC DataChannel.

### Local AI Descriptions

Install [Ollama](https://ollama.com/) on the same computer that runs PeerDrop, then download a vision-capable model:

```bash
ollama pull llava
npm start
```

PeerDrop uses `http://127.0.0.1:11434` and the `llava` model by default. Set `OLLAMA_URL` or `OLLAMA_MODEL` before starting the server to use another local Ollama instance or model. AI descriptions are generated for JPEG, PNG, WebP, and text-based files up to 5 MB; unsupported files still transfer normally without an AI description. The sender uploads the selected file to the PeerDrop server for local analysis, then the description is shown to both peers. This endpoint does not save the file, but if PeerDrop is hosted remotely, file content will be sent to that host; use a local PeerDrop server to keep analysis on your own network.

## 📁 Project Structure

```
peerdrop/
├── server.js              # Express + Socket.io signaling server
├── package.json
├── public/
│   ├── index.html         # Single-page app
│   ├── manifest.json      # PWA manifest
│   ├── sw.js              # Service worker (offline cache)
│   ├── css/
│   │   └── style.css      # Full UI styles
│   └── js/
│       ├── app.js          # PeerDrop client (WebRTC + UI)
│       ├── qrcode.min.js   # QR code generator
│       └── html5-qrcode.min.js  # QR camera scanner
└── devproject.md          # Development notes
```

## 🏗️ Architecture

```
┌──────────┐    WiFi (LAN)    ┌──────────┐
│  Sender  │ ◄──────────────► │ Receiver │
│ (Browser)│   WebRTC P2P     │ (Browser)│
└────┬─────┘                  └────┬─────┘
     │    Socket.io (signaling)    │
     └────────────┬────────────────┘
            ┌─────┴─────┐
            │  server.js │  ← Rooms, chat, offers/answers/ICE
            │ (LAN host) │     Never receives direct file data
            └───────────┘
```

## 🔧 How It Works

1. **Discovery and membership** — `server.js` lists rooms on this server and requires host approval for discovery/code-based joins; QR pairing uses the room token.
2. **Chat and signaling** — Group and private messages are relayed to room members by Socket.io; WebRTC offers/answers/ICE candidates are also relayed by Socket.io.
3. **Connection** — The host and first guest exchange offers, then open a direct WebRTC DataChannel over the local WiFi network.
4. **Transfer** — Files are chunked and streamed through the DataChannel at full LAN speed.
5. **AI descriptions** — When local Ollama is available, supported files are analyzed by the PeerDrop server before the receiver approval prompt; only the description is added to the peer-to-peer metadata.
6. **Resilience** — If WiFi hiccups, ICE restartIce() repairs the connection. If the socket drops, a 15-second grace period lets the peer rejoin without losing the room.

## 📱 Mobile Tips

- **Keep the screen on** — PeerDrop requests a wake lock, but some phones still throttle background tabs
- **Use 5 GHz WiFi** — Much faster and more stable than 2.4 GHz
- **Stay on the same network** — Both devices must be on the same WiFi / hotspot

## License

MIT
