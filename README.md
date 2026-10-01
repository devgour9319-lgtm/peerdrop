# ⚡ PeerDrop

**Direct browser-to-browser file transfer over WiFi. Zero cloud. Zero internet.**

PeerDrop lets two devices on the same WiFi network share files instantly through WebRTC DataChannels — your files never leave your local network.

![Architecture](https://img.shields.io/badge/Architecture-Sender_→_WiFi_→_Receiver-00d9aa?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)

## ✨ Features

- 📶 **Direct WiFi P2P** — Files transfer directly between browsers, no cloud relay
- 🔒 **E2E Encrypted** — WebRTC DTLS encryption + token-authenticated rooms
- 📷 **QR Code Pairing** — Scan to connect, no typing needed
- 🚀 **Fast** — 256KB chunks, 8-chunk read-ahead pipeline, backpressure tuning
- 🔄 **Resilient** — ICE restart on disconnect, heartbeat dead-connection detection, 15s server grace period
- 📱 **Wake Lock** — Keeps screen on during transfer so phone doesn't kill the connection
- 🔁 **Persistent Connection** — Send multiple files without reconnecting
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

Open `http://localhost:3000` on both devices (connected to the same WiFi).

### How to Use

1. **Device A** (Receiver): Click **Receive Files** → QR code appears
2. **Device B** (Sender): Click **Send Files** → Scan the QR code
3. WebRTC connects directly over WiFi → Drop files to send!

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
            │  server.js │  ← Only passes offers/answers/ICE
            │ (localhost)│     Never touches file data
            └───────────┘
```

## 🔧 How It Works

1. **Signaling** — `server.js` creates rooms and relays WebRTC offers/answers/ICE candidates via Socket.io. It never sees file data.
2. **Connection** — Once both peers exchange offers, a direct WebRTC DataChannel opens over the local WiFi network.
3. **Transfer** — Files are chunked and streamed through the DataChannel at full LAN speed.
4. **Resilience** — If WiFi hiccups, ICE restartIce() repairs the connection. If the socket drops, a 15-second grace period lets the peer rejoin without losing the room.

## 📱 Mobile Tips

- **Keep the screen on** — PeerDrop requests a wake lock, but some phones still throttle background tabs
- **Use 5 GHz WiFi** — Much faster and more stable than 2.4 GHz
- **Stay on the same network** — Both devices must be on the same WiFi / hotspot

## License

MIT
