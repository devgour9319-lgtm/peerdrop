# ⚡ PeerDrop

**Direct browser-to-browser file transfer over WiFi. Zero cloud. Zero internet.**

PeerDrop lets two devices on the same WiFi network share files instantly through WebRTC DataChannels — your files never leave your local network.

![Architecture](https://img.shields.io/badge/Architecture-Sender_→_WiFi_→_Receiver-00d9aa?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)

## ✨ Features

- 📶 **Direct WiFi P2P** — Files transfer directly between browsers, no cloud relay
- 🔒 **Encrypted file channel** — WebRTC DTLS encrypts direct file transfers; pairing links include an unguessable token
- 💬 **Room and private chat** — Group messages and one-to-one direct messages for up to 8 room members
- 🏠 **Local Chat Rooms** — Standalone, multi-user chat rooms hosted on your LAN, with optional passwords, links, images, and small-file sharing
- 🔎 **LAN room discovery** — Browse active rooms on the same PeerDrop server; hosts approve new members
- 📷 **QR Code Pairing** — Scan to connect, no typing needed
- 🚀 **Reliable** — 64KB WebRTC chunks for broader browser compatibility, read-ahead and backpressure
- 💻 **Connect to PC** — PIN-protected LAN inbox with streamed disk uploads and browser downloads
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

In PowerShell environments where the `npm` script is blocked by execution policy, use `npm.cmd install` and `npm.cmd start`.

Open `http://localhost:3000` on the server device, then open the server's WiFi address (shown in the header) on the other devices. Devices must use the same PeerDrop server instance to discover rooms and chat.

## 🏠 Offline Local Chat Rooms

Local Chat uses the PeerDrop server's Socket.IO WebSocket connection. The host serves the UI and relays room traffic over the local network; no internet, cloud service, or external asset is needed. Each room supports up to 50 users, an optional password, live membership and join/leave notices, linked URLs, and sequential 64 KB file chunks. Files are limited to 10 MB on the server; each device can choose a lower limit of 1 MB or 5 MB. HTML, scripts, styles, QR libraries, and the Socket.IO client are served locally; HTTP compression reduces text asset transfer size, and the camera QR scanner is fetched only when first used. Recent chat messages are stored in the browser's IndexedDB.

### Host and join

1. Install Node.js on the device that will host, then run `npm install` and `npm start` in the PeerDrop folder. Allow Node.js on **Private networks** if Windows Firewall prompts.
2. Start a Wi-Fi hotspot on the host phone/computer, or connect every device to the same router. An internet connection is not required. A phone hotspot may turn off when battery saver is enabled or the screen is locked.
3. Open PeerDrop on the host at `http://localhost:3000` (computer) or `http://<host-local-IP>:3000` (phone). Tap **Local Chat**, enter a room name, optionally set a password, and tap **Create Room**.
4. Share the displayed QR code or the `ws://<host-local-IP>:3000/<roomId>` address. Guests open PeerDrop on that same host address, choose **Local Chat**, and scan the QR (where camera access is supported), paste the address, or enter its room code. Enter the password if prompted.
5. To find the host IP, look at the address shown in the Local Chat room header or the PeerDrop server console. On Windows, `ipconfig` shows the active Wi-Fi adapter's **IPv4 Address**. On a phone, check the hotspot/tethering page or the connected Wi-Fi network details. The address is usually in a private range such as `192.168.x.x`, `10.x.x.x`, or `172.16`–`172.31.x.x`.
6. **Rooms on this host** lists rooms served by the page's host. When Bonjour/mDNS multicast is allowed on the LAN, PeerDrop also discovers other PeerDrop hosts and lists their rooms. Network isolation on guest Wi-Fi/hotspots can prevent discovery; use the room IP and code instead.

### Connect to PC inbox

The **Connect to PC** inbox is an HTTP client/server option alongside the direct WebRTC flow. The device running PeerDrop hosts the web UI and stores uploaded files on disk; it is not peer-to-peer and does not provide end-to-end encryption. By default, files are saved under `~/PeerDrop/Inbox`. Set `PEERDROP_INBOX_DIR` before starting PeerDrop to choose another folder. Uploads are streamed to disk and limited to 20 GB per file.

1. Start PeerDrop on the phone or computer that will host the inbox. On Android, run the Node.js server from Termux; PeerDrop is not a native Android app and cannot start a hotspot itself.
2. On the host device, open PeerDrop locally. The **Connect to PC** card displays its six-digit PIN; the same PIN is printed in the server console. A fresh PIN is generated on each server start. Set `PEERDROP_PIN` to a chosen six-digit PIN if needed.
3. Connect the PC and phone to the same Wi-Fi or hotspot. Open the host's LAN URL (for example, `http://192.168.1.15:3000`) in the PC browser and enter the PIN.
4. Upload files into the host inbox, download files from it, or delete files when no longer needed. The PIN is rate-limited, and authenticated browser sessions expire after eight hours.

Keep the server on a trusted LAN and do not forward its port to the public internet. On plain HTTP, the PIN, session cookie, and inbox file contents are not encrypted in transit; use a trusted network or configure HTTPS through a trusted reverse proxy. The PIN is an access gate, not encryption.

### Troubleshooting and limitations

- **Cannot connect:** Confirm all devices are on the same Wi-Fi/hotspot and use the host's current LAN IPv4 address and port `3000`. Do not use `localhost` from a guest device.
- **Firewall/router:** Permit inbound TCP traffic on the configured PeerDrop port (default `3000`) for private networks. Some guest Wi-Fi networks enable client isolation and block device-to-device traffic; disable isolation or use another LAN.
- **QR camera unavailable:** Browser camera access requires a secure context (HTTPS or localhost). On plain HTTP LAN addresses, use a phone's QR scanner and paste/type the displayed address, or configure trusted HTTPS for the LAN host.
- **mDNS unavailable:** Bonjour discovery depends on multicast DNS being supported and permitted by the operating system, firewall, router, and hotspot. Manual IP/room-code joining does not depend on mDNS.
- **Offline PWA:** Open PeerDrop once while connected to the host so the service worker can cache the bundled UI. The server and the local Wi-Fi link must still be running for live chat. Browser service-worker installation requires HTTPS or localhost; some mobile browsers do not enable camera access or service workers for plain HTTP LAN origins.
- **Very large direct transfers:** The WebRTC receiver assembles a file in browser memory before triggering its download. For files that may exceed available browser memory, use the **Connect to PC** inbox, which streams uploads to host storage instead.
- **Trust and privacy:** Optional room passwords protect Local Chat room entry, not traffic encryption. Direct Send/Receive file payloads use the encrypted WebRTC channel; Local Chat traffic is relayed by the host; Connect to PC files are stored on the host and use plain HTTP unless HTTPS is configured. Use a trusted LAN. Shared file extensions and MIME types do not prove that contents are safe.

Bonjour discovery is provided by the `bonjour-service` Node package. Bluetooth LE text chat is not included: browser Web Bluetooth support is limited (notably absent in iOS Safari), BLE has small payloads/throughput and permissions constraints, and browser pages cannot provide reliable general-purpose BLE advertising/discovery across devices. WebRTC mesh is also not included; the current signaling and chat model is server-relayed, while existing PeerDrop file transfer remains a separate direct WebRTC flow.

### Direct File-Transfer Sessions

The original **Send Files** / **Receive Files** flow remains separate from Local Chat Rooms. The receiver creates a pairing session and shares its QR code; the sender scans it or enters the pairing code. The host and first guest establish the direct WebRTC file-transfer channel. The existing session chat supports group and private messages; additional members do not receive their own direct file-transfer channel in this MVP.

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
├── server.js              # Express + Socket.io app server
├── server/
│   └── pc-connect.js      # PIN-protected, streamed LAN inbox routes
├── package.json
├── public/
│   ├── index.html         # Single-page app
│   ├── manifest.json      # PWA manifest
│   ├── sw.js              # Service worker (offline cache)
│   ├── css/
│   │   └── style.css      # Full UI styles
│   └── js/
│       ├── app.js          # PeerDrop client (WebRTC + transfer-room UI)
│       ├── pc-connect.js   # Connect to PC inbox UI
│       ├── local-chat.js   # Local Chat Rooms, IndexedDB, QR and file UI
│       ├── qrcode.min.js   # QR code generator
│       └── html5-qrcode.min.js  # QR camera scanner
├── test/
│   └── pc-connect.test.js # PIN and inbox integration tests
└── devproject.md          # Development notes
```

Run focused feature tests with `npm test`.

Suggested next modularization step: extract the Local Chat server handlers and validation helpers from `server.js` into a `server/local-chat.js` module, with shared configuration passed in explicitly.

## 🏗️ Architecture

```
┌──────────┐    WiFi (LAN)    ┌──────────┐
│  Sender  │ ◄──────────────► │ Receiver │
│ (Browser)│   WebRTC P2P     │ (Browser)│
└────┬─────┘                  └────┬─────┘
     │    Socket.io (signaling)    │
     └────────────┬────────────────┘
            ┌─────┴─────┐
            │  server.js │  ← App, rooms, chat, offers/answers/ICE
            │ (LAN host) │     Direct WebRTC files bypass it
            └───────────┘
```

The separate **Connect to PC** inbox uses HTTP to stream files to and from the host's disk; unlike direct WebRTC transfers, those files do pass through and are stored on the server.

## 🔧 How It Works

1. **Discovery and membership** — `server.js` lists rooms on this server and requires host approval for discovery/code-based joins; QR pairing uses the room token.
2. **Chat and signaling** — Group and private messages are relayed to room members by Socket.io; WebRTC offers/answers/ICE candidates are also relayed by Socket.io.
3. **Connection** — The host and first guest exchange offers, then open a direct WebRTC DataChannel over the local WiFi network.
4. **Transfer** — Files are sent in 64 KB chunks through the DataChannel at LAN speed.
5. **AI descriptions** — When local Ollama is available, supported files are analyzed by the PeerDrop server before the receiver approval prompt; only the description is added to the peer-to-peer metadata.
6. **Resilience** — If WiFi hiccups, ICE restartIce() repairs the connection. If the socket drops, a 15-second grace period lets the peer rejoin without losing the room.

## 📱 Mobile Tips

- **Keep the screen on** — PeerDrop requests a wake lock, but some phones still throttle background tabs
- **Use 5 GHz WiFi** — Much faster and more stable than 2.4 GHz
- **Stay on the same network** — Both devices must be on the same WiFi / hotspot

## License

MIT
