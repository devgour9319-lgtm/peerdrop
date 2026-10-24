/* =================================================================
   PeerDrop v11 — Persistent Connection + File-Complete Ack
   Fix: connection stays open after transfer so you can send more
        files without reconnecting.
   ================================================================= */
'use strict';

// ── Tuning ──────────────────────────────────────────────────────
const CHUNK   = 64 * 1024;         // 64 KB works with conservative SCTP message limits
const BUF_HI  = 8 * 1024 * 1024;  // 8 MB — pause sending
const BUF_LO  = 2 * 1024 * 1024;  // 2 MB — resume sending
const UI_MS   = 250;               // DOM throttle
const PERM_T  = 60_000;            // permission timeout 60 s
const PING_MS = 5_000;             // heartbeat every 5 s
const PONG_MS = 12_000;            // dead if no pong in 12 s
const ICE_RESTART_DELAY = 3_000;   // wait before restartIce
const AHEAD   = 6;                 // read-ahead chunks

// ── ICE: no STUN on LAN ─────────────────────────────────────────
const isLocal = /^(localhost|127\.|10\.|192\.168\.|172\.)/.test(location.hostname);
const ICE = isLocal
  ? { iceServers: [] }
  : { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

let qrScannerLoadPromise;
function loadQrScanner() {
  if (typeof Html5Qrcode !== 'undefined') return Promise.resolve();
  if (!qrScannerLoadPromise) {
    qrScannerLoadPromise = new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.src = '/js/html5-qrcode.min.js';
      script.onload = () => resolve();
      script.onerror = () => {
        qrScannerLoadPromise = null;
        reject(new Error('The local QR scanner could not be loaded.'));
      };
      document.head.appendChild(script);
    });
  }
  return qrScannerLoadPromise;
}

class PeerDrop {
  constructor() {
    this.socket  = io({ transports: ['websocket', 'polling'], reconnection: true });
    this.pc      = null;
    this.dc      = null;
    this.peerId  = null;
    this.roomId  = null;
    this.token   = null;
    this.isHost  = false;
    this.role    = 'idle';
    this.displayName = localStorage.getItem('peerdrop-name') || '';
    this.members = [];
    this.canTransfer = false;
    this._joinRequests = [];

    // Send
    this.queue        = [];
    this.preSelected  = [];
    this.sending      = false;
    this._sendUid     = null;
    this._sendMeta    = null;
    this._cancelled   = new Set();
    this._permResolve = null;
    this._permTimer   = null;
    this._pendingMeta = null;
    this._sendAbort   = false;
    // Promise resolve for file-complete ack from receiver
    this._completeResolve = null;
    this._completeTimer   = null;

    // Receive
    this.rxMeta    = null;
    this.rxChunks  = [];
    this.rxBytes   = 0;
    this.rxStart   = 0;
    this._rxId     = null;
    this._rxCancel = false;
    this._lastRxUI = 0;

    // Connection
    this._connected       = false;
    this._iceQ            = [];
    this._iceRestartTimer = null;
    this._hostIP          = null;
    this._hostPort        = null;

    // Heartbeat
    this._pingTimer = null;
    this._pongTimer = null;
    this._pingSeq   = 0;

    // Wake lock
    this._wakeLock = null;

    // Camera
    this.qrScanner  = null;
    this.isScanning = false;
    this.cameras    = [];
    this.camIdx     = 0;

    this._bindSocket();
    this._bindUI();
    $('display-name').value = this.displayName;
    this._checkURL();
    this._initWifi();

    this.log('PeerDrop v11 — persistent connection mode');
  }

  log(m) { console.log(`[PeerDrop] ${m}`); }

  // ═══════════════════════════════════════════════════════════════
  // SOCKET  (signaling only — never closes with transfer)
  // ═══════════════════════════════════════════════════════════════
  _bindSocket() {
    const s = this.socket;

    s.on('connect', () => {
      this.log('Socket connected');
      // Auto-rejoin: if socket dropped mid-session, rejoin same room
      if (this.roomId && this.token && !this.isHost) {
        this.log('Socket reconnected — rejoining room ' + this.roomId);
        this._joinRoom(this.roomId, this.token);
      }
    });

    s.on('disconnect', (reason) => {
      this.log(`Socket disconnected — reason: ${reason}`);
      // Do NOT call reset() here. Socket.io will reconnect automatically.
    });

    s.on('room-created', ({ roomId, token, localIP, port }) => {
      this.log(`Room created: ${roomId}`);
      this.roomId    = roomId;
      this.token     = token;
      this.isHost    = true;
      this.canTransfer = false;
      this.members = [{ id: s.id, name: this.displayName || 'Host' }];
      this._hostIP   = localIP;
      this._hostPort = port || 3000;
      this._showScreen('waiting');
      this._renderWaiting(roomId, token, localIP, port || 3000);
      this._renderMembers();
    });

    s.on('room-joined', ({ roomId, hostId, members = [], canTransfer = true }) => {
      this.log(`Joined room ${roomId}, host=${hostId}`);
      this.roomId = roomId;
      this.peerId = hostId;
      this.isHost = false;
      this.canTransfer = canTransfer;
      this.members = members;
      this._renderMembers();
      if (canTransfer) {
        this._showScreen('waiting');
        this._renderJoining(roomId);
      } else {
        this._showScreen('transfer');
        this._setChatOnlyMode();
      }
    });

    s.on('peer-joined', ({ peerId }) => {
      this.log(`Peer ${peerId} joined — starting WebRTC (initiator)`);
      this.peerId = peerId;
      this.canTransfer = true;
      this._startRTC(true);
    });

    s.on('room-members', (members) => {
      this.members = Array.isArray(members) ? members : [];
      this._renderMembers();
    });

    s.on('join-request', ({ peerId, name }) => {
      this._joinRequests.push({ peerId, name });
      this._showNextJoinRequest();
    });

    s.on('join-request-pending', ({ hostName }) => {
      this.toast(`Join request sent to ${hostName}. Waiting for approval…`, 'info');
    });

    s.on('chat-message', (message) => this._appendChatMessage(message));

    s.on('room-closed', () => {
      this.toast('The room host closed this room.', 'error');
      this.reset();
    });

    s.on('offer', async ({ offer, from }) => {
      this.log(`Offer from ${from}`);
      this.peerId = from;
      await this._startRTC(false);
      try {
        await this.pc.setRemoteDescription(new RTCSessionDescription(offer));
        await this._drainICE();
        const ans = await this.pc.createAnswer();
        await this.pc.setLocalDescription(ans);
        s.emit('answer', { answer: ans, to: this.peerId });
        this.log('Answer sent');
      } catch (e) { this.log(`Offer error: ${e.message}`); this.toast('Connection failed: ' + e.message, 'error'); }
    });

    s.on('answer', async ({ answer }) => {
      this.log('Answer received');
      try {
        if (this.pc && this.pc.signalingState !== 'stable') {
          await this.pc.setRemoteDescription(new RTCSessionDescription(answer));
          await this._drainICE();
        }
      } catch (e) { this.log(`Answer error: ${e.message}`); }
    });

    s.on('ice-candidate', async ({ candidate }) => {
      if (!candidate) return;
      if (this.pc && this.pc.remoteDescription?.type) {
        try { await this.pc.addIceCandidate(new RTCIceCandidate(candidate)); }
        catch (e) { this.log(`ICE add: ${e.message}`); }
      } else { this._iceQ.push(candidate); }
    });

    s.on('peer-disconnected', ({ reason } = {}) => {
      this.log(`Peer disconnected — server reason: ${reason || 'unknown'}`);
      const b = $('conn-badge');
      if (b) { b.textContent = '🔴 Peer Left'; b.className = 'status-badge disconnected'; }
      this._hidePermModal();
      // Abort any in-progress transfer
      if (this._sendUid) { this._sendAbort = true; this._cancelled.add(this._sendUid); }
      // Show disconnect popup instead of just a toast
      this._showDisconnectPopup('Peer left the session.', reason);
    });

    s.on('join-error', ({ message }) => {
      this.log(`Join error: ${message}`);
      this.toast(message, 'error');
    });
  }

  async _drainICE() {
    const q = this._iceQ.splice(0);
    for (const c of q) {
      try { await this.pc.addIceCandidate(new RTCIceCandidate(c)); }
      catch (e) { this.log(`ICE flush: ${e.message}`); }
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // WEBRTC
  // ═══════════════════════════════════════════════════════════════
  async _startRTC(initiator) {
    this.log(`_startRTC(init=${initiator})`);
    this._stopHeartbeat();
    if (this.pc) { try { this.pc.close(); } catch(_){} }

    this._connected = false;
    this.pc = new RTCPeerConnection(ICE);

    this.pc.onicecandidate = ({ candidate }) => {
      if (candidate) this.socket.emit('ice-candidate', { candidate, to: this.peerId });
    };

    this.pc.oniceconnectionstatechange = () => {
      const st = this.pc.iceConnectionState;
      this.log(`ICE state → ${st}`);

      if ((st === 'connected' || st === 'completed') && !this._connected) {
        clearTimeout(this._iceRestartTimer);
        this._onConnected();
      }

      if (st === 'disconnected') {
        this.log('ICE disconnected — scheduling restartIce in 3 s');
        this._iceRestartTimer = setTimeout(async () => {
          if (!this.pc || this.pc.iceConnectionState === 'closed') return;
          this.log('Calling restartIce()');
          this.toast('WiFi hiccup — repairing…', 'info');
          try {
            this.pc.restartIce();
            if (this.isHost) {
              const offer = await this.pc.createOffer({ iceRestart: true });
              await this.pc.setLocalDescription(offer);
              this.socket.emit('offer', { offer, to: this.peerId });
            }
          } catch (e) { this.log(`restartIce error: ${e.message}`); }
        }, ICE_RESTART_DELAY);
      }

      if (st === 'failed') {
        this.log('ICE failed — connection could not be established');
        this._stopHeartbeat();
        this._showDisconnectPopup(
          'Direct connection failed.',
          'If both devices are on the same Wi-Fi, check local-network/firewall access. Across different networks, this connection may need a TURN relay; the demo tunnel only carries signaling.'
        );
      }
    };

    if (initiator) {
      const dc = this.pc.createDataChannel('pd', { ordered: true });
      this.dc = dc;
      this._wireDC(dc);
      this.log('DataChannel created');
      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      this.socket.emit('offer', { offer, to: this.peerId });
      this.log('Offer sent');
    } else {
      this.pc.ondatachannel = (e) => {
        this.log('DataChannel received');
        this.dc = e.channel;
        this._wireDC(this.dc);
      };
    }
  }

  _wireDC(dc) {
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = BUF_LO;

    dc.onopen = () => {
      this.log('✅ DataChannel OPEN — ready for transfers');
      this._onConnected();
      this._startHeartbeat();
      if (this.preSelected.length > 0) {
        this.enqueue(this.preSelected);
        this.preSelected = [];
        this._resetPickBtn();
      }
    };

    // NOTE: dc.onclose does NOT call reset() — closing the DC while
    // the ICE connection is still up just means we need to renegotiate.
    dc.onclose = () => {
      this.log('DataChannel closed — reason: browser/network event (not transfer completion)');
      this._stopHeartbeat();
    };

    dc.onerror = (e) => this.log(`DC error: ${e?.error?.message || 'unknown'}`);

    dc.onmessage = (e) => {
      if (typeof e.data === 'string') this._onSignal(e.data);
      else                            this._ingestChunk(e.data);
    };
  }

  _onConnected() {
    if (this._connected) return;
    this._connected = true;
    this._showScreen('transfer');
    this.canTransfer = true;
    this._setChatOnlyMode();
    $('dropzone').classList.remove('hidden');
    const b = $('mode-badge');
    if (b) { b.textContent = '📶 Direct WiFi (Full Speed)'; b.className = 'mode-badge wlan'; }
    this.toast('✅ Connected — Direct WiFi. Send as many files as you want!', 'success');
    this.log('Connection ready — persistent session started');
  }

  // ═══════════════════════════════════════════════════════════════
  // HEARTBEAT — detect dead connections quickly
  // ═══════════════════════════════════════════════════════════════
  _startHeartbeat() {
    this._stopHeartbeat();
    this._pingTimer = setInterval(() => {
      if (!this.dc || this.dc.readyState !== 'open') return;
      this._pingSeq++;
      try { this.dc.send(JSON.stringify({ t: 'ping', seq: this._pingSeq })); } catch(_) {}
      this._pongTimer = setTimeout(() => {
        this.log(`No pong for seq ${this._pingSeq} — connection may be dead`);
        this.toast('Connection appears lost — check WiFi', 'error');
      }, PONG_MS);
    }, PING_MS);
  }

  _stopHeartbeat() {
    clearInterval(this._pingTimer);
    clearTimeout(this._pongTimer);
    this._pingTimer = null;
    this._pongTimer = null;
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCONNECT POPUP
  // ═══════════════════════════════════════════════════════════════
  _showDisconnectPopup(title, detail) {
    // Don't show if not on the transfer/waiting screen
    if (!this._connected && !this.roomId) return;

    this._hideDCPopup();   // clear any previous countdown

    const overlay = $('disconnect-overlay');
    const reason  = $('dc-reason');
    const hint    = $('dc-hint');
    const numEl   = $('dc-timer-num');
    const arcEl   = $('dc-timer-arc');

    if (reason) reason.textContent = title || 'Connection was lost.';
    if (hint)   hint.textContent   = detail
      ? detail
      : (this.roomId && !this.isHost)
        ? 'Tap Reconnect to rejoin the same room without going home.'
        : 'Ask the other device to rejoin using the same QR / code.';

    // Countdown: 30 s
    const TOTAL = 30;
    const CIRC  = 2 * Math.PI * 18; // ≈ 113.1
    let remaining = TOTAL;

    const tick = () => {
      remaining--;
      if (numEl) numEl.textContent = remaining;
      // Shrink the arc proportionally
      if (arcEl) arcEl.style.strokeDashoffset = `${CIRC * (1 - remaining / TOTAL)}`;
      if (remaining <= 0) {
        this._hideDCPopup();
        // Auto-attempt reconnect
        this.log('Disconnect popup timed out — auto-reconnecting');
        this._dcPopupReconnect();
      }
    };

    // Initial arc
    if (arcEl) { arcEl.style.strokeDasharray = CIRC; arcEl.style.strokeDashoffset = '0'; }
    if (numEl) numEl.textContent = TOTAL;

    this._dcCountdown = setInterval(tick, 1000);
    overlay.classList.remove('hidden');
    this.log(`Disconnect popup shown: "${title}"`);
  }

  _hideDCPopup() {
    clearInterval(this._dcCountdown);
    this._dcCountdown = null;
    const o = $('disconnect-overlay');
    if (o) o.classList.add('hidden');
  }

  _dcPopupReconnect() {
    this._hideDCPopup();
    if (!this.roomId) { this.toast('No room to reconnect to', 'error'); return; }
    if (this.isHost) {
      this.toast('Waiting for the other device to rejoin…', 'info');
    } else {
      this.toast('Reconnecting…', 'info');
      this._joinRoom(this.roomId, this.token);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // WAKE LOCK
  // ═══════════════════════════════════════════════════════════════
  async _acquireWakeLock() {
    if (!('wakeLock' in navigator)) return;
    try {
      this._wakeLock = await navigator.wakeLock.request('screen');
      this.log('WakeLock acquired');
      this._wakeLock.addEventListener('release', () => { this.log('WakeLock released'); this._wakeLock = null; });
    } catch (e) { this.log(`WakeLock: ${e.message}`); }
  }

  async _releaseWakeLock() {
    if (this._wakeLock) { try { await this._wakeLock.release(); } catch(_){} this._wakeLock = null; }
  }

  // ═══════════════════════════════════════════════════════════════
  // SIGNAL helper
  // ═══════════════════════════════════════════════════════════════
  _sig(msg) {
    if (this.dc && this.dc.readyState === 'open') { this.dc.send(msg); return true; }
    this.log(`⚠ Signal dropped (dc=${this.dc?.readyState || 'null'}): ${msg.slice(0, 60)}`);
    return false;
  }

  // ═══════════════════════════════════════════════════════════════
  // SENDER
  // ═══════════════════════════════════════════════════════════════
  enqueue(files) {
    for (const f of files) { this.queue.push(f); this._addSendUI(f); }
    if (!this.sending) this._drain();
  }

  async _drain() {
    if (this.sending) return;
    this.sending = true;
    try {
      while (this.queue.length) {
        const file = this.queue.shift();
        try {
          await this._send(file);
        } catch (error) {
          this.log(`Transfer failed for "${file.name}": ${error.message}`);
          this._sig(JSON.stringify({ t: 'cancel' }));
          this._markFailed(file._uid, error.message);
          this.toast(`Transfer failed: ${error.message}`, 'error');
          clearTimeout(this._completeTimer);
          this._completeTimer = null;
          this._completeResolve = null;
          this._sendMeta = null;
          await this._releaseWakeLock();
        }
      }
    } finally {
      this.sending = false;
    }
  }

  async _send(file) {
    this._sendUid  = file._uid;
    this._sendMeta = {
      name: file.name.replace(/[^\w.\- ]+/g, '_') || 'file',
      size: file.size,
    };
    this._sendAbort = false;
    this.log(`Sending: "${file.name}" (${fmt(file.size)})`);
    file._description = await this._describeFile(file);
    this._setDescription(file._uid, file._description || 'AI description unavailable.');
    this._updateStatus(file._uid, 'Waiting for approval…', '⏳');

    const ok = await this._askPermission(file);
    if (!ok || this._cancelled.has(file._uid)) {
      this.log(`Send cancelled/rejected: ${file.name}`);
      this._markReject(file._uid);
      this._sendMeta = null;
      return;
    }

    await this._acquireWakeLock();

    const total = Math.ceil(file.size / CHUNK);
    const t0    = Date.now();
    let lastUI  = 0;
    let sent    = 0;

    if (!this._sig(JSON.stringify({
      t: 'start', name: this._sendMeta.name,
      size: file.size, mime: (file.type || 'application/octet-stream').slice(0, 120),
      chunks: total
    }))) throw new Error('The connection closed before the file transfer could start.');

    this.log(`Chunking: ${total} × ${CHUNK >> 10} KB`);

    const readChunk = (off) => {
      if (off >= file.size) return null;
      return file.slice(off, Math.min(off + CHUNK, file.size)).arrayBuffer();
    };
    const pending = [];
    for (let j = 0; j < Math.min(AHEAD, total); j++) pending.push(readChunk(j * CHUNK));
    let nextReadOff = Math.min(AHEAD, total) * CHUNK;

    for (let i = 0; i < total; i++) {
      if (this._sendAbort || this._cancelled.has(file._uid)) {
        this.log(`Aborting send at chunk ${i}/${total} — _sendAbort=${this._sendAbort}`);
        this._sig(JSON.stringify({ t: 'cancel' }));
        this._markCancel(file._uid);
        this._sendMeta = null;
        await this._releaseWakeLock();
        return;
      }

      const buf = await pending.shift();
      if (!buf) { this.log('Read-ahead returned null — breaking'); break; }

      if (nextReadOff < file.size) { pending.push(readChunk(nextReadOff)); nextReadOff += CHUNK; }

      // Wait for DC — brief retry handles momentary blips
      let waited = 0;
      while (this.dc?.readyState !== 'open') {
        if (this._sendAbort || this._cancelled.has(file._uid)) {
          this._sig(JSON.stringify({ t: 'cancel' }));
          this._markCancel(file._uid);
          this._sendMeta = null;
          await this._releaseWakeLock();
          return;
        }
        if (waited >= 30_000) throw new Error('The connection was lost during transfer.');
        this.log(`DC not open (${this.dc?.readyState}) — waiting 200 ms`);
        await new Promise(r => setTimeout(r, 200));
        waited += 200;
      }

      // Backpressure
      while (this.dc?.readyState === 'open' && this.dc.bufferedAmount >= BUF_HI) {
        const channel = this.dc;
        await new Promise(r => {
          const h = () => { channel.removeEventListener('bufferedamountlow', h); r(); };
          channel.addEventListener('bufferedamountlow', h);
          setTimeout(() => { channel.removeEventListener('bufferedamountlow', h); r(); }, 100);
        });
      }
      if (this.dc?.readyState !== 'open') throw new Error('The connection closed before the next file chunk could be sent.');

      this.dc.send(buf);
      sent += buf.byteLength;

      const now = Date.now();
      if (now - lastUI >= UI_MS || i === total - 1) {
        const sec = (now - t0) / 1000 || 0.001;
        const spd = sent / sec;
        const eta = spd > 0 ? (file.size - sent) / spd : 0;
        this._updateSend(file._uid, sent / file.size, spd, eta);
        lastUI = now;
      }
    }

    // Wait for bufferedAmount to drain to 0 (all bytes delivered to OS)
    if (this.dc && this.dc.readyState === 'open') {
      this.log('All chunks sent — waiting for DC buffer to drain to 0…');
      const drainDeadline = Date.now() + 60_000;
      while (this.dc.bufferedAmount > 0) {
        if (this.dc.readyState !== 'open') throw new Error('The connection closed before all file data was sent.');
        if (Date.now() >= drainDeadline) throw new Error('Timed out waiting for the connection to finish sending data.');
        await new Promise(r => setTimeout(r, 50));
      }
      this.log('DC buffer drained to 0 — sending end signal');
    }

    if (this.dc?.readyState !== 'open') throw new Error('The connection closed before the file transfer could finish.');
    if (!this._sig(JSON.stringify({ t: 'end', size: file.size, chunks: total }))) {
      throw new Error('Could not send the end-of-file marker.');
    }

    // Wait for receiver's "file-complete" ack (max 15 s)
    this.log('Waiting for file-complete ack from receiver…');
    const acked = await new Promise(resolve => {
      this._completeResolve = resolve;
      this._completeTimer = setTimeout(() => {
        this.log('file-complete ack timed out (15 s)');
        resolve(false);
      }, 15_000);
    });
    this._completeResolve = null;
    clearTimeout(this._completeTimer);
    this._completeTimer = null;

    if (acked) this._finishSend(file._uid);
    else {
      this._markFailed(file._uid, 'The receiver did not confirm the completed file.');
      this.toast(`The receiver did not confirm "${file.name}".`, 'error');
    }
    this._sendMeta = null;
    await this._releaseWakeLock();
    this.log(`${acked ? '✅ Sent' : '⚠️ Not confirmed'}: "${file.name}"`);
    this.log('Connection stays open — ready for next file');
  }

  // ═══════════════════════════════════════════════════════════════
  // RECEIVER
  // ═══════════════════════════════════════════════════════════════
  _ingestChunk(buf) {
    if (this._rxCancel || !this.rxMeta) {
      this.log('Discarding file data received without an active transfer');
      if (!this._rxCancel) this._sig(JSON.stringify({ t: 'cancel' }));
      return;
    }
    const byteLength = buf instanceof ArrayBuffer
      ? buf.byteLength
      : ArrayBuffer.isView(buf)
        ? buf.byteLength
        : 0;
    if (!byteLength || byteLength > CHUNK || this.rxChunks.length >= this.rxMeta.chunks
      || this.rxBytes + byteLength > this.rxMeta.size) {
      this._failRx('The incoming file data was invalid or exceeded its declared size.');
      return;
    }
    this.rxChunks.push(buf);
    this.rxBytes += byteLength;

    const now = Date.now();
    if (now - this._lastRxUI >= UI_MS) {
      const sec = (now - this.rxStart) / 1000 || 0.001;
      const spd = this.rxBytes / sec;
      const eta = (this.rxMeta && spd > 0) ? (this.rxMeta.size - this.rxBytes) / spd : 0;
      this._updateRx(this.rxBytes / (this.rxMeta?.size || 1), spd, eta);
      this._lastRxUI = now;
    }

  }

  _completeRx() {
    if (!this.rxMeta || this._rxCancel) return;
    try {
      const meta = this.rxMeta;
      if (this.rxBytes !== meta.size || this.rxChunks.length !== meta.chunks) {
        this._failRx('The received file did not match its declared size.');
        return;
      }
      const blob = new Blob(this.rxChunks, { type: meta.mime });
      if (blob.size !== meta.size) throw new Error('The assembled file size did not match its metadata.');
      const url = URL.createObjectURL(blob);
      const a = Object.assign(document.createElement('a'), { href: url, download: meta.name });
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(() => URL.revokeObjectURL(url), 60_000);

      this._sig(JSON.stringify({ t: 'file-complete', name: meta.name, size: meta.size }));
      this._updateRx(1, 0, 0);
      this._finishRx();
      this.toast(`"${meta.name}" download started`, 'success');
      this._releaseWakeLock();
      this.rxChunks = [];
      this.rxMeta = null;
      this._rxCancel = false;
      this.log('Receive state reset — connection stays open for next file');
    } catch (error) {
      this._failRx(`Could not prepare the received file for download: ${error.message}`);
    }
  }

  _failRx(message) {
    this._sig(JSON.stringify({ t: 'cancel' }));
    if (this.rxMeta && this._rxId) this._markFailed(this._rxId, message);
    this.rxChunks = [];
    this.rxMeta = null;
    this.rxBytes = 0;
    this._rxCancel = true;
    this._releaseWakeLock();
    this.log(message);
    this.toast(message, 'error');
  }

  // ═══════════════════════════════════════════════════════════════
  // IN-BAND SIGNALING (over DataChannel)
  // ═══════════════════════════════════════════════════════════════
  _onSignal(raw) {
    let m;
    try { m = JSON.parse(raw); } catch(_) { return; }

    switch (m.t) {
      case 'ping':
        try { this.dc.send(JSON.stringify({ t: 'pong', seq: m.seq })); } catch(_) {}
        break;

      case 'pong':
        clearTimeout(this._pongTimer);
        this._pongTimer = null;
        break;

      case 'file-request':
        this._pendingMeta = m;
        this._showPermModal(m);
        break;

      case 'file-accept':
        clearTimeout(this._permTimer);
        if (this._permResolve) { this._permResolve(true); this._permResolve = null; }
        break;

      case 'file-reject':
        clearTimeout(this._permTimer);
        if (this._permResolve) { this._permResolve(false); this._permResolve = null; }
        this.toast('Receiver declined', 'error');
        break;

      case 'start':
        if (this.rxMeta || typeof m.name !== 'string' || !m.name || m.name.length > 255
          || !Number.isSafeInteger(m.size) || m.size < 0
          || !Number.isSafeInteger(m.chunks) || m.chunks !== Math.ceil(m.size / CHUNK)
          || typeof m.mime !== 'string' || m.mime.length > 120) {
          this._failRx('The sender provided invalid file metadata.');
          break;
        }
        this._rxCancel = false;
        this.rxMeta    = {
          ...m,
          name: m.name.replace(/[\\/\u0000-\u001f\u007f]/g, '_').slice(0, 255) || 'file',
        };
        this.rxChunks  = [];
        this.rxBytes   = 0;
        this.rxStart   = Date.now();
        this._lastRxUI = 0;
        this._addRxUI(m);
        this._acquireWakeLock();
        this.log(`Receiving: "${this.rxMeta.name}" (${fmt(m.size)})`);
        break;

      case 'cancel':
        this.log('Peer cancelled transfer');
        if (this.rxMeta) this._markCancelRx();
        if (this._sendUid) { this._sendAbort = true; this._cancelled.add(this._sendUid); }
        if (this._permResolve) { this._permResolve(false); this._permResolve = null; }
        this.toast('Transfer cancelled by peer', 'error');
        this._releaseWakeLock();
        break;

      case 'end':
        if (!this.rxMeta) break;
        if (m.size !== this.rxMeta.size || m.chunks !== this.rxMeta.chunks) {
          this._failRx('The sender ended the transfer with mismatched file metadata.');
        } else if (this.rxBytes !== this.rxMeta.size || this.rxChunks.length !== this.rxMeta.chunks) {
          this._failRx('The transfer ended before all file data arrived.');
        } else this._completeRx();
        break;

      case 'file-complete':
        if (this._sendMeta && m.name === this._sendMeta.name && m.size === this._sendMeta.size) {
          this.log(`file-complete ack received for "${m.name}"`);
          clearTimeout(this._completeTimer);
          if (this._completeResolve) { this._completeResolve(true); this._completeResolve = null; }
        }
        break;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PERMISSION
  // ═══════════════════════════════════════════════════════════════
  _askPermission(file) {
    return new Promise(resolve => {
      this._permResolve = resolve;
      if (!this._sig(JSON.stringify({ t: 'file-request', name: file.name, size: file.size, mime: file.type || '', description: file._description || '' }))) {
        this._permResolve = null;
        this.toast('The connection is unavailable. Try reconnecting.', 'error');
        resolve(false);
        return;
      }
      this._permTimer = setTimeout(() => {
        if (this._permResolve) { this._permResolve(false); this._permResolve = null; this.toast('Permission timed out', 'error'); }
      }, PERM_T);
    });
  }

  _showPermModal(meta) {
    $('pm-icon').textContent = fileIcon(meta.name);
    $('pm-name').textContent = meta.name;
    $('pm-size').textContent = fmt(meta.size);
    $('pm-description').textContent = meta.description || 'AI description unavailable.';
    $('permission-overlay').classList.remove('hidden');
  }
  _hidePermModal() { $('permission-overlay').classList.add('hidden'); }

  async _describeFile(file) {
    const mime = (file.type || '').split(';')[0].toLowerCase();
    const isImage = /^image\/(jpeg|png|webp)$/.test(mime);
    const isText = mime.startsWith('text/') || [
      'application/json', 'application/xml', 'application/javascript',
      'application/x-javascript', 'application/csv'
    ].includes(mime);
    if ((!isImage && !isText) || file.size > 5 * 1024 * 1024) return '';

    this._setDescription(file._uid, 'Generating local AI description…');
    try {
      const response = await fetch('/api/describe', {
        method: 'POST',
        headers: { 'Content-Type': file.type || 'application/octet-stream' },
        body: file,
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Description unavailable');
      return result.description || '';
    } catch (error) {
      this.log(`AI description unavailable for "${file.name}": ${error.message}`);
      return '';
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // QR CAMERA
  // ═══════════════════════════════════════════════════════════════
  async _startScanner() {
    $('camera-overlay').classList.remove('hidden');
    $('scanner-status').textContent = 'Starting camera…';
    try {
      await loadQrScanner();
      if (!this.qrScanner) this.qrScanner = new Html5Qrcode('qr-reader');
      this.cameras = await Html5Qrcode.getCameras();
      if (!this.cameras?.length) throw new Error('No camera found');
      this.camIdx = this.cameras.findIndex(c => /back|rear|env/i.test(c.label));
      if (this.camIdx < 0) this.camIdx = 0;
      await this._runCam();
    } catch (e) { $('scanner-status').textContent = `Error: ${e.message}`; this.toast(e.message, 'error'); }
  }

  async _runCam() {
    if (!this.qrScanner || !this.cameras.length) return;
    if (this.isScanning) { await this.qrScanner.stop(); this.isScanning = false; }
    await this.qrScanner.start(
      this.cameras[this.camIdx].id,
      { fps: 15, qrbox: { width: 220, height: 220 }, aspectRatio: 1.0 },
      (text) => this._onQR(text), () => {}
    );
    this.isScanning = true;
    $('scanner-status').textContent = 'Align QR code within frame';
  }

  async _stopScanner() {
    if (this.qrScanner && this.isScanning) {
      try { await this.qrScanner.stop(); } catch(_){}
      this.isScanning = false;
    }
    $('camera-overlay').classList.add('hidden');
  }

  async _switchCam() {
    if (!this.cameras || this.cameras.length <= 1) { this.toast('Only one camera', 'info'); return; }
    this.camIdx = (this.camIdx + 1) % this.cameras.length;
    await this._runCam();
  }

  _onQR(text) {
    if (navigator.vibrate) try { navigator.vibrate(60); } catch(_){}
    this._stopScanner();
    this.log(`QR scanned: ${text}`);
    let room = null, token = null;
    try {
      const u = new URL(text);
      room  = u.searchParams.get('room');
      token = u.searchParams.get('token');
    } catch(_) {
      if (text.length === 6) room = text.toUpperCase();
      else if (text.includes(':')) { [room, token] = text.split(':'); room = room.toUpperCase(); }
    }
    if (room) {
      $('join-input').value = room.toUpperCase();
      this.token = token;
      this.toast(`Room ${room} found — connecting…`, 'success');
      this._joinRoom(room.toUpperCase(), token);
    } else { this.toast('Invalid QR code', 'error'); }
  }

  // ═══════════════════════════════════════════════════════════════
  // UI RENDERING
  // ═══════════════════════════════════════════════════════════════
  _pairingBaseUrl(localIP, port) {
    const isLoopback = /^(localhost|127\.0\.0\.1|\[::1\])$/i.test(location.hostname);
    return isLoopback && localIP ? `http://${localIP}:${port}` : location.origin;
  }

  _renderWaiting(roomId, token, localIP, port) {
    $('room-code-display').textContent = roomId;
    const recv = this.role === 'receiver';
    $('waiting-status').innerHTML = recv
      ? '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#00d9aa;margin-right:6px;animation:pulseDot 1.5s infinite"></span>Ready — Waiting for Sender'
      : '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#00d9aa;margin-right:6px;animation:pulseDot 1.5s infinite"></span>Ready — Waiting for Connection';
    $('qr-instruction-text').innerHTML = recv ? '📷 Scan this QR with sending device' : '📷 Scan this QR to connect';
    const base = this._pairingBaseUrl(localIP, port);
    const url  = `${base}/?room=${roomId}&token=${token}`;
    const ip   = $('local-ip-badge');
    if (ip) ip.textContent = localIP ? `${localIP}:${port}` : location.host;
    const qr = $('qr-code');
    qr.innerHTML = '';
    try { new QRCode(qr, { text: url, width: 170, height: 170, colorDark: '#000', colorLight: '#fff', correctLevel: QRCode.CorrectLevel.M }); }
    catch(_) { qr.textContent = url; }
  }

  _renderJoining(roomId) {
    $('room-code-display').textContent = roomId;
    $('waiting-status').innerHTML = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#f59e0b;margin-right:6px;animation:pulseDot 1.5s infinite"></span>Connecting via local WiFi…';
  }

  _showConnectionInfo() {
    if (!this.roomId) { this.toast('No active room', 'error'); return; }
    $('ci-code-display').textContent = this.roomId;
    const box = $('ci-qr-box'), qrEl = $('ci-qr-code');
    qrEl.innerHTML = '';
    if (this.isHost) {
      box.classList.remove('hidden');
      const base = this._pairingBaseUrl(this._hostIP, this._hostPort || 3000);
      const url  = `${base}/?room=${this.roomId}&token=${this.token || ''}`;
      try { new QRCode(qrEl, { text: url, width: 150, height: 150, colorDark: '#000', colorLight: '#fff', correctLevel: QRCode.CorrectLevel.M }); }
      catch(_) { qrEl.textContent = url; }
    } else { box.classList.add('hidden'); }
    $('ci-note').textContent = this.isHost
      ? 'If the connection drops, the other device can rescan this QR to reconnect.'
      : 'If the connection drops, tap Reconnect to rejoin without going back home.';
    $('connection-info-overlay').classList.remove('hidden');
  }

  _hideConnectionInfo() { $('connection-info-overlay').classList.add('hidden'); }

  _reconnectPeer() {
    if (!this.roomId) return;
    if (this.isHost) this.toast('Waiting for the other device to rejoin…', 'info');
    else { this.toast('Reconnecting…', 'info'); this._joinRoom(this.roomId, this.token); }
    this._hideConnectionInfo();
  }

  _showScreen(id) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    const t = $(`screen-${id}`);
    if (t) t.classList.add('active');
    document.body.classList.toggle('local-chat-active', id === 'local-chat');
    $('room-chat').classList.toggle('hidden', id === 'home' || id === 'local-chat' || !this.roomId);
    document.querySelectorAll('.sidebar-link').forEach(link => {
      const active = (id === 'home' && link.id === 'nav-send')
        || (id === 'transfer' && link.id === 'nav-chat');
      link.classList.toggle('active', active);
      if (active) link.setAttribute('aria-current', 'page');
      else link.removeAttribute('aria-current');
    });
  }

  toast(msg, type = 'info') {
    const t = document.createElement('div');
    t.className = `toast toast-${type}`;
    t.textContent = msg;
    $('toast-container').appendChild(t);
    requestAnimationFrame(() => requestAnimationFrame(() => t.classList.add('show')));
    setTimeout(() => { t.classList.remove('show'); setTimeout(() => t.remove(), 300); }, 3500);
  }

  // ── Send UI ─────────────────────────────────────────────────────
  _addSendUI(file) {
    const id = uid(); file._uid = id;
    const el = mk('div', 'transfer-item sending', id);
    el.innerHTML = `
      <div class="tf-icon">${fileIcon(file.name)}</div>
      <div class="tf-info">
        <div class="tf-name">${esc(file.name)}</div>
        <div class="tf-meta">${fmt(file.size)} &middot; <span class="tf-status">Queued</span></div>
        <div class="tf-description">AI description pending</div>
        <div class="tf-bar"><div class="tf-fill" style="width:0%"></div></div>
      </div>
      <div class="tf-right">
        <div class="tf-speed"></div>
        <div class="tf-eta"></div>
        <button class="tf-cancel">✕</button>
      </div>`;
    el.querySelector('.tf-cancel').onclick = () => this._cancelSend(file._uid, el);
    $('transfer-list').prepend(el);
  }

  _setDescription(id, description) {
    const el = document.getElementById(id);
    if (el) el.querySelector('.tf-description').textContent = description;
  }

  _updateStatus(id, s, ic) {
    const el = document.getElementById(id); if (!el) return;
    el.querySelector('.tf-status').textContent = s;
    if (ic) el.querySelector('.tf-icon').textContent = ic;
  }

  _updateSend(id, p, spd, eta) {
    const el = document.getElementById(id); if (!el) return;
    el.querySelector('.tf-fill').style.width   = `${(p * 100).toFixed(1)}%`;
    el.querySelector('.tf-status').textContent = `Sending… ${(p * 100) | 0}%`;
    el.querySelector('.tf-speed').textContent  = fmtSpd(spd);
    el.querySelector('.tf-eta').textContent    = fmtEta(eta);
  }

  _finishSend(id) {
    const el = document.getElementById(id); if (!el) return;
    el.querySelector('.tf-fill').style.width   = '100%';
    el.querySelector('.tf-status').textContent = 'Sent ✓';
    el.querySelector('.tf-speed').textContent  = '';
    el.querySelector('.tf-eta').textContent    = '';
    const b = el.querySelector('.tf-cancel'); if (b) b.disabled = true;
    el.classList.add('done');
    // Show the "Send another / Done" bar but do NOT call reset()
    this._showDoneBar();
  }

  _markFailed(id, message) {
    const el = document.getElementById(id); if (!el) return;
    el.querySelector('.tf-status').textContent = message || 'Transfer failed';
    el.querySelector('.tf-speed').textContent = '';
    el.querySelector('.tf-eta').textContent = '';
    const b = el.querySelector('.tf-cancel'); if (b) b.disabled = true;
    el.classList.add('cancelled');
  }

  _markReject(id) {
    const el = document.getElementById(id); if (!el) return;
    el.querySelector('.tf-status').textContent = 'Rejected';
    el.querySelector('.tf-icon').textContent   = '🚫';
    const b = el.querySelector('.tf-cancel'); if (b) b.disabled = true;
    el.classList.add('cancelled');
  }

  _markCancel(id) {
    const el = document.getElementById(id); if (!el) return;
    el.querySelector('.tf-status').textContent = 'Cancelled';
    el.querySelector('.tf-speed').textContent  = '';
    const b = el.querySelector('.tf-cancel'); if (b) b.disabled = true;
    el.classList.add('cancelled');
  }

  _cancelSend(fuid, el) {
    this._cancelled.add(fuid);
    this._sendAbort = true;
    this.log(`User cancelled send for uid=${fuid}`);
    if (this._permResolve) { this._permResolve(false); this._permResolve = null; clearTimeout(this._permTimer); }
    if (el) { el.querySelector('.tf-status').textContent = 'Cancelling…'; el.querySelector('.tf-cancel').disabled = true; }
    this.toast('Cancelled', 'info');
  }

  // ── Receive UI ──────────────────────────────────────────────────
  _addRxUI(meta) {
    const id = uid(); this._rxId = id;
    const el = mk('div', 'transfer-item receiving', id);
    el.innerHTML = `
      <div class="tf-icon">${fileIcon(meta.name)}</div>
      <div class="tf-info">
        <div class="tf-name">${esc(meta.name)}</div>
        <div class="tf-meta">${fmt(meta.size)} &middot; <span class="tf-status">Receiving…</span></div>
        <div class="tf-bar"><div class="tf-fill" style="width:0%"></div></div>
      </div>
      <div class="tf-right">
        <div class="tf-speed"></div>
        <div class="tf-eta"></div>
        <button class="tf-cancel">✕</button>
      </div>`;
    el.querySelector('.tf-cancel').onclick = () => this._cancelRx(el);
    $('transfer-list').prepend(el);
  }

  _updateRx(p, spd, eta) {
    const el = document.getElementById(this._rxId); if (!el) return;
    el.querySelector('.tf-fill').style.width = `${(p * 100).toFixed(1)}%`;
    if (spd > 0) {
      el.querySelector('.tf-status').textContent = `Receiving… ${(p * 100) | 0}%`;
      el.querySelector('.tf-speed').textContent  = fmtSpd(spd);
      el.querySelector('.tf-eta').textContent    = fmtEta(eta);
    }
  }

  _finishRx() {
    const el = document.getElementById(this._rxId); if (!el) return;
    el.querySelector('.tf-fill').style.width   = '100%';
    el.querySelector('.tf-status').textContent = 'Saved ✓';
    el.querySelector('.tf-speed').textContent  = '';
    el.querySelector('.tf-eta').textContent    = '';
    const b = el.querySelector('.tf-cancel'); if (b) b.disabled = true;
    el.classList.add('done');
    this._showDoneBar();
  }

  _cancelRx(el) {
    this._rxCancel = true;
    this.rxChunks  = []; this.rxBytes = 0;
    this.rxMeta = null;
    this._sig(JSON.stringify({ t: 'cancel' }));
    if (el) { el.querySelector('.tf-status').textContent = 'Cancelled'; el.querySelector('.tf-cancel').disabled = true; el.classList.add('cancelled'); }
    this.toast('Cancelled', 'info');
    this._releaseWakeLock();
  }

  _markCancelRx() {
    const el = document.getElementById(this._rxId);
    if (el) {
      el.querySelector('.tf-status').textContent = 'Cancelled by sender';
      el.querySelector('.tf-fill').style.width   = '0%';
      const b = el.querySelector('.tf-cancel'); if (b) b.disabled = true;
      el.classList.add('cancelled');
    }
    this.rxChunks = []; this.rxBytes = 0;
    this.rxMeta = null;
    this._rxCancel = true;
  }

  // Show the action bar WITHOUT disconnecting
  _showDoneBar() {
    const b = $('done-bar'); if (b) b.classList.remove('hidden');
  }

  // ═══════════════════════════════════════════════════════════════
  // INIT / RESET / URL
  // ═══════════════════════════════════════════════════════════════
  _initWifi() {
    fetch('/local-info').then(r => r.json()).then(info => {
      if (info.ip) $('wifi-label').textContent = `📶 WiFi: ${info.ip}`;
    }).catch(() => {});
    this._refreshRooms();
    this._discoveryTimer = setInterval(() => {
      if (!document.hidden && !this.roomId) this._refreshRooms();
    }, 15_000);
  }

  async _refreshRooms() {
    const list = $('discovery-list');
    try {
      const response = await fetch('/api/rooms', { cache: 'no-store' });
      if (!response.ok) throw new Error(`Room discovery returned ${response.status}`);
      const { rooms = [] } = await response.json();
      list.replaceChildren();
      if (!rooms.length) {
        const empty = document.createElement('p');
        empty.className = 'discovery-empty';
        empty.textContent = 'No open rooms found on this PeerDrop server.';
        list.appendChild(empty);
        return;
      }
      for (const room of rooms) {
        const row = document.createElement('div');
        row.className = 'discovery-room';
        const info = document.createElement('div');
        const owner = document.createElement('strong');
        owner.textContent = room.hostName;
        const details = document.createElement('span');
        details.textContent = `${room.memberCount} / ${room.maxMembers} members · Code ${room.roomId}`;
        info.append(owner, details);
        const join = document.createElement('button');
        join.type = 'button';
        join.className = 'discovery-join';
        join.textContent = 'Request to join';
        join.onclick = () => this._joinRoom(room.roomId, null);
        row.append(info, join);
        list.appendChild(row);
      }
    } catch (error) {
      this.log(`Room discovery failed: ${error.message}`);
      list.replaceChildren();
      const empty = document.createElement('p');
      empty.className = 'discovery-empty';
      empty.textContent = 'Room discovery is unavailable. You can still pair by QR or code.';
      list.appendChild(empty);
    }
  }

  _renderMembers() {
    const members = $('chat-members');
    const recipient = $('chat-recipient');
    if (!members || !recipient) return;
    members.replaceChildren();
    recipient.replaceChildren();
    const everyone = document.createElement('option');
    everyone.value = '';
    everyone.textContent = 'Everyone';
    recipient.appendChild(everyone);
    for (const member of this.members) {
      const chip = document.createElement('span');
      chip.className = 'chat-member';
      chip.textContent = member.id === this.socket.id ? `${member.name} (you)` : member.name;
      members.appendChild(chip);
      if (member.id !== this.socket.id) {
        const option = document.createElement('option');
        option.value = member.id;
        option.textContent = `Private: ${member.name}`;
        recipient.appendChild(option);
      }
    }
    const label = $('chat-members-label');
    if (label) label.textContent = `${this.members.length} member${this.members.length === 1 ? '' : 's'} · messages are relayed by this PeerDrop server`;
  }

  _appendChatMessage(message) {
    const feed = $('chat-feed');
    if (!feed || !message || typeof message.text !== 'string') return;
    const empty = feed.querySelector('.chat-empty');
    if (empty) empty.remove();
    const item = document.createElement('article');
    const isOwn = message.from === this.socket.id;
    const recipient = this.members.find(member => member.id === message.to);
    item.className = `chat-message${isOwn ? ' own' : ''}${message.to ? ' private' : ''}`;
    const meta = document.createElement('div');
    meta.className = 'chat-message-meta';
    meta.textContent = `${isOwn ? 'You' : message.name}${message.to ? ` · ${isOwn ? `to ${recipient?.name || 'peer'}` : 'private message'}` : ''}`;
    const text = document.createElement('div');
    text.className = 'chat-message-text';
    text.textContent = message.text;
    item.append(meta, text);
    feed.appendChild(item);
    while (feed.querySelectorAll('.chat-message').length > 200) feed.firstElementChild.remove();
    feed.scrollTop = feed.scrollHeight;
  }

  _showNextJoinRequest() {
    const request = this._joinRequests[0];
    if (!request) {
      $('join-request-overlay').classList.add('hidden');
      return;
    }
    $('join-request-name').textContent = request.name;
    $('join-request-overlay').classList.remove('hidden');
  }

  _setChatOnlyMode() {
    const chatOnly = !this.canTransfer;
    $('chat-notice').classList.toggle('hidden', !chatOnly);
    $('dropzone').classList.toggle('hidden', chatOnly);
    $('transfer-list').classList.toggle('hidden', chatOnly);
    $('done-bar').classList.toggle('hidden', chatOnly);
  }

  _checkURL() {
    const p = new URLSearchParams(location.search);
    const r = p.get('room'), t = p.get('token');
    if (r) {
      this.role = 'sender';
      const sp = $('send-panel'); if (sp) sp.classList.remove('hidden');
      $('join-input').value = r.toUpperCase();
      this.token = t;
      setTimeout(() => this._joinRoom(r.toUpperCase(), t), 500);
    }
  }

  _joinRoom(code, token) {
    this.socket.emit('join-room', {
      roomId: code,
      token: token || this.token || null,
      displayName: this._getDisplayName(),
    });
  }

  _getDisplayName() {
    const input = $('display-name');
    const name = input ? input.value.trim().replace(/\s+/g, ' ').slice(0, 32) : '';
    this.displayName = name || 'Peer';
    localStorage.setItem('peerdrop-name', this.displayName);
    return this.displayName;
  }

  _resetPickBtn() {
    const l = $('selected-files-label'), b = $('send-pick-btn');
    if (l) l.textContent = 'Select Files to Send';
    if (b) b.classList.remove('selected');
  }

  // Full reset — disconnects everything, goes back to home screen
  // Only called when user explicitly taps "← Back to Home"
  reset() {
    this.log('reset() called — disconnecting and going home');
    clearTimeout(this._permTimer);
    clearTimeout(this._iceRestartTimer);
    clearTimeout(this._completeTimer);
    this._stopHeartbeat();
    this._hideDCPopup();
    this._releaseWakeLock();
    this._hidePermModal();
    this._hideConnectionInfo();
    this._stopScanner();
    this._joinRequests = [];
    $('join-request-overlay').classList.add('hidden');

    // Tell server we're leaving explicitly (triggers peer-disconnected on peer)
    this.socket.emit('leave-room');
    this.log('leave-room emitted to server');

    if (this.pc) { try { this.pc.close(); this.log('RTCPeerConnection closed'); } catch(_){} this.pc = null; }

    this.dc            = null;
    this.peerId        = null;
    this.roomId        = null;
    this.token         = null;
    this.role          = 'idle';
    this.members       = [];
    this.canTransfer   = false;
    this._joinRequests = [];
    this.queue         = [];
    this.preSelected   = [];
    this.sending       = false;
    this._sendAbort    = false;
    this.rxMeta        = null;
    this.rxChunks      = [];
    this.rxBytes       = 0;
    this._cancelled.clear();
    this._permResolve  = null;
    this._completeResolve = null;
    this._rxCancel     = false;
    this._connected    = false;
    this._iceQ         = [];
    this._hostIP       = null;
    this._hostPort     = null;

    $('transfer-list').innerHTML = '';
    $('chat-feed').replaceChildren();
    $('chat-feed').innerHTML = '<p class="chat-empty">Messages in this session will appear here.</p>';
    $('chat-members').replaceChildren();
    $('chat-recipient').innerHTML = '<option value="">Everyone</option>';
    $('dropzone').classList.remove('hidden');
    $('transfer-list').classList.remove('hidden');
    $('chat-notice').classList.add('hidden');
    $('join-input').value        = '';
    const bar = $('done-bar'); if (bar) bar.classList.add('hidden');
    const sp  = $('send-panel'); if (sp)  sp.classList.add('hidden');
    this._resetPickBtn();
    history.replaceState({}, '', '/');
    this._showScreen('home');
    this.log('reset() complete — at home screen');
  }

  // ═══════════════════════════════════════════════════════════════
  // UI BINDINGS
  // ═══════════════════════════════════════════════════════════════
  _bindUI() {
    const themeToggle = $('theme-toggle');
    const applyTheme = theme => {
      document.body.dataset.theme = theme;
      if (themeToggle) {
        const dark = theme === 'dark';
        themeToggle.textContent = dark ? 'Light mode' : 'Dark mode';
        themeToggle.setAttribute('aria-label', `Switch to ${dark ? 'light' : 'dark'} theme`);
        themeToggle.setAttribute('aria-pressed', String(dark));
      }
      const themeColor = $('theme-color');
      if (themeColor) themeColor.content = theme === 'dark' ? '#07070f' : '#f5f7fb';
    };
    applyTheme(localStorage.getItem('peerdrop-theme') === 'dark' ? 'dark' : 'light');
    if (themeToggle) {
      themeToggle.addEventListener('click', () => {
        console.log('[PeerDrop] Button click: theme-toggle');
        const theme = document.body.dataset.theme === 'dark' ? 'light' : 'dark';
        localStorage.setItem('peerdrop-theme', theme);
        applyTheme(theme);
      });
    }

    const safeBind = (id, handler, label) => {
      const el = $(id);
      if (!el) {
        console.warn(`[PeerDrop] Missing element #${id} while binding ${label}`);
        return;
      }
      el.addEventListener('click', event => {
        console.log(`[PeerDrop] Button click: ${label}`);
        handler(event);
      });
    };

    const setActiveNav = id => {
      document.querySelectorAll('.sidebar-link').forEach(link => {
        const active = link.id === id;
        link.classList.toggle('active', active);
        if (active) link.setAttribute('aria-current', 'page');
        else link.removeAttribute('aria-current');
      });
    };
    const scrollWorkspaceTo = id => {
      const workspace = document.querySelector('.workspace');
      const target = $(id);
      if (!workspace || !target) return;
      const top = workspace.scrollTop + target.getBoundingClientRect().top - workspace.getBoundingClientRect().top - 20;
      workspace.scrollTo({ top, behavior: 'smooth' });
    };
    safeBind('nav-send', () => {
      this._showScreen('home');
      setActiveNav('nav-send');
    }, 'nav-send');
    safeBind('nav-devices', () => {
      this._showScreen('home');
      setActiveNav('nav-devices');
      scrollWorkspaceTo('nearby-devices');
    }, 'nav-devices');
    safeBind('nav-chat', () => {
      if (!this.roomId) {
        this.toast('Connect to a peer to open room chat.', 'info');
        return;
      }
      this._showScreen('transfer');
      setActiveNav('nav-chat');
      scrollWorkspaceTo('room-chat');
    }, 'nav-chat');
    safeBind('nav-theme', () => themeToggle?.click(), 'nav-theme');

    safeBind('recv-create-btn', () => {
      this.role = 'receiver';
      this.socket.emit('create-room', { displayName: this._getDisplayName() });
    }, 'recv-create-btn');
    safeBind('refresh-rooms-btn', () => this._refreshRooms(), 'refresh-rooms-btn');
    $('display-name').onchange = () => this._getDisplayName();

    const sb = $('send-open-btn'), sp = $('send-panel');
    if (sb && sp) {
      sb.addEventListener('click', event => {
        console.log('[PeerDrop] Button click: send-open-btn');
        this.role = 'sender';
        sp.classList.toggle('hidden');
      });
    }

    const pb = $('send-pick-btn'), pf = $('pre-send-file-input');
    if (pb && pf) {
      const selectPreFiles = files => {
        if (!files.length) return;
        this.preSelected = files;
        const sz = files.reduce((a, f) => a + f.size, 0);
        pb.classList.add('selected');
        $('selected-files-label').textContent = `✓ ${files.length} file(s) (${fmt(sz)})`;
        this.toast(`${files.length} file(s) ready. Scan QR!`, 'success');
      };
      pb.addEventListener('click', event => {
        console.log('[PeerDrop] Button click: send-pick-btn');
        pf.click();
      });
      pf.onchange = e => selectPreFiles(Array.from(e.target.files));
      const folderButton = $('send-folder-btn');
      const folderInput = $('pre-send-folder-input');
      if (folderButton && folderInput) {
        folderButton.addEventListener('click', () => folderInput.click());
        folderInput.onchange = e => {
          selectPreFiles(Array.from(e.target.files));
          folderInput.value = '';
        };
      }
      const sendPanel = $('send-panel');
      if (sendPanel) {
        sendPanel.ondragenter = sendPanel.ondragover = event => {
          event.preventDefault();
          sendPanel.classList.add('is-dragover');
        };
        sendPanel.ondragleave = event => {
          if (!sendPanel.contains(event.relatedTarget)) sendPanel.classList.remove('is-dragover');
        };
        sendPanel.ondrop = event => {
          event.preventDefault();
          sendPanel.classList.remove('is-dragover');
          selectPreFiles(Array.from(event.dataTransfer.files));
        };
      }
    }

    safeBind('paste-link-btn', async () => {
      if (!navigator.clipboard?.readText) {
        this.toast('Clipboard access is unavailable. Enter the room code manually.', 'error');
        $('join-input')?.focus();
        return;
      }
      try {
        const invite = (await navigator.clipboard.readText()).trim();
        let room = null, token = null;
        try {
          const url = new URL(invite);
          room = url.searchParams.get('room');
          if (room) room = room.toUpperCase();
          token = url.searchParams.get('token');
        } catch (_) {}
        if (!room && /^[A-Z0-9]{6}$/i.test(invite)) {
          room = invite.toUpperCase();
        } else if (!room && /^[A-Z0-9]{6}:[^\s]+$/i.test(invite)) {
          [room, token] = invite.split(':', 2);
          room = room.toUpperCase();
        }
        if (!room || !/^[A-Z0-9]{6}$/.test(room)) {
          this.toast('Clipboard does not contain a valid PeerDrop link or room code.', 'error');
          return;
        }
        $('join-input').value = room;
        this.role = 'sender';
        this.token = token;
        this._joinRoom(room, token);
      } catch (error) {
        this.log(`Could not read invite from clipboard: ${error.message}`);
        this.toast('Could not read the clipboard. Enter the room code manually.', 'error');
        $('join-input')?.focus();
      }
    }, 'paste-link-btn');
    safeBind('open-camera-btn', () => { this.role = 'sender'; this._startScanner(); }, 'open-camera-btn');
    safeBind('close-camera-btn', () => this._stopScanner(), 'close-camera-btn');
    safeBind('switch-camera-btn', () => this._switchCam(), 'switch-camera-btn');
    safeBind('sender-make-qr-link', () => {
      this.role = 'sender';
      this.socket.emit('create-room', { displayName: this._getDisplayName() });
    }, 'sender-make-qr-link');

    const ji = $('join-input');
    if (ji) {
      $('join-btn').addEventListener('click', () => {
        console.log('[PeerDrop] Button click: join-btn');
        const c = ji.value.trim().toUpperCase();
        if (c.length === 6) this._joinRoom(c, this.token);
        else this.toast('Enter 6-char code', 'error');
      });
      ji.oninput   = e => { e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6); };
      ji.onkeydown = e => { if (e.key === 'Enter') $('join-btn').click(); };
    }

    safeBind('copy-btn', () => {
      if (!this.roomId) return;
      navigator.clipboard.writeText(this.roomId).then(() => this.toast('Copied!', 'success')).catch(() => this.toast('Failed', 'error'));
    }, 'copy-btn');

    // ── Navigation — these call reset() which properly cleans up ──
    safeBind('back-btn', () => this.reset(), 'back-btn');
    safeBind('new-session-btn', () => this.reset(), 'new-session-btn');

    // "Done" button: ONLY clears the transfer list, does NOT disconnect
    safeBind('done-btn', () => {
      this.log('"Send more" tapped — clearing transfer list, keeping connection open');
      $('transfer-list').innerHTML = '';
      const bar = $('done-bar'); if (bar) bar.classList.add('hidden');
      this.toast('Ready to send more files!', 'success');
    }, 'done-btn');

    // Connection Info modal
    safeBind('conn-info-btn', () => this._showConnectionInfo(), 'conn-info-btn');
    safeBind('ci-close-btn', () => this._hideConnectionInfo(), 'ci-close-btn');
    safeBind('ci-reconnect-btn', () => this._reconnectPeer(), 'ci-reconnect-btn');
    safeBind('ci-copy-btn', () => {
      if (!this.roomId) return;
      navigator.clipboard.writeText(this.roomId).then(() => this.toast('Copied!', 'success')).catch(() => this.toast('Failed', 'error'));
    }, 'ci-copy-btn');

    // Disconnect popup buttons
    safeBind('dc-reconnect-btn', () => this._dcPopupReconnect(), 'dc-reconnect-btn');
    safeBind('dc-home-btn', () => { this._hideDCPopup(); this.reset(); }, 'dc-home-btn');

    safeBind('join-request-accept', () => {
      const request = this._joinRequests.shift();
      if (request) this.socket.emit('approve-join', { peerId: request.peerId });
      this._showNextJoinRequest();
    }, 'join-request-accept');
    safeBind('join-request-decline', () => {
      const request = this._joinRequests.shift();
      if (request) this.socket.emit('decline-join', { peerId: request.peerId });
      this._showNextJoinRequest();
    }, 'join-request-decline');

    const chatForm = $('chat-form');
    if (chatForm) {
      chatForm.addEventListener('submit', event => {
        event.preventDefault();
        console.log('[PeerDrop] Button click: chat-form submit');
        const input = $('chat-input');
        const text = input.value.trim();
        if (!text || !this.roomId) return;
        this.socket.emit('chat-message', { text, to: $('chat-recipient').value || null });
        input.value = '';
      });
    }

    // Permission modal
    safeBind('pm-accept', () => {
      this._hidePermModal();
      this._sig(JSON.stringify({ t: 'file-accept' }));
      this._pendingMeta = null;
    }, 'pm-accept');
    safeBind('pm-reject', () => {
      this._hidePermModal();
      this._sig(JSON.stringify({ t: 'file-reject' }));
      this._pendingMeta = null;
      this.toast('Rejected', 'info');
    }, 'pm-reject');

    // Drop zone
    const dz = $('dropzone'), fi = $('file-input');
    if (dz && fi) {
      $('browse-btn').addEventListener('click', event => {
        console.log('[PeerDrop] Button click: browse-btn');
        event.stopPropagation();
        fi.click();
      });
      dz.onclick = () => {
        console.log('[PeerDrop] Button click: dropzone');
        fi.click();
      };
      dz.ondragenter = dz.ondragover = e => { e.preventDefault(); dz.classList.add('drag-over'); };
      dz.ondragleave = e => { if (!dz.contains(e.relatedTarget)) dz.classList.remove('drag-over'); };
      dz.ondrop      = e => { e.preventDefault(); dz.classList.remove('drag-over'); const f = Array.from(e.dataTransfer.files); if (f.length) this.enqueue(f); };
      fi.onchange    = e => { this.enqueue(Array.from(e.target.files)); fi.value = ''; };
    }
  }
}

// ═══════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════
function $(id)       { return document.getElementById(id); }
function mk(t, c, i) { const e = document.createElement(t); e.className = c; e.id = i; return e; }
function uid()       { return `pd-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`; }
function fmt(b)      { return b < 1024 ? `${b} B` : b < 1<<20 ? `${(b/1024).toFixed(1)} KB` : b < 1<<30 ? `${(b/(1<<20)).toFixed(1)} MB` : `${(b/(1<<30)).toFixed(2)} GB`; }
function fmtSpd(b)   { return `${fmt(b)}/s`; }
function fmtEta(s)   { if (!isFinite(s)||s<=0) return ''; if (s<60) return `${Math.ceil(s)}s`; if (s<3600) return `${Math.floor(s/60)}m ${Math.ceil(s%60)}s`; return `${Math.floor(s/3600)}h`; }
function esc(s)      { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function fileIcon(name) {
  const ext = (name.split('.').pop()||'').toLowerCase();
  return { pdf:'📄',doc:'📝',docx:'📝',txt:'📃',xls:'📊',xlsx:'📊',csv:'📊',ppt:'📋',pptx:'📋',
    jpg:'🖼️',jpeg:'🖼️',png:'🖼️',gif:'🖼️',webp:'🖼️',svg:'🖼️',bmp:'🖼️',
    mp4:'🎬',mov:'🎬',avi:'🎬',mkv:'🎬',webm:'🎬',
    mp3:'🎵',wav:'🎵',flac:'🎵',aac:'🎵',ogg:'🎵',
    zip:'🗜️',rar:'🗜️','7z':'🗜️',tar:'🗜️',gz:'🗜️',
    js:'💻',ts:'💻',py:'💻',html:'💻',css:'💻',json:'💻',
    exe:'⚙️',msi:'⚙️',apk:'📱' }[ext] || '📁';
}

window.app = new PeerDrop();
