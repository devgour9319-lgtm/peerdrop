/* =================================================================
   PeerDrop v11 — Persistent Connection + File-Complete Ack
   Fix: connection stays open after transfer so you can send more
        files without reconnecting.
   ================================================================= */
'use strict';

// ── Tuning ──────────────────────────────────────────────────────
const CHUNK   = 128 * 1024;        // 128 KB per chunk
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

    // Send
    this.queue        = [];
    this.preSelected  = [];
    this.sending      = false;
    this._sendUid     = null;
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
      this._hostIP   = localIP;
      this._hostPort = port || 3000;
      this._showScreen('waiting');
      this._renderWaiting(roomId, token, localIP, port || 3000);
    });

    s.on('room-joined', ({ roomId, hostId }) => {
      this.log(`Joined room ${roomId}, host=${hostId}`);
      this.roomId = roomId;
      this.peerId = hostId;
      this.isHost = false;
      this._showScreen('waiting');
      this._renderJoining(roomId);
    });

    s.on('peer-joined', ({ peerId }) => {
      this.log(`Peer ${peerId} joined — starting WebRTC (initiator)`);
      this.peerId = peerId;
      this._startRTC(true);
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
    this._iceQ = [];
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
        this._showDisconnectPopup('WiFi connection failed.', 'ICE negotiation failed — check both devices are on the same WiFi network.');
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
    if (!this.queue.length) { this.sending = false; return; }
    this.sending = true;
    await this._send(this.queue.shift());
    this._drain();
  }

  async _send(file) {
    this._sendUid  = file._uid;
    this._sendAbort = false;
    this.log(`Sending: "${file.name}" (${fmt(file.size)})`);
    this._updateStatus(file._uid, 'Waiting for approval…', '⏳');

    const ok = await this._askPermission(file);
    if (!ok || this._cancelled.has(file._uid)) {
      this.log(`Send cancelled/rejected: ${file.name}`);
      this._markReject(file._uid);
      return;
    }

    await this._acquireWakeLock();

    const total = Math.ceil(file.size / CHUNK);
    const t0    = Date.now();
    let lastUI  = 0;
    let sent    = 0;

    this._sig(JSON.stringify({
      t: 'start', name: file.name.replace(/[^\w.\- ]+/g, '_'),
      size: file.size, mime: file.type || 'application/octet-stream',
      chunks: total
    }));

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
        await this._releaseWakeLock();
        return;
      }

      const buf = await pending.shift();
      if (!buf) { this.log('Read-ahead returned null — breaking'); break; }

      if (nextReadOff < file.size) { pending.push(readChunk(nextReadOff)); nextReadOff += CHUNK; }

      // Wait for DC — brief retry handles momentary blips
      let waited = 0;
      while (this.dc?.readyState !== 'open') {
        if (waited > 8000) { this.toast('Connection lost during transfer', 'error'); await this._releaseWakeLock(); return; }
        this.log(`DC not open (${this.dc?.readyState}) — waiting 200 ms`);
        await new Promise(r => setTimeout(r, 200));
        waited += 200;
      }

      // Backpressure
      while (this.dc.bufferedAmount >= BUF_HI) {
        await new Promise(r => {
          const h = () => { this.dc.removeEventListener('bufferedamountlow', h); r(); };
          this.dc.addEventListener('bufferedamountlow', h);
          setTimeout(() => { this.dc.removeEventListener('bufferedamountlow', h); r(); }, 100);
        });
      }

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
      while (this.dc.bufferedAmount > 0) {
        await new Promise(r => setTimeout(r, 50));
      }
      this.log('DC buffer drained to 0 — sending end signal');
    }

    this._sig(JSON.stringify({ t: 'end', size: file.size, chunks: total }));

    // Wait for receiver's "file-complete" ack (max 15 s)
    this.log('Waiting for file-complete ack from receiver…');
    const acked = await new Promise(resolve => {
      this._completeResolve = resolve;
      this._completeTimer = setTimeout(() => {
        this.log('file-complete ack timed out (15 s) — proceeding anyway');
        resolve(false);
      }, 15_000);
    });

    this._finishSend(file._uid);
    await this._releaseWakeLock();
    this.log(`✅ Sent: "${file.name}" ${acked ? '(ack received)' : '(no ack, timed out)'}`);
    this.log('Connection stays open — ready for next file');
  }

  // ═══════════════════════════════════════════════════════════════
  // RECEIVER
  // ═══════════════════════════════════════════════════════════════
  _ingestChunk(buf) {
    if (this._rxCancel) return;
    this.rxChunks.push(buf);
    this.rxBytes += buf.byteLength;

    const now = Date.now();
    if (now - this._lastRxUI >= UI_MS) {
      const sec = (now - this.rxStart) / 1000 || 0.001;
      const spd = this.rxBytes / sec;
      const eta = (this.rxMeta && spd > 0) ? (this.rxMeta.size - this.rxBytes) / spd : 0;
      this._updateRx(this.rxBytes / (this.rxMeta?.size || 1), spd, eta);
      this._lastRxUI = now;
    }

    if (this.rxMeta && this.rxBytes >= this.rxMeta.size && !this._rxCancel) {
      this._completeRx();
    }
  }

  _completeRx() {
    if (!this.rxMeta || this._rxCancel) return;
    this.log(`✅ Received: "${this.rxMeta.name}" (${fmt(this.rxMeta.size)}) — sending file-complete ack`);

    // Send ack to sender BEFORE triggering download (keeps pipeline going)
    this._sig(JSON.stringify({ t: 'file-complete', name: this.rxMeta.name, size: this.rxMeta.size }));

    const blob = new Blob(this.rxChunks, { type: this.rxMeta.mime });
    const url  = URL.createObjectURL(blob);
    const a    = Object.assign(document.createElement('a'), { href: url, download: this.rxMeta.name });
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 5000);

    this._updateRx(1, 0, 0);
    this._finishRx();
    this.toast(`"${this.rxMeta.name}" saved!`, 'success');
    this._releaseWakeLock();

    // Reset receive state — leave connection open
    this.rxChunks  = [];
    this.rxMeta    = null;
    this._rxCancel = false;
    this.log('Receive state reset — connection stays open for next file');
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
        this._rxCancel = false;
        this.rxMeta    = m;
        this.rxChunks  = [];
        this.rxBytes   = 0;
        this.rxStart   = Date.now();
        this._lastRxUI = 0;
        this._addRxUI(m);
        this._acquireWakeLock();
        this.log(`Receiving: "${m.name}" (${fmt(m.size)})`);
        break;

      case 'cancel':
        this.log('Peer cancelled transfer');
        if (this._rxId)    this._markCancelRx();
        if (this._sendUid) { this._sendAbort = true; this._cancelled.add(this._sendUid); }
        if (this._permResolve) { this._permResolve(false); this._permResolve = null; }
        this.toast('Transfer cancelled by peer', 'error');
        this._releaseWakeLock();
        break;

      case 'end':
        // Trigger completion if bytes haven't arrived yet
        if (this.rxMeta && this.rxBytes >= m.size && !this._rxCancel) {
          this._completeRx();
        }
        break;

      case 'file-complete':
        // Receiver has confirmed it got all bytes → unblock sender
        this.log(`file-complete ack received for "${m.name}"`);
        clearTimeout(this._completeTimer);
        if (this._completeResolve) { this._completeResolve(true); this._completeResolve = null; }
        break;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PERMISSION
  // ═══════════════════════════════════════════════════════════════
  _askPermission(file) {
    return new Promise(resolve => {
      this._permResolve = resolve;
      this._sig(JSON.stringify({ t: 'file-request', name: file.name, size: file.size, mime: file.type || '' }));
      this._permTimer = setTimeout(() => {
        if (this._permResolve) { this._permResolve(false); this._permResolve = null; this.toast('Permission timed out', 'error'); }
      }, PERM_T);
    });
  }

  _showPermModal(meta) {
    $('pm-icon').textContent = fileIcon(meta.name);
    $('pm-name').textContent = meta.name;
    $('pm-size').textContent = fmt(meta.size);
    $('permission-overlay').classList.remove('hidden');
  }
  _hidePermModal() { $('permission-overlay').classList.add('hidden'); }

  // ═══════════════════════════════════════════════════════════════
  // QR CAMERA
  // ═══════════════════════════════════════════════════════════════
  async _startScanner() {
    $('camera-overlay').classList.remove('hidden');
    $('scanner-status').textContent = 'Starting camera…';
    try {
      if (typeof Html5Qrcode === 'undefined') throw new Error('QR engine not loaded');
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
  _renderWaiting(roomId, token, localIP, port) {
    $('room-code-display').textContent = roomId;
    const recv = this.role === 'receiver';
    $('waiting-status').innerHTML = recv
      ? '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#00d9aa;margin-right:6px;animation:pulseDot 1.5s infinite"></span>Ready — Waiting for Sender'
      : '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#00d9aa;margin-right:6px;animation:pulseDot 1.5s infinite"></span>Ready — Waiting for Connection';
    $('qr-instruction-text').innerHTML = recv ? '📷 Scan this QR with sending device' : '📷 Scan this QR to connect';
    const base = localIP ? `http://${localIP}:${port}` : location.origin;
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
      const base = this._hostIP ? `http://${this._hostIP}:${this._hostPort || 3000}` : location.origin;
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
    this._sig(JSON.stringify({ t: 'cancel' }));
    if (el) { el.querySelector('.tf-status').textContent = 'Cancelled'; el.querySelector('.tf-cancel').disabled = true; el.classList.add('cancelled'); }
    this.toast('Cancelled', 'info');
    this._releaseWakeLock();
  }

  _markCancelRx() {
    const el = document.getElementById(this._rxId); if (!el) return;
    el.querySelector('.tf-status').textContent = 'Cancelled by sender';
    el.querySelector('.tf-fill').style.width   = '0%';
    const b = el.querySelector('.tf-cancel'); if (b) b.disabled = true;
    el.classList.add('cancelled');
    this.rxChunks = []; this.rxBytes = 0;
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
    this.socket.emit('join-room', { roomId: code, token: token || this.token || null });
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

    // Tell server we're leaving explicitly (triggers peer-disconnected on peer)
    this.socket.emit('leave-room');
    this.log('leave-room emitted to server');

    if (this.pc) { try { this.pc.close(); this.log('RTCPeerConnection closed'); } catch(_){} this.pc = null; }

    this.dc            = null;
    this.peerId        = null;
    this.roomId        = null;
    this.token         = null;
    this.role          = 'idle';
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
    $('recv-create-btn').onclick = () => { this.role = 'receiver'; this.socket.emit('create-room'); };

    const sb = $('send-open-btn'), sp = $('send-panel');
    if (sb && sp) sb.onclick = () => { this.role = 'sender'; sp.classList.toggle('hidden'); };

    const pb = $('send-pick-btn'), pf = $('pre-send-file-input');
    if (pb && pf) {
      pb.onclick = () => pf.click();
      pf.onchange = e => {
        const files = Array.from(e.target.files);
        if (files.length) {
          this.preSelected = files;
          const sz = files.reduce((a, f) => a + f.size, 0);
          pb.classList.add('selected');
          $('selected-files-label').textContent = `✓ ${files.length} file(s) (${fmt(sz)})`;
          this.toast(`${files.length} file(s) ready. Scan QR!`, 'success');
        }
      };
    }

    $('open-camera-btn').onclick   = () => { this.role = 'sender'; this._startScanner(); };
    $('close-camera-btn').onclick  = () => this._stopScanner();
    $('switch-camera-btn').onclick = () => this._switchCam();
    $('sender-make-qr-link').onclick = () => { this.role = 'sender'; this.socket.emit('create-room'); };

    const ji = $('join-input');
    $('join-btn').onclick = () => {
      const c = ji.value.trim().toUpperCase();
      if (c.length === 6) this._joinRoom(c, this.token);
      else this.toast('Enter 6-char code', 'error');
    };
    ji.oninput   = e => { e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6); };
    ji.onkeydown = e => { if (e.key === 'Enter') $('join-btn').click(); };

    $('copy-btn').onclick = () => {
      if (!this.roomId) return;
      navigator.clipboard.writeText(this.roomId).then(() => this.toast('Copied!', 'success')).catch(() => this.toast('Failed', 'error'));
    };

    // ── Navigation — these call reset() which properly cleans up ──
    $('back-btn').onclick       = () => this.reset();
    $('new-session-btn').onclick = () => this.reset();

    // "Done" button: ONLY clears the transfer list, does NOT disconnect
    $('done-btn').onclick = () => {
      this.log('"Send more" tapped — clearing transfer list, keeping connection open');
      $('transfer-list').innerHTML = '';
      const bar = $('done-bar'); if (bar) bar.classList.add('hidden');
      this.toast('Ready to send more files!', 'success');
    };

    // Connection Info modal
    $('conn-info-btn').onclick    = () => this._showConnectionInfo();
    $('ci-close-btn').onclick     = () => this._hideConnectionInfo();
    $('ci-reconnect-btn').onclick = () => this._reconnectPeer();
    $('ci-copy-btn').onclick      = () => {
      if (!this.roomId) return;
      navigator.clipboard.writeText(this.roomId).then(() => this.toast('Copied!', 'success')).catch(() => this.toast('Failed', 'error'));
    };

    // Disconnect popup buttons
    $('dc-reconnect-btn').onclick = () => this._dcPopupReconnect();
    $('dc-home-btn').onclick      = () => { this._hideDCPopup(); this.reset(); };

    // Permission modal
    $('pm-accept').onclick = () => {
      this._hidePermModal();
      this._sig(JSON.stringify({ t: 'file-accept' }));
      this._pendingMeta = null;
    };
    $('pm-reject').onclick = () => {
      this._hidePermModal();
      this._sig(JSON.stringify({ t: 'file-reject' }));
      this._pendingMeta = null;
      this.toast('Rejected', 'info');
    };

    // Drop zone
    const dz = $('dropzone'), fi = $('file-input');
    $('browse-btn').onclick = e => { e.stopPropagation(); fi.click(); };
    dz.onclick     = () => fi.click();
    dz.ondragenter = dz.ondragover = e => { e.preventDefault(); dz.classList.add('drag-over'); };
    dz.ondragleave = e => { if (!dz.contains(e.relatedTarget)) dz.classList.remove('drag-over'); };
    dz.ondrop      = e => { e.preventDefault(); dz.classList.remove('drag-over'); const f = Array.from(e.dataTransfer.files); if (f.length) this.enqueue(f); };
    fi.onchange    = e => { this.enqueue(Array.from(e.target.files)); fi.value = ''; };
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
