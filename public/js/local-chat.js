'use strict';

class LocalChatRooms {
  constructor() {
    this.defaultSocket = window.app?.socket || io({ transports: ['websocket', 'polling'] });
    this.sockets = new Map();
    this.activeSocket = this.defaultSocket;
    this.activeRoom = null;
    this.activeMembers = [];
    this.transfers = new Map();
    this.scanner = null;
    this.scanning = false;
    this.discoveryTimer = null;
    this.maxFileSize = Number(localStorage.getItem('peerdrop-chat-max-file-mb')) || 5;
    this.name = localStorage.getItem('peerdrop-name') || '';
    this.bindSocket(this.defaultSocket);
    this.bindUI();
    this.restoreRoomFromURL();
  }

  bindSocket(socket) {
    if (this.sockets.has(socket)) return;
    this.sockets.set(socket, new Map());
    socket.on('chat-room:members', packet => {
      if (packet.roomId !== this.activeRoom?.roomId) return;
      this.activeMembers = packet.members;
      this.renderMembers();
    });
    socket.on('chat-room:notice', notice => {
      if (notice.roomId !== this.activeRoom?.roomId) return;
      this.appendMessage({ ...notice, kind: 'notice' });
    });
    socket.on('chat-room:message', message => {
      if (message.roomId !== this.activeRoom?.roomId) return;
      this.appendMessage({ ...message, kind: 'text' });
    });
    socket.on('chat-room:file-start', packet => {
      if (packet.roomId !== this.activeRoom?.roomId || packet.userId === socket.id) return;
      this.transfers.set(packet.transferId, { ...packet, chunks: [], received: 0, nextChunk: 0 });
      this.appendMessage({ ...packet, kind: 'file', id: packet.transferId });
      this.updateFileProgress(packet.name, 0);
    });
    socket.on('chat-room:file-chunk', packet => this.receiveChunk(packet));
    socket.on('chat-room:file-complete', packet => this.finishReceive(packet.transferId));
    socket.on('chat-room:file-error', packet => {
      if (this.transfers.has(packet.transferId)) {
        this.transfers.delete(packet.transferId);
        this.setStatus(packet.error || 'File transfer failed.', true);
      }
    });
    socket.on('chat-room:closed', packet => {
      if (packet.roomId !== this.activeRoom?.roomId) return;
      this.setStatus(packet.reason || 'This room is closed.', true);
      this.activeRoom = null;
      this.activeMembers = [];
      this.renderActiveRoom();
      this.refreshRooms();
    });
  }

  bindUI() {
    $('local-chat-open-btn').addEventListener('click', () => this.open());
    $('local-chat-back-btn').addEventListener('click', () => this.goHome());
    $('chat-create-form').addEventListener('submit', event => this.createRoom(event));
    $('chat-join-form').addEventListener('submit', event => this.joinFromForm(event));
    $('chat-message-form').addEventListener('submit', event => this.sendMessage(event));
    $('chat-file-input').addEventListener('change', event => {
      const file = event.target.files?.[0];
      event.target.value = '';
      if (file) this.sendFile(file);
    });
    $('chat-max-size').value = String(this.maxFileSize);
    $('chat-max-size').addEventListener('change', event => {
      this.maxFileSize = Number(event.target.value);
      localStorage.setItem('peerdrop-chat-max-file-mb', String(this.maxFileSize));
    });
    $('chat-refresh-rooms').addEventListener('click', () => this.refreshRooms());
    $('chat-copy-invite').addEventListener('click', () => this.copyInvite());
    $('chat-leave-room').addEventListener('click', () => this.leaveRoom());
    $('chat-close-room').addEventListener('click', () => this.closeRoom());
    $('chat-scan-btn').addEventListener('click', () => this.toggleScanner());
  }

  open() {
    if (window.app) window.app._showScreen('local-chat');
    else {
      document.querySelectorAll('.screen').forEach(screen => screen.classList.remove('active'));
      $('screen-local-chat').classList.add('active');
    }
    document.body.classList.add('local-chat-active');
    this.refreshRooms();
    clearInterval(this.discoveryTimer);
    this.discoveryTimer = setInterval(() => {
      if (!document.hidden) this.refreshRooms();
    }, 15_000);
  }

  goHome() {
    this.stopScanner();
    clearInterval(this.discoveryTimer);
    this.discoveryTimer = null;
    if (window.app) window.app._showScreen('home');
    else {
      $('screen-local-chat').classList.remove('active');
      $('screen-home').classList.add('active');
    }
    document.body.classList.remove('local-chat-active');
  }

  restoreRoomFromURL() {
    const url = new URL(location.href);
    const invite = url.searchParams.get('localchat');
    if (!invite) return;
    setTimeout(() => {
      this.open();
      $('chat-room-code').value = invite;
      $('chat-join-form').requestSubmit();
      url.searchParams.delete('localchat');
      history.replaceState({}, '', url.pathname + url.search + url.hash);
    }, 0);
  }

  username() {
    const field = $('display-name');
    const name = (field?.value || this.name || '').trim().replace(/\s+/g, ' ').slice(0, 32) || 'Guest';
    this.name = name;
    localStorage.setItem('peerdrop-name', name);
    return name;
  }

  async createRoom(event) {
    event.preventDefault();
    const name = $('chat-room-name').value.trim();
    const password = $('chat-room-password').value;
    const result = await this.emitAck(this.defaultSocket, 'chat-room:create', {
      name,
      password,
      username: this.username(),
    });
    if (result.error) return this.setStatus(result.error, true);
    this.activeSocket = this.defaultSocket;
    this.activeRoom = result.room;
    this.activeRoom.joinUrl = result.joinUrl;
    this.activeRoom.localIP = result.localIP;
    this.activeRoom.port = result.port;
    this.activeRoom.isHost = true;
    this.activeMembers = result.members;
    $('chat-room-password').value = '';
    this.renderActiveRoom();
    this.appendMessage({ kind: 'notice', text: 'Room is ready. Share the QR code or room address with people on this network.', timestamp: Date.now() });
    this.renderInviteQR();
    this.setStatus(`Room created on ${result.localIP || location.hostname}:${result.port}.`);
    this.refreshRooms();
  }

  leaveRoom() {
    if (!this.activeRoom || this.activeRoom.isHost) return;
    this.activeSocket.emit('chat-room:leave', { roomId: this.activeRoom.roomId });
    this.activeRoom = null;
    this.activeMembers = [];
    this.transfers.clear();
    this.hideFileProgress();
    this.renderActiveRoom();
    this.refreshRooms();
    this.setStatus('You left the room.');
  }

  async joinFromForm(event) {
    event.preventDefault();
    const input = $('chat-room-code').value.trim();
    const password = $('chat-join-password').value;
    const invite = this.parseInvite(input);
    if (!invite) return this.setStatus('Enter a room code or a ws://IP:port/chat/ROOMID address.', true);
    const socket = await this.socketForOrigin(invite.origin);
    if (!socket) return;
    const result = await this.emitAck(socket, 'chat-room:join', {
      roomId: invite.roomId,
      password,
      username: this.username(),
    });
    if (result.error) return this.setStatus(result.error, true);
    this.activeSocket = socket;
    this.activeRoom = { ...result.room, hostId: result.hostId, isHost: result.hostId === socket.id, joinUrl: invite.joinUrl || '' };
    this.activeMembers = result.members;
    $('chat-join-password').value = '';
    this.renderActiveRoom();
    this.setStatus(`Joined ${result.room.name}.`);
    this.refreshRooms();
  }

  parseInvite(value) {
    const currentOrigin = this.originForSocket(this.defaultSocket);
    let input = value;
    if (/^https?:\/\//i.test(input)) {
      try {
        const url = new URL(input);
        const roomId = url.searchParams.get('localchat')
          || url.pathname.match(/\/chat\/([A-Za-z0-9]{6,8})\/?/)?.[1]
          || url.pathname.match(/^\/([A-Za-z0-9]{6,8})\/?$/)?.[1];
        if (!roomId) return null;
        return { roomId: roomId.toUpperCase(), origin: url.origin, joinUrl: `ws://${url.host}/${roomId.toUpperCase()}` };
      } catch { return null; }
    }
    if (/^wss?:\/\//i.test(input)) {
      try {
        const url = new URL(input);
        const roomId = url.pathname.match(/\/chat\/([A-Za-z0-9]{6,8})\/?/)?.[1]
          || url.pathname.match(/^\/([A-Za-z0-9]{6,8})\/?$/)?.[1];
        if (!roomId) return null;
        return {
          roomId: roomId.toUpperCase(),
          origin: `${url.protocol === 'wss:' ? 'https:' : 'http:'}//${url.host}`,
          joinUrl: `${url.protocol}//${url.host}${url.pathname}`,
        };
      } catch { return null; }
    }
    const hostAndCode = input.match(/^(?:https?:\/\/)?([^/\s]+)\/(?:chat\/)?([A-Za-z0-9]{6,8})$/i);
    if (hostAndCode) {
      const host = hostAndCode[1].includes(':') ? hostAndCode[1] : `${hostAndCode[1]}:3000`;
      return { roomId: hostAndCode[2].toUpperCase(), origin: `http://${host}`, joinUrl: `ws://${host}/chat/${hostAndCode[2].toUpperCase()}` };
    }
    if (/^[A-Za-z0-9]{6,8}$/.test(input)) {
      return { roomId: input.toUpperCase(), origin: currentOrigin, joinUrl: '' };
    }
    return null;
  }

  async socketForOrigin(origin) {
    const baseOrigin = origin || this.originForSocket(this.defaultSocket);
    if (baseOrigin === this.originForSocket(this.defaultSocket)) {
      this.activeSocket = this.defaultSocket;
      this.bindSocket(this.defaultSocket);
      return this.defaultSocket;
    }
    let socket = this.sockets.get(baseOrigin);
    if (!socket) {
      socket = io(baseOrigin, { transports: ['websocket', 'polling'], reconnection: true });
      this.sockets.set(baseOrigin, socket);
      this.bindSocket(socket);
    }
    this.activeSocket = socket;
    try {
      await new Promise((resolve, reject) => {
        if (socket.connected) return resolve();
        const timer = setTimeout(() => reject(new Error('Connection timed out. Check the host IP, port, and firewall.')), 10_000);
        socket.once('connect', () => { clearTimeout(timer); resolve(); });
        socket.once('connect_error', error => { clearTimeout(timer); reject(error); });
      });
      return socket;
    } catch (error) {
      this.setStatus(`Could not reach ${baseOrigin}: ${error.message}`, true);
      return null;
    }
  }

  originForSocket(socket) {
    if (socket === this.defaultSocket) return location.origin;
    return socket.io?.uri || location.origin;
  }

  emitAck(socket, event, payload) {
    return new Promise(resolve => {
      let completed = false;
      const finish = result => {
        if (completed) return;
        completed = true;
        clearTimeout(timer);
        resolve(result || { error: 'The server did not return a response.' });
      };
      const timer = setTimeout(() => finish({ error: 'The server did not respond in time.' }), 12_000);
      socket.emit(event, payload, finish);
    });
  }

  async sendMessage(event) {
    event.preventDefault();
    if (!this.activeRoom) return this.setStatus('Join or create a room first.', true);
    const input = $('chat-message-input');
    const text = input.value.trim();
    if (!text) return;
    const result = await this.emitAck(this.activeSocket, 'chat-room:message', {
      roomId: this.activeRoom.roomId,
      text,
    });
    if (result.error) return this.setStatus(result.error, true);
    input.value = '';
  }

  async sendFile(file) {
    if (!this.activeRoom) return this.setStatus('Join or create a room first.', true);
    const maxBytes = this.maxFileSize * 1024 * 1024;
    if (!this.validFile(file, maxBytes)) return;
    const transferId = crypto.randomUUID();
    const started = await this.emitAck(this.activeSocket, 'chat-room:file-start', {
      roomId: this.activeRoom.roomId,
      transferId,
      name: file.name,
      size: file.size,
      type: file.type || 'application/octet-stream',
    });
    if (started.error) return this.setStatus(started.error, true);
    this.transfers.set(transferId, { sender: true, name: file.name });
    this.appendMessage({
      kind: 'file',
      id: transferId,
      username: this.username(),
      name: file.name,
      size: file.size,
      type: file.type || 'application/octet-stream',
      timestamp: Date.now(),
      own: true,
    });
    this.updateFileProgress(file.name, 0);
    try {
      for (let offset = 0, index = 0; offset < file.size; offset += started.chunkSize, index++) {
        const data = await file.slice(offset, offset + started.chunkSize).arrayBuffer();
        const result = await this.emitAck(this.activeSocket, 'chat-room:file-chunk', {
          roomId: this.activeRoom.roomId,
          transferId,
          index,
          data,
        });
        if (result.error) throw new Error(result.error);
        this.updateFileProgress(file.name, Math.min(100, Math.round(((offset + data.byteLength) / file.size) * 100)));
      }
      this.setStatus(`Shared ${file.name}.`);
      this.hideFileProgress();
    } catch (error) {
      this.hideFileProgress();
      this.setStatus(`Could not share ${file.name}: ${error.message}`, true);
    }
  }

  validFile(file, maxBytes) {
    const blocked = /\.(ade|adp|app|bat|cab|cmd|com|cpl|dll|dmg|exe|hta|htm|html|jar|js|jse|lnk|msi|msp|mst|ps1|reg|scr|sh|svg|vbe|vbs|wsf|wsh)$/i;
    const blockedTypes = new Set(['application/javascript', 'application/x-javascript', 'application/x-msdownload', 'text/html', 'image/svg+xml']);
    if (!file.size || file.size > maxBytes) {
      this.setStatus(`File must be smaller than ${this.maxFileSize} MB.`, true);
      return false;
    }
    if (blocked.test(file.name) || blockedTypes.has((file.type || '').toLowerCase())) {
      this.setStatus('That file type cannot be shared in Local Chat.', true);
      return false;
    }
    return true;
  }

  receiveChunk(packet) {
    if (packet.roomId !== this.activeRoom?.roomId) return;
    const transfer = this.transfers.get(packet.transferId);
    if (!transfer || packet.index !== transfer.nextChunk) return;
    const chunk = packet.data instanceof ArrayBuffer
      ? new Uint8Array(packet.data)
      : ArrayBuffer.isView(packet.data)
        ? new Uint8Array(packet.data.buffer, packet.data.byteOffset, packet.data.byteLength)
        : null;
    if (!chunk || transfer.received + chunk.byteLength > transfer.size) {
      this.transfers.delete(packet.transferId);
      return this.setStatus('Received an invalid file chunk.', true);
    }
    transfer.chunks.push(chunk);
    transfer.nextChunk++;
    transfer.received += chunk.byteLength;
    this.updateFileProgress(transfer.name, Math.round((transfer.received / transfer.size) * 100));
  }

  finishReceive(transferId) {
    const transfer = this.transfers.get(transferId);
    if (!transfer) return;
    this.transfers.delete(transferId);
    if (transfer.sender) return;
    if (transfer.received !== transfer.size) return this.setStatus('The received file was incomplete.', true);
    const blob = new Blob(transfer.chunks, { type: transfer.type || 'application/octet-stream' });
    const url = URL.createObjectURL(blob);
    this.appendFileDownload(transferId, transfer.name, transfer.size, url, transfer.type);
    this.hideFileProgress();
    this.setStatus(`Received ${transfer.name}.`);
  }

  appendMessage(message, save = true) {
    const feed = $('chat-room-feed');
    if (message.id && feed.querySelector(`[data-message-id="${CSS.escape(message.id)}"]`)) return;
    const row = document.createElement('article');
    row.className = `chat-feed-item ${message.kind === 'notice' ? 'is-notice' : ''} ${message.userId === this.activeSocket.id || message.own ? 'is-own' : ''}`;
    if (message.id) row.dataset.messageId = message.id;
    if (message.kind === 'notice') {
      row.textContent = message.text;
    } else {
      const meta = document.createElement('div');
      meta.className = 'chat-feed-meta';
      const sender = document.createElement('strong');
      sender.textContent = message.username || this.username();
      const time = document.createElement('time');
      time.dateTime = new Date(message.timestamp || Date.now()).toISOString();
      time.textContent = new Date(message.timestamp || Date.now()).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      meta.append(sender, time);
      row.append(meta);
      if (message.kind === 'file') {
        const info = document.createElement('div');
        info.className = 'chat-file-message';
        info.textContent = `${message.type?.startsWith('image/') ? 'Image' : 'File'} · ${message.name} · ${this.formatSize(message.size)}`;
        row.append(info);
        if (message.own) {
          this.addDownloadLink(row, message.name, null, false);
        } else if (!message.archived && message.type?.startsWith('image/')) {
          const preview = document.createElement('p');
          preview.className = 'chat-file-pending';
          preview.textContent = 'Receiving image…';
          row.append(preview);
        }
      } else {
        const body = document.createElement('p');
        body.className = 'chat-message-body';
        this.appendLinkedText(body, message.text || '');
        row.append(body);
      }
    }
    feed.append(row);
    feed.scrollTop = feed.scrollHeight;
    if (save) this.saveHistory(message);
  }

  appendLinkedText(container, text) {
    const pattern = /https?:\/\/[^\s<>"']+/gi;
    let cursor = 0;
    for (const match of text.matchAll(pattern)) {
      const start = match.index;
      let value = match[0];
      const trailing = value.match(/[),.!?;:]+$/)?.[0] || '';
      value = value.slice(0, value.length - trailing.length);
      if (start > cursor) container.append(document.createTextNode(text.slice(cursor, start)));
      try {
        const url = new URL(value);
        if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new Error('Unsafe link');
        const link = document.createElement('a');
        link.href = url.href;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = value;
        container.append(link, document.createTextNode(trailing));
        cursor = start + match[0].length;
      } catch {
        container.append(document.createTextNode(match[0]));
        cursor = start + match[0].length;
      }
    }
    if (cursor < text.length) container.append(document.createTextNode(text.slice(cursor)));
  }

  appendFileDownload(messageId, name, size, url, type) {
    const row = $('chat-room-feed').querySelector(`[data-message-id="${CSS.escape(messageId)}"]`);
    if (!row) return;
    const pending = row.querySelector('.chat-file-pending');
    if (pending) pending.remove();
    this.addDownloadLink(row, name, url, true);
    if (type?.startsWith('image/')) {
      const image = document.createElement('img');
      image.className = 'chat-image-preview';
      image.src = url;
      image.alt = `Shared image: ${name}`;
      row.append(image);
    }
  }

  addDownloadLink(row, name, url, available) {
    const link = document.createElement('a');
    link.className = 'chat-download-link';
    link.textContent = available ? `Download ${name}` : `Sending ${name}…`;
    if (available) {
      link.href = url;
      link.download = name;
    }
    row.append(link);
  }

  async saveHistory(message) {
    if (!this.activeRoom || !message.kind) return;
    try {
      const db = await this.openDatabase();
      const record = {
        id: `${this.activeRoom.roomId}:${message.id || `${message.timestamp}-${Math.random()}`}`,
        roomId: this.activeRoom.roomId,
        kind: message.kind,
        username: message.username || '',
        text: message.text || '',
        name: message.name || '',
        size: message.size || 0,
        type: message.type || '',
        timestamp: message.timestamp || Date.now(),
      };
      const tx = db.transaction('messages', 'readwrite');
      tx.objectStore('messages').put(record);
      tx.oncomplete = () => db.close();
      tx.onerror = () => { db.close(); console.error('Could not store Local Chat history.', tx.error); };
    } catch (error) {
      console.error('Could not open Local Chat history database.', error);
    }
  }

  openDatabase() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open('peerdrop-local-chat', 1);
      request.onupgradeneeded = () => {
        const store = request.result.createObjectStore('messages', { keyPath: 'id' });
        store.createIndex('roomId', 'roomId', { unique: false });
        store.createIndex('timestamp', 'timestamp', { unique: false });
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  async loadHistory(roomId) {
    $('chat-room-feed').replaceChildren();
    try {
      const db = await this.openDatabase();
      const records = await new Promise((resolve, reject) => {
        const request = db.transaction('messages').objectStore('messages').index('roomId').getAll(roomId);
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      db.close();
      if (this.activeRoom?.roomId !== roomId) return;
      records.sort((a, b) => a.timestamp - b.timestamp).slice(-500)
        .forEach(record => this.appendMessage({ ...record, archived: true }, false));
    } catch (error) {
      console.error('Could not load Local Chat history.', error);
    }
  }

  renderActiveRoom() {
    const active = Boolean(this.activeRoom);
    $('chat-empty-state').classList.toggle('hidden', active);
    $('chat-active-room').classList.toggle('hidden', !active);
    $('chat-invite-qr').classList.toggle('hidden', !active || !this.activeRoom.isHost);
    $('chat-leave-room').classList.toggle('hidden', !active || this.activeRoom.isHost);
    $('chat-close-room').classList.toggle('hidden', !active || !this.activeRoom.isHost);
    if (!active) return;
    $('chat-active-name').textContent = this.activeRoom.name;
    $('chat-active-address').textContent = this.activeRoom.localIP
      ? `${this.activeRoom.localIP}:${this.activeRoom.port} · room ${this.activeRoom.roomId}`
      : `Room ${this.activeRoom.roomId}${this.activeRoom.hasPassword ? ' · password protected' : ''}`;
    this.renderMembers();
    this.loadHistory(this.activeRoom.roomId);
  }

  renderMembers() {
    const list = $('chat-room-members');
    list.replaceChildren();
    for (const member of this.activeMembers) {
      const item = document.createElement('li');
      const dot = document.createElement('span');
      dot.className = 'chat-online-dot';
      const label = document.createElement('span');
      label.textContent = member.username || member.name || 'Guest';
      item.append(dot, label);
      if (member.isHost || member.id === this.activeRoom?.hostId) {
        const host = document.createElement('small');
        host.textContent = 'HOST';
        item.append(host);
      }
      list.append(item);
    }
    $('chat-member-count').textContent = String(this.activeMembers.length);
  }

  renderInviteQR() {
    const box = $('chat-invite-qr');
    box.replaceChildren();
    if (!this.activeRoom?.isHost || typeof QRCode === 'undefined') return;
    const qr = document.createElement('div');
    qr.className = 'chat-qr-code';
    const invite = this.activeRoom.joinUrl;
    try {
      new QRCode(qr, { text: invite, width: 144, height: 144, colorDark: '#10101a', colorLight: '#ffffff', correctLevel: QRCode.CorrectLevel.M });
      const caption = document.createElement('span');
      caption.textContent = invite;
      box.append(qr, caption);
    } catch (error) {
      console.error('Could not render Local Chat QR code.', error);
      box.textContent = invite;
    }
  }

  async refreshRooms() {
    const list = $('chat-room-list');
    try {
      const origin = this.originForSocket(this.activeSocket);
      const localResponse = await fetch(`${origin}/api/chat/rooms`, { cache: 'no-store' });
      if (!localResponse.ok) throw new Error(`Server returned ${localResponse.status}`);
      const localResult = await localResponse.json();
      const roomGroups = [{
        origin,
        rooms: localResult.rooms.map(room => ({ ...room, hostAddress: location.hostname, hostPort: new URL(origin).port || 3000 })),
      }];
      try {
        const discoveryResponse = await fetch(`${origin}/api/chat/discovery`, { cache: 'no-store' });
        if (discoveryResponse.ok) {
          const { hosts } = await discoveryResponse.json();
          const discovered = await Promise.all(hosts.map(async host => {
            const hostOrigin = `http://${host.address}:${host.port}`;
            const response = await fetch(`${hostOrigin}/api/chat/rooms`, { cache: 'no-store' });
            if (!response.ok) throw new Error(`Server returned ${response.status}`);
            const result = await response.json();
            return {
              origin: hostOrigin,
              rooms: result.rooms.map(room => ({ ...room, hostAddress: host.address, hostPort: host.port })),
            };
          }));
          roomGroups.push(...discovered);
        }
      } catch (error) {
        console.warn('Some Bonjour-discovered hosts did not respond.', error);
      }
      const rooms = roomGroups.flatMap(group => group.rooms);
      list.replaceChildren();
      if (!rooms.length) {
        const empty = document.createElement('p');
        empty.className = 'chat-muted';
        empty.textContent = 'No active rooms on this host.';
        list.append(empty);
        return;
      }
      for (const room of rooms) {
        const item = document.createElement('div');
        item.className = 'chat-room-list-item';
        const info = document.createElement('div');
        const title = document.createElement('strong');
        title.textContent = room.name;
        const detail = document.createElement('span');
        detail.textContent = `${room.roomId} · ${room.memberCount} online · ${room.hostAddress}${room.hasPassword ? ' · locked' : ''}`;
        info.append(title, detail);
        const join = document.createElement('button');
        join.type = 'button';
        join.textContent = this.activeRoom?.roomId === room.roomId ? 'Open' : 'Join';
        join.addEventListener('click', () => {
          $('chat-room-code').value = `ws://${room.hostAddress}:${room.hostPort}/${room.roomId}`;
          if (room.hasPassword) $('chat-join-password').focus();
          else $('chat-join-form').requestSubmit();
        });
        item.append(info, join);
        list.append(item);
      }
    } catch (error) {
      list.replaceChildren();
      const message = document.createElement('p');
      message.className = 'chat-muted';
      message.textContent = `Room list unavailable: ${error.message}`;
      list.append(message);
    }
  }

  parseInviteFromScan(value) {
    const parsed = this.parseInvite(value.trim());
    if (!parsed) {
      $('chat-scan-status').textContent = 'That QR code is not a PeerDrop Local Chat invite.';
      return;
    }
    $('chat-room-code').value = value.trim();
    $('chat-join-form').requestSubmit();
  }

  async toggleScanner() {
    const area = $('chat-qr-reader');
    if (this.scanning) return this.stopScanner();
    if (!window.isSecureContext && location.hostname !== 'localhost' && location.hostname !== '127.0.0.1') {
      $('chat-scan-status').textContent = 'Camera scanning needs HTTPS or localhost. Use a QR scanner app, then paste its ws:// address here.';
      return;
    }
    try {
      await loadQrScanner();
      area.classList.remove('hidden');
      $('chat-scan-status').textContent = 'Starting camera…';
      this.scanner ||= new Html5Qrcode('chat-qr-reader');
      const cameras = await Html5Qrcode.getCameras();
      if (!cameras.length) throw new Error('No camera was found.');
      const camera = cameras.find(item => /back|rear|environment/i.test(item.label)) || cameras[0];
      await this.scanner.start(camera.id, { fps: 12, qrbox: { width: 220, height: 220 } }, value => {
        this.stopScanner();
        this.parseInviteFromScan(value);
      }, () => {});
      this.scanning = true;
      $('chat-scan-status').textContent = 'Point the camera at a Local Chat QR code.';
    } catch (error) {
      area.classList.add('hidden');
      $('chat-scan-status').textContent = error.message;
    }
  }

  async stopScanner() {
    if (this.scanner && this.scanning) {
      try { await this.scanner.stop(); } catch (error) { console.warn('Could not stop QR scanner cleanly.', error); }
    }
    this.scanning = false;
    $('chat-qr-reader').classList.add('hidden');
  }

  async copyInvite() {
    if (!this.activeRoom) return;
    const invite = this.activeRoom.joinUrl || `ws://${location.host}/${this.activeRoom.roomId}`;
    try {
      await navigator.clipboard.writeText(invite);
      this.setStatus('Room address copied.');
    } catch {
      this.setStatus(invite);
    }
  }

  async closeRoom() {
    if (!this.activeRoom?.isHost) return;
    if (!confirm(`Close "${this.activeRoom.name}" for everyone?`)) return;
    const result = await this.emitAck(this.activeSocket, 'chat-room:close', { roomId: this.activeRoom.roomId });
    if (result.error) return this.setStatus(result.error, true);
    this.activeRoom = null;
    this.activeMembers = [];
    this.renderActiveRoom();
    this.refreshRooms();
  }

  updateFileProgress(name, value) {
    $('chat-file-progress-wrap').classList.remove('hidden');
    $('chat-file-progress-name').textContent = name;
    $('chat-file-progress-value').textContent = `${value}%`;
    $('chat-file-progress').value = value;
  }

  hideFileProgress() {
    $('chat-file-progress-wrap').classList.add('hidden');
    $('chat-file-progress').value = 0;
  }

  setStatus(message, isError = false) {
    const status = $('chat-action-status');
    status.textContent = message;
    status.classList.toggle('is-error', isError);
  }

  formatSize(bytes) {
    return bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(0)} KB` : `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
}

window.localChat = new LocalChatRooms();
