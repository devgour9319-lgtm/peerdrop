'use strict';

(() => {
  const $ = id => document.getElementById(id);
  const loginForm = $('pc-login-form');
  const pinInput = $('pc-pin-input');
  const inboxPanel = $('pc-inbox-panel');
  const fileInput = $('pc-file-input');
  const fileList = $('pc-file-list');
  const uploadList = $('pc-upload-list');
  const status = $('pc-connect-status');

  function showStatus(message, kind = '') {
    status.textContent = message;
    status.className = `pc-connect-status${kind ? ` ${kind}` : ''}`;
  }

  async function readResponse(response) {
    let body;
    try {
      body = await response.json();
    } catch {
      throw new Error(`PeerDrop returned an unreadable response (${response.status}).`);
    }
    if (!response.ok) throw new Error(body.error || `Request failed (${response.status}).`);
    return body;
  }

  async function refreshFiles() {
    const { files } = await readResponse(await fetch('/api/pc-connect/files', { cache: 'no-store' }));
    fileList.replaceChildren();
    if (!files.length) {
      const empty = document.createElement('p');
      empty.className = 'pc-empty';
      empty.textContent = 'The inbox is empty. Upload a file from this device to get started.';
      fileList.appendChild(empty);
      return;
    }
    for (const file of files) {
      const row = document.createElement('article');
      row.className = 'pc-file-row';
      const info = document.createElement('div');
      info.className = 'pc-file-info';
      const name = document.createElement('strong');
      name.textContent = file.name;
      const meta = document.createElement('span');
      meta.textContent = `${formatSize(file.size)} · ${new Date(file.uploadedAt).toLocaleString()}`;
      info.append(name, meta);
      const actions = document.createElement('div');
      actions.className = 'pc-file-actions';
      const download = document.createElement('a');
      download.className = 'pc-download-link';
      download.href = `/api/pc-connect/files/${encodeURIComponent(file.id)}`;
      download.textContent = 'Download';
      const remove = document.createElement('button');
      remove.type = 'button';
      remove.className = 'pc-delete-button';
      remove.textContent = 'Delete';
      remove.addEventListener('click', async () => {
        if (!window.confirm(`Delete "${file.name}" from the host inbox?`)) return;
        remove.disabled = true;
        try {
          await readResponse(await fetch(`/api/pc-connect/files/${encodeURIComponent(file.id)}`, {
            method: 'DELETE',
          }));
          await refreshFiles();
          showStatus(`Deleted "${file.name}".`, 'success');
        } catch (error) {
          remove.disabled = false;
          showStatus(error.message, 'error');
        }
      });
      actions.append(download, remove);
      row.append(info, actions);
      fileList.appendChild(row);
    }
  }

  function formatSize(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let size = bytes / 1024;
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
      size /= 1024;
      unit++;
    }
    return `${size.toFixed(size < 10 ? 1 : 0)} ${units[unit]}`;
  }

  async function showInbox() {
    loginForm.classList.add('hidden');
    inboxPanel.classList.remove('hidden');
    try {
      await refreshFiles();
    } catch (error) {
      if (error.message.includes('Enter the host PIN')) {
        inboxPanel.classList.add('hidden');
        loginForm.classList.remove('hidden');
      }
      throw error;
    }
  }

  async function uploadFile(file) {
    const row = document.createElement('div');
    row.className = 'pc-upload-row';
    const label = document.createElement('span');
    label.textContent = file.name;
    const progress = document.createElement('progress');
    progress.max = 100;
    progress.value = 0;
    const result = document.createElement('span');
    result.textContent = 'Preparing…';
    row.append(label, progress, result);
    uploadList.prepend(row);

    await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/pc-connect/upload');
      xhr.withCredentials = true;
      xhr.setRequestHeader('Content-Type', 'application/octet-stream');
      xhr.setRequestHeader('X-PeerDrop-Filename', encodeURIComponent(file.name));
      xhr.upload.onprogress = event => {
        if (!event.lengthComputable) return;
        progress.value = event.total ? (event.loaded / event.total) * 100 : 100;
        result.textContent = `${Math.round(progress.value)}%`;
      };
      xhr.onload = () => {
        let body;
        try {
          body = JSON.parse(xhr.responseText);
        } catch {
          reject(new Error(`PeerDrop returned an unreadable response (${xhr.status}).`));
          return;
        }
        if (xhr.status < 200 || xhr.status >= 300) {
          reject(new Error(body.error || `Upload failed (${xhr.status}).`));
          return;
        }
        progress.value = 100;
        result.textContent = 'Uploaded';
        resolve();
      };
      xhr.onerror = () => reject(new Error('Upload failed. Check the Wi-Fi connection and try again.'));
      xhr.onabort = () => reject(new Error('Upload was cancelled.'));
      result.textContent = 'Uploading…';
      xhr.send(file);
    }).catch(error => {
      row.classList.add('failed');
      result.textContent = error.message;
      throw error;
    });
  }

  loginForm.addEventListener('submit', async event => {
    event.preventDefault();
    const button = loginForm.querySelector('button[type="submit"]');
    button.disabled = true;
    showStatus('Connecting to the host…');
    try {
      await readResponse(await fetch('/api/pc-connect/auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pin: pinInput.value }),
      }));
      pinInput.value = '';
      await showInbox();
      showStatus('Connected to the PeerDrop host inbox.', 'success');
    } catch (error) {
      showStatus(error.message, 'error');
    } finally {
      button.disabled = false;
    }
  });

  fileInput.addEventListener('change', async () => {
    const files = Array.from(fileInput.files || []);
    fileInput.value = '';
    let uploaded = 0;
    let uploadError = null;
    for (const file of files) {
      try {
        await uploadFile(file);
        uploaded++;
      } catch (error) {
        uploadError = error;
        break;
      }
    }
    if (uploaded) {
      try {
        await refreshFiles();
      } catch (error) {
        showStatus(`Uploaded ${uploaded} file${uploaded === 1 ? '' : 's'}, but inbox refresh failed: ${error.message}`, 'error');
        return;
      }
    }
    if (uploadError) {
      showStatus(uploaded
        ? `Uploaded ${uploaded} file${uploaded === 1 ? '' : 's'}; stopped at the next file: ${uploadError.message}`
        : uploadError.message, 'error');
    } else if (uploaded) {
      showStatus(`Uploaded ${uploaded} file${uploaded === 1 ? '' : 's'} to the host inbox.`, 'success');
    }
  });

  $('pc-logout-btn').addEventListener('click', async () => {
    try {
      await readResponse(await fetch('/api/pc-connect/logout', { method: 'POST' }));
      inboxPanel.classList.add('hidden');
      loginForm.classList.remove('hidden');
      fileList.replaceChildren();
      showStatus('Disconnected from the host inbox.');
    } catch (error) {
      showStatus(error.message, 'error');
    }
  });

  $('pc-copy-url-btn').addEventListener('click', async () => {
    const address = $('pc-host-url').textContent;
    try {
      await navigator.clipboard.writeText(address);
      showStatus('Host address copied. Open it in the PC browser.', 'success');
    } catch {
      showStatus(`Copy unavailable here. Enter ${address} in the PC browser.`, 'error');
    }
  });

  fetch('/local-info', { cache: 'no-store' }).then(async response => {
    if (!response.ok) throw new Error(`Could not load the host address (${response.status}).`);
    const info = await response.json();
    $('pc-host-url').textContent = location.protocol === 'https:'
      ? location.origin
      : `http://${info.ip || location.hostname}:${info.port}`;
  }).catch(error => showStatus(error.message, 'error'));

  fetch('/api/pc-connect/host', { cache: 'no-store' }).then(async response => {
    if (response.ok) {
      const { pin } = await response.json();
      $('pc-host-pin').textContent = pin;
      $('pc-host-note').textContent = 'Share this PIN only with the person you want to pair with.';
    } else if (response.status === 403) {
      $('pc-host-note').textContent = 'For security, the PIN is shown only on the host device or in its server console.';
    } else {
      throw new Error(`Could not load host PIN (${response.status}).`);
    }
  }).catch(error => showStatus(error.message, 'error'));

  showInbox().then(() => showStatus('Connected to the PeerDrop host inbox.', 'success')).catch(error => {
    if (error.message.includes('Enter the host PIN')) return;
    showStatus(error.message, 'error');
  });
})();
