const socket = io();
let isRunning = false;
let appEnabled = true;
let activeProcessLog = null;
let userRole = document.body.dataset.role || 'viewer';

// TAB NAVIGATION
function switchTab(tabName) {
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.sidebar-btn').forEach(b => b.classList.remove('active'));
    document.getElementById('tab-' + tabName).classList.add('active');
    document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
    if (tabName === 'dashboard') loadDashboardStats();
    if (tabName === 'manager') loadManager();
    if (tabName === 'profiles') loadProfiles();
    if (tabName === 'channels') loadManagedChannels();
    if (tabName === 'activity') loadAuditLog();
    if (tabName === 'settings') { loadAccountSettings(); loadSecurityQuestions(); }
    if (tabName === 'scheduler') loadScheduler();
}

// INIT
document.addEventListener('DOMContentLoaded', () => { loadAppStatus(); loadScheduler(); loadProfiles(); loadDashboardStats(); });

// SOCKET EVENTS
socket.on('log', (data) => addLog(data.text, data.type));
socket.on('scheduler_update', (config) => {
    const nr = document.getElementById('next-run'); const lr = document.getElementById('last-run');
    if (nr) nr.textContent = config.next_run || 'Not scheduled';
    if (lr) lr.textContent = config.last_run || 'Never';
});
socket.on('status', (data) => updateStatus(data.running, data.progress, data.channel));
socket.on('channel_done', (data) => addLog('Done ' + data.channel + ': ' + data.count + ' messages', 'success'));
socket.on('done', (data) => {
    isRunning = false; updateStatus(false, 100); showResults(data.results);
    const sb = document.getElementById('start-btn'); if (sb) { sb.disabled = false; sb.classList.remove('opacity-50'); }
});
socket.on('profile_log', (data) => { if (activeProcessLog === data.process_id) addProcessLog(data.text, data.type); });
socket.on('profile_progress', (data) => {
    const el = document.getElementById('proc-count-' + data.process_id);
    if (el) el.textContent = data.messages.toLocaleString();
    if (data.current_date) { const dateEl = document.getElementById('proc-date-' + data.process_id); if (dateEl) dateEl.textContent = data.current_date; }
});
socket.on('profile_process_done', (data) => loadProfileDetail(data.profile_id));
socket.on('app_disabled', () => { appEnabled = false; updateAppToggleButton(); });
socket.on('app_enabled', () => { appEnabled = true; updateAppToggleButton(); });

// STATUS
function updateStatus(running, progress, channel) {
    channel = channel || '';
    const badge = document.getElementById('status-badge');
    const bar = document.getElementById('progress-bar');
    const pct = document.getElementById('progress-percent');
    const task = document.getElementById('current-task');
    const startBtn = document.getElementById('start-btn');
    const killBtn = document.getElementById('kill-btn');
    if (bar) bar.style.width = progress + '%';
    if (pct) pct.textContent = progress + '%';
    if (running) {
        if (badge) { badge.textContent = 'Running'; badge.className = 'badge bg-blue-600 text-white text-sm px-3 py-1 pulse'; }
        if (task) task.textContent = channel ? 'Scraping: ' + channel : 'Processing...';
        if (startBtn) { startBtn.disabled = true; startBtn.classList.add('opacity-50'); }
        if (killBtn) killBtn.classList.remove('hidden');
    } else {
        if (badge) { badge.textContent = progress === 100 ? 'Completed' : 'Ready'; badge.className = 'badge ' + (progress === 100 ? 'bg-green-600 text-white' : 'bg-slate-700 text-slate-300') + ' text-sm px-3 py-1'; }
        if (task) task.textContent = progress === 100 ? 'Scraping completed!' : 'Waiting to start...';
        if (startBtn) { startBtn.disabled = false; startBtn.classList.remove('opacity-50'); }
        if (killBtn) killBtn.classList.add('hidden');
    }
}

function loadAppStatus() {
    fetch('/api/app/status').then(r => r.json()).then(data => { appEnabled = data.enabled; updateAppToggleButton(); updateLocalStats(data.local_messages); }).catch(() => {});
}
function toggleApp() {
    fetch('/api/app/toggle', { method: 'POST', headers: { 'Content-Type': 'application/json' } }).then(r => r.json()).then(data => { appEnabled = data.enabled; updateAppToggleButton(); addLog('App ' + (appEnabled ? 'ENABLED' : 'DISABLED'), appEnabled ? 'success' : 'warning'); });
}
function updateAppToggleButton() {
    const btn = document.getElementById('app-toggle-btn');
    const startBtn = document.getElementById('start-btn');
    const banner = document.getElementById('app-halt-banner');
    const scrapeButtons = document.querySelectorAll('.run-process-btn');
    if (appEnabled) {
        btn.textContent = 'Enabled'; btn.className = 'badge bg-green-600/20 text-green-400 cursor-pointer text-xs';
        if (startBtn) { startBtn.disabled = false; startBtn.classList.remove('opacity-50', 'cursor-not-allowed'); }
        if (banner) banner.classList.add('hidden');
        scrapeButtons.forEach(b => { b.disabled = false; b.classList.remove('opacity-50', 'cursor-not-allowed'); });
    } else {
        btn.textContent = 'Disabled'; btn.className = 'badge bg-red-600/20 text-red-400 cursor-pointer text-xs';
        if (startBtn) { startBtn.disabled = true; startBtn.classList.add('opacity-50', 'cursor-not-allowed'); }
        if (banner) banner.classList.remove('hidden');
        scrapeButtons.forEach(b => { b.disabled = true; b.classList.add('opacity-50', 'cursor-not-allowed'); });
    }
}
function updateLocalStats(stats) {
    const el = document.getElementById('local-stats-sidebar');
    if (el) el.textContent = (stats && stats.total > 0) ? 'DB: ' + stats.total + ' msgs' : '';
}

// LOG
function addLog(text, type) {
    type = type || 'info';
    const container = document.getElementById('log-container');
    if (!container) return;
    if (container.querySelector('.text-slate-500')) container.innerHTML = '';
    const colors = { info: 'text-slate-300', success: 'text-green-400', error: 'text-red-400', warning: 'text-yellow-400' };
    const entry = document.createElement('div');
    entry.className = 'log-entry ' + (colors[type] || colors.info);
    entry.innerHTML = '<span class="text-slate-600">[' + new Date().toLocaleTimeString() + ']</span> ' + text;
    container.appendChild(entry);
    container.scrollTop = container.scrollHeight;
}
function clearLogs() { document.getElementById('log-container').innerHTML = '<div class="text-slate-500">Waiting for activity...</div>'; }
function showResults(results) {
    const card = document.getElementById('results-card');
    const list = document.getElementById('results-list');
    if (!results || !results.length) { if (card) card.classList.add('hidden'); return; }
    card.classList.remove('hidden');
    list.innerHTML = results.map(r => '<div class="flex items-center justify-between bg-[#0b1120] rounded-lg p-3"><span class="font-medium">' + r.channel + '</span><span class="' + (r.error ? 'text-red-400' : 'text-green-400') + '">' + (r.error ? 'Error' : r.messages + ' messages') + '</span></div>').join('');
}

// SCRAPE
function startScrape() {
    if (isRunning) return;
    isRunning = true;
    const pushToSheets = document.getElementById('push-sheets').checked;
    const exportLocal = document.getElementById('export-local').checked;
    const localFormat = document.getElementById('local-format').value;
    const localMode = document.getElementById('local-mode').value;
    const localAppend = localMode === 'append';
    const saveLocation = document.getElementById('save-location').value;
    const customPath = document.getElementById('custom-path').value.trim() || null;
    let localFilename = null;
    if (exportLocal) localFilename = localAppend ? document.getElementById('existing-file').value : (document.getElementById('custom-filename').value.trim() || null);
    clearLogs(); addLog('Starting scraper...', 'info');
    fetch('/api/scrape', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ push_to_sheets: pushToSheets, export_local: exportLocal, local_format: localFormat, local_filename: localFilename, local_append: localAppend, save_location: saveLocation, custom_path: customPath }) })
        .then(r => r.json()).then(data => { if (data.error) { addLog(data.error, 'error'); isRunning = false; updateStatus(false, 0); } })
        .catch(err => { addLog('Failed: ' + err, 'error'); isRunning = false; updateStatus(false, 0); });
}
function killScrape() {
    if (!isRunning) return;
    addLog('Sending kill signal...', 'warning');
    fetch('/api/kill', { method: 'POST' }).then(r => r.json()).then(data => { if (data.error) addLog(data.error, 'error'); else addLog('Kill signal sent', 'warning'); });
}

// EXPORT OPTIONS
function toggleLocalExportOptions() { const on = document.getElementById('export-local').checked; document.getElementById('local-export-options').classList.toggle('hidden', !on); if (on) loadExportFiles(); }
function toggleFilenameInput() { const append = document.getElementById('local-mode').value === 'append'; document.getElementById('existing-files-section').classList.toggle('hidden', !append); document.getElementById('custom-filename-section').classList.toggle('hidden', append); if (append) loadExportFiles(); }
function toggleCustomPath() { document.getElementById('custom-path-section').classList.toggle('hidden', document.getElementById('save-location').value !== 'custom'); loadExportFiles(); }
function loadExportFiles() {
    const format = document.getElementById('local-format').value;
    const location = document.getElementById('save-location').value;
    const customPath = document.getElementById('custom-path').value.trim() || null;
    let url = '/api/exports?location=' + location;
    if (location === 'custom' && customPath) url += '&custom_path=' + encodeURIComponent(customPath);
    fetch(url).then(r => r.json()).then(files => {
        const select = document.getElementById('existing-file');
        const filtered = files.filter(f => f.format === format);
        select.innerHTML = filtered.length === 0 ? '<option value="">No files</option>' : filtered.map(f => '<option value="' + f.name + '">' + f.name + ' (' + formatBytes(f.size) + ')</option>').join('');
    }).catch(() => {});
}
function formatBytes(bytes) { if (bytes === 0) return '0 B'; const k = 1024, sizes = ['B', 'KB', 'MB', 'GB'], i = Math.floor(Math.log(bytes) / Math.log(k)); return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]; }

// CHANNELS
function addChannel() {
    const list = document.getElementById('channels-list');
    const count = list.querySelectorAll('.channel-item').length + 1;
    const item = document.createElement('div');
    item.className = 'channel-item card';
    item.innerHTML = '<div class="flex items-center gap-3 mb-3"><div class="flex-1"><input type="text" placeholder="Channel username" class="channel-name input-field text-lg font-medium bg-transparent border-none"></div><button onclick="removeChannel(this)" class="text-red-400 hover:text-red-300 p-1"><svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg></button></div><div class="flex items-center gap-4 mb-3"><label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="mode-new-' + count + '" value="hours" checked onchange="toggleChannelMode(this)" class="w-4 h-4"><span class="text-sm text-slate-400">Last X hours</span></label><label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="mode-new-' + count + '" value="daterange" onchange="toggleChannelMode(this)" class="w-4 h-4"><span class="text-sm text-slate-400">Date range</span></label></div><div class="channel-hours-mode flex items-center gap-2"><span class="text-slate-400 text-sm">Last</span><input type="number" value="24" min="1" max="8760" class="channel-hours w-20 input-field text-center"><span class="text-slate-400 text-sm">hours</span></div><div class="channel-daterange-mode flex flex-wrap items-center gap-3 hidden"><div class="flex items-center gap-2"><span class="text-slate-400 text-sm">From:</span><input type="datetime-local" class="channel-from-date input-field text-sm"></div><div class="flex items-center gap-2"><span class="text-slate-400 text-sm">To:</span><input type="datetime-local" class="channel-to-date input-field text-sm"></div><button onclick="setToNow(this)" class="text-blue-400 text-xs px-2 py-1 bg-slate-700 rounded">Now</button></div>';
    list.appendChild(item);
}
function removeChannel(btn) { btn.closest('.channel-item').remove(); }
function toggleChannelMode(radio) { const item = radio.closest('.channel-item'); item.querySelector('.channel-hours-mode').classList.toggle('hidden', radio.value !== 'hours'); item.querySelector('.channel-daterange-mode').classList.toggle('hidden', radio.value !== 'daterange'); }
function setToNow(btn) { const item = btn.closest('.channel-item'); const now = new Date(); item.querySelector('.channel-to-date').value = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0') + 'T' + String(now.getHours()).padStart(2, '0') + ':' + String(now.getMinutes()).padStart(2, '0'); }
function saveChannels() {
    const items = document.querySelectorAll('.channel-item');
    const channels = [];
    items.forEach(item => {
        const name = item.querySelector('.channel-name').value.trim();
        const hours = parseInt(item.querySelector('.channel-hours').value) || 24;
        const modeRadio = item.querySelector('input[type="radio"]:checked');
        const useDateRange = modeRadio && modeRadio.value === 'daterange';
        if (name) { const ch = { name, hours_back: hours, use_date_range: useDateRange }; if (useDateRange) { ch.from_date_str = item.querySelector('.channel-from-date').value; ch.to_date_str = item.querySelector('.channel-to-date').value; } channels.push(ch); }
    });
    fetch('/api/channels', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ channels, default_hours: 24 }) }).then(r => r.json()).then(data => { if (data.success) addLog('Channels saved!', 'success'); });
}

// SCHEDULER
function loadScheduler() {
    fetch('/api/scheduler').then(r => r.json()).then(config => {
        const en = document.getElementById('scheduler-enabled'); if (en) en.checked = config.enabled;
        const st = document.getElementById('schedule-time'); if (st) st.value = config.time || '08:00';
        if (config.interval_hours) { const ir = document.querySelector('input[name="schedule-mode"][value="interval"]'); if (ir) { ir.checked = true; updateScheduleMode(); } const is = document.getElementById('schedule-interval'); if (is) is.value = config.interval_hours; }
        const nr = document.getElementById('next-run'); if (nr) nr.textContent = config.next_run || 'Not scheduled';
        const lr = document.getElementById('last-run'); if (lr) lr.textContent = config.last_run || 'Never';
        toggleSchedulerUI(config.enabled);
    }).catch(() => {});
}
function toggleScheduler() { toggleSchedulerUI(document.getElementById('scheduler-enabled').checked); }
function toggleSchedulerUI(enabled) { const opts = document.getElementById('scheduler-options'); if (opts) { if (enabled) opts.classList.remove('opacity-50', 'pointer-events-none'); else opts.classList.add('opacity-50', 'pointer-events-none'); } }
function updateScheduleMode() { const mode = document.querySelector('input[name="schedule-mode"]:checked'); if (!mode) return; const d = document.getElementById('daily-options'); const i = document.getElementById('interval-options'); if (d) d.classList.toggle('hidden', mode.value !== 'daily'); if (i) i.classList.toggle('hidden', mode.value !== 'interval'); }
function saveScheduler() {
    const enabled = document.getElementById('scheduler-enabled').checked;
    const mode = document.querySelector('input[name="schedule-mode"]:checked').value;
    const time = document.getElementById('schedule-time').value;
    const intervalHours = mode === 'interval' ? parseInt(document.getElementById('schedule-interval').value) : null;
    fetch('/api/scheduler', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled, time, interval_hours: intervalHours }) }).then(r => r.json()).then(data => { if (data.success) { addLog('Schedule saved!', 'success'); const nr = document.getElementById('next-run'); if (nr) nr.textContent = data.config.next_run || 'Not scheduled'; } });
}

// AUDIT LOG
function loadAuditLog() {
    fetch('/api/audit?limit=50').then(r => r.json()).then(logs => {
        const container = document.getElementById('audit-container');
        if (!logs || !logs.length) { container.innerHTML = '<div class="text-slate-500 text-sm">No audit history yet</div>'; return; }
        container.innerHTML = logs.map(log => {
            const sc = log.status === 'success' ? 'text-green-400' : log.status === 'error' ? 'text-red-400' : log.status === 'warning' ? 'text-yellow-400' : 'text-slate-400';
            const icon = log.status === 'success' ? '✓' : log.status === 'error' ? '✗' : log.status === 'warning' ? '⚠' : '•';
            const userLabel = log.user_email ? '<span class="text-slate-500 text-xs ml-2">' + log.user_email + '</span>' : '';
            return '<div class="flex items-start gap-2 text-sm border-b border-slate-800 pb-2"><span class="' + sc + '">' + icon + '</span><div class="flex-1"><div class="flex justify-between"><span class="font-medium text-slate-300">' + log.action.replace(/_/g, ' ') + userLabel + '</span><span class="text-slate-600 text-xs">' + log.timestamp + '</span></div>' + (log.details ? '<div class="text-slate-500 text-xs mt-1">' + log.details + '</div>' : '') + '</div></div>';
        }).join('');
    }).catch(() => { document.getElementById('audit-container').innerHTML = '<div class="text-red-400 text-sm">Failed to load</div>'; });
}
function clearAuditLog() { if (!confirm('Clear all audit history?')) return; fetch('/api/audit', { method: 'DELETE' }).then(r => r.json()).then(data => { if (data.success) { loadAuditLog(); addLog('Audit cleared', 'success'); } }); }

// SETTINGS / ACCOUNTS (per-user credentials)
function loadAccountSettings() {
    const isShared = userRole !== 'admin' && userRole !== 'editor';
    const credsUrl = isShared ? '/api/settings/shared-credentials' : '/api/user/credentials';
    fetch(credsUrl).then(r => r.json()).then(data => {
        const setVal = (id, key) => { const el = document.getElementById(id); if (el) el.value = data[key] || ''; };
        setVal('set-tg-api-id', 'telegram.api_id');
        setVal('set-tg-api-hash', 'telegram.api_hash');
        setVal('set-tg-phone', 'telegram.phone');
        setVal('set-tg-session', 'telegram.session_name');
        setVal('set-google-creds', 'google.creds_json');
        setVal('set-sheet-id', 'google.sheet_id');
        const tgS = document.getElementById('tg-status');
        if (data.tg_session_exists) { tgS.textContent = 'Session Active'; tgS.className = 'badge bg-green-600/20 text-green-400'; }
        else if (data['telegram.api_id_set'] || data['telegram.api_id']) { tgS.textContent = 'Configured'; tgS.className = 'badge bg-yellow-600/20 text-yellow-400'; }
        else { tgS.textContent = 'Not Set'; tgS.className = 'badge bg-red-600/20 text-red-400'; }
        const gS = document.getElementById('google-status');
        if ((data['google.sheet_id_set'] || data['google.sheet_id']) && (data['google.creds_json_set'] || data['google.creds_json'])) { gS.textContent = 'Configured'; gS.className = 'badge bg-green-600/20 text-green-400'; }
        else { gS.textContent = 'Not Set'; gS.className = 'badge bg-red-600/20 text-red-400'; }
        // Shared credentials: make inputs readonly for non-admin/editor
        if (isShared) {
            const sharedLabel = document.getElementById('shared-creds-label');
            if (sharedLabel) sharedLabel.classList.remove('hidden');
            ['set-tg-api-id','set-tg-api-hash','set-tg-phone','set-tg-session','set-google-creds','set-sheet-id'].forEach(id => {
                const el = document.getElementById(id);
                if (el) { el.disabled = true; el.classList.add('opacity-60', 'cursor-not-allowed'); }
            });
        }
    }).catch(() => {});
}
function saveUserCreds(section) {
    let data = {};
    if (section === 'telegram') data = { 'telegram.api_id': document.getElementById('set-tg-api-id').value, 'telegram.api_hash': document.getElementById('set-tg-api-hash').value, 'telegram.phone': document.getElementById('set-tg-phone').value, 'telegram.session_name': document.getElementById('set-tg-session').value };
    else if (section === 'google') data = { 'google.creds_json': document.getElementById('set-google-creds').value, 'google.sheet_id': document.getElementById('set-sheet-id').value };
    fetch('/api/user/credentials', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }).then(r => r.json()).then(result => { if (result.success) { addLog(section + ' credentials saved!', 'success'); loadAccountSettings(); } });
}
function testTelegram() {
    const el = document.getElementById('tg-test-result'); el.className = 'mt-3 text-sm text-yellow-400'; el.textContent = 'Testing connection...'; el.classList.remove('hidden');
    fetch('/api/user/test-telegram', { method: 'POST' }).then(r => r.json()).then(data => { el.className = 'mt-3 text-sm ' + (data.success ? 'text-green-400' : 'text-red-400'); el.textContent = data.success ? 'Connected as: ' + data.user : 'Error: ' + data.error; });
}
function testGoogle() {
    const el = document.getElementById('google-test-result'); el.className = 'mt-3 text-sm text-yellow-400'; el.textContent = 'Testing connection...'; el.classList.remove('hidden');
    fetch('/api/user/test-google', { method: 'POST' }).then(r => r.json()).then(data => { el.className = 'mt-3 text-sm ' + (data.success ? 'text-green-400' : 'text-red-400'); el.textContent = data.success ? 'Sheet: "' + data.sheet_title + '" (' + data.worksheets.length + ' worksheets)' : 'Error: ' + data.error; });
}
function resetSettings() {
    if (!confirm('Reset all settings to defaults?')) return;
    fetch('/api/reset', { method: 'POST', headers: { 'Content-Type': 'application/json' } }).then(r => r.json()).then(data => { if (data.success) { addLog('Settings reset!', 'success'); setTimeout(() => location.reload(), 1000); } });
}

// USER PROFILE & AUTH
function saveUserProfile() {
    const data = { name: document.getElementById('set-user-name').value.trim(), email: document.getElementById('set-user-email').value.trim() };
    fetch('/api/auth/profile', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }).then(r => r.json()).then(result => { if (result.success) addLog('Profile updated!', 'success'); else alert(result.error || 'Failed'); });
}
function changePassword() {
    const pw = document.getElementById('new-password').value;
    if (pw.length < 6) { alert('Password must be at least 6 characters'); return; }
    fetch('/api/auth/password', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ new_password: pw }) }).then(r => r.json()).then(result => { if (result.success) { addLog('Password updated!', 'success'); document.getElementById('new-password').value = ''; } else alert(result.error || 'Failed'); });
}
async function handleLogout() {
    await fetch('/api/auth/logout', { method: 'POST' });
    window.location.href = '/login';
}
function toggleUserMenu() {
    const dd = document.getElementById('user-dropdown');
    dd.classList.toggle('show');
    // Close on outside click
    const close = (e) => { if (!e.target.closest('.user-menu')) { dd.classList.remove('show'); document.removeEventListener('click', close); } };
    setTimeout(() => document.addEventListener('click', close), 10);
}

// PROFILES
function loadProfiles() {
    fetch('/api/profiles').then(r => r.json()).then(profiles => {
        const container = document.getElementById('profiles-list');
        if (!profiles.length) { container.innerHTML = '<div class="text-slate-500 text-sm text-center py-8">No profiles yet. Click "+ New Profile" to create one.</div>'; return; }
        container.innerHTML = profiles.map(p => '<div class="card !p-0 overflow-hidden" id="profile-card-' + p.id + '"><div class="p-4 cursor-pointer hover:bg-slate-800/50 transition-colors" onclick="toggleProfileExpand(' + p.id + ')"><div class="flex items-center justify-between"><div class="flex items-center gap-3"><span class="text-lg" id="profile-arrow-' + p.id + '">&#9654;</span><div><h3 class="font-semibold text-white">' + p.name + '</h3><span class="text-slate-400 text-sm">@' + p.channel_username + (p.channel_title ? ' - ' + p.channel_title : '') + '</span></div></div><div class="flex items-center gap-3 flex-wrap justify-end"><span class="text-sm font-medium text-blue-400">' + (p.total_messages || 0).toLocaleString() + ' msgs</span>' + (p.running_count > 0 ? '<span class="badge bg-blue-600/30 text-blue-300 pulse">Running</span>' : '') + '<span class="badge ' + (p.is_active ? 'bg-green-600/20 text-green-400' : 'bg-red-600/20 text-red-400') + '">' + (p.is_active ? 'Active' : 'Disabled') + '</span><span class="badge bg-slate-700 text-slate-300">' + (p.process_count || 0) + ' processes</span></div></div></div><div id="profile-detail-' + p.id + '" class="hidden border-t border-slate-700"></div></div>').join('');
    });
}

function toggleProfileExpand(pid) {
    const detail = document.getElementById('profile-detail-' + pid);
    const arrow = document.getElementById('profile-arrow-' + pid);
    if (detail.classList.contains('hidden')) { detail.classList.remove('hidden'); arrow.innerHTML = '&#9660;'; loadProfileDetail(pid); }
    else { detail.classList.add('hidden'); arrow.innerHTML = '&#9654;'; }
}

function loadProfileDetail(pid) {
    fetch('/api/profiles/' + pid).then(r => r.json()).then(profile => {
        const detail = document.getElementById('profile-detail-' + pid);
        if (!detail) return;
        const processes = profile.processes || [];
        const progressPromises = processes.map(proc => fetch('/api/processes/' + proc.id).then(r => r.json()).catch(() => proc));
        Promise.all(progressPromises).then(detailedProcs => {
            detail.innerHTML = '<div class="p-4 bg-slate-900/50"><div class="flex items-center justify-between mb-3"><div class="flex gap-2"><button onclick="showCreateProcessModal(' + pid + ')" class="btn-primary text-xs py-1 px-3">+ Add Process</button><button onclick="editProfile(' + pid + ')" class="btn-secondary text-xs py-1 px-3">Edit</button></div><button onclick="deleteProfile(' + pid + ')" class="text-red-400 hover:text-red-300 text-xs">Delete</button></div><div class="text-xs text-slate-400 mb-3">Export: ' + profile.export_format.toUpperCase() + ' to ' + profile.export_location + (profile.push_to_sheets ? ' + Google Sheets' : '') + '</div>' + (detailedProcs.length > 0 && detailedProcs[0].channel_stats ? renderChannelStats(detailedProcs[0].channel_stats) : '') + (processes.length === 0 ? '<div class="text-slate-500 text-sm text-center py-3">No processes. Add one to start scraping.</div>' : '') + '<div class="space-y-3">' + detailedProcs.map(proc => renderProcess(proc, pid)).join('') + '</div></div>';
        });
    });
}

function renderChannelStats(stats) {
    if (!stats || !stats.total_unique) return '';
    return '<div class="bg-[#0b1120] border border-slate-700 rounded-lg p-3 mb-3"><div class="flex items-center gap-2 mb-2"><span class="text-xs font-medium text-green-400">&#x2713; Duplicate Protection Active</span></div><div class="grid grid-cols-3 gap-3 text-center"><div><div class="text-lg font-bold text-white">' + (stats.total_unique || 0).toLocaleString() + '</div><div class="text-xs text-slate-500">Unique in DB</div></div><div><div class="text-sm font-medium text-white">' + (stats.earliest_date || 'N/A') + '</div><div class="text-xs text-slate-500">Earliest</div></div><div><div class="text-sm font-medium text-white">' + (stats.latest_date || 'N/A') + '</div><div class="text-xs text-slate-500">Latest</div></div></div></div>';
}

function renderProcess(proc, pid) {
    const statusColors = { idle: 'bg-slate-600/60 text-slate-300 border-slate-600', running: 'bg-blue-600/30 text-blue-300 border-blue-500/50', paused: 'bg-yellow-600/30 text-yellow-300 border-yellow-500/50', completed: 'bg-green-600/30 text-green-300 border-green-500/50', error: 'bg-red-600/30 text-red-300 border-red-500/50' };
    const statusClass = statusColors[proc.status] || statusColors.idle;
    const isRunning = proc.status === 'running';
    const typeLabels = { date_range: 'Date Range', rolling: 'Rolling', one_time: 'One-Time' };
    let pct = 0, daysTotal = 0, daysDone = 0, daysLeft = 0;
    if (proc.process_type === 'date_range' && proc.from_date && proc.to_date) {
        const from = new Date(proc.from_date), to = new Date(proc.to_date);
        daysTotal = Math.round((to - from) / 86400000);
        if (proc.current_position_date) { const cur = new Date(proc.current_position_date); daysDone = Math.round((to - cur) / 86400000); pct = daysTotal > 0 ? Math.min(100, Math.round((daysDone / daysTotal) * 100)) : 0; daysLeft = daysTotal - daysDone; }
    }
    let progressSection = '';
    if (proc.process_type === 'date_range' && proc.from_date && proc.to_date) {
        const posDate = proc.current_position_date || proc.from_date;
        const barColor = proc.status === 'completed' ? 'bg-green-500' : (isRunning ? 'bg-blue-500' : 'bg-blue-600');
        progressSection = '<div class="mt-3 bg-[#0b1120] rounded-lg p-3"><div class="flex items-center justify-between mb-2"><span class="text-xs text-slate-400">Progress</span><span class="text-xs font-bold ' + (pct >= 100 ? 'text-green-400' : 'text-blue-400') + '">' + pct + '%</span></div><div class="w-full bg-slate-700 rounded-full h-2.5 mb-2"><div class="' + barColor + ' h-2.5 rounded-full transition-all duration-500" style="width:' + pct + '%"></div></div><div class="grid grid-cols-3 gap-2 text-center text-xs"><div><div class="text-slate-500">Start</div><div class="text-slate-300 font-medium">' + proc.from_date + '</div></div><div><div class="text-slate-500">Currently at</div><div class="text-yellow-400 font-bold" id="proc-date-' + proc.id + '">' + posDate + '</div></div><div><div class="text-slate-500">End</div><div class="text-slate-300 font-medium">' + proc.to_date + '</div></div></div>' + (daysTotal > 0 ? '<div class="flex justify-between text-xs text-slate-500 mt-2 pt-2 border-t border-slate-700"><span>' + daysDone + ' of ' + daysTotal + ' days</span><span>' + (daysLeft > 0 ? daysLeft + ' remaining' : 'Complete!') + '</span></div>' : '') + '</div>';
    }
    const dailyRemaining = proc.daily_remaining;
    const dailyLimit = proc.daily_limit;
    const todayScraped = proc.today_scraped || 0;
    const isToday = (proc.today_date || '') === new Date().toISOString().split('T')[0];
    const statsGrid = '<div class="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-3"><div class="stat-box"><div class="text-sm font-bold text-white" id="proc-count-' + proc.id + '">' + (proc.messages_scraped || 0).toLocaleString() + '</div><div class="text-xs text-slate-500">Total</div></div><div class="stat-box"><div class="text-sm font-bold ' + (isToday && todayScraped > 0 ? 'text-blue-400' : 'text-slate-400') + '">' + (isToday ? todayScraped.toLocaleString() : '0') + '</div><div class="text-xs text-slate-500">Today</div></div><div class="stat-box"><div class="text-sm font-bold text-white">' + (dailyLimit ? dailyLimit.toLocaleString() : '&#8734;') + '</div><div class="text-xs text-slate-500">Limit</div></div><div class="stat-box"><div class="text-sm font-bold ' + (dailyRemaining !== null && dailyRemaining <= 0 ? 'text-red-400' : 'text-green-400') + '">' + (dailyRemaining !== null ? dailyRemaining.toLocaleString() : '&#8734;') + '</div><div class="text-xs text-slate-500">Remaining</div></div></div>';
    let lastInfo = '';
    if (proc.last_message_id || proc.last_message_date) lastInfo = '<div class="flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-500 mt-2 pt-2 border-t border-slate-700/50">' + (proc.last_message_id ? '<span>Msg ID: #' + proc.last_message_id + '</span>' : '') + (proc.last_message_date ? '<span>Date: ' + proc.last_message_date + '</span>' : '') + (proc.last_run_at ? '<span>Last Run: ' + proc.last_run_at + '</span>' : '') + (proc.schedule_enabled ? '<span class="text-blue-400">Scheduled</span>' : '') + '</div>';
    let errorDisp = proc.error_message ? '<div class="mt-2 bg-red-900/20 border border-red-800/30 rounded p-2 text-xs text-red-400">' + proc.error_message + '</div>' : '';
    return '<div class="bg-[#0b1120] rounded-lg p-4 border border-slate-700"><div class="flex items-center justify-between mb-2"><div class="flex items-center gap-2"><span class="font-semibold text-sm text-white">' + proc.name + '</span><span class="badge border ' + statusClass + '">' + proc.status + (isRunning ? '...' : '') + '</span><span class="text-xs text-slate-500">' + (typeLabels[proc.process_type] || proc.process_type) + '</span></div><div class="flex items-center gap-2">' + (isRunning ? '<button onclick="stopProcess(' + proc.id + ')" class="btn-danger text-xs py-1 px-3">Stop</button>' : '<button onclick="runProcess(' + proc.id + ')" class="btn-primary text-xs py-1 px-3">Run</button>') + '<button onclick="showProcessLog(' + proc.id + ',\'' + proc.name + '\')" class="text-blue-400 hover:text-blue-300 text-xs">Log</button><button onclick="deleteProcess(' + proc.id + ',' + pid + ')" class="text-red-400 hover:text-red-300 text-xs">Del</button></div></div>' + progressSection + statsGrid + lastInfo + errorDisp + '</div>';
}

// PROFILE MODAL
function showCreateProfileModal() {
    document.getElementById('profile-modal-title').textContent = 'New Profile';
    document.getElementById('profile-edit-id').value = '';
    document.getElementById('profile-name').value = '';
    document.getElementById('profile-channel').value = '';
    document.getElementById('profile-desc').value = '';
    document.getElementById('profile-format').value = 'xlsx';
    document.getElementById('profile-location').value = 'default';
    document.getElementById('profile-sheets').checked = false;
    document.getElementById('profile-from-date').value = '';
    document.getElementById('profile-to-date').value = new Date().toISOString().split('T')[0];
    document.getElementById('profile-daily-limit').value = '50000';
    document.getElementById('profile-delay').value = '1.0';
    // Load managed channels into select dropdown
    const channelSelect = document.getElementById('profile-channel-select');
    if (channelSelect) {
        fetch('/api/managed-channels').then(r => r.json()).then(channels => {
            channelSelect.innerHTML = '<option value="">-- Select a channel --</option>' + channels.map(c => '<option value="' + c.id + '" data-username="' + c.username + '">' + (c.title || c.username) + ' (@' + c.username + ')</option>').join('');
        }).catch(() => {});
    }
    document.getElementById('profile-modal').classList.remove('hidden');
}
function editProfile(pid) {
    fetch('/api/profiles/' + pid).then(r => r.json()).then(p => {
        document.getElementById('profile-modal-title').textContent = 'Edit Profile';
        document.getElementById('profile-edit-id').value = p.id;
        document.getElementById('profile-name').value = p.name;
        document.getElementById('profile-channel').value = p.channel_username;
        document.getElementById('profile-desc').value = p.description || '';
        document.getElementById('profile-format').value = p.export_format;
        document.getElementById('profile-location').value = p.export_location;
        document.getElementById('profile-sheets').checked = !!p.push_to_sheets;
        const mainProc = (p.processes || []).find(pr => pr.process_type === 'date_range') || (p.processes || [])[0];
        document.getElementById('profile-from-date').value = mainProc ? mainProc.from_date || '' : '';
        document.getElementById('profile-to-date').value = mainProc ? mainProc.to_date || '' : new Date().toISOString().split('T')[0];
        document.getElementById('profile-daily-limit').value = mainProc ? mainProc.daily_limit || '0' : '50000';
        document.getElementById('profile-delay').value = mainProc ? mainProc.batch_delay || '1.0' : '1.0';
        document.getElementById('profile-modal').classList.remove('hidden');
    });
}
function closeProfileModal() { document.getElementById('profile-modal').classList.add('hidden'); }
function saveProfile() {
    const editId = document.getElementById('profile-edit-id').value;
    const dailyLimit = parseInt(document.getElementById('profile-daily-limit').value) || 0;
    const channelSelect = document.getElementById('profile-channel-select');
    const channelId = channelSelect && channelSelect.value ? parseInt(channelSelect.value) : null;
    const channelUsername = channelSelect && channelSelect.selectedOptions[0] ? channelSelect.selectedOptions[0].dataset.username || '' : '';
    const data = { name: document.getElementById('profile-name').value.trim(), channel_username: document.getElementById('profile-channel').value.trim() || channelUsername, channel_id: channelId, description: document.getElementById('profile-desc').value.trim(), export_format: document.getElementById('profile-format').value, export_location: document.getElementById('profile-location').value, push_to_sheets: document.getElementById('profile-sheets').checked, from_date: document.getElementById('profile-from-date').value || null, to_date: document.getElementById('profile-to-date').value || null, daily_limit: dailyLimit > 0 ? dailyLimit : null, batch_delay: parseFloat(document.getElementById('profile-delay').value) || 1.0 };
    if (!data.name || !data.channel_username) { alert('Profile name and channel username are required.'); return; }
    const url = editId ? '/api/profiles/' + editId : '/api/profiles';
    const method = editId ? 'PUT' : 'POST';
    fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }).then(r => r.json()).then(result => { if (result.success || result.id) { closeProfileModal(); loadProfiles(); } else alert(result.error || 'Failed'); });
}
function deleteProfile(pid) { if (!confirm('Delete this profile and all processes?')) return; fetch('/api/profiles/' + pid, { method: 'DELETE' }).then(r => r.json()).then(() => loadProfiles()); }

// PROCESS MODAL
function showCreateProcessModal(pid) {
    document.getElementById('process-modal-title').textContent = 'New Process';
    document.getElementById('process-profile-id').value = pid;
    document.getElementById('process-edit-id').value = '';
    document.getElementById('process-name').value = '';
    document.getElementById('process-type').value = 'date_range';
    document.getElementById('process-from-date').value = '';
    document.getElementById('process-to-date').value = new Date().toISOString().split('T')[0];
    document.getElementById('process-hours').value = '24';
    document.getElementById('process-daily-limit').value = '50000';
    document.getElementById('process-delay').value = '1.0';
    document.getElementById('process-schedule').checked = false;
    document.getElementById('process-schedule-time').value = '08:00';
    document.getElementById('process-interval').value = '';
    toggleProcessFields(); toggleScheduleFields();
    document.getElementById('process-modal').classList.remove('hidden');
}
function closeProcessModal() { document.getElementById('process-modal').classList.add('hidden'); }
function toggleProcessFields() { const t = document.getElementById('process-type').value; document.getElementById('process-daterange-fields').classList.toggle('hidden', t !== 'date_range'); document.getElementById('process-rolling-fields').classList.toggle('hidden', t === 'date_range'); }
function toggleScheduleFields() { document.getElementById('process-schedule-fields').classList.toggle('hidden', !document.getElementById('process-schedule').checked); }
function saveProcess() {
    const pid = document.getElementById('process-profile-id').value;
    const dailyLimit = parseInt(document.getElementById('process-daily-limit').value) || 0;
    const data = { name: document.getElementById('process-name').value.trim(), process_type: document.getElementById('process-type').value, from_date: document.getElementById('process-from-date').value || null, to_date: document.getElementById('process-to-date').value || null, hours_back: parseInt(document.getElementById('process-hours').value) || 24, daily_limit: dailyLimit > 0 ? dailyLimit : null, batch_delay: parseFloat(document.getElementById('process-delay').value) || 1.0, schedule_enabled: document.getElementById('process-schedule').checked, schedule_time: document.getElementById('process-schedule-time').value || null, schedule_interval_hours: parseInt(document.getElementById('process-interval').value) || null };
    if (!data.name) { alert('Process name is required.'); return; }
    fetch('/api/profiles/' + pid + '/processes', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }).then(r => r.json()).then(result => { if (result.success || result.id) { closeProcessModal(); loadProfileDetail(parseInt(pid)); } else alert(result.error || 'Failed'); });
}
function runProcess(procId) {
    fetch('/api/processes/' + procId + '/run', { method: 'POST' }).then(r => r.json()).then(result => {
        if (result.error) alert(result.error);
        else setTimeout(() => { document.querySelectorAll('[id^="profile-detail-"]').forEach(el => { if (!el.classList.contains('hidden')) { const pid = el.id.replace('profile-detail-', ''); loadProfileDetail(parseInt(pid)); } }); }, 500);
    });
}
function stopProcess(procId) {
    fetch('/api/processes/' + procId + '/stop', { method: 'POST' }).then(r => r.json()).then(result => {
        if (result.error) alert(result.error);
        setTimeout(() => { document.querySelectorAll('[id^="profile-detail-"]').forEach(el => { if (!el.classList.contains('hidden')) { const pid = el.id.replace('profile-detail-', ''); loadProfileDetail(parseInt(pid)); } }); }, 1000);
    });
}
function deleteProcess(procId, profileId) { if (!confirm('Delete this process?')) return; fetch('/api/processes/' + procId, { method: 'DELETE' }).then(r => r.json()).then(() => loadProfileDetail(profileId)); }

// PROCESS LOG
function showProcessLog(procId, procName) { activeProcessLog = procId; document.getElementById('process-log-title').textContent = 'Log: ' + procName; document.getElementById('process-log-container').innerHTML = '<div class="text-slate-500">Listening...</div>'; document.getElementById('process-log-modal').classList.remove('hidden'); }
function closeProcessLogModal() { activeProcessLog = null; document.getElementById('process-log-modal').classList.add('hidden'); }
function addProcessLog(text, type) {
    const container = document.getElementById('process-log-container');
    if (container.querySelector('.text-slate-500')) container.innerHTML = '';
    const colors = { info: 'text-slate-300', success: 'text-green-400', error: 'text-red-400', warning: 'text-yellow-400' };
    const div = document.createElement('div');
    div.className = 'log-entry ' + (colors[type] || colors.info);
    div.textContent = '[' + new Date().toLocaleTimeString() + '] ' + text;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

// ═══════════════════════════════════════════
// MANAGER
// ═══════════════════════════════════════════
function loadManager() {
    fetch('/api/manager/overview').then(r => r.json()).then(data => {
        // Stat cards
        document.getElementById('mgr-profile-count').textContent = data.profile_count;
        document.getElementById('mgr-process-count').textContent = data.process_count;
        document.getElementById('mgr-cron-count').textContent = data.scheduled_count;
        document.getElementById('mgr-msg-count').textContent = data.total_messages.toLocaleString();

        // Status breakdown bar
        renderStatusBar(data.status_counts, data.process_count);

        // Global scheduler
        renderGlobalScheduler(data.global_scheduler, data.app_enabled);

        // Profiles table
        renderManagerProfiles(data.profiles);

        // Jobs table
        renderManagerJobs(data.processes);
    }).catch(err => { console.error('Manager load error:', err); });
}

function renderStatusBar(counts, total) {
    const bar = document.getElementById('mgr-status-bar');
    const legend = document.getElementById('mgr-status-legend');
    if (!total) {
        bar.innerHTML = '<div class="w-full bg-slate-600 h-full"></div>';
        legend.innerHTML = '<span class="text-slate-500">No processes</span>';
        return;
    }
    const colors = { running: '#3b82f6', idle: '#64748b', completed: '#22c55e', error: '#ef4444', paused: '#eab308' };
    const labels = { running: 'Running', idle: 'Idle', completed: 'Completed', error: 'Error', paused: 'Paused' };
    let barHtml = '', legendHtml = '';
    for (const [status, count] of Object.entries(counts)) {
        const pct = ((count / total) * 100).toFixed(1);
        const color = colors[status] || '#64748b';
        barHtml += '<div style="width:' + pct + '%;background:' + color + '" title="' + (labels[status] || status) + ': ' + count + '"></div>';
        legendHtml += '<span class="flex items-center gap-1"><span class="w-2.5 h-2.5 rounded-full inline-block" style="background:' + color + '"></span>' + (labels[status] || status) + ': ' + count + '</span>';
    }
    bar.innerHTML = barHtml;
    legend.innerHTML = legendHtml;
}

function renderGlobalScheduler(sched, appEnabled) {
    const el = document.getElementById('mgr-global-sched');
    const appBadge = appEnabled
        ? '<span class="badge bg-green-600/20 text-green-400">App Enabled</span>'
        : '<span class="badge bg-red-600/20 text-red-400">App Disabled</span>';
    const schedBadge = sched.enabled
        ? '<span class="badge bg-blue-600/20 text-blue-400">Scheduler ON</span>'
        : '<span class="badge bg-slate-700 text-slate-400">Scheduler OFF</span>';
    let schedInfo = '';
    if (sched.enabled) {
        if (sched.interval_hours) schedInfo = 'Every ' + sched.interval_hours + ' hours';
        else schedInfo = 'Daily at ' + (sched.time || '08:00');
        schedInfo += ' &middot; Next: <span class="text-white">' + (sched.next_run || 'N/A') + '</span>';
        schedInfo += ' &middot; Last: <span class="text-white">' + (sched.last_run || 'Never') + '</span>';
    }
    el.innerHTML = '<div class="flex flex-wrap items-center gap-2 mb-2">' + appBadge + ' ' + schedBadge + '</div>' + (schedInfo ? '<div class="text-xs text-slate-400 mt-1">' + schedInfo + '</div>' : '');
}

function renderManagerProfiles(profiles) {
    const tbody = document.getElementById('mgr-profiles-table');
    const empty = document.getElementById('mgr-profiles-empty');
    if (!profiles.length) { tbody.innerHTML = ''; empty.classList.remove('hidden'); return; }
    empty.classList.add('hidden');
    tbody.innerHTML = profiles.map(p => {
        const statusBadge = p.is_active
            ? '<span class="badge bg-green-600/20 text-green-400">Active</span>'
            : '<span class="badge bg-red-600/20 text-red-400">Disabled</span>';
        const runBadge = p.running_count > 0
            ? '<span class="badge bg-blue-600/30 text-blue-300 pulse">' + p.running_count + ' running</span>'
            : '<span class="text-slate-500">-</span>';
        return '<tr class="hover:bg-slate-800/50"><td class="py-2.5 px-3"><div class="font-medium text-white">' + p.name + '</div><div class="text-xs text-slate-500">@' + p.channel_username + '</div></td><td class="py-2.5 px-3 text-slate-300">' + p.process_count + '</td><td class="py-2.5 px-3 text-blue-400 font-medium">' + (p.total_messages || 0).toLocaleString() + '</td><td class="py-2.5 px-3">' + runBadge + '</td><td class="py-2.5 px-3">' + statusBadge + '</td><td class="py-2.5 px-3 text-slate-500 text-xs">' + (p.created_at || '-') + '</td></tr>';
    }).join('');
}

// ═══════════════════════════════════════
// USER THEME
// ═══════════════════════════════════════
function hexToRgb(hex) {
    const h = hex.replace('#', '');
    return `${parseInt(h.substring(0,2),16)}, ${parseInt(h.substring(2,4),16)}, ${parseInt(h.substring(4,6),16)}`;
}

function previewUserTheme() {
    const ids = ['primary','secondary','bg','bg-sidebar','bg-card','border'];
    ids.forEach(id => {
        const el = document.getElementById('ut-' + id);
        if (!el) return;
        const hex = el.value;
        const label = document.getElementById('ut-' + id + '-hex');
        if (label) label.textContent = hex;
        const cssVar = '--td-' + id;
        document.documentElement.style.setProperty(cssVar, hex);
        if (id === 'primary') document.documentElement.style.setProperty('--td-primary-rgb', hexToRgb(hex));
        if (id === 'secondary') document.documentElement.style.setProperty('--td-secondary-rgb', hexToRgb(hex));
    });
}

async function saveUserTheme() {
    const theme = {};
    const map = {'primary':'primary','secondary':'secondary','bg':'bg','bg-sidebar':'bg_sidebar','bg-card':'bg_card','border':'border'};
    for (const [id, key] of Object.entries(map)) {
        const el = document.getElementById('ut-' + id);
        if (el) theme[key] = el.value;
    }
    await fetch('/api/user/theme', { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify(theme) });
    alert('Theme saved! Reload to see full effect.');
}

async function resetUserTheme() {
    if (!confirm('Reset your theme to site defaults?')) return;
    await fetch('/api/user/theme/reset', { method: 'POST' });
    location.reload();
}

function renderManagerJobs(processes) {
    const tbody = document.getElementById('mgr-jobs-table');
    const empty = document.getElementById('mgr-jobs-empty');
    if (!processes.length) { tbody.innerHTML = ''; empty.classList.remove('hidden'); return; }
    empty.classList.add('hidden');
    const statusColors = { running: 'bg-blue-600/30 text-blue-300', idle: 'bg-slate-600/60 text-slate-300', completed: 'bg-green-600/30 text-green-300', error: 'bg-red-600/30 text-red-300', paused: 'bg-yellow-600/30 text-yellow-300' };
    const typeLabels = { date_range: 'Date Range', rolling: 'Rolling', one_time: 'One-Time' };
    tbody.innerHTML = processes.map(proc => {
        const sc = statusColors[proc.status] || statusColors.idle;
        let schedText = '-';
        if (proc.schedule_enabled) {
            if (proc.schedule_interval_hours) schedText = '<span class="badge bg-yellow-600/20 text-yellow-400">Every ' + proc.schedule_interval_hours + 'h</span>';
            else if (proc.schedule_time) schedText = '<span class="badge bg-yellow-600/20 text-yellow-400">Daily ' + proc.schedule_time + '</span>';
            else schedText = '<span class="badge bg-yellow-600/20 text-yellow-400">Enabled</span>';
        }
        const isRunning = proc.status === 'running';
        return '<tr class="hover:bg-slate-800/50' + (isRunning ? ' bg-blue-900/10' : '') + '"><td class="py-2.5 px-3"><div class="font-medium text-white">' + proc.name + '</div>' + (proc.error_message ? '<div class="text-xs text-red-400 truncate max-w-[200px]">' + proc.error_message + '</div>' : '') + '</td><td class="py-2.5 px-3 text-slate-400 text-xs">' + proc.profile_name + '<br>@' + proc.channel_username + '</td><td class="py-2.5 px-3 text-xs text-slate-400">' + (typeLabels[proc.process_type] || proc.process_type) + '</td><td class="py-2.5 px-3"><span class="badge ' + sc + '">' + proc.status + (isRunning ? '...' : '') + '</span></td><td class="py-2.5 px-3">' + schedText + '</td><td class="py-2.5 px-3 text-slate-300">' + (proc.messages_scraped || 0).toLocaleString() + '</td><td class="py-2.5 px-3 text-slate-500 text-xs">' + (proc.last_run_at || '-') + '</td><td class="py-2.5 px-3 text-slate-500 text-xs">' + (proc.next_run_at || '-') + '</td></tr>';
    }).join('');
}

// ═══════════════════════════════════════
// DASHBOARD STATS
// ═══════════════════════════════════════
function loadDashboardStats() {
    fetch('/api/dashboard/stats').then(r => r.json()).then(data => {
        const container = document.getElementById('dashboard-stats');
        if (!container) return;
        const lastProc = data.last_process || {};
        const lastProcStatus = lastProc.status || 'N/A';
        const lastProcColor = lastProcStatus === 'completed' ? 'text-green-400' : lastProcStatus === 'running' ? 'text-blue-400' : lastProcStatus === 'error' ? 'text-red-400' : 'text-slate-400';
        container.innerHTML = '<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">' +
            '<div class="card"><div class="text-xs text-slate-500 mb-1">Last Process</div><div class="font-semibold text-white text-sm">' + (lastProc.name || 'None') + '</div><div class="flex items-center gap-2 mt-1"><span class="badge ' + lastProcColor + ' text-xs">' + lastProcStatus + '</span>' + (lastProc.last_run_at ? '<span class="text-xs text-slate-500">' + lastProc.last_run_at + '</span>' : '') + '</div></div>' +
            '<div class="card"><div class="text-xs text-slate-500 mb-1">Total Messages</div><div class="text-2xl font-bold text-white">' + (data.total_messages || 0).toLocaleString() + '</div></div>' +
            '<div class="card"><div class="text-xs text-slate-500 mb-1">Success Rate</div><div class="text-2xl font-bold ' + ((data.success_rate || 0) >= 80 ? 'text-green-400' : (data.success_rate || 0) >= 50 ? 'text-yellow-400' : 'text-red-400') + '">' + (data.success_rate || 0).toFixed(1) + '%</div></div>' +
            '<div class="card"><div class="text-xs text-slate-500 mb-1">Today\'s Activity</div><div class="text-2xl font-bold text-blue-400">' + (data.today_activity || 0).toLocaleString() + '</div><div class="text-xs text-slate-500 mt-1">messages today</div></div>' +
            '</div>';
    }).catch(() => {});
}

// ═══════════════════════════════════════
// MANAGED CHANNELS
// ═══════════════════════════════════════
function loadManagedChannels() {
    fetch('/api/managed-channels').then(r => r.json()).then(channels => {
        const container = document.getElementById('managed-channels-list');
        if (!container) return;
        if (!channels.length) { container.innerHTML = '<div class="text-slate-500 text-sm text-center py-8">No managed channels yet. Click "+ Add Channel" to create one.</div>'; return; }
        container.innerHTML = channels.map(c => '<div class="card flex items-center justify-between"><div class="flex-1"><div class="flex items-center gap-2"><h3 class="font-semibold text-white">@' + c.username + '</h3>' + (c.title ? '<span class="text-slate-400 text-sm">- ' + c.title + '</span>' : '') + '</div>' + (c.description ? '<div class="text-xs text-slate-500 mt-1">' + c.description + '</div>' : '') + '<div class="flex items-center gap-4 mt-2"><span class="text-xs text-slate-400">' + (c.profile_count || 0) + ' profiles</span><span class="text-xs text-blue-400">' + (c.total_messages || 0).toLocaleString() + ' messages</span></div></div><button onclick="deleteManagedChannel(' + c.id + ')" class="text-red-400 hover:text-red-300 p-1 ml-3"><svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg></button></div>').join('');
    }).catch(() => {});
}
function createManagedChannel() {
    const username = document.getElementById('mc-username').value.trim();
    const title = document.getElementById('mc-title').value.trim();
    const description = document.getElementById('mc-desc').value.trim();
    if (!username) { alert('Channel username is required.'); return; }
    fetch('/api/managed-channels', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username, title, description }) }).then(r => r.json()).then(result => {
        if (result.success || result.id) {
            document.getElementById('mc-username').value = '';
            document.getElementById('mc-title').value = '';
            document.getElementById('mc-desc').value = '';
            loadManagedChannels();
            addLog('Channel @' + username + ' added', 'success');
        } else alert(result.error || 'Failed');
    });
}
function deleteManagedChannel(id) {
    if (!confirm('Delete this managed channel?')) return;
    fetch('/api/managed-channels/' + id, { method: 'DELETE' }).then(r => r.json()).then(() => { loadManagedChannels(); addLog('Managed channel deleted', 'success'); });
}

// ═══════════════════════════════════════
// THEME PRESETS
// ═══════════════════════════════════════
function loadThemePresets() {
    fetch('/api/themes/presets').then(r => r.json()).then(presets => {
        const container = document.getElementById('theme-presets-container');
        if (!container) return;
        if (!presets.length) { container.innerHTML = '<div class="text-slate-500 text-sm">No theme presets available</div>'; return; }
        container.innerHTML = presets.map(p => {
            const swatches = (p.colors || []).map(c => '<span class="w-5 h-5 rounded-full inline-block border border-slate-600" style="background:' + c + '"></span>').join('');
            return '<div class="card flex items-center justify-between"><div><div class="font-medium text-white">' + p.display_name + '</div><div class="flex items-center gap-1 mt-2">' + swatches + '</div></div><button onclick="applyThemePreset(\'' + p.name + '\')" class="btn-primary text-xs py-1 px-3">Apply</button></div>';
        }).join('');
    }).catch(() => {});
}
function applyThemePreset(presetName) {
    fetch('/api/user/theme/preset', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ preset: presetName }) }).then(r => r.json()).then(result => {
        if (result.success) { addLog('Theme preset "' + presetName + '" applied!', 'success'); setTimeout(() => location.reload(), 500); }
        else alert(result.error || 'Failed to apply preset');
    });
}

// ═══════════════════════════════════════
// SECURITY QUESTION
// ═══════════════════════════════════════
function loadSecurityQuestions() {
    fetch('/api/auth/security-questions').then(r => r.json()).then(data => {
        const container = document.getElementById('security-question-section');
        if (!container) return;
        const questions = data.questions || [];
        const currentQ = data.current_question || '';
        const selectHtml = questions.length > 0
            ? '<select id="sec-question" class="input-field w-full mb-3">' + questions.map(q => '<option value="' + q + '"' + (q === currentQ ? ' selected' : '') + '>' + q + '</option>').join('') + '</select>'
            : '<input type="text" id="sec-question" class="input-field w-full mb-3" placeholder="Enter your security question" value="' + currentQ + '">';
        container.innerHTML = '<h3 class="text-sm font-semibold text-slate-300 mb-3">Security Question</h3>' + selectHtml + '<input type="text" id="sec-answer" class="input-field w-full mb-3" placeholder="Your answer">' + '<button onclick="saveSecurityQuestion()" class="btn-primary text-sm py-2 px-4">Save Security Question</button>';
    }).catch(() => {});
}
function saveSecurityQuestion() {
    const question = document.getElementById('sec-question').value.trim();
    const answer = document.getElementById('sec-answer').value.trim();
    if (!question || !answer) { alert('Both question and answer are required.'); return; }
    fetch('/api/auth/security-question', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ question, answer }) }).then(r => r.json()).then(result => {
        if (result.success) { addLog('Security question updated!', 'success'); document.getElementById('sec-answer').value = ''; }
        else alert(result.error || 'Failed');
    });
}
