class FileManager {
    constructor() {
        this.setupEventListeners();
        this.historyDialog = document.getElementById('file-history-dialog');
        this.historyList = document.getElementById('history-list');
        this.historyTitle = document.getElementById('history-title');
        this.historyResolve = null;

        document.getElementById('close-history-dialog').addEventListener('click', () => this.closeHistory(null));
        document.getElementById('cancel-history-dialog').addEventListener('click', () => this.closeHistory(null));
        document.getElementById('browse-from-history').addEventListener('click', () => this.closeHistory('__browse__'));
    }

    setupEventListeners() {
        document.getElementById('btn-save').addEventListener('click', () => this.saveStream());
        document.getElementById('btn-save-as').addEventListener('click', () => this.saveStreamAs());
        document.getElementById('btn-load').addEventListener('click', () => this.loadStream());
        document.getElementById('btn-import-file').addEventListener('click', () => this.importDocument());
    }

    async pickPath(pathType, title) {
        this.historyTitle.textContent = title;
        this.historyList.innerHTML = '<div class="history-loading">Loading...</div>';
        this.historyDialog.style.display = 'flex';

        const result = await api.getFileHistory(pathType, 10);
        
        if (result.success && result.data.length > 0) {
            this.historyList.innerHTML = result.data.map(e => `
                <div class="history-item" data-path="${e.path}">
                    <div class="history-item-info">
                        <div class="history-item-label">${e.label}</div>
                        <div class="history-item-path">${e.path}</div>
                    </div>
                    <div class="history-item-meta">
                        <span class="history-item-count">${e.use_count}x</span>
                        <button class="btn-icon btn-remove-history" data-id="${e.id}">×</button>
                    </div>
                </div>
            `).join('');

            this.historyList.querySelectorAll('.history-item').forEach(item => {
                item.addEventListener('click', (ev) => {
                    if (!ev.target.closest('.btn-remove-history')) {
                        this.closeHistory(item.dataset.path);
                    }
                });
            });

            this.historyList.querySelectorAll('.btn-remove-history').forEach(btn => {
                btn.addEventListener('click', async (ev) => {
                    ev.stopPropagation();
                    await api.deleteFileHistory(btn.dataset.id);
                    btn.closest('.history-item').remove();
                    if (!this.historyList.children.length) {
                        this.historyList.innerHTML = '<div class="history-empty">No history</div>';
                    }
                });
            });
        } else {
            this.historyList.innerHTML = '<div class="history-empty">No history</div>';
        }

        return new Promise(resolve => { this.historyResolve = resolve; });
    }

    closeHistory(value) {
        this.historyDialog.style.display = 'none';
        if (this.historyResolve) {
            this.historyResolve(value);
            this.historyResolve = null;
        }
    }

    async pickDirectory(pathType, title) {
        const choice = await this.pickPath(pathType, title);
        if (choice === null) return null;
        if (choice === '__browse__') {
            const r = await api.selectDirectory();
            return r.success ? r.data.path : null;
        }
        return choice;
    }

    async pickFile(pathType, title) {
        const choice = await this.pickPath(pathType, title);
        if (choice === null) return null;
        if (choice === '__browse__') {
            const r = await api.selectFile([["JSON Config", "*.json"], ["All Files", "*.*"]]);
            return r.success ? r.data.path : null;
        }
        return choice;
    }

    async saveStream() {
        const tab = state.getActiveTab();
        if (!tab) return;

        const content = state.getEditorState(tab.id).content;
        state.setLoading(true);
        try {
            const r = await api.updateStream(tab.systemId, tab.streamName, content);
            if (r.success) state.markTabModified(tab.id, false);
            else state.setError(r.error || 'Save failed');
        } catch (e) {
            state.setError('Save failed: ' + e.message);
        } finally {
            state.setLoading(false);
        }
    }

    _showSaveStreamsDialog(streams) {
        const dialog = document.getElementById('save-streams-dialog');
        const listEl = document.getElementById('save-streams-list');
        const selectAllCheckbox = document.getElementById('save-streams-select-all');
        const countEl = document.getElementById('save-streams-count');
        const confirmBtn = document.getElementById('btn-confirm-save-streams');
        const dirInput = document.getElementById('save-streams-dir-input');

        // Populate stream list
        listEl.innerHTML = streams.map(s => `
            <label class="save-streams-item">
                <input type="checkbox" class="save-stream-checkbox" value="${s.name}" checked>
                <div class="save-streams-item-info">
                    <div class="save-streams-item-name">${s.name}</div>
                    <div class="save-streams-item-meta">
                        <span>v${s.version || '1'}</span>
                        <span>${tabManager.formatLength(s.content_length)}</span>
                    </div>
                </div>
            </label>
        `).join('');

        // Update count & button state
        const updateCount = () => {
            const checked = listEl.querySelectorAll('.save-stream-checkbox:checked');
            countEl.textContent = `${checked.length} selected`;
            confirmBtn.disabled = checked.length === 0 || !dirInput.value;
        };

        // Select All toggle
        selectAllCheckbox.addEventListener('change', () => {
            listEl.querySelectorAll('.save-stream-checkbox').forEach(cb => {
                cb.checked = selectAllCheckbox.checked;
            });
            updateCount();
        });

        // Individual checkbox toggle
        listEl.querySelectorAll('.save-stream-checkbox').forEach(cb => {
            cb.addEventListener('change', () => {
                const all = listEl.querySelectorAll('.save-stream-checkbox');
                const checked = listEl.querySelectorAll('.save-stream-checkbox:checked');
                selectAllCheckbox.checked = checked.length === all.length;
                selectAllCheckbox.indeterminate = checked.length > 0 && checked.length < all.length;
                updateCount();
            });
        });

        // Directory input enables/disables confirm
        dirInput.addEventListener('input', updateCount);

        updateCount();

        // Browse button
        document.getElementById('btn-browse-save-dir').onclick = async () => {
            const r = await api.selectDirectory();
            if (r.success && r.data.path) {
                dirInput.value = r.data.path;
                updateCount();
            }
        };

        // Cancel
        document.getElementById('btn-cancel-save-streams').onclick = () => dialog.close();
        document.getElementById('close-save-streams-dialog').onclick = () => dialog.close();

        // Confirm
        confirmBtn.onclick = async () => {
            const selectedNames = Array.from(
                listEl.querySelectorAll('.save-stream-checkbox:checked')
            ).map(cb => cb.value);

            if (selectedNames.length === 0) return;

            const outputDir = dirInput.value.trim();
            if (!outputDir) return;

            const overwrite = document.getElementById('save-streams-overwrite').checked;

            dialog.close();
            state.setLoading(true);
            try {
                // Flush active tab content to text-stream backend first,
                // so stream_obj.read() in the backend returns latest data.
                const activeTab = state.getActiveTab();
                if (activeTab && activeTab.isModified) {
                    await fileManager.saveStream();
                }

                const r = await api.saveFile({
                    system_id: state.activeSystemId,
                    stream_names: selectedNames,
                    output_dir: outputDir,
                    overwrite: overwrite
                });

                if (r.success) {
                    await api.addFileHistory({
                        path: outputDir,
                        path_type: 'save_streams_dir',
                        label: outputDir
                    });
                    showToast(`Saved ${r.data.saved_count} stream(s) to ${outputDir}`, 'success');
                } else {
                    state.setError(r.error || 'Save failed');
                }
            } catch (e) {
                state.setError('Save failed: ' + e.message);
            } finally {
                state.setLoading(false);
            }
        };

        dialog.showModal();
    }

    async saveStreamAs() {
        if (!state.activeSystemId) return state.setError('No active system');

        const streams = state.getStreams(state.activeSystemId) || [];

        if (streams.length === 0) {
            showToast('No streams to save', 'warning');
            return;
        }

        this._showSaveStreamsDialog(streams);
    }

    async loadStream() {
        if (!state.activeSystemId) return state.setError('No active system');

        const path = await this.pickFile('load_config', 'Recent Config Files');
        if (!path) return;

        state.setLoading(true);
        try {
            const r = await api.loadFile({ system_id: state.activeSystemId, config_path: path });
            if (r.success) {
                await api.addFileHistory({ path, path_type: 'load_config', label: path });
                await tabManager.loadStreams();
                alert(`Loaded ${r.data.streams.length} stream(s)`);
            } else {
                state.setError(r.error || 'Load failed');
            }
        } catch (e) {
            state.setError('Load failed: ' + e.message);
        } finally {
            state.setLoading(false);
        }
    }

    async importDocument() {
        if (!state.activeSystemId) return state.setError('No active system');

        const r = await api.selectFile([
            ["Text & Markdown", "*.txt;*.md;*.markdown;*.py;*.js;*.ts;*.json;*.xml;*.yaml;*.yml;*.html;*.css;*.csv"],
            ["All Files", "*.*"]
        ]);
        if (!r.success || !r.data.path) return;

        const filePath = r.data.path;
        state.setLoading(true);
        try {
            const result = await api.loadDocument({
                system_id: state.activeSystemId,
                file_path: filePath
            });
            if (result.success) {
                await api.addFileHistory({ path: filePath, path_type: 'load_document', label: filePath });
                await tabManager.loadStreams();
                alert(`Imported as stream: ${result.data.stream_name}`);
            } else {
                state.setError(result.error || 'Import failed');
            }
        } catch (e) {
            state.setError('Import failed: ' + e.message);
        } finally {
            state.setLoading(false);
        }
    }


}

const fileManager = new FileManager();
