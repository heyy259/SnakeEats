class SystemManager {
    constructor() {
        this.container = document.getElementById('system-list');
        this.setupEventListeners();
        this.loadSystems();
    }

    setupEventListeners() {
        // Restart button
        const restartBtn = document.getElementById('btn-restart-system');
        if (restartBtn) {
            restartBtn.addEventListener('click', () => this.restartSystem());
        }

        state.on('systemsChanged', () => this.render());
        state.on('activeSystemChanged', () => this.render());
        // system restart → refresh tool list
        state.on('systemsChanged', () => {
            if (window.toolPanel) toolPanel.loadTools();
        });
    }

    async loadSystems() {
        state.setLoading(true);
        try {
            const result = await api.getSystems();
            if (result.success) {
                state.setSystems(result.data);
                if (result.data.length > 0 && !state.activeSystemId) {
                    state.setActiveSystem(result.data[0].id);
                }
            } else {
                state.setError(result.error || 'Failed to load systems');
            }
        } catch (e) {
            state.setError('System load failed: ' + e.message);
        } finally {
            state.setLoading(false);
        }
    }

    render() {
        const systems = state.systems;
        const system = systems.length > 0 ? systems[0] : null;

        if (!system) {
            this.container.innerHTML = `
                <div class="system-status-bar offline">
                    <span class="status-dot offline">○</span>
                    <span class="system-status-text">No system running</span>
                </div>
            `;
            document.getElementById('system-info').innerHTML = 
                '<span class="status-label">No system selected</span>';
            return;
        }

        const statusClass = system.status === 'connected' ? 'online' : 'offline';
        const statusIcon = system.status === 'connected' ? '●' : '○';

        this.container.innerHTML = `
            <div class="system-status-bar ${statusClass}">
                <div class="system-status-info">
                    <span class="status-dot ${statusClass}">${statusIcon}</span>
                    <span class="system-name">${system.name}</span>
                    <span class="system-port">:${system.port}</span>
                </div>
                <div class="system-status-meta">
                    <span class="stream-count">${system.streamCount} stream${system.streamCount !== 1 ? 's' : ''}</span>
                </div>
            </div>
        `;

        // Update header system info
        document.getElementById('system-info').innerHTML = 
            `<span class="status-label">${system.name} (${system.port})</span>`;
    }

    async restartSystem() {
        // Show confirmation dialog — returns 'save', 'discard', or null (cancel)
        const streamCount = state.systems.length > 0 ? state.systems[0].streamCount : 0;
        const action = await showRestartDialog(streamCount);
        if (!action) return;

        // ── Pre-restart cleanup: disconnect subscriptions, close all tabs ──
        // tabManager is const (not window property), access it directly.
        if (typeof tabManager !== 'undefined') {
            // Disconnect and clear stale subscription callbacks to prevent
            // the reconnect loop when old streams no longer exist after restart.
            tabManager.subscriptionManager.disconnect();
            tabManager._pollingPaused = true;

            // Close all open tabs silently (don't prompt about unsaved changes —
            // the user already chose Save/Discard in the dialog).
            const openTabs = [...state.tabs];
            for (const tab of openTabs) {
                state.removeTab(tab.id);
            }
        }

        state.setLoading(true);
        try {
            const response = await fetch('/api/system/restart', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: 'Default',
                    auto_save: action === 'save'
                })
            });
            const result = await response.json();

            if (result.success) {
                state.setSystems([result.data]);
                state.setActiveSystem(result.data.id);

                // Refresh streams (subscription manager reconnects via
                // activeSystemChanged event with empty callbacks — clean state)
                if (typeof tabManager !== 'undefined') {
                    tabManager._pollingPaused = false;
                    await tabManager.loadStreams();
                    // Ensure renderStreamList is called to update sidebar
                    tabManager.renderStreamList();
                }

                // Feedback toast
                if (action === 'save') {
                    const msg = result.auto_loaded > 0
                        ? `Restarted with ${result.auto_loaded} stream(s) restored`
                        : `Restarted — save succeeded but restore failed (${result.auto_saved || 0} streams were saved)`;
                    window.showToast(msg, result.auto_loaded > 0 ? 'success' : 'warning');
                } else {
                    window.showToast('System restarted — fresh instance', 'info');
                }
            } else {
                state.setError(result.error || 'Failed to restart system');
                window.showToast('Restart failed: ' + (result.error || 'unknown error'), 'error');
            }
        } catch (e) {
            state.setError('Restart failed: ' + e.message);
            window.showToast('Restart failed: ' + e.message, 'error');
        }
        state.setLoading(false);
    }

}

/**
 * Show restart confirmation dialog.
 * @param {number} streamCount - current stream count for display
 * @returns {Promise<'save'|'discard'|null>} user's choice, or null if cancelled
 */
function showRestartDialog(streamCount) {
    return new Promise((resolve) => {
        const overlay = document.getElementById('restart-dialog');
        const countEl = document.getElementById('restart-dialog-stream-count');
        const options = overlay.querySelectorAll('.modal-option');
        const confirmBtn = document.getElementById('btn-restart-confirm');

        if (!overlay) {
            // Fallback to simple confirm if dialog element missing
            resolve(confirm('Restart system? Current streams will be lost.') ? 'discard' : null);
            return;
        }

        // Display stream count
        if (countEl) {
            if (streamCount > 0) {
                countEl.textContent = `${streamCount} stream(s) currently open`;
            } else {
                countEl.textContent = 'No streams to save';
            }
        }

        // Disable confirm if no streams (only discard makes sense)
        if (confirmBtn) {
            confirmBtn.disabled = false;
        }

        overlay.style.display = 'flex';

        // Radio option selection
        options.forEach(opt => {
            opt.onclick = () => {
                options.forEach(o => o.classList.remove('selected'));
                opt.classList.add('selected');
                opt.querySelector('input[type="radio"]').checked = true;
            };
        });

        // Cancel button
        document.getElementById('btn-restart-cancel').onclick = () => {
            overlay.style.display = 'none';
            resolve(null);
        };

        // Confirm button
        confirmBtn.onclick = () => {
            overlay.style.display = 'none';
            const selected = overlay.querySelector('input[name="restart-action"]:checked');
            resolve(selected ? selected.value : null);
        };

        // Click overlay background to cancel
        overlay.onclick = (e) => {
            if (e.target === overlay) {
                overlay.style.display = 'none';
                resolve(null);
            }
        };

        // Keyboard: Escape to cancel
        const onKey = (e) => {
            if (e.key === 'Escape') {
                overlay.style.display = 'none';
                resolve(null);
                document.removeEventListener('keydown', onKey);
            }
        };
        document.addEventListener('keydown', onKey);
    });
}

const systemManager = new SystemManager();
