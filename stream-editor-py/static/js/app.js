class App {
    constructor() {
        this.setupGlobalListeners();
        this.setupSidebarToggle();
        this.setupErrorHandling();
        this.setupToolInteraction();
    }

    setupToolInteraction() {
        if (typeof toolInteraction === 'undefined') return;

        toolInteraction.onSend = async (message) => {
            const match = message.match(/^@(\w+)\|(.+)$/s);
            if (match) {
                const [, toolName, jsonStr] = match;
                try {
                    const params = JSON.parse(jsonStr);
                    toolInteraction.addOutput(`> @${toolName} ${jsonStr}`, 'system');
                    const result = await api.executeTool(toolName, params);
                    if (result.success) {
                        toolInteraction.addOutput(JSON.stringify(result.data, null, 2), 'success');
                    } else {
                        toolInteraction.addOutput('Error: ' + (result.error || 'Unknown'), 'error');
                    }
                } catch (e) {
                    toolInteraction.addOutput('Error: ' + e.message, 'error');
                }
                return;
            }

            const proc = toolInteraction.selectedProcess;
            if (proc) {
                try {
                    await api.sendProcessInput(proc.process_name, message);
                } catch (e) {
                    toolInteraction.addOutput('Send failed: ' + e.message, 'error');
                }
                return;
            }

            toolInteraction.addOutput('No target. Use @toolName|{params} or select a process.', 'warning');
        };
    }

    setupGlobalListeners() {
        state.on('errorChanged', (error) => {
            if (error) {
                this.showError(error);
            }
        });

        state.on('loadingChanged', (loading) => {
            document.getElementById('loading-status').style.display = loading ? 'flex' : 'none';
        });

        state.on('activeSystemChanged', () => {
            const system = state.getActiveSystem();
            const statusEl = document.getElementById('connection-status');
            if (system) {
                statusEl.innerHTML = `<span class="status-icon" style="color: ${system.status === 'connected' ? 'var(--color-success)' : 'var(--text-muted)'}">●</span><span class="status-label">${system.status}</span>`;
            }
        });
    }

    setupSidebarToggle() {
        const toggleBtn = document.getElementById('sidebar-toggle');
        const sidebar = document.getElementById('sidebar');

        toggleBtn.addEventListener('click', () => {
            const isOpen = sidebar.style.display !== 'none';
            sidebar.style.display = isOpen ? 'none' : 'flex';
            toggleBtn.textContent = isOpen ? '▶' : '◀';
        });
    }

    setupErrorHandling() {
        document.getElementById('close-error-toast').addEventListener('click', () => {
            state.clearError();
        });

        setTimeout(() => {
            if (state.error) {
                state.clearError();
            }
        }, 5000);
    }

    showError(message) {
        const toast = document.getElementById('error-toast');
        const messageEl = document.getElementById('error-message');
        messageEl.textContent = message;
        toast.style.display = 'block';

        setTimeout(() => {
            toast.style.display = 'none';
            state.clearError();
        }, 5000);
    }
}

const app = new App();
