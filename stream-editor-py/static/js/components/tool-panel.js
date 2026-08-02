class ToolPanel {
    constructor() {
        this.toolList = document.getElementById('tool-list');
        this.toolCount = document.getElementById('tool-count');
        this.searchInput = document.getElementById('tool-search-input');
        this.clearSearchBtn = document.getElementById('clear-tool-search');
        this.detailDialog = document.getElementById('tool-detail-dialog');
        this.registerDialog = document.getElementById('tool-register-dialog');
        this.selectedTool = null;

        // New elements for tag filter UI
        this.tagKeyPills = document.getElementById('tag-key-pills');
        this.activeFilters = document.getElementById('active-filters');
        this.tagValueDropdown = document.getElementById('tag-value-dropdown');
        this.openDropdownKey = null;
        this._clickTimer = null;

        this.setupEventListeners();
        this.loadTools();
    }

    setupEventListeners() {
        this.searchInput.addEventListener('input', () => this.onSearchChange());
        this.clearSearchBtn.addEventListener('click', () => this.clearSearch());

        document.getElementById('close-tool-detail').addEventListener('click', () => this.hideDetailDialog());
        document.getElementById('cancel-tool-detail').addEventListener('click', () => this.hideDetailDialog());
        document.getElementById('save-tool-detail').addEventListener('click', () => this.saveTool());
        document.getElementById('edit-add-tag-row').addEventListener('click', () => this.addEditTagRow());

        document.getElementById('btn-register-tool').addEventListener('click', () => this.showRegisterDialog());
        document.getElementById('btn-save-tools-config').addEventListener('click', () => this.saveToolsConfig());
        document.getElementById('btn-load-tools-config').addEventListener('click', () => this.loadToolsConfig());
        document.getElementById('close-tool-register').addEventListener('click', () => this.hideRegisterDialog());
        document.getElementById('cancel-tool-register').addEventListener('click', () => this.hideRegisterDialog());
        document.getElementById('submit-tool-register').addEventListener('click', () => this.registerTool());
        document.getElementById('add-tag-row').addEventListener('click', () => this.addTagRow());

        document.querySelectorAll('.browse-btn').forEach(btn => {
            btn.addEventListener('click', (e) => this.openFilePicker(e.currentTarget.dataset.field));
        });

        state.on('toolsChanged', () => this.render());

        // Listen for tool registry changes pushed via WebSocket
        state.on('toolRegistryChanged', (change) => this._onToolRegistryChanged(change));

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.tag-key-pill') && !e.target.closest('.tag-value-dropdown')) {
                this.closeDropdown();
            }
        });
    }

    async loadTools(retries = 3) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const result = await api.getTools();
                if (result.success) {
                    state.setTools(result.data);
                    return;
                }
                if (attempt < retries) {
                    console.warn(`[ToolPanel] loadTools attempt ${attempt}/${retries} returned failure, retrying...`);
                    await new Promise(r => setTimeout(r, 1000));
                }
            } catch (e) {
                if (attempt < retries) {
                    console.warn(`[ToolPanel] loadTools attempt ${attempt}/${retries} failed: ${e.message}, retrying...`);
                    await new Promise(r => setTimeout(r, 1000));
                } else {
                    console.warn('[ToolPanel] loadTools all retries exhausted:', e.message);
                }
            }
        }
    }

    /**
     * Tool registry change notification handler (pushed via WebSocket)
     * @param {Object} change - { type: "registered"|"unregistered", tool_name, tool_definition }
     */
    _onToolRegistryChanged(change) {
        console.log('[ToolPanel] Registry changed:', change.type, change.tool_name);

        // Debounce: avoid duplicate refreshes with loadTools() after HTTP responses
        if (this._reloadTimer) clearTimeout(this._reloadTimer);
        this._reloadTimer = setTimeout(() => {
            this.loadTools();
            this._reloadTimer = null;
        }, 200);
    }

    onSearchChange() {
        state.searchQuery = this.searchInput.value;
        this.clearSearchBtn.style.display = state.searchQuery ? 'block' : 'none';
        this.render();
    }

    clearSearch() {
        this.searchInput.value = '';
        state.searchQuery = '';
        this.clearSearchBtn.style.display = 'none';
        this.render();
    }

    // ═══════════════════════════════════════════════════════════════
    //  Main Render
    // ═══════════════════════════════════════════════════════════════

    render() {
        const filteredTools = state.getFilteredTools();
        const suggestions = state.getTagSuggestions();

        this.toolCount.textContent = `${filteredTools.length} / ${state.tools.length}`;

        this.renderTagKeyPills(suggestions);
        this.renderActiveFilters();
        this.renderToolRows(filteredTools);
    }

    // ═══════════════════════════════════════════════════════════════
    //  Tag Key Pills (clickable keys below search bar)
    // ═══════════════════════════════════════════════════════════════

    renderTagKeyPills(suggestions) {
        const keys = Array.from(suggestions.keys()).sort();
        if (keys.length === 0) {
            this.tagKeyPills.innerHTML = '';
            return;
        }

        const html = keys.map(key => {
            const hasActiveFilter = state.selectedTags.some(t => t.startsWith(key + ': '));
            return `<button class="tag-key-pill${hasActiveFilter ? ' active' : ''}" data-key="${this.escapeHtml(key)}">${this.escapeHtml(key)}</button>`;
        }).join('');

        this.tagKeyPills.innerHTML = html;

        this.tagKeyPills.querySelectorAll('.tag-key-pill').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleTagDropdown(btn.dataset.key, btn);
            });
        });
    }

    toggleTagDropdown(key, anchorEl) {
        if (this.openDropdownKey === key) {
            this.closeDropdown();
            return;
        }

        const values = Array.from(state.getTagSuggestions().get(key) || []).sort();
        const selectedValues = state.selectedTags
            .filter(t => t.startsWith(key + ': '))
            .map(t => t.substring(key.length + 2));

        const rect = anchorEl.getBoundingClientRect();
        const dropdown = this.tagValueDropdown;

        dropdown.style.display = 'block';
        dropdown.style.position = 'fixed';
        dropdown.style.left = rect.left + 'px';
        dropdown.style.minWidth = Math.max(rect.width, 80) + 'px';

        // Position below or above based on available space
        const dropdownHeight = values.length * 26 + 8;
        const spaceBelow = window.innerHeight - rect.bottom;
        if (spaceBelow < dropdownHeight && rect.top > spaceBelow) {
            dropdown.style.top = 'auto';
            dropdown.style.bottom = (window.innerHeight - rect.top + 4) + 'px';
        } else {
            dropdown.style.top = (rect.bottom + 4) + 'px';
            dropdown.style.bottom = 'auto';
        }

        dropdown.innerHTML = values.map(v => {
            const isSelected = selectedValues.includes(v);
            return `<div class="tag-value-option${isSelected ? ' selected' : ''}" data-key="${this.escapeHtml(key)}" data-value="${this.escapeHtml(v)}">${this.escapeHtml(v)}</div>`;
        }).join('');

        dropdown.querySelectorAll('.tag-value-option').forEach(opt => {
            opt.addEventListener('click', (e) => {
                e.stopPropagation();
                this.selectTagValue(opt.dataset.key, opt.dataset.value);
            });
        });

        this.openDropdownKey = key;
    }

    closeDropdown() {
        if (this.tagValueDropdown) {
            this.tagValueDropdown.style.display = 'none';
        }
        this.openDropdownKey = null;
    }

    selectTagValue(key, value) {
        const tagString = `${key}: ${value}`;
        const index = state.selectedTags.indexOf(tagString);
        if (index === -1) {
            state.selectedTags.push(tagString);
        } else {
            state.selectedTags.splice(index, 1);
        }
        this.closeDropdown();
        this.render();
    }

    // ═══════════════════════════════════════════════════════════════
    //  Active Filters (selected key:value chips)
    // ═══════════════════════════════════════════════════════════════

    renderActiveFilters() {
        if (state.selectedTags.length === 0) {
            this.activeFilters.innerHTML = '';
            return;
        }

        const chipsHtml = state.selectedTags.map(tag =>
            `<span class="filter-chip">${this.escapeHtml(tag)} <button data-tag="${this.escapeHtml(tag)}" title="Remove filter">×</button></span>`
        ).join('');

        const clearHtml = `<button class="clear-all-filters">Clear all</button>`;
        this.activeFilters.innerHTML = chipsHtml + clearHtml;

        this.activeFilters.querySelectorAll('.filter-chip button').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const tag = btn.dataset.tag;
                const index = state.selectedTags.indexOf(tag);
                if (index !== -1) state.selectedTags.splice(index, 1);
                this.render();
            });
        });

        const clearAllBtn = this.activeFilters.querySelector('.clear-all-filters');
        if (clearAllBtn) {
            clearAllBtn.addEventListener('click', () => {
                state.selectedTags = [];
                this.render();
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Tool Rows (compact single-line layout)
    // ═══════════════════════════════════════════════════════════════

    renderToolRows(tools) {
        if (tools.length === 0) {
            this.toolList.innerHTML = '<div class="empty-state"><p>No tools found</p></div>';
            return;
        }

        this.toolList.innerHTML = tools.map(tool => {
            const tagsHtml = this.getCompactTags(tool.tags);
            const safeName = this.escapeHtml(tool.name);
            const safeDesc = this.escapeHtml(tool.description || '');
            return `<div class="tool-row" data-tool-name="${safeName}">
                <span class="tool-row-name" title="${safeDesc}">${safeName}</span>
                <span class="tool-row-tags">${tagsHtml}</span>
                <span class="tool-row-actions">
                    <button class="btn-icon btn-edit-tool" data-tool-name="${safeName}" title="Edit">✏️</button>
                    <button class="btn-icon btn-insert-tool" data-tool-name="${safeName}" title="Insert @call">📋</button>
                    <button class="btn-icon btn-delete-tool" data-tool-name="${safeName}" title="Delete">✕</button>
                </span>
            </div>`;
        }).join('');

        this.attachToolRowEvents();
    }

    attachToolRowEvents() {
        this.toolList.querySelectorAll('.tool-row').forEach(row => {
            const name = row.dataset.toolName;

            // Single click → edit dialog (with delay to distinguish from dblclick)
            row.addEventListener('click', (e) => {
                if (e.target.closest('.tool-row-actions')) return;
                if (this._clickTimer) clearTimeout(this._clickTimer);
                this._clickTimer = setTimeout(() => {
                    this.showDetail(name);
                    this._clickTimer = null;
                }, 250);
            });

            // Double click → insert tool call
            row.addEventListener('dblclick', (e) => {
                if (e.target.closest('.tool-row-actions')) return;
                if (this._clickTimer) {
                    clearTimeout(this._clickTimer);
                    this._clickTimer = null;
                }
                this.insertToolCall(name);
            });
        });

        // Action buttons
        this.toolList.querySelectorAll('.btn-edit-tool').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.showDetail(btn.dataset.toolName);
            });
        });

        this.toolList.querySelectorAll('.btn-insert-tool').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.insertToolCall(btn.dataset.toolName);
            });
        });

        this.toolList.querySelectorAll('.btn-delete-tool').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.unregisterTool(btn.dataset.toolName);
            });
        });
    }

    getCompactTags(tags) {
        if (!tags) return '';
        let entries;
        if (Array.isArray(tags)) {
            entries = tags.map(t => ({ key: null, value: t }));
        } else {
            entries = Object.entries(tags).map(([k, v]) => ({ key: k, value: v }));
        }
        return entries.map(({ key, value }) => {
            const color = this.getTagColor(key || value);
            const title = key ? `${key}: ${value}` : value;
            return `<span class="compact-tag" style="background:${color}" title="${this.escapeHtml(title)}">${this.escapeHtml(value)}</span>`;
        }).join('');
    }

    getTagColor(key) {
        const colors = [
            '#4a9eff', '#28a745', '#ffc107', '#17a2b8',
            '#6f42c1', '#e83e8c', '#fd7e14', '#20c997',
            '#6610f2', '#d63384', '#0dcaf0', '#198754'
        ];
        let hash = 0;
        const str = key || '';
        for (let i = 0; i < str.length; i++) {
            hash = str.charCodeAt(i) + ((hash << 5) - hash);
        }
        return colors[Math.abs(hash) % colors.length];
    }

    // ═══════════════════════════════════════════════════════════════
    //  Insert Tool Call into Editor
    // ═══════════════════════════════════════════════════════════════

    insertToolCall(toolName) {
        const tool = state.tools.find(t => t.name === toolName);
        if (!tool) return;

        const activeTab = state.getActiveTab();
        if (!activeTab) {
            this.showToast('No active tab open — open a stream first', 'error');
            return;
        }

        // Build default params from input_schema
        let params = {};
        if (tool.input_schema && tool.input_schema.properties) {
            const required = tool.input_schema.required || [];
            Object.entries(tool.input_schema.properties).forEach(([key, prop]) => {
                if (required.includes(key)) {
                    params[key] = prop.type === 'string' ? '' : `<${prop.type}>`;
                }
            });
        }

        const paramsJson = Object.keys(params).length > 0
            ? JSON.stringify(params)
            : '{}';
        const callText = `@{${toolName}|${paramsJson}}`;

        if (typeof markdownEditor !== 'undefined') {
            markdownEditor.insertText(callText);
            this.showToast(`Inserted: @${toolName}`, 'success');
        } else {
            this.showToast('Editor not initialised', 'error');
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Detail / Edit Dialog
    // ═══════════════════════════════════════════════════════════════

    showDetail(toolName) {
        this.selectedTool = state.tools.find(t => t.name === toolName);
        if (!this.selectedTool) return;

        document.getElementById('edit-name').value = this.selectedTool.name || '';
        document.getElementById('edit-description').value = this.selectedTool.description || '';
        document.getElementById('edit-tool-path').value = this.selectedTool.tool_path || '';
        document.getElementById('edit-execution-path').value = this.selectedTool.execution_path || '';
        document.getElementById('edit-brief-description').value = this.selectedTool.brief_description || '';
        document.getElementById('edit-detailed-description').value = this.selectedTool.detailed_description || '';
        document.getElementById('edit-manual-path').value = this.selectedTool.manual_path || '';
        document.getElementById('edit-version').value = this.selectedTool.version || '1.0.0';

        this.removeAllEditTagRows();
        if (this.selectedTool.tags && typeof this.selectedTool.tags === 'object') {
            Object.entries(this.selectedTool.tags).forEach(([key, value]) => {
                this.addEditTagRow(key, value);
            });
        }
        if (document.querySelectorAll('#edit-tags-rows .tag-row').length === 0) {
            this.addEditTagRow();
        }

        this.detailDialog.style.display = 'flex';
    }

    hideDetailDialog() {
        this.detailDialog.style.display = 'none';
        this.selectedTool = null;
    }

    // ═══════════════════════════════════════════════════════════════
    //  Register Dialog
    // ═══════════════════════════════════════════════════════════════

    showRegisterDialog() {
        document.getElementById('reg-name').value = '';
        document.getElementById('reg-description').value = '';
        document.getElementById('reg-tool-path').value = '';
        document.getElementById('reg-execution-path').value = '';
        document.getElementById('reg-brief-description').value = '';
        document.getElementById('reg-detailed-description').value = '';
        document.getElementById('reg-manual-path').value = '';
        document.getElementById('reg-version').value = '1.0.0';

        this.removeAllTagRows();
        this.addTagRow();

        this.registerDialog.style.display = 'flex';
    }

    hideRegisterDialog() {
        this.registerDialog.style.display = 'none';
    }

    // ═══════════════════════════════════════════════════════════════
    //  Tag Editor — Register Form
    // ═══════════════════════════════════════════════════════════════

    addTagRow(key = '', value = '') {
        const template = document.getElementById('tag-row-template');
        const row = template.content.cloneNode(true);
        const container = document.getElementById('tags-rows');

        const keyInput = row.querySelector('.tag-key-input');
        const valueInput = row.querySelector('.tag-value-input');
        const removeBtn = row.querySelector('.btn-remove-tag');

        keyInput.value = key;
        valueInput.value = value;

        keyInput.addEventListener('input', () => this.updateTagSuggestions());
        removeBtn.addEventListener('click', () => {
            container.removeChild(removeBtn.closest('.tag-row'));
        });

        container.appendChild(row);
        this.updateTagSuggestions();
    }

    removeAllTagRows() {
        const container = document.getElementById('tags-rows');
        container.innerHTML = '';
    }

    updateTagSuggestions() {
        const keys = state.getTagKeys();
        const keysList = document.getElementById('tag-keys-list');
        keysList.innerHTML = keys.map(k => `<option value="${k}">`).join('');
    }

    getTagsFromEditor() {
        const tags = {};
        const rows = document.querySelectorAll('#tags-rows .tag-row');
        rows.forEach(row => {
            const key = row.querySelector('.tag-key-input').value.trim();
            const value = row.querySelector('.tag-value-input').value.trim();
            if (key && value) {
                tags[key] = value;
            }
        });
        return tags;
    }

    // ═══════════════════════════════════════════════════════════════
    //  Tag Editor — Edit Form
    // ═══════════════════════════════════════════════════════════════

    getTagsFromEditEditor() {
        const tags = {};
        const rows = document.querySelectorAll('#edit-tags-rows .tag-row');
        rows.forEach(row => {
            const key = row.querySelector('.tag-key-input').value.trim();
            const value = row.querySelector('.tag-value-input').value.trim();
            if (key && value) {
                tags[key] = value;
            }
        });
        return tags;
    }

    addEditTagRow(key = '', value = '') {
        const template = document.getElementById('tag-row-template');
        const row = template.content.cloneNode(true);
        const container = document.getElementById('edit-tags-rows');

        const keyInput = row.querySelector('.tag-key-input');
        const valueInput = row.querySelector('.tag-value-input');
        const removeBtn = row.querySelector('.btn-remove-tag');

        keyInput.value = key;
        valueInput.value = value;

        removeBtn.addEventListener('click', () => {
            container.removeChild(removeBtn.closest('.tag-row'));
        });

        container.appendChild(row);
    }

    removeAllEditTagRows() {
        const container = document.getElementById('edit-tags-rows');
        container.innerHTML = '';
    }

    // ═══════════════════════════════════════════════════════════════
    //  Save / Register / Delete
    // ═══════════════════════════════════════════════════════════════

    async saveTool() {
        if (!this.selectedTool) return;

        const name = document.getElementById('edit-name').value.trim();
        const description = document.getElementById('edit-description').value.trim();
        const toolPath = document.getElementById('edit-tool-path').value.trim();
        const executionPath = document.getElementById('edit-execution-path').value.trim();
        const briefDescription = document.getElementById('edit-brief-description').value.trim();
        const detailedDescription = document.getElementById('edit-detailed-description').value.trim();
        const manualPath = document.getElementById('edit-manual-path').value.trim();
        const version = document.getElementById('edit-version').value.trim();

        if (!name || !description || !toolPath) {
            this.showToast('Please fill in all required fields (Name, Description, Tool Path)', 'error');
            return;
        }

        if (!/^[a-zA-Z0-9_]+$/.test(name)) {
            this.showToast('Tool name can only contain letters, numbers, and underscores', 'error');
            return;
        }

        const tags = this.getTagsFromEditEditor();

        const toolData = { name, description, tool_path: toolPath, version: version || '1.0.0' };
        if (executionPath) toolData.execution_path = executionPath;
        if (briefDescription) toolData.brief_description = briefDescription;
        if (detailedDescription) toolData.detailed_description = detailedDescription;
        if (manualPath) toolData.manual_path = manualPath;
        if (Object.keys(tags).length > 0) toolData.tags = tags;

        try {
            const oldName = this.selectedTool.name;
            // Editing an existing tool → PUT; the backend handles rename (unregister old → register new)
            const result = await api.updateTool(oldName, toolData);
            if (result.success) {
                this.hideDetailDialog();
                this.showToast(`Tool "${name}" updated successfully`, 'success');
                this.loadTools();
            } else {
                this.showToast(`Update failed: ${result.error}`, 'error');
            }
        } catch (err) {
            this.showToast(`Update error: ${err.message}`, 'error');
        }
    }

    async registerTool() {
        const name = document.getElementById('reg-name').value.trim();
        const description = document.getElementById('reg-description').value.trim();
        const toolPath = document.getElementById('reg-tool-path').value.trim();
        const executionPath = document.getElementById('reg-execution-path').value.trim();
        const briefDescription = document.getElementById('reg-brief-description').value.trim();
        const detailedDescription = document.getElementById('reg-detailed-description').value.trim();
        const manualPath = document.getElementById('reg-manual-path').value.trim();
        const version = document.getElementById('reg-version').value.trim();

        if (!name || !description || !toolPath) {
            this.showToast('Please fill in all required fields (Name, Description, Tool Path)', 'error');
            return;
        }

        if (!/^[a-zA-Z0-9_]+$/.test(name)) {
            this.showToast('Tool name can only contain letters, numbers, and underscores', 'error');
            return;
        }

        const tags = this.getTagsFromEditor();

        const toolData = { name, description, tool_path: toolPath, version: version || '1.0.0' };
        if (executionPath) toolData.execution_path = executionPath;
        if (briefDescription) toolData.brief_description = briefDescription;
        if (detailedDescription) toolData.detailed_description = detailedDescription;
        if (manualPath) toolData.manual_path = manualPath;
        if (Object.keys(tags).length > 0) toolData.tags = tags;

        try {
            const result = await api.registerTool(toolData);
            if (result.success) {
                this.hideRegisterDialog();
                this.showToast(`Tool "${name}" registered successfully`, 'success');
                this.loadTools();
            } else {
                this.showToast(`Registration failed: ${result.error}`, 'error');
            }
        } catch (err) {
            this.showToast(`Registration error: ${err.message}`, 'error');
        }
    }

    async unregisterTool(toolName) {
        if (!confirm(`Are you sure you want to unregister tool "${toolName}"?`)) return;

        try {
            const result = await api.unregisterTool(toolName);
            if (result.success) {
                this.showToast(`Tool "${toolName}" unregistered`, 'success');
                this.loadTools();
            } else {
                this.showToast(`Unregister failed: ${result.error}`, 'error');
            }
        } catch (err) {
            this.showToast(`Unregister error: ${err.message}`, 'error');
        }
    }

    async deleteTag(toolName, key) {
        const tool = state.tools.find(t => t.name === toolName);
        if (!tool || !tool.tags) return;

        let updatedTags;
        if (Array.isArray(tool.tags)) {
            updatedTags = tool.tags.filter(t => t !== key);
        } else {
            updatedTags = { ...tool.tags };
            delete updatedTags[key];
        }

        try {
            const result = await api.registerTool({ ...tool, tags: updatedTags });
            if (result.success) {
                this.showToast(`Tag "${key}" removed`, 'success');
                await this.loadTools();
                this.showDetail(toolName);
            } else {
                this.showToast(`Failed to remove tag: ${result.error}`, 'error');
            }
        } catch (err) {
            this.showToast(`Error removing tag: ${err.message}`, 'error');
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  File Picker
    // ═══════════════════════════════════════════════════════════════

    async openFilePicker(fieldType) {
        try {
            let result, targetId;
            switch (fieldType) {
                case 'tool-path':
                    result = await api.selectFile([["Python Files", "*.py"]]);
                    targetId = 'reg-tool-path';
                    break;
                case 'execution-path':
                    result = await api.selectDirectory();
                    targetId = 'reg-execution-path';
                    break;
                case 'manual-path':
                    result = await api.selectFile([["Markdown Files", "*.md"]]);
                    targetId = 'reg-manual-path';
                    break;
                case 'edit-tool-path':
                    result = await api.selectFile([["Python Files", "*.py"]]);
                    targetId = 'edit-tool-path';
                    break;
                case 'edit-execution-path':
                    result = await api.selectDirectory();
                    targetId = 'edit-execution-path';
                    break;
                case 'edit-manual-path':
                    result = await api.selectFile([["Markdown Files", "*.md"]]);
                    targetId = 'edit-manual-path';
                    break;
                default:
                    return;
            }
            if (result.success && result.data.path) {
                document.getElementById(targetId).value = result.data.path;
            }
        } catch (err) {
            this.showToast(`File picker error: ${err.message}`, 'error');
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════

    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    showToast(message, type = 'info') {
        window.showToast(message, type, 3000);
    }

    // ═══════════════════════════════════════════════════════════════
    //  Save / Load Tool Config
    // ═══════════════════════════════════════════════════════════════

    async saveToolsConfig() {
        if (state.tools.length === 0) {
            this.showToast('No tools to save', 'warning');
            return;
        }
        this._showSaveToolsDialog(state.tools);
    }

    _showSaveToolsDialog(tools) {
        const dialog = document.getElementById('save-tools-dialog');
        const listEl = document.getElementById('save-tools-list');
        const selectAllCheckbox = document.getElementById('save-tools-select-all');
        const countEl = document.getElementById('save-tools-count');
        const confirmBtn = document.getElementById('btn-confirm-save-tools');
        const dirInput = document.getElementById('save-tools-dir-input');

        listEl.innerHTML = tools.map(t => `
            <label class="save-streams-item">
                <input type="checkbox" class="save-tools-checkbox" value="${this.escapeHtml(t.name)}" checked>
                <div class="save-streams-item-info">
                    <div class="save-streams-item-name">${this.escapeHtml(t.name)}</div>
                    <div class="save-streams-item-meta">${this.getCompactTags(t.tags)}</div>
                </div>
            </label>
        `).join('');

        const updateCount = () => {
            const checked = listEl.querySelectorAll('.save-tools-checkbox:checked');
            countEl.textContent = `${checked.length} selected`;
            confirmBtn.disabled = checked.length === 0 || !dirInput.value;
        };

        selectAllCheckbox.checked = true;
        selectAllCheckbox.indeterminate = false;
        selectAllCheckbox.addEventListener('change', () => {
            listEl.querySelectorAll('.save-tools-checkbox').forEach(cb => {
                cb.checked = selectAllCheckbox.checked;
            });
            updateCount();
        });

        listEl.querySelectorAll('.save-tools-checkbox').forEach(cb => {
            cb.addEventListener('change', () => {
                const all = listEl.querySelectorAll('.save-tools-checkbox');
                const checked = listEl.querySelectorAll('.save-tools-checkbox:checked');
                selectAllCheckbox.checked = checked.length === all.length;
                selectAllCheckbox.indeterminate = checked.length > 0 && checked.length < all.length;
                updateCount();
            });
        });

        // Browse → directly open the system native directory picker (consistent with btn-browse-save-dir in Save As),
        // no longer show the "Recent Output Directories" history window (it is obscured by the native <dialog> top layer and cannot be used).
        document.getElementById('btn-browse-save-tools-dir').onclick = async () => {
            const r = await api.selectDirectory();
            if (r.success && r.data.path) {
                dirInput.value = r.data.path;
                updateCount();
            }
        };

        document.getElementById('btn-cancel-save-tools').onclick = () => dialog.close();
        document.getElementById('close-save-tools-dialog').onclick = () => dialog.close();

        confirmBtn.onclick = async () => {
            const selectedNames = Array.from(
                listEl.querySelectorAll('.save-tools-checkbox:checked')
            ).map(cb => cb.value);
            const outputDir = dirInput.value.trim();
            if (selectedNames.length === 0 || !outputDir) return;

            const overwrite = document.getElementById('save-tools-overwrite').checked;
            dialog.close();
            state.setLoading(true);
            try {
                const r = await api.saveToolsConfig({
                    output_dir: outputDir,
                    tool_names: selectedNames,
                    overwrite: overwrite,
                });
                if (r.success) {
                    await api.addFileHistory({ path: outputDir, path_type: 'save_tools_dir', label: outputDir });
                    this.showToast(`Saved ${r.data.tool_count} tool(s) to ${r.data.config_path}`, 'success');
                } else {
                    this.showToast(r.error || 'Save failed', 'error');
                }
            } catch (e) {
                this.showToast('Save failed: ' + e.message, 'error');
            } finally {
                state.setLoading(false);
            }
        };

        updateCount();
        dialog.showModal();
    }

    async loadToolsConfig() {
        // Directly open the system native file picker (consistent with the __browse__ branch in file-manager pickFile),
        // no longer show the "Recent Tool Config Files" history window.
        const r = await api.selectFile([["JSON Config", "*.json"], ["All Files", "*.*"]]);
        if (!r.success || !r.data.path) return;
        const path = r.data.path;

        state.setLoading(true);
        let preview;
        try {
            preview = await api.previewToolsConfig(path);
        } finally {
            state.setLoading(false);
        }
        if (!preview.success) {
            this.showToast(preview.error || 'Invalid config file', 'error');
            return;
        }

        const conflictCount = preview.data.conflicts.length;
        document.getElementById('load-tools-confirm-summary').textContent =
            `Config contains ${preview.data.tool_count} tool(s)` +
            (conflictCount > 0
                ? `, ${conflictCount} with the same name as existing tools`
                : ', no name conflicts');
        document.getElementById('load-tools-confirm-dialog').style.display = 'flex';

        document.getElementById('btn-load-tools-confirm').onclick = async () => {
            document.getElementById('load-tools-confirm-dialog').style.display = 'none';
            const onConflict = document.querySelector('input[name="load-tools-conflict"]:checked').value;
            state.setLoading(true);
            try {
                const r = await api.loadToolsConfig({ config_path: path, on_conflict: onConflict });
                if (r.success) {
                    await api.addFileHistory({ path, path_type: 'load_tools_config', label: path });
                    const d = r.data;
                    this.showToast(
                        `Loaded ${d.loaded.length} · Skipped ${d.skipped.length} · Overwritten ${d.overwritten.length} · Failed ${d.errors.length}`,
                        d.errors.length > 0 ? 'error' : 'success'
                    );
                } else {
                    this.showToast(r.error || 'Import failed', 'error');
                }
            } catch (e) {
                this.showToast('Import failed: ' + e.message, 'error');
            } finally {
                state.setLoading(false);
            }
        };

        document.getElementById('btn-load-tools-cancel').onclick = () => {
            document.getElementById('load-tools-confirm-dialog').style.display = 'none';
        };
    }
}

const toolPanel = new ToolPanel();
