class AppState {
    constructor() {
        this.systems = [];
        this.activeSystemId = null;
        this.streams = {};
        this.activeStreamName = null;
        this.tabs = [];
        this.activeTabId = null;
        this.editorStates = {};
        this.scanState = null;
        this.tools = [];
        this.selectedTags = [];
        this.searchQuery = '';
        this.isLoading = false;
        this.error = null;
        this.listeners = {};
        this.streamPollTimers = {};
        this.streamSnapshots = {};
        this.scanPositionTimer = null;
    }

    on(event, callback) {
        if (!this.listeners[event]) {
            this.listeners[event] = [];
        }
        this.listeners[event].push(callback);
    }

    emit(event, data) {
        if (this.listeners[event]) {
            this.listeners[event].forEach(callback => {
                try {
                    callback(data);
                } catch (e) {
                    console.error('[state] emit error in', event, 'listener:', e);
                }
            });
        }
    }

    setSystems(systems) {
        this.systems = systems;
        this.emit('systemsChanged', systems);
    }

    setActiveSystem(systemId) {
        this.activeSystemId = systemId;
        this.emit('activeSystemChanged', systemId);
    }

    getActiveSystem() {
        return this.systems.find(s => s.id === this.activeSystemId);
    }

    setStreams(systemId, streams) {
        this.streams[systemId] = streams;
        this.emit('streamsChanged', { systemId, streams });
    }

    getStreams(systemId) {
        return this.streams[systemId] || [];
    }

    setActiveStream(streamName) {
        this.activeStreamName = streamName;
    }

    addTab(tab) {
        const existing = this.tabs.find(t => t.id === tab.id);
        if (!existing) {
            this.tabs.push(tab);
        }
        this.activeTabId = tab.id;
        this.emit('tabsChanged', this.tabs);
        this.emit('activeTabChanged', tab.id);
    }

    removeTab(tabId) {
        const wasActive = this.activeTabId === tabId;
        this.tabs = this.tabs.filter(t => t.id !== tabId);
        if (wasActive) {
            this.activeTabId = this.tabs.length > 0 ? this.tabs[this.tabs.length - 1].id : null;
        }
        delete this.editorStates[tabId];
        this.emit('tabsChanged', this.tabs);
        if (wasActive) {
            this.emit('activeTabChanged', this.activeTabId);
        }
    }

    setActiveTab(tabId) {
        this.activeTabId = tabId;
        this.emit('activeTabChanged', tabId);
    }

    getActiveTab() {
        return this.tabs.find(t => t.id === this.activeTabId);
    }

    markTabModified(tabId, modified) {
        const tab = this.tabs.find(t => t.id === tabId);
        if (tab) {
            tab.isModified = modified;
            this.emit('tabModified', { tabId, modified });
        }
    }

    lockTab(tabId, locked) {
        const tab = this.tabs.find(t => t.id === tabId);
        if (tab) {
            tab.isLocked = locked;
            this.emit('tabLocked', { tabId, locked });
            this.emit('tabsChanged', this.tabs);
        }
    }

    updateEditorState(tabId, state) {
        if (!this.editorStates[tabId]) {
            this.editorStates[tabId] = { content: '', cursorPosition: 0, scrollTop: 0 };
        }
        Object.assign(this.editorStates[tabId], state);
    }

    getEditorState(tabId) {
        return this.editorStates[tabId] || { content: '', cursorPosition: 0, scrollTop: 0 };
    }

    setScanState(state) {
        this.scanState = state;
        this.emit('scanStateChanged', state);
    }

    setTools(tools) {
        this.tools = tools;
        this.emit('toolsChanged', tools);
    }

    getFilteredTools() {
        let filtered = [...this.tools];

        if (this.searchQuery) {
            const query = this.searchQuery.toLowerCase();
            filtered = filtered.filter(tool =>
                tool.name.toLowerCase().includes(query) ||
                tool.description.toLowerCase().includes(query)
            );
        }

        if (this.selectedTags.length > 0) {
            // Group selected tags by key for AND across keys, OR within keys
            const selectedByKey = new Map();
            this.selectedTags.forEach(tag => {
                const colonIndex = tag.indexOf(': ');
                if (colonIndex !== -1) {
                    const key = tag.substring(0, colonIndex);
                    const value = tag.substring(colonIndex + 2);
                    if (!selectedByKey.has(key)) selectedByKey.set(key, []);
                    selectedByKey.get(key).push(value);
                }
            });

            filtered = filtered.filter(tool => {
                const toolTags = this._normalizeTags(tool.tags);
                // For each key with selected values, at least one value must match (OR within key)
                // ALL keys must have a match (AND across keys)
                for (const [key, values] of selectedByKey) {
                    const hasMatch = values.some(value => toolTags.includes(`${key}: ${value}`));
                    if (!hasMatch) return false;
                }
                return true;
            });
        }

        return filtered;
    }

    getAllTags() {
        const tagSet = new Set();
        this.tools.forEach(tool => {
            const tags = this._normalizeTags(tool.tags);
            tags.forEach(tag => tagSet.add(tag));
        });
        return Array.from(tagSet).sort();
    }

    /**
     * Get tag suggestions from all tools (dict-format tags only).
     * @returns {Map<string, Set<string>>} Map of tag keys to sets of values
     */
    getTagSuggestions() {
        const suggestions = new Map();
        this.tools.forEach(tool => {
            if (tool.tags && typeof tool.tags === 'object' && !Array.isArray(tool.tags)) {
                Object.entries(tool.tags).forEach(([k, v]) => {
                    if (!suggestions.has(k)) suggestions.set(k, new Set());
                    suggestions.get(k).add(v);
                });
            }
        });
        return suggestions;
    }

    /**
     * Get all unique tag keys across tools (dict-format tags only).
     * @returns {string[]} Sorted array of tag keys
     */
    getTagKeys() {
        const keys = new Set();
        this.tools.forEach(tool => {
            if (tool.tags && typeof tool.tags === 'object' && !Array.isArray(tool.tags)) {
                Object.keys(tool.tags).forEach(k => keys.add(k));
            }
        });
        return Array.from(keys).sort();
    }

    _normalizeTags(tags) {
        if (!tags) return [];
        if (Array.isArray(tags)) return tags;
        // dict format: {"key": "value", ...} → ["key: value", ...]
        return Object.entries(tags).map(([key, value]) => `${key}: ${value}`);
    }

    setLoading(loading) {
        this.isLoading = loading;
        this.emit('loadingChanged', loading);
    }

    setError(error) {
        this.error = error;
        this.emit('errorChanged', error);
    }

    clearError() {
        this.error = null;
        this.emit('errorChanged', null);
    }
}

const state = new AppState();
