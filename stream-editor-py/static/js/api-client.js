class ApiClient {
    constructor(baseUrl = '') {
        this.baseUrl = baseUrl;
    }

    async request(method, path, data = null) {
        const options = {
            method,
            headers: { 'Content-Type': 'application/json' }
        };
        if (data) {
            options.body = JSON.stringify(data);
        }

        try {
            const response = await fetch(`${this.baseUrl}${path}`, options);
            return await response.json();
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    async getSystems() {
        return this.request('GET', '/api/systems');
    }

    async createSystem(options) {
        return this.request('POST', '/api/systems', options);
    }

    async deleteSystem(systemId) {
        return this.request('DELETE', `/api/systems/${systemId}`);
    }

    async getStreams(systemId) {
        return this.request('GET', `/api/streams/${systemId}`);
    }

    async getStreamContent(systemId, streamName) {
        return this.request('GET', `/api/streams/${systemId}/${streamName}`);
    }

    async createStream(systemId, options) {
        return this.request('POST', `/api/streams/${systemId}`, options);
    }

    async updateStream(systemId, streamName, content) {
        return this.request('PUT', `/api/streams/${systemId}/${streamName}`, { content });
    }

    async deleteStream(systemId, streamName) {
        return this.request('DELETE', `/api/streams/${systemId}/${streamName}`);
    }

    async renameStream(systemId, oldName, newName) {
        return this.request('PUT', `/api/streams/${systemId}/${oldName}/rename`, { new_name: newName });
    }

    async updateStreamDescription(systemId, streamName, description) {
        return this.request('PUT', `/api/streams/${systemId}/${streamName}/description`, { description });
    }

    async setStreamTag(systemId, streamName, key, value) {
        return this.request('PUT', `/api/streams/${systemId}/${streamName}/tags`, { key, value });
    }

    async removeStreamTag(systemId, streamName, key) {
        return this.request('DELETE', `/api/streams/${systemId}/${streamName}/tags/${key}`);
    }

    async getTools() {
        return this.request('GET', '/api/tools');
    }

    async getTool(toolName) {
        return this.request('GET', `/api/tools/${toolName}`);
    }

    async registerTool(toolData) {
        return this.request('POST', '/api/tools', toolData);
    }

    async updateTool(toolName, toolData) {
        return this.request('PUT', `/api/tools/${encodeURIComponent(toolName)}`, toolData);
    }

    async unregisterTool(toolName) {
        return this.request('DELETE', `/api/tools/${toolName}`);
    }

    async executeTool(toolName, params) {
        return await this.request('POST', `/api/tools/${toolName}/execute`, params);
    }

    async listProcesses(toolName = null, status = 'alive') {
        let path = `/api/tools/processes?status=${status}`;
        if (toolName) path += `&tool_name=${encodeURIComponent(toolName)}`;
        return this.request('GET', path);
    }

    async sendProcessInput(processName, data, appendNewline = true, encoding = 'utf-8') {
        return this.request('POST', `/api/tools/processes/${encodeURIComponent(processName)}/send-input`, {
            data, append_newline: appendNewline, encoding
        });
    }

    async stopProcess(processName, timeout = 2) {
        return this.request('POST', `/api/tools/processes/${encodeURIComponent(processName)}/stop`, { timeout });
    }

    async stopAllProcesses(timeout = 2) {
        return this.request('POST', '/api/tools/processes/stop-all', { timeout });
    }

    async getScanStatus() {
        return this.request('GET', '/api/scan/status');
    }

    async startScan(systemId, streamName) {
        return this.request('POST', '/api/scan/start', { system_id: systemId, stream_name: streamName });
    }

    async pauseScan() {
        return this.request('POST', '/api/scan/pause');
    }

    async resumeScan() {
        return this.request('POST', '/api/scan/resume');
    }

    async stopScan() {
        return this.request('POST', '/api/scan/stop');
    }

    async setScanPosition(position) {
        return this.request('PUT', '/api/scan/position', { position });
    }

    async saveFile(data) {
        return this.request('POST', '/api/files/save', data);
    }

    async loadFile(data) {
        return this.request('POST', '/api/files/load', data);
    }

    async selectDirectory() {
        return this.request('POST', '/api/files/select-directory');
    }

    async selectFile(filetypes) {
        return this.request('POST', '/api/files/select-file', { filetypes });
    }

    async loadDocument(data) {
        return this.request('POST', '/api/files/load-document', data);
    }

    async getFileHistory(type = null, limit = 10) {
        let path = `/api/files/history?limit=${limit}`;
        if (type) path += `&type=${type}`;
        return this.request('GET', path);
    }

    async addFileHistory(data) {
        return this.request('POST', '/api/files/history', data);
    }

    async deleteFileHistory(entryId) {
        return this.request('DELETE', `/api/files/history/${entryId}`);
    }
}

const api = new ApiClient();
