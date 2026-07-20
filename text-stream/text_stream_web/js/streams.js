class StreamManager {
    constructor(wsClient) {
        this.wsClient = wsClient;
        this.streams = [];
        this.currentStream = null;
    }

    async list(tags = null) {
        const params = {};
        if (tags) {
            params.tags = tags;
        }
        const result = await this.wsClient.send('stream.list', params);
        this.streams = result;
        return result;
    }

    async create(name, description = '', tags = {}) {
        const result = await this.wsClient.send('stream.create', {
            name,
            description,
            tags
        });
        await this.list();
        return result;
    }

    async delete(name) {
        await this.wsClient.send('stream.delete', { name });
        if (this.currentStream && this.currentStream.name === name) {
            this.currentStream = null;
        }
        await this.list();
    }

    async get(name) {
        const result = await this.wsClient.send('stream.get', { name });
        this.currentStream = { name, ...result };
        return result;
    }

    async query(name = null, description = null, tags = null) {
        const params = {};
        if (name !== null && name !== undefined) params.name = name;
        if (description !== null && description !== undefined) params.description = description;
        if (tags !== null && tags !== undefined) params.tags = tags;
        return await this.wsClient.send('stream.query', params);
    }

    async search(pattern, regex = false) {
        return await this.wsClient.send('stream.search', {
            pattern,
            regex
        });
    }

    async read(name, start = null, end = null) {
        const params = { name };
        if (start !== null) params.start = start;
        if (end !== null) params.end = end;
        const result = await this.wsClient.send('stream.read', params);
        return result.text;
    }

    async write(name, text) {
        await this.wsClient.send('stream.write', { name, text });
    }

    async append(name, text) {
        await this.wsClient.send('stream.append', { name, text });
    }

    async insert(name, pos, text) {
        await this.wsClient.send('stream.insert', { name, pos, text });
    }

    async deleteRange(name, start, end) {
        await this.wsClient.send('stream.delete_range', { name, start, end });
    }

    async replaceRange(name, start, end, text) {
        await this.wsClient.send('stream.replace_range', { name, start, end, text });
    }

    async searchInStream(name, pattern, contextBefore = 100, contextAfter = 100, regex = false) {
        return await this.wsClient.send('stream.search_in_stream', {
            name,
            pattern,
            context_before: contextBefore,
            context_after: contextAfter,
            regex
        });
    }

    async searchBetween(name, before, after, regex = false) {
        return await this.wsClient.send('stream.search_between', {
            name,
            before,
            after,
            regex
        });
    }

    async getMetadata(name) {
        return await this.wsClient.send('stream.get_metadata', { name });
    }

    async updateDescription(name, text) {
        await this.wsClient.send('stream.update_description', { name, text });
    }

    async setTag(name, key, value) {
        await this.wsClient.send('stream.set_tag', { name, key, value });
    }

    async removeTag(name, key) {
        await this.wsClient.send('stream.remove_tag', { name, key });
    }

    async rename(name, newName) {
        await this.wsClient.send('stream.rename', { old_name: name, new_name: newName });
        await this.list();
    }

    async undo(name, n = 1) {
        await this.wsClient.send('stream.undo', { name, n });
    }

    async prepend(name, text) {
        await this.wsClient.send('stream.prepend', { name, text });
    }

    async readLines(name, startLine = null, endLine = null) {
        const params = { name };
        if (startLine !== null) params.start_line = startLine;
        if (endLine !== null) params.end_line = endLine;
        return await this.wsClient.send('stream.read_lines', params);
    }

    async searchInRange(name, pattern, start, end, regex = false) {
        return await this.wsClient.send('stream.search_in_range', {
            name,
            pattern,
            start,
            end,
            regex
        });
    }

    async replaceLines(name, startLine, endLine, text) {
        await this.wsClient.send('stream.replace_lines', {
            name,
            start_line: startLine,
            end_line: endLine,
            text
        });
    }

    async deleteLines(name, startLine, endLine) {
        await this.wsClient.send('stream.delete_lines', {
            name,
            start_line: startLine,
            end_line: endLine
        });
    }

    async streamAppendStart(name, position = null) {
        const params = { name };
        if (position !== null) params.position = position;
        return await this.wsClient.send('stream.stream_append_start', params);
    }

    async streamAppendChunk(operationId, text) {
        return await this.wsClient.send('stream.stream_append_chunk', {
            operation_id: operationId,
            text
        });
    }

    async streamAppendEnd(operationId) {
        return await this.wsClient.send('stream.stream_append_end', {
            operation_id: operationId
        });
    }

    async subscribe(name, fromPosition = null) {
        const params = { name };
        if (fromPosition !== null) params.from_position = fromPosition;
        return await this.wsClient.send('stream.subscribe', params);
    }

    async unsubscribe(subscriptionId) {
        await this.wsClient.send('stream.unsubscribe', {
            subscription_id: subscriptionId
        });
    }
}

window.StreamManager = StreamManager;
