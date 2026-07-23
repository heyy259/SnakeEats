class MarkdownEditor {
    constructor() {
        this.container = document.getElementById('editor');
        this.content = '';
        this.setupEditor();
    }

    setupEditor() {
        this.container.innerHTML = `<textarea class="editor-textarea" spellcheck="false"></textarea>`;
        this.textarea = this.container.querySelector('.editor-textarea');
        
        this.textarea.addEventListener('input', () => {
            this.content = this.textarea.value;
            const activeTab = state.getActiveTab();
            if (activeTab) {
                state.updateEditorState(activeTab.id, { content: this.content });
                state.markTabModified(activeTab.id, true);
            }
        });

        this.textarea.addEventListener('keydown', (e) => {
            if (e.ctrlKey || e.metaKey) {
                switch (e.key) {
                    case 's':
                        e.preventDefault();
                        fileManager.saveStream();
                        break;
                    case 'b':
                        e.preventDefault();
                        this.wrapSelection('**', '**');
                        break;
                    case 'i':
                        e.preventDefault();
                        this.wrapSelection('*', '*');
                        break;
                    case 'k':
                        e.preventDefault();
                        this.wrapSelection('[', '](url)');
                        break;
                    case 'e':
                        e.preventDefault();
                        this.wrapSelection('`', '`');
                        break;
                }
            }

            if (e.key === 'Tab') {
                e.preventDefault();
                this.insertText('    ');
            }
        });

        this.textarea.addEventListener('click', () => {
            this.updateCursorPosition();
        });

        this.textarea.addEventListener('keyup', () => {
            this.updateCursorPosition();
        });

        // 扫描期间仅禁止用户编辑，但保持订阅更新（内容仍可被后端推送刷新）
        state.on('tabLocked', ({ tabId, locked }) => {
            const activeTab = state.getActiveTab();
            if (activeTab && activeTab.id === tabId && this.textarea) {
                this.textarea.readOnly = locked;
            }
        });
        state.on('activeTabChanged', () => {
            const activeTab = state.getActiveTab();
            if (activeTab && this.textarea) {
                this.textarea.readOnly = activeTab.isLocked;
            }
        });

        this.applyStyles();
    }

    applyStyles() {
        this.container.style.cssText = `
            height: 100%;
            width: 100%;
            overflow: hidden;
        `;

        this.textarea.style.cssText = `
            width: 100%;
            height: 100%;
            padding: 16px;
            background-color: var(--bg-primary);
            color: var(--text-primary);
            border: none;
            outline: none;
            resize: none;
            font-family: var(--font-family-mono);
            font-size: 14px;
            line-height: 1.6;
            tab-size: 4;
        `;
    }

    setContent(content) {
        this.textarea.value = content;
        this.updateCursorPosition();
    }

    getContent() {
        return this.textarea.value;
    }

    insertText(text) {
        const start = this.textarea.selectionStart;
        const end = this.textarea.selectionEnd;
        const value = this.textarea.value;

        this.textarea.value = value.substring(0, start) + text + value.substring(end);
        this.textarea.selectionStart = this.textarea.selectionEnd = start + text.length;
        this.content = this.textarea.value;

        const activeTab = state.getActiveTab();
        if (activeTab) {
            state.updateEditorState(activeTab.id, { content: this.content });
            state.markTabModified(activeTab.id, true);
        }

        this.textarea.focus();
    }

    wrapSelection(before, after) {
        const start = this.textarea.selectionStart;
        const end = this.textarea.selectionEnd;
        const value = this.textarea.value;
        const selectedText = value.substring(start, end);

        const newText = before + (selectedText || 'text') + after;
        this.textarea.value = value.substring(0, start) + newText + value.substring(end);

        if (selectedText) {
            this.textarea.selectionStart = start + before.length;
            this.textarea.selectionEnd = start + before.length + selectedText.length;
        } else {
            this.textarea.selectionStart = start + before.length;
            this.textarea.selectionEnd = start + before.length + 4;
        }

        this.content = this.textarea.value;

        const activeTab = state.getActiveTab();
        if (activeTab) {
            state.updateEditorState(activeTab.id, { content: this.content });
            state.markTabModified(activeTab.id, true);
        }

        this.textarea.focus();
    }

    updateCursorPosition() {
        const value = this.textarea.value;
        const pos = this.textarea.selectionStart;
        const lines = value.substring(0, pos).split('\n');
        const line = lines.length;
        const col = lines[lines.length - 1].length + 1;

        document.getElementById('cursor-position').innerHTML = `<span class="status-label">Ln ${line}, Col ${col}</span>`;
    }

    showScanPosition(charOffset) {
        this.removeScanPosition();
        if (charOffset < 0) return;

        const text = this.textarea.value;
        if (!text || charOffset > text.length) return;

        const lineNumber = text.substring(0, charOffset).split('\n').length;

        // 动态获取实际行高和 padding（替代硬编码 22.4）
        const computedStyle = window.getComputedStyle(this.textarea);
        const lineHeight = parseFloat(computedStyle.lineHeight) || 22.4;
        const paddingTop = parseFloat(computedStyle.paddingTop) || 0;
        const scrollTop = this.textarea.scrollTop;

        const y = (lineNumber - 1) * lineHeight + paddingTop - scrollTop;

        // 主标记箭头
        const marker = document.createElement('div');
        marker.className = 'scan-position-indicator';
        marker.id = 'scan-position-indicator';
        marker.textContent = '▶';
        marker.style.top = `${Math.max(0, y)}px`;
        this.container.appendChild(marker);

        // 行高亮背景
        const highlight = document.createElement('div');
        highlight.id = 'scan-line-highlight';
        highlight.className = 'scan-line-highlight';
        highlight.style.top = `${Math.max(0, y)}px`;
        highlight.style.height = `${lineHeight}px`;
        this.container.appendChild(highlight);
    }

    removeScanPosition() {
        const old = document.getElementById('scan-position-indicator');
        if (old) old.remove();
        const highlight = document.getElementById('scan-line-highlight');
        if (highlight) highlight.remove();
    }
}

const markdownEditor = new MarkdownEditor();
