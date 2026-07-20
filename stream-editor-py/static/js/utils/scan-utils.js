/**
 * 扫描位置换算工具函数
 *
 * 提供字符偏移 → 行/列转换、前后字符截取，
 * 供 scan-control、tab-manager、位置预览共用。
 */

/**
 * 字符偏移 → 行/列转换（基于编辑器内容）
 * @param {number} offset - 字符偏移量（0-based）
 * @param {string} text - 流完整内容字符串
 * @returns {{row: number, col: number}} 行号和列号（均为 1-based）
 */
function charOffsetToRowCol(offset, text) {
    if (!text || offset <= 0) return { row: 1, col: 1 };
    const clamped = Math.min(offset, text.length);
    const before = text.substring(0, clamped);
    const lines = before.split('\n');
    return { row: lines.length, col: lines[lines.length - 1].length + 1 };
}

/**
 * 获取偏移位置前N个字符和后N个字符
 * @param {number} offset - 字符偏移量
 * @param {string} text - 流完整内容字符串
 * @param {number} count - 前后截取的字符数（默认5）
 * @returns {{before: string, after: string}}
 */
function getSurroundingChars(offset, text, count = 5) {
    if (!text) return { before: '', after: '' };
    const safeOffset = Math.max(0, Math.min(offset, text.length));
    const before = text.substring(Math.max(0, safeOffset - count), safeOffset);
    const after = text.substring(safeOffset, Math.min(text.length, safeOffset + count));
    return { before, after };
}
