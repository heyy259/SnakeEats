/**
 * Scan position conversion utility functions
 *
 * Provides character offset → row/col conversion and surrounding character extraction,
 * shared by scan-control, tab-manager, and the position preview.
 */

/**
 * Character offset → row/col conversion (based on editor content)
 * @param {number} offset - character offset (0-based)
 * @param {string} text - full stream content string
 * @returns {{row: number, col: number}} row and column numbers (both 1-based)
 */
function charOffsetToRowCol(offset, text) {
    if (!text || offset <= 0) return { row: 1, col: 1 };
    const clamped = Math.min(offset, text.length);
    const before = text.substring(0, clamped);
    const lines = before.split('\n');
    return { row: lines.length, col: lines[lines.length - 1].length + 1 };
}

/**
 * Get N characters before and after the offset position
 * @param {number} offset - character offset
 * @param {string} text - full stream content string
 * @param {number} count - number of characters to extract on each side (default 5)
 * @returns {{before: string, after: string}}
 */
function getSurroundingChars(offset, text, count = 5) {
    if (!text) return { before: '', after: '' };
    const safeOffset = Math.max(0, Math.min(offset, text.length));
    const before = text.substring(Math.max(0, safeOffset - count), safeOffset);
    const after = text.substring(safeOffset, Math.min(text.length, safeOffset + count));
    return { before, after };
}
