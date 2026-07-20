/**
 * Shared toast notification utility.
 * Used by scan-control.js (5000ms) and tool-panel.js (3000ms).
 *
 * @param {string} message - Toast message text
 * @param {string} [type='info'] - Toast type: success | error | warning | info
 * @param {number} [duration=5000] - Auto-hide delay in milliseconds
 */
window.showToast = function(message, type, duration) {
    type = type || 'info';
    duration = duration || 5000;

    var toast = document.getElementById('error-toast');
    var messageEl = document.getElementById('error-message');
    var iconEl = toast ? toast.querySelector('.toast-icon') : null;

    if (toast && messageEl && iconEl) {
        messageEl.textContent = message;
        toast.className = 'error-toast ' + type;
        var icons = { success: '\u2705', error: '\u274C', warning: '\u26A0\uFE0F', info: '\u2139\uFE0F' };
        iconEl.textContent = icons[type] || icons.info;
        toast.style.display = 'block';
        setTimeout(function() { toast.style.display = 'none'; }, duration);
    } else {
        console.warn('[Toast]', message);
    }
};
