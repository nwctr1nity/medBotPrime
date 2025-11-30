function escapeHtml(str) {
  if (!str && str !== 0) return '';
  return String(str).replace(/[&<>"]/g, ch => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[ch]));
}

function makeUserLink(userId, username, name) {
  if (username) return `<a href="tg://user?id=${userId}">@${escapeHtml(username)}</a>`;
  return `<a href="tg://user?id=${userId}">${escapeHtml(name || 'User')}</a>`;
}

function slugifyName(name) {
  if (!name || !String(name).trim()) return '';
  const s = String(name).toLowerCase().trim()
    .replace(/\s+/g, '_')
    .replace(/[^\p{L}\p{N}_-]+/gu, '')
    .replace(/^_+|_+$/g, '')
    .replace(/^-+|-+$/g, '');
  return s;
}

function parseSlotDateTimeInterval(text) {
  const m = text.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/);
  if (!m) return null;
  const day = Number(m[1]), month = Number(m[2]), year = Number(m[3]);
  const sh = Number(m[4]), sm = Number(m[5]), eh = Number(m[6]), em = Number(m[7]);

  if (month < 1 || month > 12) return null;
  if (day < 1 || day > 31) return null;
  if (sh < 0 || sh > 23 || eh < 0 || eh > 23) return null;
  if (sm < 0 || sm > 59 || em < 0 || em > 59) return null;

  const start = new Date(Date.UTC(year, month - 1, day, sh, sm, 0, 0));
  const end = new Date(Date.UTC(year, month - 1, day, eh, em, 0, 0));

  if (end.getTime() <= start.getTime()) return null;
  return { start, end };
}

function parseDateDDMMYYYY(text) {
  const m = text.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (!m) return null;
  const day = Number(m[1]), month = Number(m[2]), year = Number(m[3]);
  if (month < 1 || month > 12) return null;
  if (day < 1 || day > 31) return null;
  return { year, month, day };
}

function isInPast(date) {
  return date.getTime() < Date.now() - 1000;
}

function intervalsOverlap(aStart, aEnd, bStart, bEnd) {
  return aStart < bEnd && bStart < aEnd;
}

module.exports = {
  escapeHtml,
  makeUserLink,
  slugifyName,
  parseSlotDateTimeInterval,
  parseDateDDMMYYYY,
  isInPast,
  intervalsOverlap
};