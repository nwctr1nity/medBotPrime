require('dotenv').config();

const { Telegraf, Markup } = require('telegraf');
const { Pool } = require('pg');
const { randomUUID } = require('crypto');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_ID = Number(process.env.ADMIN_ID) || 0;
const DATABASE_URL = process.env.DATABASE_URL;

if (!BOT_TOKEN) {
  console.error('ERROR: set BOT_TOKEN in env');
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error('ERROR: set DATABASE_URL in env (Postgres connection string)');
  process.exit(1);
}

const bot = new Telegraf(BOT_TOKEN);

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// db schema init
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS slots (
      id uuid PRIMARY KEY,
      time text NOT NULL,
      start timestamptz NOT NULL,
      "end" timestamptz NOT NULL
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS procedures (
      key text PRIMARY KEY,
      name text NOT NULL
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS requests (
      id uuid PRIMARY KEY,
      user_id bigint NOT NULL,
      username text,
      name text,
      slot_id uuid,
      time text,
      procedure text,
      status text,
      created_at timestamptz,
      pending_move_slot_id uuid,
      pending_move_time text,
      original_slot_id uuid,
      original_slot_time text,
      original_slot_start timestamptz,
      original_slot_end timestamptz,
      prev_status text
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS history (
      id serial PRIMARY KEY,
      user_id bigint NOT NULL,
      date text,
      procedure text,
      status text
    );
  `);

  const res = await pool.query('SELECT COUNT(*) FROM procedures');
  if (res.rows[0].count === '0') {
    const defaults = [
      { key: 'botulinotherapy', name: 'Ботулинотерапия' },
      { key: 'mesoniti', name: 'Мезонити' },
    ];
    for (const p of defaults) {
      await pool.query('INSERT INTO procedures(key, name) VALUES ($1, $2) ON CONFLICT DO NOTHING', [p.key, p.name]);
    }
  }
}
initDb().then(() => console.log('DB initialized')).catch(err => { console.error('DB init error', err); process.exit(1); });

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

  const start = new Date(Date.UTC(year, month - 1, day, sh, sm));
  const end = new Date(Date.UTC(year, month - 1, day, eh, em));

  if (end.getTime() <= start.getTime()) return null;
  return { start, end };
}

function isInPast(date) {
  return date.getTime() < Date.now() - 1000;
}
function intervalsOverlap(aStart, aEnd, bStart, bEnd) {
  return aStart < bEnd && bStart < aEnd;
}

// db functions
async function getAllSlots() {
  const res = await pool.query('SELECT * FROM slots ORDER BY start');
  return res.rows;
}
async function getSlotById(id) {
  const res = await pool.query('SELECT * FROM slots WHERE id=$1', [id]);
  return res.rows[0] || null;
}
async function addSlotToDb(id, time, startIso, endIso) {
  await pool.query('INSERT INTO slots(id, time, start, "end") VALUES ($1,$2,$3,$4)', [id, time, startIso, endIso]);
}
async function deleteSlotById(id) {
  await pool.query('DELETE FROM slots WHERE id=$1', [id]);
}

async function getProcedures() {
  const res = await pool.query('SELECT * FROM procedures ORDER BY name');
  return res.rows;
}
async function addProcedureDb(key, name) {
  await pool.query('INSERT INTO procedures(key, name) VALUES ($1,$2) ON CONFLICT DO NOTHING', [key, name]);
}
async function deleteProcedureDb(key) {
  await pool.query('DELETE FROM procedures WHERE key=$1', [key]);
}

async function addRequestDb(req) {
  await pool.query(
    `INSERT INTO requests(id, user_id, username, name, slot_id, time, procedure, status, created_at)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [req.id, req.userId, req.username, req.name, req.slotId, req.time, req.procedure, req.status, req.createdAt]
  );
}
async function getRequestById(id) {
  const res = await pool.query('SELECT * FROM requests WHERE id=$1', [id]);
  return res.rows[0] || null;
}
async function updateRequest(id, fields) {
  const keys = Object.keys(fields);
  if (keys.length === 0) return;
  const set = keys.map((k, i) => `"${k}" = $${i+2}`).join(', ');
  const values = [id, ...keys.map(k => fields[k])];
  const q = `UPDATE requests SET ${set} WHERE id = $1`;
  await pool.query(q, values);
}
async function getRequestsByStatus(status) {
  const res = await pool.query('SELECT * FROM requests WHERE status=$1 ORDER BY created_at', [status]);
  return res.rows;
}
async function deleteRequestById(id) {
  await pool.query('DELETE FROM requests WHERE id=$1', [id]);
}

async function addHistoryItem(userId, date, procedure, status) {
  await pool.query('INSERT INTO history(user_id, date, procedure, status) VALUES($1,$2,$3,$4)', [userId, date, procedure, status]);
}
async function getHistoryForUser(userId) {
  const res = await pool.query('SELECT * FROM history WHERE user_id=$1 ORDER BY id DESC', [userId]);
  return res.rows;
}

// inmemory states for admins
const adminStates = {}; // { <adminId>: { mode, moveReqId, choosingSlotId } }

async function showRequestsByStatus(ctx, status, label) {
  try {
    const list = await getRequestsByStatus(status);
    if (!list || list.length === 0) {
      try { return await ctx.editMessageText(`${label}: нет заявок.`); } catch (_) { return await ctx.reply(`${label}: нет заявок.`); }
    }

    for (const r of list) {
      const userLink = makeUserLink(r.user_id, r.username, r.name);
      const text =
        `${label}\n` +
        `Клиент: ${userLink}\n` +
        `Время: ${escapeHtml(r.time)}\n` +
        `Процедура: ${escapeHtml(r.procedure || '-')}\n` +
        `Статус: ${escapeHtml(r.status)}`;

      let kb;
      if (status === 'pending') {
        kb = Markup.inlineKeyboard([
          [Markup.button.callback('✔ Подтвердить', `approve_${r.id}`)],
          [Markup.button.callback('🔁 Перенести', `move_${r.id}`)],
          [Markup.button.callback('❌ Отклонить', `reject_${r.id}`)]
        ]);
      } else if (status === 'approved') {
        kb = Markup.inlineKeyboard([
          [Markup.button.callback('✅ Выполнено', `complete_${r.id}`), Markup.button.callback('🚫 Неявка', `no_show_${r.id}`)],
          [Markup.button.callback('🔁 Перенести', `move_${r.id}`), Markup.button.callback('❌ Отклонить', `reject_${r.id}`)]
        ]);
      } else if (status === 'move_pending') {
        kb = Markup.inlineKeyboard([
          [Markup.button.callback('✔ Применить перенос (админ)', `applymove_${r.id}`)],
          [Markup.button.callback('❌ Отменить заявку', `reject_${r.id}`), Markup.button.callback('🚫 Неявка', `no_show_${r.id}`)]
        ]);
      } else if (status === 'rejected' || status === 'completed' || status === 'no_show') {
        kb = Markup.inlineKeyboard([[Markup.button.callback('🗑 Удалить', `delete_${r.id}`)]]);
      } else {
        kb = Markup.inlineKeyboard([]);
      }

      try {
        await ctx.replyWithHTML(text, kb);
      } catch (e) {
        console.error('Failed to send request card:', e);
      }
    }

    try { await ctx.answerCbQuery(); } catch (_) {}
  } catch (e) {
    console.error('showRequestsByStatus error:', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
}

// admin ui
function adminPanelKeyboard() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🟡 Ожидающие', 'req_pending')],
    [Markup.button.callback('🟢 Подтверждённые', 'req_approved')],
    [Markup.button.callback('🔴 Отклонённые', 'req_rejected')],
    [Markup.button.callback('🔵 Ожидающие переноса', 'req_move_pending')],
    [Markup.button.callback('✅ Выполненные', 'req_completed'), Markup.button.callback('🚫 Неявки', 'req_no_show')],
    [Markup.button.callback('🛠 Управлять процедурами', 'manage_procedures')],
    [Markup.button.callback('➕ Добавить слот', 'admin_addslot'), Markup.button.callback('❌ Удалить слот', 'admin_delslot')]
  ]);
}

async function openAdminPanel(ctx) {
  if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
  await ctx.reply('Админ-панель:', adminPanelKeyboard());
  try { await ctx.answerCbQuery(); } catch (_) {}
}

bot.start(async ctx => {
  try {
    const keyboard = [
      ['📅 Свободное время', '📝 Оставить заявку'],
      ['📚 История посещений']
    ];
    if (ctx.from.id === ADMIN_ID) keyboard[0].push('🛠 Открыть панель');
    await ctx.reply('Привет! Я бот записи.\nВыбери действие:', Markup.keyboard(keyboard).resize());
  } catch (e) { console.error('start error', e); }
});

bot.hears('🛠 Открыть панель', ctx => openAdminPanel(ctx));

bot.hears('📅 Свободное время', async ctx => {
  try {
    const slots = await getAllSlots();
    if (!slots || slots.length === 0) return ctx.reply('Свободных интервалов пока нет.');
    let msg = 'Свободные интервалы:\n\n';
    slots.forEach(s => msg += `• ${escapeHtml(s.time)}\n`);
    await ctx.reply(msg);
  } catch (e) { console.error('free slots error', e); }
});

bot.hears('📝 Оставить заявку', async ctx => {
  try {
    const slots = await getAllSlots();
    if (!slots || slots.length === 0) return ctx.reply('Нет доступных интервалов.');
    const buttons = slots.map(s => [Markup.button.callback(s.time, `req_${s.id}`)]);
    await ctx.reply('Выбери интервал:', Markup.inlineKeyboard(buttons));
  } catch (e) { console.error('start request error', e); }
});

bot.action(/req_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    const slotId = ctx.match[1];
    const slot = await getSlotById(slotId);
    if (!slot) return ctx.answerCbQuery('Этот слот уже недоступен', { show_alert: true });

    adminStates[ctx.from.id] = adminStates[ctx.from.id] || {};
    adminStates[ctx.from.id].choosingSlotId = slotId;

    const procs = await getProcedures();
    const procButtons = procs.map(p => [Markup.button.callback(p.name, `proc_${slotId}_${p.key}`)]);
    if (procButtons.length === 0) {
      await ctx.reply('Процедур пока нет. Попросите администратора добавить процедуру.');
    } else {
      await ctx.reply('Выберите процедуру:', Markup.inlineKeyboard(procButtons));
    }
    await ctx.answerCbQuery();
  } catch (e) { console.error('req action error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/^proc_([0-9a-fA-F\-]{36})_(.+)$/u, async ctx => {
  try {
    const slotId = ctx.match[1];
    const procKey = ctx.match[2];

    console.log('proc callback:', { slotId, procKey, from: ctx.from.id });

    const slot = await getSlotById(slotId);
    if (!slot) return ctx.answerCbQuery('Слот стал недоступен', { show_alert: true });

    const procRes = await pool.query('SELECT * FROM procedures WHERE key=$1', [procKey]);
    if (procRes.rowCount === 0) {
      console.warn('Procedure key not found:', procKey);
      return ctx.answerCbQuery('Процедура недоступна', { show_alert: true });
    }
    const proc = procRes.rows[0];
    const procName = proc.name;

    const dupRes = await pool.query(
      `SELECT 1 FROM requests WHERE user_id=$1 AND slot_id=$2 AND status NOT IN ($3,$4,$5) LIMIT 1`,
      [ctx.from.id, slotId, 'rejected', 'completed', 'no_show']
    );
    if (dupRes.rowCount > 0) {
      return ctx.answerCbQuery('Вы уже отправляли заявку на этот слот.', { show_alert: true });
    }

    const req = {
      id: randomUUID(),
      userId: ctx.from.id,
      username: ctx.from.username || null,
      name: ctx.from.first_name || '',
      slotId,
      time: slot.time,
      procedure: procName,
      status: 'pending',
      createdAt: new Date().toISOString()
    };
    await addRequestDb(req);

    await ctx.reply('Заявка отправлена! Ожидайте подтверждения от администратора.');
    try {
      await bot.telegram.sendMessage(
        ADMIN_ID,
        `📩 Новая заявка\nКлиент: ${ctx.from.username ? '@'+ctx.from.username : ctx.from.first_name}\nВремя: ${slot.time}\nПроцедура: ${procName}`,
        { reply_markup: { inline_keyboard: [[{ text: '🛠 Открыть панель', callback_data: 'open_admin_panel' }]] } }
      );
    } catch (notifyErr) {
      console.error('notify admin failed', notifyErr);
    }

    await ctx.answerCbQuery();
  } catch (err) {
    console.error('proc handler error:', err);
    try { await ctx.answerCbQuery('Ошибка при создании заявки'); } catch (_) {}
  }
});

// history client
bot.hears('📚 История посещений', async ctx => {
  try {
    const rows = await getHistoryForUser(ctx.from.id);
    if (!rows || rows.length === 0) return ctx.reply('История пуста.');
    let msg = 'Ваша история:\n\n';
    rows.forEach(h => msg += `• ${escapeHtml(h.date)} — ${escapeHtml(h.procedure)} (${escapeHtml(h.status)})\n`);
    await ctx.reply(msg);
  } catch (e) { console.error('history error', e); }
});

// admin crud
bot.action('manage_procedures', async ctx => {
  if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
  try {
    const procs = await getProcedures();
    const buttons = procs.map(p => [Markup.button.callback(`Удалить ${escapeHtml(p.name)}`, `delproc_${p.key}`)]);
    buttons.push([Markup.button.callback('➕ Добавить процедуру', 'addproc')]);
    await ctx.reply('Список процедур:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('manage_procedures error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('addproc', async ctx => {
  if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addproc' };
  await ctx.reply('Отправьте название процедуры (например: Ботулинотерапия). Я сгенерирую ключ автоматически.');
  await ctx.answerCbQuery();
});

bot.action(/delproc_(.+)/, async ctx => {
  if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
  try {
    const key = ctx.match[1];
    await deleteProcedureDb(key);
    await ctx.reply('Процедура удалена.');
    await ctx.answerCbQuery();
  } catch (e) { console.error('delproc error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

// text handler for admin states (add procedure / add slot)
bot.on('text', async ctx => {
  try {
    const st = adminStates[ctx.from.id];
    if (!st) return;

    const text = ctx.message.text.trim();

    if (st.mode === 'addproc') {
      const rawKey = slugifyName(text);
      const key = rawKey || `proc_${randomUUID().slice(0,8)}`;
      try {
        await addProcedureDb(key, text);
        delete adminStates[ctx.from.id];
        return await ctx.reply(`Процедура "${text}" добавлена (key=${key}).`);
      } catch (err) {
        delete adminStates[ctx.from.id];
        console.error('addProcedure error:', err);
        return await ctx.reply('Не удалось добавить процедуру. Возможно, такой ключ уже существует.');
      }
    }

    if (st.mode === 'addslot') {
      const parsed = parseSlotDateTimeInterval(text);
      if (!parsed) return ctx.reply('Неправильный формат или некорректная дата/время. Формат: 12.12.2025 12:30-14:30');
      if (isInPast(parsed.start)) return ctx.reply('Нельзя создать слот, который начинается в прошлом.');

      const slots = await getAllSlots();
      for (const s of slots) {
        const sStart = new Date(s.start).getTime();
        const sEnd = new Date(s.end).getTime();
        if (intervalsOverlap(parsed.start.getTime(), parsed.end.getTime(), sStart, sEnd)) {
          delete adminStates[ctx.from.id];
          return ctx.reply(`Нельзя создать перекрывающийся слот. Конфликт с: ${s.time}`);
        }
      }

      const id = randomUUID();
      await addSlotToDb(id, text, parsed.start.toISOString(), parsed.end.toISOString());
      delete adminStates[ctx.from.id];
      return ctx.reply(`Интервал "${text}" добавлен.`);
    }
  } catch (e) {
    console.error('text handler error', e);
    try { await ctx.reply('Ошибка при обработке.'); } catch (_) {}
  }
});

bot.action('admin_addslot', async ctx => {
  if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addslot' };
  await ctx.reply('Введите интервал в формате: 12.12.2025 12:30-14:30\nИли /cancel чтобы отменить.');
  await ctx.answerCbQuery();
});

bot.action('admin_delslot', async ctx => {
  if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
  try {
    const slots = await getAllSlots();
    if (!slots || slots.length === 0) return ctx.reply('Слотов нет.');
    const buttons = slots.map(s => [Markup.button.callback(s.time, `delslot_${s.id}`)]);
    await ctx.reply('Выберите слот для удаления:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('admin_delslot error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/delslot_([0-9a-fA-F\-]{36})/, async ctx => {
  if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
  try {
    const id = ctx.match[1];
    await deleteSlotById(id);
    await ctx.answerCbQuery('Удалено');
  } catch (e) { console.error('delslot error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('req_pending', async ctx => { if (ctx.from.id === ADMIN_ID) await showRequestsByStatus(ctx, 'pending', '🟡 Ожидающие'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_approved', async ctx => { if (ctx.from.id === ADMIN_ID) await showRequestsByStatus(ctx, 'approved', '🟢 Подтверждённые'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_rejected', async ctx => { if (ctx.from.id === ADMIN_ID) await showRequestsByStatus(ctx, 'rejected', '🔴 Отклонённые'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_move_pending', async ctx => { if (ctx.from.id === ADMIN_ID) await showRequestsByStatus(ctx, 'move_pending', '🔵 Ожидающие переноса'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_completed', async ctx => { if (ctx.from.id === ADMIN_ID) await showRequestsByStatus(ctx, 'completed', '✅ Выполненные'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_no_show', async ctx => { if (ctx.from.id === ADMIN_ID) await showRequestsByStatus(ctx, 'no_show', '🚫 Неявки'); else ctx.answerCbQuery('Нет доступа'); });

bot.action('open_admin_panel', async ctx => { if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа'); try { await openAdminPanel(ctx); } catch (e) { console.error('open panel error', e); } });

// approve / reject / delete (admin)
bot.action(/approve_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await getRequestById(reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');
    if (req.status === 'approved') return ctx.answerCbQuery('Уже подтверждена');

    if (req.slot_id) {
      const slot = await getSlotById(req.slot_id);
      if (slot) {
        await deleteSlotById(req.slot_id);
        await updateRequest(reqId, {
          original_slot_id: slot.id,
          original_slot_time: slot.time,
          original_slot_start: slot.start ? slot.start.toISOString() : null,
          original_slot_end: slot.end ? slot.end.toISOString() : null
        });
      }
    }
    await updateRequest(reqId, { status: 'approved' });

    try { await ctx.editMessageText('✔ Заявка подтверждена'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `✔ Ваша запись на ${req.time} подтверждена!`); } catch (e) { console.error('notify user approval error', e); }
    await ctx.answerCbQuery();
  } catch (e) { console.error('approve error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/reject_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await getRequestById(reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    await updateRequest(reqId, { status: 'rejected' });
    try { await ctx.editMessageText('❌ Заявка отклонена'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `❌ Ваша заявка на ${req.time} была отклонена.`); } catch (e) { console.error('notify reject error', e); }
    await ctx.answerCbQuery();
  } catch (e) { console.error('reject error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/delete_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    await deleteRequestById(reqId);
    try { await ctx.editMessageText('🗑 Заявка удалена.'); } catch (_) {}
    await ctx.answerCbQuery();
  } catch (e) { console.error('delete error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

// complete / no_show (admin)
bot.action(/complete_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await getRequestById(reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    await updateRequest(reqId, { status: 'completed' });
    await addHistoryItem(req.user_id, req.time, req.procedure || 'Процедура', 'Выполнено');

    try { await ctx.editMessageText('✅ Отмечено как выполнено'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `✅ Ваша запись на ${req.time} помечена как выполненная.`); } catch (e) { console.error('notify complete error', e); }
    try {
      await bot.telegram.sendMessage(ADMIN_ID,
        `✅ Клиент ${makeUserLink(req.user_id, req.username, req.name)} — выполнено.\nВремя: ${escapeHtml(req.time)}\nПроцедура: ${escapeHtml(req.procedure || '-')}`,
        { parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: '🛠 Открыть панель', callback_data: 'open_admin_panel' }]] } }
      );
    } catch (e) { console.error('admin notify complete', e); }

    await ctx.answerCbQuery();
  } catch (e) { console.error('complete error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/no_show_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await getRequestById(reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    await updateRequest(reqId, { status: 'no_show' });
    await addHistoryItem(req.user_id, req.time, req.procedure || 'Процедура', 'Неявка');

    try { await ctx.editMessageText('🚫 Отмечено как неявка'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `🚫 Ваша запись на ${req.time} помечена как неявка.`); } catch (e) { console.error('notify no-show error', e); }
    try {
      await bot.telegram.sendMessage(ADMIN_ID,
        `🚫 Клиент ${makeUserLink(req.user_id, req.username, req.name)} — не явился.\nВремя: ${escapeHtml(req.time)}\nПроцедура: ${escapeHtml(req.procedure || '-')}`,
        { parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: '🛠 Открыть панель', callback_data: 'open_admin_panel' }]] } }
      );
    } catch (e) { console.error('admin notify no-show', e); }

    await ctx.answerCbQuery();
  } catch (e) { console.error('no_show error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

// moving requests
bot.action(/move_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    const reqId = ctx.match[1];
    const slots = await getAllSlots();
    if (!slots || slots.length === 0) return ctx.answerCbQuery('Нет свободных интервалов');
    adminStates[ctx.from.id] = { moveReqId: reqId };
    const buttons = slots.map(s => [Markup.button.callback(s.time, `moveTo_${s.id}`)]);
    await ctx.reply('Выберите новое время:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('move_ error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/moveTo_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
    const slotId = ctx.match[1];
    const state = adminStates[ctx.from.id];
    const reqId = state && state.moveReqId;
    if (!reqId) { delete adminStates[ctx.from.id]; return ctx.answerCbQuery('Нет активного переноса'); }

    const req = await getRequestById(reqId);
    const slot = await getSlotById(slotId);
    if (!req || !slot) { delete adminStates[ctx.from.id]; return ctx.answerCbQuery('Ошибка: заявка или слот не найдены'); }

    await updateRequest(reqId, {
      pending_move_slot_id: slot.id,
      pending_move_time: slot.time,
      prev_status: req.status,
      status: 'move_pending'
    });

    try { await ctx.editMessageText('📨 Запрос на перенос отправлен (клиенту)'); } catch (_) {}

    const text = `❗ Вам предлагают изменить время записи:\n\nСтарое время: ${req.time}\nНовое время: ${slot.time}\n\nПодтвердить перенос?`;
    try {
      await bot.telegram.sendMessage(req.user_id, text, {
        reply_markup: {
          inline_keyboard: [
            [{ text: 'Да', callback_data: `clientMoveYes_${req.id}` }],
            [{ text: 'Нет', callback_data: `clientMoveNo_${req.id}` }]
          ]
        }
      });
    } catch (e) { console.error('send move to client failed', e); }

    delete adminStates[ctx.from.id];
    await ctx.answerCbQuery();
  } catch (e) { console.error('moveTo_ error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/clientMoveYes_([0-9a-fA-F\-]{36})/, async ctx => {
  const reqId = ctx.match[1];
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const reqRes = await client.query('SELECT * FROM requests WHERE id=$1 FOR UPDATE', [reqId]);
    const req = reqRes.rows[0];
    if (!req || !req.pending_move_slot_id) {
      await client.query('ROLLBACK');
      return ctx.answerCbQuery('Нет запроса на перенос');
    }

    const slotRes = await client.query('SELECT * FROM slots WHERE id=$1 FOR UPDATE', [req.pending_move_slot_id]);
    const newSlot = slotRes.rows[0];
    if (!newSlot) {
      await client.query(
        `UPDATE requests SET pending_move_slot_id = NULL, pending_move_time = NULL, status = COALESCE(prev_status, status), prev_status = NULL
         WHERE id = $1`, [reqId]
      );
      await client.query('COMMIT');
      return ctx.answerCbQuery('Выбранный слот уже недоступен');
    }

    if (req.prev_status === 'approved' && req.original_slot_id && (req.original_slot_start || req.original_slot_end)) {
      try {
        await client.query(
          `INSERT INTO slots(id, time, start, "end") VALUES ($1,$2,$3,$4)
           ON CONFLICT (id) DO NOTHING`,
          [req.original_slot_id, req.original_slot_time, req.original_slot_start, req.original_slot_end]
        );
      } catch (e) {
        console.error('Failed to re-add original slot (best-effort):', e);
      }
    }

    await client.query('DELETE FROM slots WHERE id=$1', [newSlot.id]);

    await client.query(
      `UPDATE requests SET slot_id = $2, time = $3,
         status = COALESCE(prev_status, $4),
         prev_status = NULL,
         pending_move_slot_id = NULL,
         pending_move_time = NULL
       WHERE id = $1`,
      [reqId, newSlot.id, newSlot.time, 'approved']
    );

    await client.query('COMMIT');

    try { await ctx.editMessageText('✔ Перенос подтверждён!'); } catch (_) {}
    try {
      await bot.telegram.sendMessage(ADMIN_ID,
        `✔ Клиент подтвердил перенос. Новое время: ${escapeHtml(newSlot.time)}`,
        { reply_markup: { inline_keyboard: [[{ text: '🛠 Открыть панель', callback_data: 'open_admin_panel' }]] } }
      );
    } catch (e) {
      console.error('notify admin move confirmed error:', e);
    }

    await ctx.answerCbQuery();
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('clientMoveYes transaction error:', err);
    try { await ctx.answerCbQuery('Ошибка при применении переноса'); } catch (_) {}
  } finally {
    client.release();
  }
});

bot.action(/clientMoveNo_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    const reqId = ctx.match[1];
    const req = await getRequestById(reqId);
    if (!req || !req.pending_move_slot_id) return ctx.answerCbQuery('Нет запроса на перенос');

    await updateRequest(reqId, { pending_move_slot_id: null, pending_move_time: null, status: req.prev_status || req.status, prev_status: null });
    try { await ctx.editMessageText('❌ Вы отклонили перенос.'); } catch (_) {}
    try { await bot.telegram.sendMessage(ADMIN_ID, `❌ Клиент ${makeUserLink(req.user_id, req.username, req.name)} отклонил перенос.`, { parse_mode: 'HTML' }); } catch (e) { console.error('notify admin reject move', e); }
    await ctx.answerCbQuery();
  } catch (e) { console.error('clientMoveNo error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/applymove_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (ctx.from.id !== ADMIN_ID) return ctx.answerCbQuery('Нет доступа');
    await ctx.answerCbQuery('Принудительное подтверждение переноса отключено. Клиент должен подтвердить перенос сам или админ может отметить неявку/отклонить заявку.');
  } catch (e) {
    console.error('applymove stub error:', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});

// global error handling
bot.catch((err, ctx) => {
  console.error(`Bot error for update ${ctx.update?.update_id}:`, err);
});

// graceful stop
process.once('SIGINT', () => { bot.stop('SIGINT'); pool.end(); });
process.once('SIGTERM', () => { bot.stop('SIGTERM'); pool.end(); });

// start
bot.launch().then(() => console.log('Bot started (Postgres)!')).catch(err => console.error('Bot launch error:', err));