const dotenv = require('dotenv');
dotenv.config();

const { Telegraf, Markup } = require('telegraf');
const express = require('express');
const { Pool } = require('pg');
const { randomUUID, createHash } = require('crypto');

const utils = require('./utils/utils');
const db = require('./utils/db');
const notifications = require('./utils/notifications');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_ID = Number(process.env.ADMIN_ID) || 0;
const DATABASE_URL = process.env.DATABASE_URL;
const PORT = Number(process.env.PORT) || 3000;
const WEBHOOK_URL = process.env.WEBHOOK_URL || process.env.RENDER_EXTERNAL_URL || null;

const ADMIN_IDS_RAW = process.env.ADMIN_IDS || String(process.env.ADMIN_ID || ADMIN_ID);
const ADMIN_IDS = new Set(
  ADMIN_IDS_RAW.split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .map(s => Number(s))
    .filter(n => !Number.isNaN(n))
);

function isAdmin(ctxOrId) {
  const id = (typeof ctxOrId === 'object' && ctxOrId?.from?.id) ? ctxOrId.from.id : ctxOrId;
  const n = Number(id);
  if (Number.isNaN(n)) return false;
  if (ADMIN_IDS.has(n)) return true;
  if (ADMIN_ID && n === ADMIN_ID) return true;
  return false;
}

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

(async () => {
  try {
    await db.initDb(pool);
    console.log('DB initialized');
  } catch (err) {
    console.error('DB init error', err);
    process.exit(1);
  }
})();

const adminStates = {};

// --- HELPERS FOR SAFE CALLBACK KEYS ---
function shortHash(s) {
  return createHash('sha256').update(String(s)).digest('hex').slice(0, 8);
}

// Telegram limit: callback_data max 64 bytes
function makeCallbackKey(prefix, key) {
  const full = `${prefix}${key}`;
  if (Buffer.byteLength(full, 'utf8') <= 64) return full;
  return `${prefix}hash_${shortHash(key)}`;
}

// Resolve token like "hash_abcdef12" -> real procedure key by scanning DB
async function resolveProcedureKeyMaybeHashed(pool, token) {
  if (!token || !token.startsWith('hash_')) return token;
  const wanted = token.slice(5);
  const procs = await db.getProcedures(pool);
  const found = procs.find(p => shortHash(p.key) === wanted);
  return found ? found.key : null;
}
// ----------------------------------------

bot.start(async ctx => {
  try {
    const keyboard = [
      ['📅 Свободное время', '📝 Оставить заявку'],
      ['📚 История посещений', 'Обратная связь']
    ];
    if (isAdmin(ctx)) keyboard[0].push('🛠 Открыть панель');
    await ctx.reply('Привет! Я бот для записи! Чтобы записаться нажмите на кнопку "Оставить заявку" и дождитесь подтверждения специалиста.\n\nВажно: сразу оставить заявку можно только на самый ранний слот, это сделано для формирования более целостного расписания. Если ранние часы вам не подходят, то вам доступна опция занять более поздний слот, однако заявка будет сформирована только в том случае, если за 12 часов до записи не останется ни одного более раннего незанятого слота.\n\nТакже напоминаю, что данный бот находится в стадии открытого тестирования. Если у вас возникла проблема или опыт использования бота вас не удовлетворил, то вы можете поделиться своим мнением, нажав на кнопку "Обратная связь". Этим вы очень сильно поможете в дальнейшей разработке и улучшении бота. Благодарим за понимание!', Markup.keyboard(keyboard).resize());
  } catch (e) { console.error('start error', e); }
});

bot.hears('🛠 Открыть панель', ctx => openAdminPanel(ctx));

bot.hears('📅 Свободное время', async ctx => {
  try {
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.reply('Свободных интервалов пока нет.');
    const slot = await db.getEarliestSlot(pool);
    if (!slot) return ctx.reply('Свободных интервалов пока нет.');
    await ctx.reply(`Ближайший свободный интервал:\n• ${utils.escapeHtml(slot.time)}`);
  } catch (e) { console.error('free slots error', e); }
});

bot.hears('📝 Оставить заявку', async ctx => {
  try {
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.reply('Свободных интервалов пока нет.');
    const slot = await db.getEarliestSlot(pool);
    if (!slot) return ctx.reply('Нет доступных интервалов.');
    const buttons = [[Markup.button.callback(slot.time, `req_${slot.id}`)]];
    buttons.push([Markup.button.callback('Выбрать более поздний слот', 'choose_later')]);
    await ctx.reply('Выбери интервал:', Markup.inlineKeyboard(buttons));
  } catch (e) { console.error('start request error', e); }
});

bot.hears('Обратная связь', async ctx => {
  try {
    adminStates[ctx.from.id] = { mode: 'feedback' };
    await ctx.reply('Можете оставить свой комментарий, связанный с опытом использования моего бота.');
  } catch (e) { console.error('feedback start error', e); }
});

bot.action('choose_later', async ctx => {
  try {
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.answerCbQuery('Нет доступа', { show_alert: true });
    const slots = await db.getAllSlots(pool);
    if (!slots || slots.length === 0) return ctx.answerCbQuery('Нет доступных интервалов', { show_alert: true });
    const buttons = slots.map(s => [Markup.button.callback(s.time, `req_${s.id}`)]);
    await ctx.reply('Выберите желаемый интервал (поздний выбор будет резервировать слот для вас):', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('choose_later error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/req_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.answerCbQuery('Свободных интервалов пока нет.', { show_alert: true });
    const slotId = ctx.match[1];
    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery('Этот слот уже недоступен', { show_alert: true });

    adminStates[ctx.from.id] = adminStates[ctx.from.id] || {};
    adminStates[ctx.from.id].choosingSlotId = slotId;

    const procs = await db.getProcedures(pool);
    // build safe callback keys for procedures; if a procedure key is too long we will use hashed token
    const procButtons = procs.map(p => {
      const cb = makeCallbackKey(`proc_${slotId}_`, p.key);
      return [Markup.button.callback(p.name, cb)];
    });
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
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.answerCbQuery('Свободных интервалов пока нет.', { show_alert: true });
    const slotId = ctx.match[1];
    let procKeyToken = ctx.match[2];

    // If this token was a hashed token (hash_xxx), resolve to real key
    if (procKeyToken && procKeyToken.startsWith('hash_')) {
      const resolved = await resolveProcedureKeyMaybeHashed(pool, procKeyToken);
      if (!resolved) return ctx.answerCbQuery('Процедура недоступна', { show_alert: true });
      procKeyToken = resolved;
    }

    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery('Слот стал недоступен', { show_alert: true });

    const proc = await db.getProcedureByKey(pool, procKeyToken);
    if (!proc) return ctx.answerCbQuery('Процедура недоступна', { show_alert: true });

    const dup = await db.checkDuplicateRequest(pool, ctx.from.id, slotId);
    if (dup) return ctx.answerCbQuery('Вы уже отправляли заявку на этот слот.', { show_alert: true });

    const earliest = await db.getEarliestSlot(pool);
    let isEarliest = earliest && earliest.id === slot.id;

    try { await db.deleteSlotById(pool, slot.id); } catch (e) { console.error('Failed to delete slot while reserving:', e); }

    const status = isEarliest ? 'pending' : 'reserved_later';

    const req = {
      id: randomUUID(),
      userId: ctx.from.id,
      username: ctx.from.username || null,
      name: ctx.from.first_name || '',
      slotId: slot.id,
      time: slot.time,
      procedure: proc.name,
      status: status,
      createdAt: new Date().toISOString(),
      original_slot_id: slot.id,
      original_slot_time: slot.time,
      original_slot_start: slot.start,
      original_slot_end: slot.end
    };
    await db.addRequestDb(pool, req);

    if (status === 'pending') {
      await ctx.reply('Заявка отправлена! Ожидайте подтверждения от администратора.');
      try { await db.sendToAdmins(pool, bot, `📩 Новая заявка\nКлиент: ${ctx.from.username ? '@'+ctx.from.username : ctx.from.first_name}\nВремя: ${slot.time}\nПроцедура: ${proc.name}`); } catch (notifyErr) { console.error('notify admin failed', notifyErr); }
    } else {
      await ctx.reply('Слот зарезервирован за вами. Если более ранние слоты займут другие клиенты и до записи останется менее 3 часов, ваша заявка автоматически будет сформирована и отправлена на подтверждение администратора.');
      try { await db.sendToAdmins(pool, bot, `🕒 Резерв позднего слота\nКлиент: ${ctx.from.username ? '@'+ctx.from.username : ctx.from.first_name}\nРезерв: ${slot.time}\nПроцедура: ${proc.name}`); } catch (notifyErr) { console.error('notify admin failed', notifyErr); }
    }

    await ctx.answerCbQuery();
  } catch (err) {
    console.error('proc handler error:', err);
    try { await ctx.answerCbQuery('Ошибка при создании заявки'); } catch (_) {}
  }
});

bot.on('text', async ctx => {
  try {
    const st = adminStates[ctx.from.id];
    const text = ctx.message.text.trim();

    if (st && st.mode === 'feedback') {
      const uname = ctx.from.username ? '@' + ctx.from.username : ctx.from.first_name;
      try {
        await db.sendToAdmins(pool, bot, `📝 Обратная связь от ${uname}:\n\n${text}`);
      } catch (e) { console.error('notify admin feedback', e); }
      delete adminStates[ctx.from.id];
      return await ctx.reply('Спасибо! Ваше сообщение отправлено администраторам.');
    }

    if (!st) return;

    if (st.mode === 'addproc') {
      // Generate short unique key in format proc_<8hex>
      let key;
      // ensure uniqueness (very small loop because collisions are unlikely)
      do {
        key = `proc_${randomUUID().slice(0,8)}`;
      } while (await db.getProcedureByKey(pool, key));

      try {
        await db.addProcedureDb(pool, key, text);
        delete adminStates[ctx.from.id];
        return await ctx.reply(`Процедура "${text}" добавлена (key=${key}).`);
      } catch (err) {
        delete adminStates[ctx.from.id];
        console.error('addProcedure error:', err);
        return await ctx.reply('Не удалось добавить процедуру. Возможно, произошла ошибка базы данных.');
      }
    }

    if (st.mode === 'addslot') {
      const parsed = utils.parseSlotDateTimeInterval(text);
      if (!parsed) return ctx.reply('Неправильный формат или некорректная дата/время. Формат: DD.MM.YYYY 00:00-23:59');
      if (utils.isInPast(parsed.start)) return ctx.reply('Нельзя создать слот, который начинается в прошлом.');
      const slots = await db.getAllSlots(pool);
      for (const s of slots) {
        const sStart = new Date(s.start).getTime();
        const sEnd = new Date(s.end).getTime();
        if (utils.intervalsOverlap(parsed.start.getTime(), parsed.end.getTime(), sStart, sEnd)) {
          delete adminStates[ctx.from.id];
          return ctx.reply(`Нельзя создать перекрывающийся слот. Конфликт с: ${s.time}`);
        }
      }
      const id = randomUUID();
      await db.addSlotToDb(pool, id, text, parsed.start.toISOString(), parsed.end.toISOString());
      delete adminStates[ctx.from.id];
      return ctx.reply(`Интервал "${text}" добавлен.`);
    }

    if (st.mode === 'addblack') {
      const uname = text.trim().replace(/^@/, '').toLowerCase();
      if (!uname) {
        delete adminStates[ctx.from.id];
        return ctx.reply('Неверное имя пользователя.');
      }
      await db.addToBlacklist(pool, uname);
      delete adminStates[ctx.from.id];
      return ctx.reply(`Пользователь @${uname} добавлен в черный список.`);
    }

    if (st.mode === 'delblack') {
      const uname = text.trim().replace(/^@/, '').toLowerCase();
      if (!uname) {
        delete adminStates[ctx.from.id];
        return ctx.reply('Неверное имя пользователя.');
      }
      await db.removeFromBlacklist(pool, uname);
      delete adminStates[ctx.from.id];
      return ctx.reply(`Пользователь @${uname} удалён из черного списка.`);
    }

    if (st.mode === 'applypattern_wait_date') {
      const d = utils.parseDateDDMMYYYY(text);
      if (!d) {
        delete adminStates[ctx.from.id];
        return ctx.reply('Неверный формат даты. Ожидается DD.MM.YYYY');
      }
      const dateISO = `${d.year}-${String(d.month).padStart(2,'0')}-${String(d.day).padStart(2,'0')}`;
      adminStates[ctx.from.id] = { mode: 'applypattern_choose', apply_date: dateISO };
    
      const pats = await db.getPatternsDb(pool);
      if (!pats || pats.length === 0) {
        delete adminStates[ctx.from.id];
        return ctx.reply('Шаблонов нет. Сначала добавьте шаблон.');
      }
    
      const buttons = pats.map(p => [ Markup.button.callback(p.name + (p.intervals ? ` (${p.intervals})` : ''), `applypattern_date_${p.id}`) ]);
      await ctx.reply('Выберите шаблон для применения на указанную дату:', Markup.inlineKeyboard(buttons));
      return;
    }

    if (st.mode === 'addpattern_wait_name') {
      adminStates[ctx.from.id] = { mode: 'addpattern_wait_intervals', pattern_name: text };
      return ctx.reply('Отправьте интервалы шаблона в формате HH:MM-HH:MM,HH:MM-HH:MM (через запятую).');
    }

    if (st.mode === 'addpattern_wait_intervals') {
      const name = st.pattern_name || 'Шаблон';
      const intervals = text.trim();
      const pat = { id: randomUUID(), name, intervals };
      try {
        await db.addPatternDb(pool, pat);
        delete adminStates[ctx.from.id];
        return ctx.reply(`Шаблон "${name}" добавлен.`);
      } catch (e) {
        delete adminStates[ctx.from.id];
        console.error('addpattern error', e);
        return ctx.reply('Ошибка при добавлении шаблона.');
      }
    }

  } catch (e) {
    console.error('text handler error', e);
    try { await ctx.reply('Ошибка при обработке.'); } catch (_) {}
  }
});

bot.action('admin_addslot', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addslot' };
  await ctx.reply('Введите интервал в формате: DD.MM.YYYY 00:00-23:59\nИли /cancel чтобы отменить.');
  await ctx.answerCbQuery();
});

bot.action('admin_delslot', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const slots = await db.getAllSlots(pool);
    if (!slots || slots.length === 0) return ctx.reply('Слотов нет.');
    const buttons = slots.map(s => [Markup.button.callback(s.time, `delslot_${s.id}`)]);
    await ctx.reply('Выберите слот для удаления:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('admin_delslot error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/delslot_([0-9a-fA-F\-]{36})/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const id = ctx.match[1];
    await db.deleteSlotById(pool, id);
    await ctx.answerCbQuery('Удалено');
  } catch (e) { console.error('delslot error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('req_pending', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'pending', '🟡 Ожидающие'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_approved', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'approved', '🟢 Подтверждённые'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_rejected', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'rejected', '🔴 Отклонённые'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_move_pending', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'move_pending', '🔵 Ожидающие переноса'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_completed', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'completed', '✅ Выполненные'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_no_show', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'no_show', '🚫 Неявки'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_reserved', async ctx => { if (isAdmin(ctx)) await showReservedRequests(ctx); else ctx.answerCbQuery('Нет доступа'); });

async function showRequestsByStatus(ctx, status, label) {
  try {
    const list = await db.getRequestsByStatus(pool, status);
    if (!list || list.length === 0) {
      try { return await ctx.editMessageText(`${label}: нет заявок.`); } catch (_) { return await ctx.reply(`${label}: нет заявок.`); }
    }
    for (const r of list) {
      const userLink = utils.makeUserLink(r.user_id, r.username, r.name);
      const text = `${label}\nКлиент: ${userLink}\nВремя: ${utils.escapeHtml(r.time)}\nПроцедура: ${utils.escapeHtml(r.procedure || '-')}\nСтатус: ${utils.escapeHtml(r.status)}`;
      let kb;
      if (status === 'pending') {
        kb = Markup.inlineKeyboard([
          [Markup.button.callback('✔ Подтвердить', `approve_${r.id}`), Markup.button.callback('❌ Отклонить', `reject_${r.id}`)],
          [Markup.button.callback('🔁 Перенести', `move_${r.id}`)]
        ]);
      } else if (status === 'approved') {
        kb = Markup.inlineKeyboard([
          [Markup.button.callback('✅ Выполнено', `complete_${r.id}`), Markup.button.callback('🚫 Неявка', `no_show_${r.id}`)],
          [Markup.button.callback('🔁 Перенести', `move_${r.id}`), Markup.button.callback('❌ Отклонить', `reject_${r.id}`)]
        ]);
      } else if (status === 'move_pending') {
        kb = Markup.inlineKeyboard([
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

async function showReservedRequests(ctx) {
  try {
    const list = await db.getReservedRequests(pool);
    if (!list || list.length === 0) {
      try { return await ctx.editMessageText(`🔷 Зарезервированные: нет заявок.`); } catch (_) { return await ctx.reply(`🔷 Зарезервированные: нет заявок.`); }
    }
    for (const r of list) {
      const userLink = utils.makeUserLink(r.user_id, r.username, r.name);
      const text = `🔷 Зарезервировано\nКлиент: ${userLink}\nРезерв: ${utils.escapeHtml(r.original_slot_time || r.time)}\nПроцедура: ${utils.escapeHtml(r.procedure || '-')}\nСтатус: ${utils.escapeHtml(r.status)}`;
      const kb = Markup.inlineKeyboard([
        [Markup.button.callback('✔ Подтвердить (сделать заявкой)', `confirm_reserved_${r.id}`)],
        [Markup.button.callback('❌ Отменить', `reject_${r.id}`)]
      ]);
      try {
        await ctx.replyWithHTML(text, kb);
      } catch (e) {
        console.error('Failed to send reserved card:', e);
      }
    }
    try { await ctx.answerCbQuery(); } catch (_) {}
  } catch (e) {
    console.error('showReservedRequests error:', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
}

function adminPanelKeyboard() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🟡 Ожидающие', 'req_pending')],
    [Markup.button.callback('🟢 Подтверждённые', 'req_approved')],
    [Markup.button.callback('🔴 Отклонённые', 'req_rejected')],
    [Markup.button.callback('🔵 Ожидающие переноса', 'req_move_pending')],
    [Markup.button.callback('🔷 Зарезервированные', 'req_reserved')],
    [Markup.button.callback('✅ Выполненные', 'req_completed'), Markup.button.callback('🚫 Неявки', 'req_no_show')],
    [Markup.button.callback('🛠 Управлять процедурами', 'manage_procedures')],
    [Markup.button.callback('⚠️ Черный список', 'manage_blacklist')],
    [Markup.button.callback('📅 Шаблоны', 'manage_patterns')],
    [Markup.button.callback('➕ Добавить слот', 'admin_addslot'), Markup.button.callback('❌ Удалить слот', 'admin_delslot')]
  ]);
}

async function openAdminPanel(ctx) {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  await ctx.reply('Админ-панель:', adminPanelKeyboard());
  try { await ctx.answerCbQuery(); } catch (_) {}
}

bot.action(/^(approve|reject|delete)_([0-9a-fA-F\-]{36})$/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const cmd = ctx.match[1];
    const reqId = ctx.match[2];
    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    if (cmd === 'approve') {
      await db.updateRequest(pool, reqId, { status: 'approved', notification_20_sent: false, notification_1h_sent: false });
      try { await ctx.editMessageText('✔ Заявка подтверждена'); } catch (_) {}
      try { await bot.telegram.sendMessage(req.user_id, `✔ Ваша запись на ${req.time} подтверждена!`); } catch (e) {}
      await ctx.answerCbQuery();
      return;
    }

    if (cmd === 'reject') {
      if (req.original_slot_id && req.original_slot_time && (req.original_slot_start || req.original_slot_end)) {
        try { await db.addSlotToDb(pool, req.original_slot_id, req.original_slot_time, req.original_slot_start, req.original_slot_end); } catch (e) {}
      }
      await db.updateRequest(pool, reqId, { status: 'rejected' });
      try { await ctx.editMessageText('❌ Заявка отклонена'); } catch (_) {}
      try { await bot.telegram.sendMessage(req.user_id, `❌ Ваша заявка на ${req.time} была отклонена.`); } catch (e) {}
      await ctx.answerCbQuery();
      return;
    }

    if (cmd === 'delete') {
      await db.deleteRequestById(pool, reqId);
      try { await ctx.editMessageText('🗑 Заявка удалена.'); } catch (_) {}
      await ctx.answerCbQuery();
      return;
    }

    await ctx.answerCbQuery();
  } catch (e) {
    console.error('approve/reject/delete handler error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});

bot.action(/confirm_reserved_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const id = ctx.match[1];
    const req = await db.getRequestById(pool, id);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');
    await db.updateRequest(pool, id, { status: 'pending' });
    try { await ctx.editMessageText('✔ Резерв переведён в заявку'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `Ваша резервная заявка на ${req.original_slot_time || req.time} переведена в заявку и ожидает подтверждения администратора.`); } catch (e) {}
    try { await db.sendToAdmins(pool, bot, `📩 Резерв переведён в заявку вручную\nКлиент: ${req.username ? '@'+req.username : req.name}\nВремя: ${req.original_slot_time || req.time}\nПроцедура: ${req.procedure || '-'}`); } catch (e) {}
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('confirm_reserved error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});

bot.action('manage_procedures', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const procs = await db.getProcedures(pool);
    const buttons = procs.map(p => {
      // button text plain (no HTML entities), safe callback key
      const cb = makeCallbackKey('delproc_', p.key);
      return [Markup.button.callback(`Удалить ${p.name}`, cb)];
    });
    buttons.push([Markup.button.callback('➕ Добавить процедуру', 'addproc')]);
    await ctx.reply('Список процедур:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('manage_procedures error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('addproc', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addproc' };
  await ctx.reply('Отправьте название процедуры (например: Ботулинотерапия). Ключ будет сгенерирован автоматически (proc_<8hex>).');
  await ctx.answerCbQuery();
});

bot.action(/delproc_(.+)/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    let key = ctx.match[1];
    if (key && key.startsWith('hash_')) {
      const resolved = await resolveProcedureKeyMaybeHashed(pool, key);
      if (!resolved) {
        await ctx.answerCbQuery('Не удалось найти процедуру для удаления', { show_alert: true });
        return;
      }
      key = resolved;
    }
    await db.deleteProcedureDb(pool, key);
    await ctx.reply('Процедура удалена.');
    await ctx.answerCbQuery();
  } catch (e) { console.error('delproc error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('manage_patterns', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const patterns = await db.getPatternsDb(pool);
    const buttons = (patterns || []).map(p => [Markup.button.callback(`Удалить ${p.name}`, `delpattern_${p.id}`)]);
    buttons.push([Markup.button.callback('➕ Добавить шаблон', 'addpattern')]);
    buttons.push([Markup.button.callback('🗓 Применить шаблон на дату', 'applypattern_start')]);
    await ctx.reply('Шаблоны расписания:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('manage_patterns error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('addpattern', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addpattern_wait_name' };
  await ctx.reply('Отправьте название шаблона:');
  await ctx.answerCbQuery();
});

// --- NEW: present 7 nearest dates as buttons for applying pattern ---
function formatDateLabel(d) {
  return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()}`;
}
function isoDateYMD(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

bot.action('applypattern_start', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    // Generate 7 dates starting from today
    const buttons = [];
    const today = new Date();
    for (let i = 0; i < 7; i++) {
      const dt = new Date(today.getFullYear(), today.getMonth(), today.getDate() + i);
      const label = formatDateLabel(dt);
      const iso = isoDateYMD(dt);
      buttons.push([Markup.button.callback(label, `applypattern_pick_${iso}`)]);
    }
    // allow manual input as fallback
    buttons.push([Markup.button.callback('Другие...', 'applypattern_manual')]);
    adminStates[ctx.from.id] = { mode: 'applypattern_choosing' };
    await ctx.reply('Выберите дату для применения шаблона:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('applypattern_start error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});

bot.action('applypattern_manual', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'applypattern_wait_date' };
  await ctx.reply('Отправьте дату в формате DD.MM.YYYY для применения шаблона:');
  await ctx.answerCbQuery();
});

bot.action(/^applypattern_pick_(\d{4}-\d{2}-\d{2})$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const dateISO = ctx.match[1];
    adminStates[ctx.from.id] = { mode: 'applypattern_choose', apply_date: dateISO };

    const pats = await db.getPatternsDb(pool);
    if (!pats || pats.length === 0) {
      delete adminStates[ctx.from.id];
      await ctx.reply('Шаблонов нет. Сначала добавьте шаблон.');
      await ctx.answerCbQuery();
      return;
    }

    const buttons = pats.map(p => [ Markup.button.callback(p.name + (p.intervals ? ` (${p.intervals})` : ''), `applypattern_date_${p.id}`) ]);
    await ctx.reply('Выберите шаблон для применения на указанную дату:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('applypattern_pick handler error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});
// --- end new date buttons flow ---

bot.action(/^applypattern_date_(.+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const patternId = ctx.match[1];
    const st = adminStates[ctx.from.id];
    const dateISO = st && st.apply_date;
    if (!dateISO) {
      await ctx.answerCbQuery('Сначала укажите дату для применения шаблона (кнопка "Применить шаблон на дату").', { show_alert: true });
      return;
    }
    const res = await db.applyPatternToDate(pool, patternId, dateISO);
    delete adminStates[ctx.from.id];
    try { await ctx.editMessageText(`Генерация слотов завершена. Создано: ${res.created}`); } catch (_) {}
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('applypattern_date handler error', e);
    try { await ctx.answerCbQuery('Ошибка при применении шаблона'); } catch (_) {}
  }
});

bot.action(/delpattern_(.+)/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  const id = ctx.match[1];
  try {
    await db.deletePatternDb(pool, id);
    await ctx.reply('Шаблон удалён.');
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('delpattern error', e);
    try { await ctx.answerCbQuery('Ошибка при удалении шаблона'); } catch (_) {}
  }
});

bot.action('manage_blacklist', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const list = await db.getBlacklist(pool);
    const buttons = list.map(u => [Markup.button.callback(`Удалить @${u}`, `delblack_${u}`)]);
    buttons.push([Markup.button.callback('➕ Добавить в ЧС', 'addblack')]);
    await ctx.reply('Черный список:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('manage_blacklist error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});

bot.action('addblack', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addblack' };
  await ctx.reply('Отправьте @username для добавления в черный список (пример: @ivan).');
  await ctx.answerCbQuery();
});

bot.action(/delblack_(.+)/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  const uname = String(ctx.match[1] || '').replace(/^@/, '').toLowerCase();
  try {
    await db.removeFromBlacklist(pool, uname);
    await ctx.reply(`Пользователь @${uname} удалён из ЧС.`);
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('delblack error', e);
    try { await ctx.answerCbQuery('Ошибка при удалении из ЧС'); } catch (_) {}
  }
});

bot.action(/complete_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    await db.updateRequest(pool, reqId, { status: 'completed' });
    await db.addHistoryItem(pool, req.user_id, req.time, req.procedure || 'Процедура', 'Выполнено');

    try { await ctx.editMessageText('✅ Отмечено как выполнено'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `✅ Ваша запись на ${req.time} помечена как выполненная.`); } catch (e) {}
    try { await db.sendToAdmins(pool, bot, `✅ Клиент ${utils.makeUserLink(req.user_id, req.username, req.name)} — выполнено.\nВремя: ${utils.escapeHtml(req.time)}\nПроцедура: ${utils.escapeHtml(req.procedure || '-')}`, { parse_mode: 'HTML' }); } catch (e) {}

    await ctx.answerCbQuery();
  } catch (e) { console.error('complete error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/no_show_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    await db.updateRequest(pool, reqId, { status: 'no_show' });
    await db.addHistoryItem(pool, req.user_id, req.time, req.procedure || 'Процедура', 'Неявка');

    try { await ctx.editMessageText('🚫 Отмечено как неявка'); } catch (_) {}
    try { await db.sendToAdmins(pool, bot, `🚫 Клиент ${utils.makeUserLink(req.user_id, req.username, req.name)} — не явился.\nВремя: ${utils.escapeHtml(req.time)}\nПроцедура: ${utils.escapeHtml(req.procedure || '-')}`, { parse_mode: 'HTML' }); } catch (e) {}

    await ctx.answerCbQuery();
  } catch (e) { console.error('no_show error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/move_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    const reqId = ctx.match[1];
    const slots = await db.getAllSlots(pool);
    if (!slots || slots.length === 0) return ctx.answerCbQuery('Нет свободных интервалов');
    adminStates[ctx.from.id] = { moveReqId: reqId };
    const buttons = slots.map(s => [Markup.button.callback(s.time, `moveTo_${s.id}`)]);
    await ctx.reply('Выберите новое время:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('move_ error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/moveTo_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const slotId = ctx.match[1];
    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery('Слот недоступен', { show_alert: true });
    const st = adminStates[ctx.from.id];
    const reqId = st && st.moveReqId;
    if (!reqId) return ctx.answerCbQuery('Не найден запрос для переноса', { show_alert: true });
    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена', { show_alert: true });

    try { await db.deleteSlotById(pool, slot.id); } catch (e) {}

    await db.updateRequest(pool, reqId, { pending_move_slot_id: slot.id, pending_move_time: slot.time, prev_status: req.status, status: 'move_pending' });

    delete adminStates[ctx.from.id];

    const kb = Markup.inlineKeyboard([
      [Markup.button.callback('Принять', `clientMoveYes_${reqId}`), Markup.button.callback('Отклонить', `clientMoveNo_${reqId}`)]
    ]);
    try { await bot.telegram.sendMessage(req.user_id, `Предложен перенос вашей записи на: ${slot.time}\nПринять?`, kb); } catch (e) {}

    try { await ctx.reply('Предложение на перенос отправлено клиенту.'); } catch (_) {}
    try { await ctx.answerCbQuery(); } catch (_) {}
  } catch (e) {
    console.error('moveTo error', e);
    try { await ctx.answerCbQuery('Ошибка при предложении переноса'); } catch (_) {}
  }
});

bot.action(/clientMoveYes_([0-9a-fA-F\-]{36})/, async ctx => {
  const reqId = ctx.match[1];
  try {
    const res = await db.applyClientMove(pool, reqId);
    if (!res.ok) return ctx.answerCbQuery(res.message || 'Ошибка при применении переноса');
    try { await ctx.editMessageText('✔ Перенос подтверждён!'); } catch (_) {}
    try { await db.sendToAdmins(pool, bot, `✔ Клиент подтвердил перенос. Новое время: ${utils.escapeHtml(res.new_time)}`); } catch (e) {}
    await ctx.answerCbQuery();
  } catch (err) {
    console.error('clientMoveYes transaction error:', err);
    try { await ctx.answerCbQuery('Ошибка при применении переноса'); } catch (_) {}
  }
});

bot.action(/clientMoveNo_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    const reqId = ctx.match[1];
    const req = await db.getRequestById(pool, reqId);
    if (!req || !req.pending_move_slot_id) return ctx.answerCbQuery('Нет запроса на перенос');
    await db.updateRequest(pool, reqId, { pending_move_slot_id: null, pending_move_time: null, status: req.prev_status || req.status, prev_status: null });
    try { await ctx.editMessageText('❌ Вы отклонили перенос.'); } catch (_) {}
    try { await db.sendToAdmins(pool, bot, `❌ Клиент ${utils.makeUserLink(req.user_id, req.username, req.name)} отклонил перенос.`); } catch (e) {}
    await ctx.answerCbQuery();
  } catch (e) { console.error('clientMoveNo error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.catch((err, ctx) => {
  console.error(`Bot error for update ${ctx.update?.update_id}:`, err);
});

async function shutdown() {
  try { await notifications.shutdown(bot); } catch (e) {}
  try { await pool.end(); } catch (e) {}
  process.exit(0);
}
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);

notifications.start(pool, bot);

(async () => {
  if (WEBHOOK_URL) {
    const app = express();
    const hookPath = `/bot${BOT_TOKEN}`;
    app.use(bot.webhookCallback(hookPath));
    try {
      const setRes = await bot.telegram.setWebhook(`${WEBHOOK_URL}${hookPath}`);
      console.log('Webhook set result:', setRes);
    } catch (e) {}
    app.get('/', (req, res) => res.send('OK'));
    app.listen(PORT, () => console.log(`Express server listening on ${PORT}, webhook path ${hookPath}`));
  } else {
    await bot.launch();
  }
})().catch(err => {
  console.error('Startup error:', err);
  process.exit(1);
});