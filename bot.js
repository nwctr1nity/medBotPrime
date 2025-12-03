require('dotenv').config();

const { Telegraf, Markup } = require('telegraf');
const express = require('express');
const { Pool } = require('pg');
const { randomUUID } = require('crypto');

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
    if (!procs || procs.length === 0) {
      await ctx.reply('Процедур пока нет. Попросите администратора добавить процедуру.');
      await ctx.answerCbQuery();
      return;
    }

    adminStates[ctx.from.id].procMap = {};
    const procButtons = procs.map((p, i) => {
      adminStates[ctx.from.id].procMap[i] = p.key;
      // Use numeric index in callback_data to avoid long/heavy keys
      return [Markup.button.callback(p.name, `proc_${slotId}_${i}`)];
    });

    await ctx.reply('Выберите процедуру:', Markup.inlineKeyboard(procButtons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('req action error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/^proc_([0-9a-fA-F\-]{36})_(.+)$/u, async ctx => {
  try {
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.answerCbQuery('Свободных интервалов пока нет.', { show_alert: true });
    const slotId = ctx.match[1];
    let procKeyOrIdx = ctx.match[2];

    // If procKeyOrIdx is a numeric index, resolve real key from session map
    const st = adminStates[ctx.from.id] || {};
    if (/^\d+$/.test(procKeyOrIdx) && st.procMap && st.procMap[procKeyOrIdx] !== undefined) {
      procKeyOrIdx = st.procMap[procKeyOrIdx];
    }

    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery('Слот стал недоступен', { show_alert: true });

    const proc = await db.getProcedureByKey(pool, procKeyOrIdx);
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
      // Always generate a short, safe key to avoid callback_data length issues.
      const key = `proc_${randomUUID().slice(0,8)}`;
      try {
        await db.addProcedureDb(pool, key, text);
        delete adminStates[ctx.from.id];
        return await ctx.reply(`Процедура "${text}" добавлена (key=${key}).`);
      } catch (err) {
        delete adminStates[ctx.from.id];
        console.error('addProcedure error:', err);
        return await ctx.reply('Не удалось добавить процедуру. Возможно, такой ключ уже существует.');
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

bot.action(/^delprocidx_(\d+)$/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const idx = ctx.match[1];
    const map = adminStates[ctx.from.id] && adminStates[ctx.from.id].manageProcMap;
    if (!map) return ctx.answerCbQuery('Сессия устарела. Откройте "Управлять процедуры" снова.', { show_alert: true });
    const key = map[idx];
    if (!key) return ctx.answerCbQuery('Не удалось найти процедуру для удаления.', { show_alert: true });
    await db.deleteProcedureDb(pool, key);
    try { await ctx.reply('Процедура удалена.'); } catch (_) {}
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('delprocidx error', e);
    try { await ctx.answerCbQuery('Ошибка при удалении процедуры'); } catch (_) {}
  }
});

bot.action('manage_procedures', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const procs = await db.getProcedures(pool);
    const map = {};
    const buttons = (procs || []).map((p, i) => {
      const idx = String(i);
      map[idx] = p.key;
      return [Markup.button.callback(`Удалить ${utils.escapeHtml(p.name)}`, `delprocidx_${idx}`)];
    });
    buttons.push([Markup.button.callback('➕ Добавить процедуру', 'addproc')]);
    adminStates[ctx.from.id] = adminStates[ctx.from.id] || {};
    adminStates[ctx.from.id].manageProcMap = map;
    await ctx.reply('Список процедур:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('manage_procedures error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});

bot.action('addproc', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addproc' };
  await ctx.reply('Отправьте название процедуры (например: Ботулинотерапия). Я сгенерирую ключ автоматически.');
  await ctx.answerCbQuery();
});

// ... оставшиеся обработчики (patterns, blacklist, move, etc.) без изменений ...

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