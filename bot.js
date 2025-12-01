// bot.js
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
const CONDITIONAL_THRESHOLD_HOURS = Number(process.env.CONDITIONAL_THRESHOLD_HOURS) || 12;

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
  return ADMIN_IDS.has(Number(id));
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

const adminStates = {}; // { adminId: { mode, ... } }

bot.start(async ctx => {
  try {
    const keyboard = [
      ['📅 Свободное время', '📝 Оставить заявку'],
      ['📚 История посещений'],
      ['Обратная связь']
    ];
    if (isAdmin(ctx)) keyboard[0].push('🛠 Открыть панель');
    await ctx.reply('Привет! Я бот для записи! Чтобы записаться, нажмите на кнопку "Оставить заявку" и дождитесь подтверждения специалиста.\n\nВажно: сразу оставить заявку можно только на самый ранний слот, это сделано для формирования более целостного расписания. Если ранние часы вам не подходят, то вам доступна опция занять более поздний слот, однако заявка будет сформирована только в том случае, если за 12 часов до записи не останется ни одного более раннего незанятого слота.\n\nТакже напоминаю, что данный бот находится в стадии открытого тестирования. Если у вас возникла проблема или опыт использования бота вас не удовлетворил, то вы можете поделиться своим мнением, нажав на кнопку "Обратная связь". Этим вы очень сильно поможете в дальнейшей разработке и улучшении бота. Благодарим за понимание!', Markup.keyboard(keyboard).resize());
  } catch (e) { console.error('start error', e); }
});

bot.hears('Обратная связь', async ctx => {
  adminStates[ctx.from.id] = { mode: 'feedback' };
  await ctx.reply('Можете оставить свой комментарий, связанный с опытом использования моего бота.');
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
    const buttons = [
      [Markup.button.callback(slot.time, `req_${slot.id}`)],
      [Markup.button.callback('Выбрать другой слот', 'choose_slots')]
    ];
    await ctx.reply('Выбери интервал или выберите другой слот:', Markup.inlineKeyboard(buttons));
  } catch (e) { console.error('start request error', e); }
});

bot.action('choose_slots', async ctx => {
  try {
    // show only slots later than the earliest (to follow UX requirement)
    const earliest = await db.getEarliestSlot(pool);
    const allSlots = await db.getAllSlots(pool);
    if (!allSlots || allSlots.length === 0) return ctx.answerCbQuery('Слотов нет');
    let slotsToShow = allSlots;
    if (earliest) {
      const earliestTime = new Date(earliest.start).getTime();
      slotsToShow = allSlots.filter(s => new Date(s.start).getTime() > earliestTime);
    }
    if (!slotsToShow || slotsToShow.length === 0) {
      slotsToShow = allSlots;
    }
    const buttons = slotsToShow.slice(0, 30).map(s => [Markup.button.callback(s.time, `cond_${s.id}`)]);
    await ctx.reply('Выберите слот для условной записи (будет оформлена автоматически при выполнении условий):', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('choose_slots error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

// conditional: need procedure selection too
bot.action(/^cond_([0-9a-fA-F\-]{36})$/, async ctx => {
  try {
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.answerCbQuery('Недоступно', { show_alert: true });
    const slotId = ctx.match[1];
    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery('Слот недоступен', { show_alert: true });

    const dup = await db.checkDuplicateRequest(pool, ctx.from.id, slotId);
    if (dup) return ctx.answerCbQuery('У вас уже есть заявка на этот слот', { show_alert: true });

    // ask for procedure selection like normal flow
    const procs = await db.getProcedures(pool);
    if (!procs || procs.length === 0) {
      // create conditional without procedure
      const req = {
        id: randomUUID(),
        userId: ctx.from.id,
        username: ctx.from.username || null,
        name: ctx.from.first_name || '',
        slotId: slot.id,
        time: slot.time,
        procedure: null,
        status: 'conditional',
        createdAt: new Date().toISOString()
      };
      await db.addRequestDb(pool, req);
      await ctx.reply(`Ваша заявка на ${slot.time} создана условно. Она будет автоматически оформлена, если за ${CONDITIONAL_THRESHOLD_HOURS} часов до записи не останется более ранних незанятых слотов подтверждёнными заявками.`);
      try { await db.sendToAdmins(pool, bot, `🕒 Условная заявка: ${ctx.from.username ? '@'+ctx.from.username : ctx.from.first_name} → ${slot.time}`); } catch (e) { console.error('notify admin conditional failed', e); }
      return ctx.answerCbQuery();
    }

    // present procedures as buttons, encode both slotId and procKey in callback
    const procButtons = procs.map(p => [Markup.button.callback(p.name, `condproc_${slot.id}_${p.key}`)]);
    await ctx.reply('Выберите процедуру для условной заявки:', Markup.inlineKeyboard(procButtons));
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('cond handler error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
});

bot.action(/^condproc_([0-9a-fA-F\-]{36})_(.+)$/u, async ctx => {
  try {
    const slotId = ctx.match[1];
    const procKey = ctx.match[2];

    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery('Слот недоступен', { show_alert: true });

    const proc = await db.getProcedureByKey(pool, procKey);
    if (!proc) return ctx.answerCbQuery('Процедура недоступна', { show_alert: true });

    const dup = await db.checkDuplicateRequest(pool, ctx.from.id, slotId);
    if (dup) return ctx.answerCbQuery('У вас уже есть заявка на этот слот', { show_alert: true });

    const req = {
      id: randomUUID(),
      userId: ctx.from.id,
      username: ctx.from.username || null,
      name: ctx.from.first_name || '',
      slotId: slot.id,
      time: slot.time,
      procedure: proc.name,
      status: 'conditional',
      createdAt: new Date().toISOString()
    };
    await db.addRequestDb(pool, req);

    await ctx.reply(`Ваша условная заявка на ${slot.time} (процедура: ${proc.name}) создана. Она будет автоматически оформлена, если за ${CONDITIONAL_THRESHOLD_HOURS} часов до записи не останется более ранних незанятых слотов подтверждёнными заявками.`);
    try { await db.sendToAdmins(pool, bot, `🕒 Условная заявка: ${ctx.from.username ? '@'+ctx.from.username : ctx.from.first_name} → ${slot.time} (${proc.name})`); } catch (e) { console.error('notify admin conditional failed', e); }

    await ctx.answerCbQuery();
  } catch (e) {
    console.error('condproc handler error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch (_) {}
  }
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
    if (await db.isUserBlacklisted(pool, ctx.from.username)) return ctx.answerCbQuery('Свободных интервалов пока нет.', { show_alert: true });
    const slotId = ctx.match[1];
    const procKey = ctx.match[2];

    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery('Слот стал недоступен', { show_alert: true });

    const proc = await db.getProcedureByKey(pool, procKey);
    if (!proc) return ctx.answerCbQuery('Процедура недоступна', { show_alert: true });

    const dup = await db.checkDuplicateRequest(pool, ctx.from.id, slotId);
    if (dup) return ctx.answerCbQuery('Вы уже отправляли заявку на этот слот.', { show_alert: true });

    // Do NOT delete slot here (pending shouldn't claim slot)
    const req = {
      id: randomUUID(),
      userId: ctx.from.id,
      username: ctx.from.username || null,
      name: ctx.from.first_name || '',
      slotId: slot.id,
      time: slot.time,
      procedure: proc.name,
      status: 'pending',
      createdAt: new Date().toISOString(),
      original_slot_id: slot.id,
      original_slot_time: slot.time,
      original_slot_start: slot.start,
      original_slot_end: slot.end
    };
    await db.addRequestDb(pool, req);

    await ctx.reply('Заявка отправлена! Ожидайте подтверждения от администратора.');
    try { await db.sendToAdmins(pool, bot, `📩 Новая заявка\nКлиент: ${ctx.from.username ? '@'+ctx.from.username : ctx.from.first_name}\nВремя: ${slot.time}\nПроцедура: ${proc.name}`); } catch (notifyErr) { console.error('notify admin failed', notifyErr); }

    await ctx.answerCbQuery();
  } catch (err) {
    console.error('proc handler error:', err);
    try { await ctx.answerCbQuery('Ошибка при создании заявки'); } catch (_) {}
  }
});

bot.hears('📚 История посещений', async ctx => {
  try {
    const rows = await db.getHistoryForUser(pool, ctx.from.id);
    if (!rows || rows.length === 0) return ctx.reply('История пуста.');
    let msg = 'Ваша история:\n\n';
    rows.forEach(h => msg += `• ${utils.escapeHtml(h.date)} — ${utils.escapeHtml(h.procedure)} (${utils.escapeHtml(h.status)})\n`);
    await ctx.reply(msg);
  } catch (e) { console.error('history error', e); }
});

bot.action('manage_procedures', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const procs = await db.getProcedures(pool);
    const buttons = procs.map(p => [Markup.button.callback(`Удалить ${utils.escapeHtml(p.name)}`, `delproc_${p.key}`)]);
    buttons.push([Markup.button.callback('➕ Добавить процедуру', 'addproc')]);
    await ctx.reply('Список процедур:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('manage_procedures error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('addproc', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'addproc' };
  await ctx.reply('Отправьте название процедуры (например: Ботулинотерапия). Я сгенерирую ключ автоматически.');
  await ctx.answerCbQuery();
});

bot.action(/delproc_(.+)/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const key = ctx.match[1];
    await db.deleteProcedureDb(pool, key);
    await ctx.reply('Процедура удалена.');
    await ctx.answerCbQuery();
  } catch (e) { console.error('delproc error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

// text handlers (addproc, addslot, blacklist, patterns, feedback) remain unchanged
bot.on('text', async ctx => {
  try {
    const st = adminStates[ctx.from.id];
    if (!st) return;
    const text = ctx.message.text.trim();

    if (st.mode === 'addproc') {
      const rawKey = utils.slugifyName(text);
      const key = rawKey || `proc_${randomUUID().slice(0,8)}`;
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
        return ctx.reply('Шаблонов нет. Добавляйте шаблоны вручную в БД.');
      }
    
      const buttons = pats.map(p => [ Markup.button.callback(p.name + (p.intervals ? ` (${p.intervals})` : ''), `applypattern_date_${p.id}`) ]);
      await ctx.reply('Выберите шаблон для применения на указанную дату:', Markup.inlineKeyboard(buttons));
      return;
    }

    if (st.mode === 'feedback') {
      const feedback = text;
      delete adminStates[ctx.from.id];
      try {
        await db.sendToAdmins(pool, bot, `Обратная связь от ${ctx.from.username ? '@'+ctx.from.username : ctx.from.first_name}:\n\n${feedback}`, { parse_mode: 'HTML' });
      } catch (e) {
        console.error('send feedback to admins failed', e);
      }
      return ctx.reply('Спасибо! Ваше мнение очень важно для нас!.');
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
    try { await ctx.answerCbQuery('Удалено'); } catch (_) {}
  } catch (e) { console.error('delslot error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('req_pending', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'pending', '🟡 Ожидающие'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_approved', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'approved', '🟢 Подтверждённые'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_rejected', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'rejected', '🔴 Отклонённые'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_move_pending', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'move_pending', '🔵 Ожидающие переноса'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_completed', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'completed', '✅ Выполненные'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_no_show', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'no_show', '🚫 Неявки'); else ctx.answerCbQuery('Нет доступа'); });
bot.action('req_conditional', async ctx => { if (isAdmin(ctx)) await showRequestsByStatus(ctx, 'conditional', '🟣 Условные'); else ctx.answerCbQuery('Нет доступа'); });

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
          [Markup.button.callback('❌ Отменить заявку', `reject_${r.id}`), Markup.button.callback('🚫 Неявка', `no_show_${r.id}`)]
        ]);
      } else if (status === 'conditional') {
        kb = Markup.inlineKeyboard([
          [Markup.button.callback('✔ Активировать (в pending)', `activate_cond_${r.id}`), Markup.button.callback('❌ Отклонить', `reject_${r.id}`)]
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

function adminPanelKeyboard() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🟡 Ожидающие', 'req_pending')],
    [Markup.button.callback('🟢 Подтверждённые', 'req_approved')],
    [Markup.button.callback('🔴 Отклонённые', 'req_rejected')],
    [Markup.button.callback('🔵 Ожидающие переноса', 'req_move_pending')],
    [Markup.button.callback('✅ Выполненные', 'req_completed'), Markup.button.callback('🚫 Неявки', 'req_no_show')],
    [Markup.button.callback('🟣 Условные', 'req_conditional')],
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

bot.action(/activate_cond_(.+)/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const id = ctx.match[1];
    await db.updateRequest(pool, id, { status: 'pending' });
    try { await ctx.editMessageText('Условная заявка вручную переведена в pending (ожидающие).'); } catch (_) {}
    const req = await db.getRequestById(pool, id);
    if (req) {
      try { await db.sendToAdmins(pool, bot, `Админ активировал условную заявку: ${req.username ? '@'+req.username : req.name} → ${req.time}`); } catch (_) {}
      try { await bot.telegram.sendMessage(req.user_id, `Ваша условная заявка на ${req.time} переведена в ожидающие (администратор).`); } catch (_) {}
    }
    await ctx.answerCbQuery();
  } catch (e) { console.error('activate_cond error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/approve_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');
    if (req.status === 'approved') return ctx.answerCbQuery('Уже подтверждена');

    // Claim the slot (delete it) now that it's approved
    if (req.slot_id) {
      try { await db.deleteSlotById(pool, req.slot_id); } catch (e) { console.error('delete slot on approve failed', e); }
    }

    await db.updateRequest(pool, reqId, { status: 'approved', notification_20_sent: false, notification_1h_sent: false });

    try { await ctx.editMessageText('✔ Заявка подтверждена'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `✔ Ваша запись на ${req.time} подтверждена!`); } catch (e) { console.error('notify user approval error', e); }
    await ctx.answerCbQuery();
  } catch (e) { console.error('approve error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/reject_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    if (req.original_slot_id) {
        await client.query(
            `INSERT INTO slots(id, time, start, "end")
             VALUES ($1,$2,$3,$4)
             ON CONFLICT (id) DO NOTHING`,
            [req.original_slot_id, req.original_slot_time, req.original_slot_start, req.original_slot_end]
        );
    }

    await db.updateRequest(pool, reqId, { status: 'rejected' });
    try { await ctx.editMessageText('❌ Заявка отклонена'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `❌ Ваша заявка на ${req.time} была отклонена.`); } catch (e) { console.error('notify reject error', e); }
    await ctx.answerCbQuery();
  } catch (e) { console.error('reject error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action(/delete_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    await db.deleteRequestById(pool, reqId);
    try { await ctx.editMessageText('🗑 Заявка удалена.'); } catch (_) {}
    await ctx.answerCbQuery();
  } catch (e) { console.error('delete error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

// NEW: complete and no_show handlers
bot.action(/complete_([0-9a-fA-F\-]{36})/, async ctx => {
  try {
    if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
    const reqId = ctx.match[1];
    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery('Заявка не найдена');

    await db.updateRequest(pool, reqId, { status: 'completed' });
    try { await ctx.editMessageText('✅ Отмечено как выполнено'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `Ваша запись на ${req.time} отмечена как выполненная.`); } catch (e) { console.error('notify complete error', e); }
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
    try { await ctx.editMessageText('🚫 Отмечено как неявка'); } catch (_) {}
    try { await bot.telegram.sendMessage(req.user_id, `Ваша запись на ${req.time} отмечена как неявка.`); } catch (e) { console.error('notify no_show error', e); }
    await ctx.answerCbQuery();
  } catch (e) { console.error('no_show error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

// MOVE flow: admin chooses to move a request -> shows available slots -> admin picks one -> adminMoveRequest does transaction
bot.action(/^move_([0-9a-fA-F\-]{36})$/, async ctx => {
    const reqId = ctx.match[1];

    const req = await db.getRequestById(pool, reqId);
    if (!req) return ctx.answerCbQuery("Заявка не найдена");

    const slots = await db.getAllSlots(pool);

    const buttons = slots.map(s => [
        Markup.button.callback(s.time, `move_choose_${reqId}_${s.id}`)
    ]);

    await ctx.reply(
        "Выберите новый слот для переноса:",
        Markup.inlineKeyboard(buttons)
    );

    await ctx.answerCbQuery();
});

bot.action(/^move_choose_([0-9a-fA-F\-]{36})_([0-9a-fA-F\-]{36})$/, async ctx => {
    const reqId = ctx.match[1];
    const slotId = ctx.match[2];

    const slot = await db.getSlotById(pool, slotId);
    if (!slot) return ctx.answerCbQuery("Слот недоступен");

    await db.updateRequest(pool, reqId, {
        pending_move_slot_id: slot.id,
        pending_move_time: slot.time,
        status: "move_pending",
        prev_status: (await db.getRequestById(pool, reqId)).status
    });

    await ctx.editMessageText(
        `Запрос на перенос создан:\nНовый слот: ${slot.time}`
    );
    await ctx.answerCbQuery();
});

bot.action('manage_patterns', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const patterns = await db.getPatternsDb(pool);
    if (!patterns || patterns.length === 0) {
      await ctx.reply('Шаблонов нет. Добавляйте шаблоны вручную в БД.');
      await ctx.answerCbQuery();
      return;
    }
    const buttons = patterns.map(p => [Markup.button.callback(`${p.name}${p.intervals ? ` (${p.intervals})` : ''}`, `pattern_${p.id}`)]);
    buttons.push([Markup.button.callback('🗓 Применить шаблон на дату', 'applypattern_start')]);
    await ctx.reply('Шаблоны расписания:', Markup.inlineKeyboard(buttons));
    await ctx.answerCbQuery();
  } catch (e) { console.error('manage_patterns error', e); try { await ctx.answerCbQuery('Ошибка'); } catch (_) {} }
});

bot.action('applypattern_start', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  adminStates[ctx.from.id] = { mode: 'applypattern_wait_date' };
  await ctx.reply('Отправьте дату в формате DD.MM.YYYY для применения шаблона:');
  await ctx.answerCbQuery();
});

bot.action(/^pattern_(.+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  const id = ctx.match[1];
  const pat = await db.getPatternById(pool, id);
  if (!pat) return ctx.answerCbQuery('Шаблон не найден');
  const kb = Markup.inlineKeyboard([
    [Markup.button.callback('Применить на дату', `applypattern_start`)],
  ]);
  await ctx.reply(`Шаблон: ${pat.name}\nИнтервалы: ${pat.intervals || '-'}`, kb);
  await ctx.answerCbQuery();
});

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

bot.action('manage_blacklist', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('Нет доступа');
  try {
    const list = await db.getBlacklist(pool);
    const buttons = list.map(u => [Markup.button.callback(`Удалить @${utils.escapeHtml(u)}`, `delblack_${u}`)]);
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

bot.catch((err, ctx) => {
  console.error(`Bot error for update ${ctx.update?.update_id}:`, err);
});

async function shutdown() {
  try { await notifications.shutdown(bot); } catch (e) { console.error('notifications shutdown error', e); }
  try { await pool.end(); } catch (e) {}
  process.exit(0);
}
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);

notifications.start(pool, bot, { conditionalThresholdHours: CONDITIONAL_THRESHOLD_HOURS });

// webhook/polling logic: keep original behavior (if WEBHOOK_URL present it will set webhook; if you run locally without webhook, it will fall back to polling)
// IMPORTANT: If you deploy to render with webhook, ensure WEBHOOK_URL env var and that express app receives requests (original code).
// If you run locally (no WEBHOOK_URL), polling will be used (bot.launch()).
(async () => {
  if (WEBHOOK_URL) {
    const app = express();
    const hookPath = `/bot${BOT_TOKEN}`;
    app.use(bot.webhookCallback(hookPath));
    try {
      const setRes = await bot.telegram.setWebhook(`${WEBHOOK_URL}${hookPath}`);
      console.log('Webhook set result:', setRes);
    } catch (e) {
      console.error('Failed to set webhook:', e);
    }
    app.get('/', (req, res) => res.send('OK'));
    app.listen(PORT, () => console.log(`Express server listening on ${PORT}, webhook path ${hookPath}`));
  } else {
    console.warn('WEBHOOK_URL / RENDER_EXTERNAL_URL not set — falling back to polling (for local dev).');
    await bot.launch();
  }
})().catch(err => {
  console.error('Startup error:', err);
  process.exit(1);
});
