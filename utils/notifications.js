// utils/notifications.js
const INTERVAL_MS = 60 * 1000;
let timer = null;
let running = false;

function formatSlotTimeDisplay(slotStartIso) {
  try {
    const d = new Date(slotStartIso);
    return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
  } catch (e) {
    return slotStartIso;
  }
}

function start(pool, bot, opts = {}) {
  if (running) return;
  running = true;
  const thresholdHours = Number(opts.conditionalThresholdHours || 12);

  const runOnce = async () => {
    try {
      const db = require('./db');
      const rows = await db.getApprovedRequestsNeedingNotifications(pool);
      const now = new Date();

      for (const r of rows) {
        if (!r.slot_start) continue;
        const slotStart = new Date(r.slot_start);

        const dayBefore20 = new Date(slotStart);
        dayBefore20.setDate(dayBefore20.getDate() - 1);
        dayBefore20.setHours(20, 0, 0, 0);

        const oneHourBefore = new Date(slotStart.getTime() - 60 * 60 * 1000);

        if (!r.notification_20_sent && now >= dayBefore20) {
          try {
            const slotDisplay = r.slot_time || formatSlotTimeDisplay(r.slot_start);
            await bot.telegram.sendMessage(r.user_id, `Напоминание: у вас запись на ${slotDisplay}.`);
            await db.updateRequest(pool, r.id, { notification_20_sent: true });
          } catch (e) {
            console.error('notification 20 error for', r.id, e);
          }
        }

        if (!r.notification_1h_sent && now >= oneHourBefore) {
          try {
            const slotDisplay = r.slot_time || formatSlotTimeDisplay(r.slot_start);
            await bot.telegram.sendMessage(r.user_id, `Через час у вас запись на ${slotDisplay}.`);
            await db.updateRequest(pool, r.id, { notification_1h_sent: true });
          } catch (e) {
            console.error('notification 1h error for', r.id, e);
          }
        }
      }

      const conditionals = await db.getConditionalRequests(pool);
      for (const c of conditionals) {
        try {
          const res = await db.promoteConditionalRequest(pool, c.id, thresholdHours);
          if (res.ok) {
            try {
              await bot.telegram.sendMessage(c.user_id, `Ваша условная заявка на ${res.new_time} автоматически оформлена и ожидает подтверждения администратора.`);
            } catch (e) {}
            try {
              await db.sendToAdmins(pool, bot, `✔ Условная заявка оформлена: ${c.username ? '@'+c.username : c.name} → ${res.new_time}`);
            } catch (e) {}
          } else {
            if (res.reason === 'slot_taken' || res.reason === 'no_slot') {
              try { await db.updateRequest(pool, c.id, { status: 'rejected' }); } catch (_) {}
              try { await bot.telegram.sendMessage(c.user_id, `К сожалению, слот стал недоступен и ваша условная заявка отклонена.`); } catch (e) {}
            }
          }
        } catch (e) {
          console.error('processing conditional request error', e);
        }
      }

    } catch (e) {
      console.error('notificationWorker error', e);
    }
  };

  runOnce().catch(e => console.error('notification initial run failed', e));
  timer = setInterval(runOnce, INTERVAL_MS);

  return {
    stop: async () => {
      if (timer) clearInterval(timer);
      timer = null;
      running = false;
    }
  };
}

async function shutdown(bot) {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  running = false;
  return;
}

module.exports = { start, shutdown };
