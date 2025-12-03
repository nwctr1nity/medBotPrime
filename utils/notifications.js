contents
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

function start(pool, bot) {
  if (running) return;
  running = true;

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

      const reserved = await db.getReservedRequests(pool);
      for (const r of reserved) {
        if (!r.original_slot_start) continue;
        const slotStart = new Date(r.original_slot_start);
        const cutoff = new Date(slotStart.getTime() - 3 * 60 * 60 * 1000);
        const now2 = new Date();
        const earlierSlotsRes = await pool.query('SELECT 1 FROM slots WHERE start < $1 LIMIT 1', [r.original_slot_start]);
        const hasEarlier = earlierSlotsRes.rowCount > 0;
        if (!hasEarlier || now2 >= cutoff) {
          try {
            await db.updateRequest(pool, r.id, { status: 'pending' });
            try { await bot.telegram.sendMessage(r.user_id, `Ваша резервная заявка на ${r.original_slot_time} переведена в заявку и ожидает подтверждения администратора.`); } catch (e) {}
            try { await db.sendToAdmins(pool, bot, `📩 Резерв переведён в заявку\nКлиент: ${r.username ? '@'+r.username : r.name}\nВремя: ${r.original_slot_time}\nПроцедура: ${r.procedure || '-'}`); } catch (e) {}
          } catch (e) {
            console.error('error converting reserved to pending for', r.id, e);
          }
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