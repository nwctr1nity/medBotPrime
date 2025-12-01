// utils/db.js
const { randomUUID } = require('crypto');

async function initDb(pool) {
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
      prev_status text,
      notification_20_sent boolean DEFAULT false,
      notification_1h_sent boolean DEFAULT false
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS patterns (
      id uuid PRIMARY KEY,
      name text,
      intervals text,
      created_at timestamptz DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS blacklist (
      username text PRIMARY KEY
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

async function getAllSlots(pool) {
  const res = await pool.query('SELECT * FROM slots ORDER BY start');
  return res.rows;
}
async function getEarliestSlot(pool) {
  const res = await pool.query('SELECT * FROM slots ORDER BY start LIMIT 1');
  return res.rows[0] || null;
}
async function getSlotById(pool, id) {
  const res = await pool.query('SELECT * FROM slots WHERE id=$1', [id]);
  return res.rows[0] || null;
}
async function addSlotToDb(pool, id, time, startIso, endIso) {
  // prevent duplicate key errors on re-adding original slot
  await pool.query('INSERT INTO slots(id, time, start, "end") VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO NOTHING', [id, time, startIso, endIso]);
}
async function deleteSlotById(pool, id) {
  await pool.query('DELETE FROM slots WHERE id=$1', [id]);
}

async function getProcedures(pool) {
  const res = await pool.query('SELECT * FROM procedures ORDER BY name');
  return res.rows;
}
async function addProcedureDb(pool, key, name) {
  await pool.query('INSERT INTO procedures(key, name) VALUES ($1,$2) ON CONFLICT DO NOTHING', [key, name]);
}
async function deleteProcedureDb(pool, key) {
  await pool.query('DELETE FROM procedures WHERE key=$1', [key]);
}
async function getProcedureByKey(pool, key) {
  const res = await pool.query('SELECT * FROM procedures WHERE key=$1', [key]);
  return res.rows[0] || null;
}

async function addRequestDb(pool, req) {
  await pool.query(
    `INSERT INTO requests(id, user_id, username, name, slot_id, time, procedure, status, created_at, original_slot_id, original_slot_time, original_slot_start, original_slot_end, notification_20_sent, notification_1h_sent)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,false,false)`,
    [req.id, req.userId, req.username, req.name, req.slotId, req.time, req.procedure, req.status, req.createdAt,
      req.original_slot_id || null, req.original_slot_time || null, req.original_slot_start || null, req.original_slot_end || null]
  );
}

async function checkDuplicateRequest(pool, userId, slotId) {
  const res = await pool.query(
    `SELECT 1 FROM requests WHERE user_id=$1 AND slot_id=$2 AND status NOT IN ($3,$4,$5) LIMIT 1`,
    [userId, slotId, 'rejected', 'completed', 'no_show']
  );
  return res.rowCount > 0;
}

async function getRequestById(pool, id) {
  const res = await pool.query('SELECT * FROM requests WHERE id=$1', [id]);
  return res.rows[0] || null;
}

async function updateRequest(pool, id, fields) {
  const keys = Object.keys(fields);
  if (keys.length === 0) return;
  const set = keys.map((k, i) => `"${k}" = $${i+2}`).join(', ');
  const values = [id, ...keys.map(k => fields[k])];
  const q = `UPDATE requests SET ${set} WHERE id = $1`;
  await pool.query(q, values);
}

async function getRequestsByStatus(pool, status) {
  const res = await pool.query('SELECT * FROM requests WHERE status=$1 ORDER BY created_at', [status]);
  return res.rows;
}

async function deleteRequestById(pool, id) {
  await pool.query('DELETE FROM requests WHERE id=$1', [id]);
}

async function addHistoryItem(pool, userId, date, procedure, status) {
  await pool.query('INSERT INTO history(user_id, date, procedure, status) VALUES($1,$2,$3,$4)', [userId, date, procedure, status]);
}
async function getHistoryForUser(pool, userId) {
  const res = await pool.query('SELECT * FROM history WHERE user_id=$1 ORDER BY id DESC', [userId]);
  return res.rows;
}

async function addPatternDb(pool, pattern) {
  await pool.query('INSERT INTO patterns(id, name, intervals) VALUES($1,$2,$3)', [pattern.id, pattern.name, pattern.intervals]);
}
async function getPatternsDb(pool) {
  const res = await pool.query('SELECT * FROM patterns ORDER BY name');
  return res.rows;
}
async function deletePatternDb(pool, id) {
  await pool.query('DELETE FROM patterns WHERE id=$1', [id]);
}
async function getPatternById(pool, id) {
  const res = await pool.query('SELECT * FROM patterns WHERE id=$1', [id]);
  return res.rows[0] || null;
}

async function applyPatternToDate(pool, patternId, dateStr) {
  const pat = await getPatternById(pool, patternId);
  if (!pat || !pat.intervals) return { created: 0 };
  const intervals = pat.intervals.split(',').map(s => s.trim()).filter(Boolean);
  const parts = dateStr.split('-').map(Number);
  if (parts.length !== 3) return { created: 0 };
  const [year, month, day] = parts;
  let created = 0;

  for (const intv of intervals) {
    const mm = intv.match(/^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/);
    if (!mm) continue;
    const sh = Number(mm[1]), sm = Number(mm[2]), eh = Number(mm[3]), em = Number(mm[4]);
    const start = new Date(Date.UTC(year, month - 1, day, sh, sm, 0, 0));
    const end = new Date(Date.UTC(year, month - 1, day, eh, em, 0, 0));
    if (end.getTime() <= start.getTime()) continue;

    const overlapRes = await pool.query('SELECT 1 FROM slots WHERE NOT (start >= $1 OR "end" <= $2) LIMIT 1', [end.toISOString(), start.toISOString()]);
    if (overlapRes.rowCount === 0) {
      const timeStr = `${String(start.getUTCDate()).padStart(2,'0')}.${String(start.getUTCMonth()+1).padStart(2,'0')}.${start.getUTCFullYear()} ${String(start.getUTCHours()).padStart(2,'0')}:${String(start.getUTCMinutes()).padStart(2,'0')}-${String(end.getUTCHours()).padStart(2,'0')}:${String(end.getUTCMinutes()).padStart(2,'0')}`;
      const id = randomUUID();
      await addSlotToDb(pool, id, timeStr, start.toISOString(), end.toISOString());
      created++;
    }
  }

  return { created };
}

async function isUserBlacklisted(pool, username) {
  if (!username) return false;
  const uname = username.replace(/^@/, '').toLowerCase();
  const res = await pool.query('SELECT 1 FROM blacklist WHERE username=$1', [uname]);
  return res.rowCount > 0;
}
async function addToBlacklist(pool, username) {
  const uname = username.replace(/^@/, '').toLowerCase();
  await pool.query('INSERT INTO blacklist(username) VALUES($1) ON CONFLICT DO NOTHING', [uname]);
}
async function removeFromBlacklist(pool, username) {
  const uname = username.replace(/^@/, '').toLowerCase();
  await pool.query('DELETE FROM blacklist WHERE username=$1', [uname]);
}
async function getBlacklist(pool) {
  const res = await pool.query('SELECT username FROM blacklist ORDER BY username');
  return res.rows.map(r => r.username);
}

async function sendToAdmins(pool, bot, text, opts = {}) {
  const ADMIN_IDS_RAW = process.env.ADMIN_IDS || String(process.env.ADMIN_ID || '');
  const ADMIN_IDS = ADMIN_IDS_RAW.split(',').map(s => s.trim()).filter(Boolean).map(s => Number(s)).filter(n => !Number.isNaN(n));
  for (const id of ADMIN_IDS) {
    try {
      await bot.telegram.sendMessage(id, text, opts);
    } catch (e) {
      console.error('sendToAdmins error for', id, e);
    }
  }
}

/**
 * Admin move request:
 * - transactionally moves request to new slot (admin chose)
 * - re-adds original slot if needed
 * - deletes new slot and sets request.slot_id -> new slot, status -> approved
 */
async function adminMoveRequest(pool, reqId, newSlotId) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const reqRes = await client.query('SELECT * FROM requests WHERE id=$1 FOR UPDATE', [reqId]);
    const req = reqRes.rows[0];
    if (!req) {
      await client.query('ROLLBACK');
      return { ok: false, message: 'request_not_found' };
    }

    const slotRes = await client.query('SELECT * FROM slots WHERE id=$1 FOR UPDATE', [newSlotId]);
    const newSlot = slotRes.rows[0];
    if (!newSlot) {
      await client.query('ROLLBACK');
      return { ok: false, message: 'slot_not_found' };
    }

    // if request was previously approved and original slot existed -> re-add it
    if (req.prev_status === 'approved' && req.original_slot_id && (req.original_slot_start || req.original_slot_end)) {
      try {
        await client.query(
          `INSERT INTO slots(id, time, start, "end") VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO NOTHING`,
          [req.original_slot_id, req.original_slot_time, req.original_slot_start, req.original_slot_end]
        );
      } catch (e) {
        console.error('Failed to re-add original slot (best-effort):', e);
      }
    }

    // delete the chosen new slot (claim it)
    await client.query('DELETE FROM slots WHERE id=$1', [newSlot.id]);

    // update request to point to new slot and set approved
    await client.query(
      `UPDATE requests SET slot_id = $2, time = $3, status = $4, prev_status = NULL, pending_move_slot_id = NULL, pending_move_time = NULL WHERE id = $1`,
      [reqId, newSlot.id, newSlot.time, 'approved']
    );

    await client.query('COMMIT');
    return { ok: true, new_time: newSlot.time };
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('adminMoveRequest transaction error:', err);
    return { ok: false, message: 'tx_error' };
  } finally {
    client.release();
  }
}

async function applyClientMove(pool, reqId) {
  // keep existing for backward compatibility (not used in admin flow)
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const reqRes = await client.query('SELECT * FROM requests WHERE id=$1 FOR UPDATE', [reqId]);
    const req = reqRes.rows[0];
    if (!req || !req.pending_move_slot_id) {
      await client.query('ROLLBACK');
      return { ok: false, message: 'Нет запроса на перенос' };
    }

    const slotRes = await client.query('SELECT * FROM slots WHERE id=$1 FOR UPDATE', [req.pending_move_slot_id]);
    const newSlot = slotRes.rows[0];
    if (!newSlot) {
      await client.query(
        `UPDATE requests SET pending_move_slot_id = NULL, pending_move_time = NULL, status = COALESCE(prev_status, status), prev_status = NULL
         WHERE id = $1`, [reqId]
      );
      await client.query('COMMIT');
      return { ok: false, message: 'Выбранный слот уже недоступен' };
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
    return { ok: true, new_time: newSlot.time };
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('applyClientMove transaction error:', err);
    return { ok: false, message: 'Ошибка транзакции' };
  } finally {
    client.release();
  }
}

async function getApprovedRequestsNeedingNotifications(pool) {
  const rows = await pool.query(`
    SELECT r.*, s.start as slot_start, s.time as slot_time
    FROM requests r
    LEFT JOIN slots s ON r.slot_id = s.id
    WHERE r.status = 'approved' AND (COALESCE(r.notification_20_sent,false) = false OR COALESCE(r.notification_1h_sent,false) = false)
  `);
  return rows.rows;
}

async function getConditionalRequests(pool) {
  const res = await pool.query("SELECT * FROM requests WHERE status = 'conditional' ORDER BY created_at");
  return res.rows;
}

async function promoteConditionalRequest(pool, reqId, thresholdHours) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const reqRes = await client.query('SELECT * FROM requests WHERE id=$1 FOR UPDATE', [reqId]);
    const req = reqRes.rows[0];
    if (!req) {
      await client.query('ROLLBACK');
      return { ok: false, reason: 'not_found' };
    }
    if (!req.slot_id) {
      await client.query('UPDATE requests SET status = $1 WHERE id = $2', ['rejected', reqId]);
      await client.query('COMMIT');
      return { ok: false, reason: 'no_slot' };
    }

    const slotRes = await client.query('SELECT * FROM slots WHERE id=$1 FOR UPDATE', [req.slot_id]);
    if (slotRes.rowCount === 0) {
      await client.query('UPDATE requests SET status = $1 WHERE id = $2', ['rejected', reqId]);
      await client.query('COMMIT');
      return { ok: false, reason: 'slot_taken' };
    }
    const slot = slotRes.rows[0];
    const slotStart = new Date(slot.start);
    const now = new Date();
    const msThreshold = Number(thresholdHours) * 3600 * 1000;
    if (now.getTime() < slotStart.getTime() - msThreshold) {
      await client.query('ROLLBACK');
      return { ok: false, reason: 'too_early' };
    }

    // Find earlier slots and ensure ALL of them are covered by approved requests
    const earlySlotsRes = await client.query('SELECT id FROM slots WHERE start < $1', [slot.start]);
    const earlySlots = earlySlotsRes.rows.map(r => r.id);

    if (earlySlots.length > 0) {
      const notCoveredRes = await client.query(
        `SELECT s.id
         FROM slots s
         LEFT JOIN requests r ON r.slot_id = s.id AND r.status = 'approved'
         WHERE s.id = ANY($1) AND r.id IS NULL
         LIMIT 1`,
        [earlySlots]
      );
      if (notCoveredRes.rowCount > 0) {
        await client.query('ROLLBACK');
        return { ok: false, reason: 'early_slots_free' };
      }
    }

    await client.query('UPDATE requests SET status = $2 WHERE id = $1', [reqId, 'pending']);
    await client.query('COMMIT');
    return { ok: true, new_time: slot.time };
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('promoteConditionalRequest error', e);
    return { ok: false, reason: 'error' };
  } finally {
    client.release();
  }
}

module.exports = {
  initDb,

  getAllSlots,
  getEarliestSlot,
  getSlotById,
  addSlotToDb,
  deleteSlotById,

  getProcedures,
  addProcedureDb,
  deleteProcedureDb,
  getProcedureByKey,

  addRequestDb,
  checkDuplicateRequest,
  getRequestById,
  updateRequest,
  getRequestsByStatus,
  deleteRequestById,

  addHistoryItem,
  getHistoryForUser,

  addPatternDb,
  getPatternsDb,
  deletePatternDb,
  getPatternById,
  applyPatternToDate,

  isUserBlacklisted,
  addToBlacklist,
  removeFromBlacklist,
  getBlacklist,

  sendToAdmins,
  adminMoveRequest,
  applyClientMove,
  getApprovedRequestsNeedingNotifications,
  getConditionalRequests,
  promoteConditionalRequest
};
