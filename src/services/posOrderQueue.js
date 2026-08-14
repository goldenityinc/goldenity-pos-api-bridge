const { emitIncomingWebOrder, isDeviceOnline, connectedDevices } = require('./socketServer');

const DEFAULT_ACK_TIMEOUT_SECONDS = 30;
const getAckTimeoutSeconds = () => {
  const parsed = Number.parseInt(process.env.POS_ACK_TIMEOUT_SECONDS || '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_ACK_TIMEOUT_SECONDS;
};

// 🔴 CRITICAL REGRESSION FIX HR3: Fallback chain ENV VAR NAME COMPATIBILITY!
//    Lama (historical): env var = ADMIN_CORE_API_URL
//    Baru (posOrderQueue original L9): env var = ADMIN_CORE_API_BASE_URL (TIDAK ADA fallback ke nama lama)
//    Jika Railway user setup var LAMA (ADMIN_CORE_API_URL) tapi TIDAK setup var BARU →
//    posOrderQueue fallback ke http://localhost:5000 → container Railway tidak ada localhost service →
//    fetch failed ECONNREFUSED → HTTP 502 "fetch failed" (BUG B screenshot 2).
//    SOLUSI: Gunakan fallback chain SAMA SEPERTI accountingAutomationService.js + controllers:
//    ADMIN_CORE_API_BASE_URL || ADMIN_CORE_API_URL || ADMIN_CORE_URL || 'http://localhost:5000'
const ADMIN_CORE_URL_RAW =
  process.env.ADMIN_CORE_API_BASE_URL ||
  process.env.ADMIN_CORE_API_URL ||
  process.env.ADMIN_CORE_BASE_URL ||
  process.env.ADMIN_CORE_URL ||
  process.env.ADMIN_CORE_BACKEND_URL ||
  'http://localhost:5000';
const ADMIN_CORE_URL = ADMIN_CORE_URL_RAW.replace(/\/$/, '');
const ADMIN_CORE_INTERNAL_TOKEN = process.env.ADMIN_CORE_INTERNAL_TOKEN || '';
const ADMIN_CORE_TIMEOUT_MS = Number.parseInt(process.env.ADMIN_CORE_API_TIMEOUT_MS || '5000', 10) || 5000;

// ===============================================================
// 🔴 CRITICAL BRIDGE FIX #1: AGGRESSIVE METADATA INJECTOR (MISSING TABLE/CUSTOMER/PAX)
// User: "Pesanan di POS masuk MASIH tidak menampilkan no meja, nama pelanggan, total pax"
// Root Cause: Web Ordering kirim metadata di NESTED object (orderData / transactionData / data / payload),
//             atau hanya kirim tableId (row ID = 27) TANPA table_number (human-readable "1").
// Solusi: Helper AGGRESSIVELY scan 3 lapisan (top-level -> nested obj -> nested table obj),
//         jika TIDAK KETEMU → AUTO DERIVE, lalu TULIS BACK KE PAYLOAD di SEMUA key variants,
//         supaya downstream POS socket emit / polling client / submissionStates cache SEMUA DAPAT!
// ===============================================================
const _firstStrNonEmpty = (obj, keysArr) => {
  if (!obj || typeof obj !== 'object') return '';
  for (const k of keysArr) {
    try {
      const v = obj[k];
      if (v === undefined || v === null) continue;
      const s = String(v).trim();
      const lower = s.toLowerCase();
      if (!s) continue;
      if (lower === 'null' || lower === 'undefined' || lower === '-') continue;
      return s;
    } catch (_) { /* noop */ }
  }
  return '';
};
const _firstFiniteNumber = (obj, keysArr, fallback = 0) => {
  if (!obj || typeof obj !== 'object') return fallback;
  for (const k of keysArr) {
    try {
      const raw = obj[k];
      if (raw === undefined || raw === null) continue;
      const n = Number(raw);
      if (Number.isFinite(n)) return n;
      // Try parse int: e.g. "Meja 5" -> 5
      const s = String(raw).trim();
      if (!s) continue;
      const m = s.match(/(\d+(?:[.,]\d+)?)/);
      if (m && m[1]) {
        const p = Number(m[1].replace(/,/g, '.'));
        if (Number.isFinite(p)) return p;
      }
    } catch (_) { /* noop */ }
  }
  return fallback;
};
const _collectAllNestedPlainObjects = (root) => {
  const results = [];
  const seen = new WeakSet();
  const stack = [root];
  while (stack.length) {
    try {
      const cur = stack.pop();
      if (!cur || typeof cur !== 'object') continue;
      if (Array.isArray(cur)) {
        for (const it of cur) stack.push(it);
        continue;
      }
      if (seen.has(cur)) continue;
      seen.add(cur);
      results.push(cur);
      for (const key of ['table', 'orderData', 'order_data', 'transactionData', 'transaction_data', 'data', 'payload', 'body', 'raw', 'context', 'meta', 'metadata', 'detail', 'details', 'customer', 'buyer', 'guest']) {
        if (cur[key] && typeof cur[key] === 'object') stack.push(cur[key]);
      }
    } catch (_) { /* noop */ }
  }
  return results;
};
const injectMissingOrderMetadata = (payloadRaw, ctx = {}) => {
  const target = (payloadRaw && typeof payloadRaw === 'object') ? payloadRaw : Object.create(null);
  // Build candidate objects search space (top + ctx + nested)
  const candidates = [target];
  if (ctx && typeof ctx === 'object') {
    candidates.push(ctx);
    if (ctx.rawBody && typeof ctx.rawBody === 'object') candidates.push(ctx.rawBody);
    if (ctx.reqBody && typeof ctx.reqBody === 'object') candidates.push(ctx.reqBody);
  }
  for (const c of _collectAllNestedPlainObjects(target)) candidates.push(c);
  if (ctx) {
    for (const c of _collectAllNestedPlainObjects(ctx)) candidates.push(c);
  }
  // --------- TABLE NUMBER ---------
  let resolvedTable = _firstStrNonEmpty(target, [
    'tableNumber', 'table_number', 'tableNo', 'table_no', 'tableName', 'table_name',
    'tableLabel', 'table_label', 'tableNumberLabel', 'nomorMeja', 'nomor_meja', 'noMeja', 'no_meja',
    'table',
  ]);
  if (!resolvedTable) {
    for (const cand of candidates) {
      resolvedTable = _firstStrNonEmpty(cand, [
        'tableNumber', 'table_number', 'tableNo', 'table_no', 'tableName', 'table_name',
        'tableLabel', 'table_label', 'nomorMeja', 'nomor_meja', 'noMeja', 'no_meja', 'number', 'no', 'label', 'name',
      ]);
      if (resolvedTable) break;
    }
  }
  // --------- TABLE ID (numeric row id / uuid) ---------
  let resolvedTableId = _firstStrNonEmpty(target, ['tableId', 'table_id', 'tableID']);
  if (!resolvedTableId) {
    for (const cand of candidates) {
      resolvedTableId = _firstStrNonEmpty(cand, ['tableId', 'table_id', 'tableID', 'id']);
      if (resolvedTableId) break;
    }
  }
  // ✅ AUTO DERIVE TABLE NUMBER JIKA MASIH KOSONG:
  //    Jika kita punya tableId = "27" tapi tidak punya table_number → TIDAK BISA convert ke nomor meja 1.
  //    Tapi JIKA table.object punya number field (candidates diatas sudah scan), atau
  //    JIKA top-level ada NESTED number = "1" (contoh: payload.number = "1") → pakai itu.
  //    Jika TETAP KOSONG → Fallback safe: "Meja {tableId}" JIKA tableId numeric simple < 999 (bukan uuid).
  if (!resolvedTable) {
    const simpleTableIdNum = resolvedTableId.match(/^(\d{1,4})$/) ? Number(resolvedTableId) : NaN;
    if (Number.isFinite(simpleTableIdNum) && simpleTableIdNum > 0 && simpleTableIdNum < 2000) {
      resolvedTable = String(simpleTableIdNum);
    }
  }
  // --------- ORDER ID / REFERENCE ---------
  let resolvedOrderId = _firstStrNonEmpty(target, [
    'referenceId', 'reference_id', 'orderId', 'order_id', 'transactionId', 'transaction_id',
    'txId', 'tx_id', 'receiptNumber', 'receipt_number', 'invoiceNumber', 'invoice_number', 'id',
  ]);
  if (!resolvedOrderId) {
    for (const cand of candidates) {
      resolvedOrderId = _firstStrNonEmpty(cand, [
        'referenceId', 'reference_id', 'orderId', 'order_id', 'transactionId', 'transaction_id',
        'txId', 'tx_id', 'receiptNumber', 'receipt_number', 'invoiceNumber', 'invoice_number', 'id',
      ]);
      if (resolvedOrderId) break;
    }
  }
  // --------- CUSTOMER NAME ---------
  let resolvedCustomer = _firstStrNonEmpty(target, [
    'customerName', 'customer_name', 'buyerName', 'buyer_name', 'customer', 'buyer', 'guest',
    'guestName', 'guest_name', 'pelanggan', 'namaPelanggan', 'nama_pelanggan', 'nama', 'name',
  ]);
  if (!resolvedCustomer) {
    for (const cand of candidates) {
      resolvedCustomer = _firstStrNonEmpty(cand, [
        'customerName', 'customer_name', 'buyerName', 'buyer_name', 'customer', 'buyer', 'guest',
        'guestName', 'guest_name', 'pelanggan', 'namaPelanggan', 'nama_pelanggan', 'nama', 'name',
      ]);
      if (resolvedCustomer) break;
    }
  }
  if (!resolvedCustomer) resolvedCustomer = 'Guest';
  // --------- NOTES ---------
  let resolvedNotes = _firstStrNonEmpty(target, [
    'notes', 'note', 'orderNote', 'order_note', 'orderNotes', 'order_notes',
    'specialNote', 'special_note', 'specialInstruction', 'special_instruction',
    'catatan', 'remark', 'remarks',
  ]);
  if (!resolvedNotes) {
    for (const cand of candidates) {
      resolvedNotes = _firstStrNonEmpty(cand, [
        'notes', 'note', 'orderNote', 'order_note', 'orderNotes', 'order_notes',
        'specialNote', 'special_note', 'specialInstruction', 'special_instruction',
        'catatan', 'remark', 'remarks',
      ]);
      if (resolvedNotes) break;
    }
  }
  // --------- PAX (default 1) ---------
  let resolvedPax = 0;
  for (const cand of [target, ...candidates]) {
    const n = _firstFiniteNumber(cand, [
      'pax', 'guestCount', 'guest_count', 'guests', 'customerCount', 'customer_count',
      'seatCount', 'seat_count', 'seats', 'jumlahOrang', 'jumlah_orang', 'people', 'persons', 'personCount',
    ], 0);
    if (n > 0) { resolvedPax = n; break; }
  }
  if (!resolvedPax || resolvedPax < 1) resolvedPax = 1;

  // 🔥🔥🔥 WRITE BACK ALL VARIANTS TO PAYLOAD supaya POS TIDAK USAH cari fallback!
  target.tableId = target.tableId || resolvedTableId;
  target.table_id = target.table_id || resolvedTableId;
  target.tableNumber = target.tableNumber || resolvedTable;
  target.table_number = target.table_number || resolvedTable;
  target.tableName = target.tableName || resolvedTable;
  target.table_name = target.table_name || resolvedTable;
  target.tableLabel = target.tableLabel || resolvedTable;
  target.table_label = target.table_label || resolvedTable;
  target.tableNo = target.tableNo || resolvedTable;
  target.table_no = target.table_no || resolvedTable;
  target.nomorMeja = target.nomorMeja || resolvedTable;
  target.nomor_meja = target.nomor_meja || resolvedTable;
  target.referenceId = target.referenceId || resolvedOrderId;
  target.reference_id = target.reference_id || resolvedOrderId;
  target.orderId = target.orderId || resolvedOrderId;
  target.order_id = target.order_id || resolvedOrderId;
  target.transactionId = target.transactionId || resolvedOrderId;
  target.transaction_id = target.transaction_id || resolvedOrderId;
  target.receiptNumber = target.receiptNumber || resolvedOrderId;
  target.receipt_number = target.receipt_number || resolvedOrderId;
  target.customerName = target.customerName || resolvedCustomer;
  target.customer_name = target.customer_name || resolvedCustomer;
  target.buyerName = target.buyerName || resolvedCustomer;
  target.buyer_name = target.buyer_name || resolvedCustomer;
  target.customer = target.customer || resolvedCustomer;
  target.guest = target.guest || resolvedCustomer;
  target.pelanggan = target.pelanggan || resolvedCustomer;
  target.notes = target.notes || resolvedNotes;
  target.note = target.note || resolvedNotes;
  target.orderNote = target.orderNote || resolvedNotes;
  target.order_note = target.order_note || resolvedNotes;
  target.pax = target.pax || resolvedPax;
  target.guestCount = target.guestCount || resolvedPax;
  target.guest_count = target.guest_count || resolvedPax;
  target.jumlahOrang = target.jumlahOrang || resolvedPax;
  target.jumlah_orang = target.jumlah_orang || resolvedPax;
  // Extra safety: nested table sub-object juga TULIS BACK (jika ada)
  if (target.table && typeof target.table === 'object') {
    target.table.tableNumber = target.table.tableNumber || resolvedTable;
    target.table.table_number = target.table.table_number || resolvedTable;
    target.table.name = target.table.name || resolvedTable;
    target.table.label = target.table.label || resolvedTable;
    target.table.number = target.table.number || resolvedTable;
    target.table.id = target.table.id || resolvedTableId;
  }
  return target;
};

const submissionStates = new Map();

const getUnprocessedPendingOrdersByBranch = (branchId, sinceTsMs) => {
  const results = [];
  const branch = (branchId || '').toString().trim();
  if (!branch) return results;
  const since = Number(sinceTsMs) || 0;
  const seen = new Set();
  for (const [submissionId, s] of submissionStates.entries()) {
    try {
      if (!s || seen.has(submissionId)) continue;
      const sBranch = String((s.branchId) ?? (s.result && s.result.branchId) ?? '').trim();
      if (!sBranch || sBranch !== branch) continue;
      const statusRaw =
        (s.result && (s.result.ackStatus || s.result.status)) ||
        s.status ||
        (s.resolved === true && s.result && s.result.ok ? 'POS_ACKNOWLEDGED' : 'PENDING_ACK');
      const isUnresolvedTerminalAckFail =
        statusRaw === 'FAILED_DELIVERY' ||
        statusRaw === 'POS_ACK_TIMEOUT' ||
        statusRaw === 'TIMEOUT' ||
        statusRaw === 'POS_DEVICE_OFFLINE' ||
        statusRaw === 'QUEUE_JOB_FAILED' ||
        statusRaw === 'QUEUE_RESOLVE_UNKNOWN';
      const stillPending =
        s.resolved !== true && (
          statusRaw === 'PENDING_ACK' ||
          statusRaw === 'QUEUED_FOR_POS' ||
          statusRaw === 'PROCESSING' ||
          statusRaw === null ||
          statusRaw === undefined ||
          statusRaw === ''
        );
      if (!(stillPending || isUnresolvedTerminalAckFail)) continue;
      const createdAtMs = Number(s.createdAt || s.enqueuedAt || (s.result && s.result.enqueuedAt) || 0);
      if (since > 0 && createdAtMs > 0 && createdAtMs <= since) continue;
      const orderPayload = s.orderPayload || (s.result && s.result.orderPayload) || null;
      const targetDeviceUuid = s.targetDeviceUuid || s.resolvedTargetDeviceUuid || (s.result && (s.result.targetDeviceUuid || s.result.resolvedTargetDeviceUuid)) || '';
      const tenantId = String((s.tenantId) ?? (s.result && s.result.tenantId) ?? '').trim();
      const transactionId = (s.transactionId ?? (s.result && s.result.transactionId) ?? null);
      const salesRecordId = (s.salesRecordId ?? (s.result && s.result.salesRecordId) ?? null);
      seen.add(submissionId);
      results.push({
        submissionId,
        tenantId,
        branchId: sBranch,
        targetDeviceUuid,
        orderPayload,
        transactionId,
        salesRecordId,
        serverTs: createdAtMs || Date.now(),
        queuedAt: createdAtMs || Date.now(),
        status: statusRaw || 'PENDING_ACK',
        _replayFromSubmissionStates: true,
      });
    } catch (_e) {
      continue;
    }
  }
  return results;
};

const longPollBuckets = new Map();

const getLongPollBucket = (deviceUuid) => {
  if (!longPollBuckets.has(deviceUuid)) {
    longPollBuckets.set(deviceUuid, []);
  }
  return longPollBuckets.get(deviceUuid);
};

const appendLongPollOrder = (deviceUuid, branchId, orderEnvelope) => {
  const safeUuid = (deviceUuid || '').toString().trim();
  if (safeUuid) {
    const bucket = getLongPollBucket(safeUuid);
    bucket.push({
      ...orderEnvelope,
      queuedAt: Date.now(),
    });
    if (bucket.length > 100) {
      bucket.splice(0, bucket.length - 100);
    }
    notifyLongPollWaiters(safeUuid);
  }
  const branchKey = `__branch:${branchId}`;
  const tenantId =
    (orderEnvelope && typeof orderEnvelope === 'object' && (orderEnvelope.tenantId || orderEnvelope.tenant_id)) ||
    (safeUuid && connectedDevices.get(safeUuid)?.tenantId) ||
    '';
  const tenantKey = `__tenant:${tenantId}`;
  if (branchId) {
    const branchBucket = getLongPollBucket(branchKey);
    branchBucket.push({
      ...orderEnvelope,
      queuedAt: Date.now(),
    });
    if (branchBucket.length > 200) {
      branchBucket.splice(0, branchBucket.length - 200);
    }
    notifyLongPollWaiters(branchKey);
  }
  if (tenantId) {
    const tenantBucket = getLongPollBucket(tenantKey);
    tenantBucket.push({
      ...orderEnvelope,
      queuedAt: Date.now(),
    });
    if (tenantBucket.length > 300) {
      tenantBucket.splice(0, tenantBucket.length - 300);
    }
    notifyLongPollWaiters(tenantKey);
  }
};

const longPollWaiters = new Map();
const notifyLongPollWaiters = (key) => {
  const waiters = longPollWaiters.get(key) || [];
  while (waiters.length > 0) {
    const w = waiters.shift();
    try { w(); } catch (_) {}
  }
};

// 🔴🔴 RAILWAY CPU SAVE HOTFIX:
//    [dbg HR2-HR3 deviceFetch] ADMIN_CORE_ENV_SET mencetak 80+ lines SETIAP REQUEST.
//    1 polling 200ms = 5 req/sec = 400 lines/sec = CPU 90% hanya ngeprint stderr.
//    LOG INI TIDAK BERGUNA LAGI setelah fix HR2 double-slash deployed (host Railway sudah
//    terkonfirmasi resolve normal). KITA COMMENT OUT 100%. Kalau perlu debug, aktifkan
//    sementara di lokal (bukan production).
const __DEBUG_ADMIN_CORE_FETCH_VERBOSE = false;
const __SHORT_TERM_CACHE_TTL_MS = 500;
const __shortTermFetchCache = new Map(); // key: method||url||bodyHash -> { expiresAt, result }

const adminCoreFetch = async (path, options = {}) => {
  // 🔴 CRITICAL REGRESSION FIX HR2: URL Normalize double slashes (selain protocol https://)
  const rawUrl = `${ADMIN_CORE_URL}/${path.replace(/^\//, '')}`;
  const url = rawUrl.replace(/([^:])\/{2,}/g, '$1/');
  const method = (options.method || 'GET').toUpperCase();
  const bodyStr = (options.body && typeof options.body === 'string') ? options.body : '';
  // Short-term cache: GET dengan URL YANG SAMA dalam 500ms → balikin hasil cache,
  // tidak hammer Admin Core. Ini mencegah snowball infinite polling loop.
  let cacheKey = null;
  if (method === 'GET') {
    cacheKey = `${method}||${url}`;
    const cached = __shortTermFetchCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.result;
    }
  }
  if (__DEBUG_ADMIN_CORE_FETCH_VERBOSE) {
    console.error('[dbg HR2-HR3 deviceFetch] ADMIN_CORE_ENV_SET:', {
      hasADMIN_CORE_API_BASE_URL: Boolean(process.env.ADMIN_CORE_API_BASE_URL),
      hasADMIN_CORE_API_URL: Boolean(process.env.ADMIN_CORE_API_URL),
      ADMIN_CORE_URL_RAW_len: String(ADMIN_CORE_URL_RAW || '').length,
      ADMIN_CORE_URL_resolved: ADMIN_CORE_URL,
      rawUrlBeforeNormalize: rawUrl,
      finalUrlAfterNormalize: url,
      doubleSlashesAfterProtocol: (url.match(/:\/\/[^/]+\/(.*)$/) || [])[1]?.split('//').length - 1 || 0,
      path_input: path,
      method,
    });
  }
  const headers = {
    'Content-Type': 'application/json',
    ...(options.headers || {}),
  };
  if (ADMIN_CORE_INTERNAL_TOKEN) {
    headers['Authorization'] = `Bearer ${ADMIN_CORE_INTERNAL_TOKEN}`;
    headers['X-Internal-Token'] = ADMIN_CORE_INTERNAL_TOKEN;
  }
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), ADMIN_CORE_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      ...options,
      headers,
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch (_) { data = { raw: text }; }
    const result = { ok: res.ok, status: res.status, data };
    if (cacheKey) {
      __shortTermFetchCache.set(cacheKey, { expiresAt: Date.now() + __SHORT_TERM_CACHE_TTL_MS, result });
    }
    return result;
  } catch (err) {
    clearTimeout(timeoutId);
    const result = { ok: false, status: 0, data: null, error: err.message || String(err) };
    if (cacheKey) {
      // Cache failure juga (150ms only) supaya network error loop tidak retry 10x
      __shortTermFetchCache.set(cacheKey, { expiresAt: Date.now() + 150, result });
    }
    return result;
  }
};

// GC cache periodic: hapus entry expired tiap 5 detik supaya Map tidak membesar tanpa batas
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of __shortTermFetchCache) {
    if (v.expiresAt < now) __shortTermFetchCache.delete(k);
  }
}, 5000);

const resolveDefaultPrinterDevice = async (branchId, tenantId) => {
  if (!branchId) return null;
  const result = await adminCoreFetch(`/api/v1/branches/${encodeURIComponent(branchId)}/devices/default-printer?tenantId=${encodeURIComponent(tenantId || '')}`, {
    method: 'GET',
  });
  if (result.ok && result.data) {
    const d = result.data?.data?.deviceUuid || result.data?.deviceUuid || result.data?.device_uuid || result.data?.data?.device_uuid;
    if (d && String(d).trim()) return String(d).trim();
  }
  return null;
};

const updateOrderSyncStatus = async (submissionId, tenantId, syncStatus, extra = {}) => {
  if (!submissionId) return null;
  const body = {
    submissionId,
    tenantId,
    syncStatus,
    ...extra,
  };
  const result = await adminCoreFetch('/api/v1/relay/orders/sync-status', {
    method: 'PATCH',
    body: JSON.stringify(body),
  }).catch(() => ({ ok: false }));
  if (!result.ok) {
    return adminCoreFetch(`/api/v1/orders/${encodeURIComponent(submissionId)}/sync-status`, {
      method: 'PATCH',
      body: JSON.stringify({ syncStatus, ...extra }),
    }).catch(() => ({ ok: false }));
  }
  return result;
};

const notifyAdminCoreAck = async (submissionId, tenantId, ackStatus, ackPayload = {}) => {
  if (!submissionId) return null;
  const body = {
    submissionId,
    tenantId,
    ackStatus,
    ackPayload,
  };
  const result = await adminCoreFetch('/api/v1/relay/orders/ack', {
    method: 'POST',
    body: JSON.stringify(body),
  }).catch(() => ({ ok: false }));
  if (!result.ok) {
    return adminCoreFetch(`/api/v1/orders/${encodeURIComponent(submissionId)}/acknowledge`, {
      method: 'POST',
      body: JSON.stringify({ ackStatus, ackPayload, submissionId }),
    }).catch(() => ({ ok: false }));
  }
  return result;
};

let useBullMq = false;
let Queue = null;
let Worker = null;
let QueueEvents = null;
let posOrderQueueInstance = null;
let posOrderWorker = null;
let posOrderQueueEvents = null;
const queueJobMeta = new Map();

const inMemoryQueue = [];
let inMemoryProcessorRunning = false;
let inMemoryQueueLengthPerBranch = new Map();
// P4 BULLETPROOF (Concurrency Throttle per Branch):
//   User requirement #1: 10 meja order bersamaan 100% tanpa gangguan.
//   Kita throttle EMIT socket (processQueueJob) ke max 3 IN FLIGHT per branch.
//   Alasan: Printer thermal hanya bisa cetak 1 dokumen dalam 1 waktu (serial).
//     Jika kita blast 10 emit socket secara paralel → 10 print job dikirim
//     ke printer buffer BERSAMAAN → bytes campur aduk → struk & checker
//     TERCAMPUR potongan kertas.
//   Algoritma: Map<branchId, { inFlight: int, queue: Array<() => Promise<void>> }>
//     Setiap processQueueJob acquire slot: jika inFlight < CONCURRENCY_PER_BRANCH
//     → jalankan langsung, ELSE → push ke waiters array & jalankan ketika
//     release slot dipanggil (setelah emit done + ack / race timeout 25s).
const CONCURRENCY_PER_BRANCH = 3;
const BRANCH_EMIT_LOCK_TIMEOUT_MS = 25000;
const branchConcurrency = new Map();
function _getBranchConcurrencyState(branchId) {
  const key = String(branchId || '__global').trim();
  let s = branchConcurrency.get(key);
  if (!s) {
    s = { inFlight: 0, waiters: [], seq: 0 };
    branchConcurrency.set(key, s);
  }
  return s;
}
async function _acquireBranchSlot(branchId) {
  const state = _getBranchConcurrencyState(branchId);
  if (state.inFlight < CONCURRENCY_PER_BRANCH) {
    state.inFlight += 1;
    const mySeq = (state.seq = (state.seq || 0) + 1);
    return () => {
      const s2 = _getBranchConcurrencyState(branchId);
      if (s2.inFlight > 0) s2.inFlight -= 1;
      const next = s2.waiters.shift();
      if (next && typeof next === 'function') {
        setImmediate(() => next());
      }
    };
  }
  const mySeq = (state.seq = (state.seq || 0) + 1);
  return new Promise((resolve) => {
    let resolved = false;
    const timer = setTimeout(() => {
      if (resolved) return;
      resolved = true;
      state.inFlight += 1;
      resolve(() => {
        const s2 = _getBranchConcurrencyState(branchId);
        if (s2.inFlight > 0) s2.inFlight -= 1;
        const next = s2.waiters.shift();
        if (next && typeof next === 'function') setImmediate(() => next());
      });
    }, BRANCH_EMIT_LOCK_TIMEOUT_MS);
    state.waiters.push(() => {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      state.inFlight += 1;
      resolve(() => {
        const s2 = _getBranchConcurrencyState(branchId);
        if (s2.inFlight > 0) s2.inFlight -= 1;
        const next = s2.waiters.shift();
        if (next && typeof next === 'function') setImmediate(() => next());
      });
    });
  });
}
function _getQueueSnapshotForBranch(branchId) {
  const state = _getBranchConcurrencyState(branchId);
  return {
    concurrency: CONCURRENCY_PER_BRANCH,
    inFlight: state.inFlight,
    waiting: state.waiters ? state.waiters.length : 0,
    inMemoryPending: Number(inMemoryQueueLengthPerBranch.get(branchId || '__global') || 0),
  };
}

const getQueuePendingCount = () => {
  if (useBullMq && posOrderQueueInstance) {
    return posOrderQueueInstance.getWaitingCount().catch(() => inMemoryQueue.length);
  }
  return Promise.resolve(inMemoryQueue.length);
};

const getQueueLengthPerBranch = () => {
  const snapshot = {};
  for (const [k, v] of inMemoryQueueLengthPerBranch.entries()) {
    snapshot[k] = v || 0;
  }
  return snapshot;
};

const runInMemoryProcessor = async () => {
  if (inMemoryProcessorRunning) return;
  inMemoryProcessorRunning = true;
  try {
    while (inMemoryQueue.length > 0) {
      const item = inMemoryQueue.shift();
      if (!item) continue;
      const { branchId } = item || {};
      if (branchId) {
        const cur = inMemoryQueueLengthPerBranch.get(branchId) || 0;
        inMemoryQueueLengthPerBranch.set(branchId, Math.max(0, cur - 1));
      }
      try {
        await processQueueJob(item);
      } catch (err) {
        console.warn('[posOrderQueue] in-memory job error:', err?.message || err);
      }
    }
  } finally {
    inMemoryProcessorRunning = false;
    if (inMemoryQueue.length > 0) {
      setImmediate(runInMemoryProcessor);
    }
  }
};

const enqueueInMemory = (jobData) => {
  const submissionId = jobData.submissionId;
  queueJobMeta.set(submissionId, { enqueuedAt: Date.now(), retries: 0 });
  inMemoryQueue.push(jobData);
  const { branchId } = jobData || {};
  if (branchId) {
    const cur = inMemoryQueueLengthPerBranch.get(branchId) || 0;
    inMemoryQueueLengthPerBranch.set(branchId, cur + 1);
  }
  setImmediate(runInMemoryProcessor);
  return Promise.resolve();
};

const estimateEtaSeconds = async (branchId) => {
  // 🔴 CRITICAL ETA DYNAMIC FIX (User exact request):
  //    - 30 detik = benchmark JIKA SIBUK (bukan hardcoded 30 selaulu!)
  //    - JIKA TIDAK ADA antrian (pending count=0) → 10 detik default (LEBIH CEPAT).
  //    - BISA BERTAMBAH SESUAI QUEUE: base 10 + 15 detik per pending order, MAX 120s.
  const pending = await getQueuePendingCount();
  const perPendingBonusSeconds = 15;
  const baseNoQueueSeconds = 10;
  const maxSeconds = 120;
  // Branch length extra jika ada:
  let branchAdjust = 0;
  if (branchId && typeof inMemoryQueueLengthPerBranch.get === 'function') {
    const branchLen = Number(inMemoryQueueLengthPerBranch.get(branchId)) || 0;
    if (branchLen > 0) branchAdjust = Math.min(60, branchLen * 10); // max 60s extra dari branch
  }
  const total = Math.max(0, Math.round(baseNoQueueSeconds + (Number.isFinite(pending) ? pending : 0) * perPendingBonusSeconds + branchAdjust));
  return Math.min(maxSeconds, total);
};

// Sync variant (TIDAK BOLEH await apapun, INSTANT, safe dipanggil sebelum response header)
const estimateEtaSecondsSync = (branchId) => {
  const perPendingBonusSeconds = 15;
  const baseNoQueueSeconds = 10;
  const maxSeconds = 120;
  const inMemLen = Number(inMemoryQueue && Array.isArray(inMemoryQueue) ? inMemoryQueue.length : 0) || 0;
  let branchAdjust = 0;
  if (branchId && typeof inMemoryQueueLengthPerBranch.get === 'function') {
    const branchLen = Number(inMemoryQueueLengthPerBranch.get(branchId)) || 0;
    if (branchLen > 0) branchAdjust = Math.min(60, branchLen * 10);
  }
  const total = baseNoQueueSeconds + inMemLen * perPendingBonusSeconds + branchAdjust;
  return Math.min(maxSeconds, Math.max(baseNoQueueSeconds, Math.round(total)));
};

const enqueueWebOrderForPrinting = async ({
  tenantId,
  branchId,
  targetDeviceUuid,
  orderPayload,
  submissionId,
  transactionId,
  salesRecordId,
  rawBody,
}) => {
  const originalInputSubmissionId = (submissionId || '').toString().trim();
  if (!originalInputSubmissionId) {
    throw Object.assign(new Error('submissionId wajib disediakan'), { statusCode: 400, code: 'MISSING_SUBMISSION_ID' });
  }

  // 🔴 [MISSING METADATA FIX] Panggil injectMissingOrderPayload SEBELUM APA-APA, passing rawBody
  //    supaya NESTED metadata di rawBody (orderData/transactionData) juga di scan.
  //    Hasilnya: orderPayload table_number/customer_name/pax TIDAK KOSONG LAGI.
  try {
    injectMissingOrderMetadata(orderPayload, { rawBody, tenantId, branchId, transactionId, salesRecordId, submissionId: originalInputSubmissionId });
  } catch (_) { /* noop */ }

  // ================================================================
  // 🔴 CRITICAL FIX 2B (DUPLICATE SUBMISSION_ID REWRITE SAFETY NET):
  //    User request: "Tambahkan logic BE check idempotent agar ketika
  //    ada orderan BEDA MEJA yang masuk dengan submissionId SAMA
  //    (karena race condition cache localStorage / Date.now() collision
  //    di frontend web order), backend HARUS rewrite submissionId
  //    dengan suffix _dup_N agar POS MENERIMA KEDUA ORDER SECARA
  //    BERURUTAN, BUKAN reject duplicate / return cache pertama.
  //
  //    Sebelumnya (L703-711 existing check): submissionId SAMA →
  //    order kedua akan langsung return existing.result (cache) atau
  //    existing.promise → ORDER KEDUA HILANG TOTAL karena dianggap
  //    idempotent retry order YANG SAMA. Tapi real case-nya: ini 2
  //    MEJA BEDA (mis Meja 1 dan Meja 5) yang kebetulan collision
  //    TX-ID sama (user bug 3 screenshot bukti nyata!).
  //
  //    STRATEGI:
  //    a) Key unik = `${tenantId}::${branchId}::${cleanSubmissionId}`.
  //       Ini memisahkan cache antar tenant (jangan cross-tenant compare).
  //    b) Check di submissionStates: ada existing stateEntry untuk key
  //       ini yang masih aktif / resolved masih dalam 30 menit TTL?
  //    c) JIKA YA → increment counter rewrite = 1..99
  //       submissionId BARU = `${original}__dup_${N}`
  //       (double underscore agar mudah dibedakan dengan suffix random
  //       hex dari frontend, yang pakai single dash TX-xxx-5-a1b2c3)
  //    d) Loop ulang check sampai menemukan submissionId BENAR-BENAR
  //       UNIQUE (hingga counter 100 untuk safety).
  //    e) SETELAH rewrite SUCCESS, inject submissionId BARU ke
  //       orderPayload.submissionId + orderPayload.submission_id
  //       agar data konsisten (socket broadcast, DB, dan receipt
  //       POS semuanya melihat ID BARU).
  //    f) Tulis log supaya audit trail Railway jelas kapan rewrite
  //       terjadi dan ID lama vs ID baru.
  // ================================================================
  let rewriteIterations = 0;
  const MAX_REWRITE_ATTEMPTS = 100;
  const rewriteState = {
    hadCollision: false,
    original: originalInputSubmissionId,
    finalSubmissionId: originalInputSubmissionId,
    attempts: 0,
  };
  const cleanTenant = String(tenantId || '').trim();
  const cleanBranch = String(branchId || '').trim();
  const _buildCacheCollisionCheckKey = (subm) => `${cleanTenant}::${cleanBranch}::${String(subm || '').trim()}`;

  while (rewriteIterations < MAX_REWRITE_ATTEMPTS) {
    const currentCandidate = rewriteIterations === 0
      ? originalInputSubmissionId
      : `${originalInputSubmissionId}__dup_${rewriteIterations}`;
    const collisionCacheKey = _buildCacheCollisionCheckKey(currentCandidate);
    const existing = submissionStates.get(currentCandidate);

    if (existing) {
      // 🔴 COLLISION DETECTED pada submissionId ini.
      //    Safety guard: coba lagi dengan counter berikutnya.
      rewriteState.hadCollision = true;
      rewriteIterations += 1;
      rewriteState.attempts = rewriteIterations;
      continue;
    }

    // 🟢 Tidak ada collision pada candidate ini.
    rewriteState.finalSubmissionId = currentCandidate;
    rewriteState.attempts = rewriteIterations;
    break;
  }

  let cleanSubmissionId = rewriteState.finalSubmissionId;
  if (rewriteState.hadCollision && typeof console === 'object' && typeof console.warn === 'function') {
    try {
      const ts = new Date().toISOString();
      // eslint-disable-next-line no-console
      console.warn(
        `[${ts}] [POS-ORDER-QUEUE] [IDEMPOTENCY-RENAME] submissionId collision detected. ` +
        `original="${rewriteState.original}" final="${cleanSubmissionId}" ` +
        `tenant="${cleanTenant}" branch="${cleanBranch}" ` +
        `tableId="${(orderPayload && ((orderPayload.tableId || orderPayload.table_id || '')))}" ` +
        `tableNum="${(orderPayload && ((orderPayload.tableNumber || orderPayload.table_number || orderPayload.tableName || '')))}" ` +
        `attempts=${rewriteState.attempts}`
      );
    } catch (_logE) { /* DOUBLE SAFETY: log tidak boleh crash flow */ }
  }
  // ✅ Rewrite orderPayload fields agar SEMUA downstream pakai ID BARU:
  if (rewriteState.hadCollision && orderPayload && typeof orderPayload === 'object') {
    try { orderPayload.submissionId = cleanSubmissionId; } catch (_) {}
    try { orderPayload.submission_id = cleanSubmissionId; } catch (_) {}
  }

  const existing = submissionStates.get(cleanSubmissionId);
  if (existing) {
    if (existing.resolved) {
      return { ...existing.result, _fromCache: true, _idempotencyRenameApplied: rewriteState.hadCollision, _originalSubmissionId: rewriteState.hadCollision ? rewriteState.original : undefined };
    }
    if (existing.processing && existing.promise) {
      return existing.promise;
    }
  }

  const stateEntry = {
    processing: true,
    resolved: false,
    result: null,
    resolvePromise: null,
    rejectPromise: null,
    retries: 0,
    watchdogTimer: null,
    resolvedTargetDeviceUuid: '',
    promise: null,
    enqueuedAt: Date.now(),
  };

  stateEntry.promise = new Promise((resolve, reject) => {
    stateEntry.resolvePromise = resolve;
    stateEntry.rejectPromise = reject;
  });

  submissionStates.set(cleanSubmissionId, stateEntry);

  try {
    // 🔴 CRITICAL FIX - BROADCAST TO ENTIRE BRANCH ROOM, NOT ONE DEVICE:
    //    User melaporkan TABLET Android pake fallback UUID TIDAK PERNAH dapat notif,
    //    karena sebelumnya logic resolveDefaultPrinterDevice SELALU pilih 1 device
    //    (Printer Default = Windows PC saja). SEKARANG: PAKSA resolvedTargetDeviceUuid = ''
    //    supaya emitIncomingWebOrder LANGSUNG broadcast BRANCH ROOM DULU ke SEMUA
    //    device (Tablet + PC + Printer lain) yang connected di branch itu.
    //    Frontend aplikasi masing-masing yang decide mau print / tampilkan notif atau tidak.
    let resolvedTargetDeviceUuid = '';
    // (Line 373-378 resolveDefaultPrinterDevice DIBYPASS SEMUA)
    stateEntry.resolvedTargetDeviceUuid = resolvedTargetDeviceUuid;

    let upstreamSaved = false;
    try {
      await updateOrderSyncStatus(cleanSubmissionId, tenantId, 'QUEUED_FOR_POS', {
        resolvedTargetDeviceUuid,
        transactionId,
        salesRecordId,
      });
      stateEntry.upstreamSavedQueuedAt = Date.now();
      upstreamSaved = true;
    } catch (err) {
      stateEntry.upstreamSavedQueuedAt = 0;
      upstreamSaved = false;
      console.error(`[posOrderQueue:enqueue] ❌ UPSTREAM DB FAIL for submission=${cleanSubmissionId}. Will NOT broadcast socket to avoid GHOST ORDER. err=`, err?.message || err);
      try { if (stateEntry.watchdogTimer) clearTimeout(stateEntry.watchdogTimer); } catch (_) {}
      stateEntry.watchdogTimer = null;
      try { finalizeReject(stateEntry, cleanSubmissionId, 'UPSTREAM_DB_SAVE_FAILED', 502, 'Gagal menyimpan pesanan ke pusat. Silakan coba lagi.', { retryAvailable: true, _dbFailed: true }); } catch (_) {}
      submissionStates.delete(cleanSubmissionId);
      throw Object.assign(new Error('UPSTREAM_DB_SAVE_FAILED: order tidak dapat disimpan ke pusat'), { code: 'UPSTREAM_DB_SAVE_FAILED', statusCode: 502, retryAvailable: true });
    }

    if (!upstreamSaved) {
      submissionStates.delete(cleanSubmissionId);
      throw Object.assign(new Error('UPSTREAM_DB_SAVE_FAILED'), { code: 'UPSTREAM_DB_SAVE_FAILED', statusCode: 502, retryAvailable: true });
    }

    const jobData = {
      tenantId,
      branchId,
      targetDeviceUuid: resolvedTargetDeviceUuid,
      orderPayload,
      submissionId: cleanSubmissionId,
      transactionId: transactionId || null,
      salesRecordId: salesRecordId || null,
      resolvedTargetDeviceUuid,
      attempt: 0,
    };

    startWatchdog(stateEntry, cleanSubmissionId, tenantId, branchId, resolvedTargetDeviceUuid, orderPayload, transactionId, salesRecordId);

    if (useBullMq && posOrderQueueInstance) {
      try {
        await posOrderQueueInstance.add(`pos-order-${cleanSubmissionId}`, jobData, {
          jobId: `pos-order-${cleanSubmissionId}`,
          removeOnComplete: true,
          removeOnFail: true,
          attempts: 2,
          backoff: { type: 'exponential', delay: 2000 },
        });
        queueJobMeta.set(cleanSubmissionId, { enqueuedAt: Date.now(), retries: 0 });
      } catch (_) {
        await enqueueInMemory(jobData);
      }
    } else {
      await enqueueInMemory(jobData);
    }

    return await stateEntry.promise;
  } catch (err) {
    if (stateEntry.watchdogTimer) {
      clearTimeout(stateEntry.watchdogTimer);
      stateEntry.watchdogTimer = null;
    }
    const errCode = err?.code || err?.error;
    const errStatus = Number(err?.statusCode) || 0;
    const shouldKeepInQueue =
      errCode === 'POS_DEVICE_OFFLINE' ||
      errCode === 'POS_ACK_TIMEOUT' ||
      errCode === 'QUEUE_RESOLVE_UNKNOWN' ||
      errStatus === 503 || errStatus === 504 || errStatus === 202 || errStatus === 502;
    if (shouldKeepInQueue && submissionStates.has(cleanSubmissionId)) {
      stateEntry.status = (errCode === 'POS_ACK_TIMEOUT' ? 'TIMEOUT' : (errCode === 'POS_DEVICE_OFFLINE' ? 'POS_DEVICE_OFFLINE' : 'PENDING_ACK'));
      stateEntry.resolved = false;
      stateEntry.processing = false;
      stateEntry.result = stateEntry.result || { error: err, _keepInQueue: true };
      stateEntry.retryAvailable = err.retryAvailable !== false;
    } else {
      submissionStates.delete(cleanSubmissionId);
    }
    throw err;
  }
};

const startWatchdog = (stateEntry, submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId) => {
  const timeoutSec = getAckTimeoutSeconds();
  if (stateEntry.watchdogTimer) {
    clearTimeout(stateEntry.watchdogTimer);
  }
  // 🔴 100% UNHANDLED REJECTION SAFETY WRAPPER:
  // onWatchdogTimeout is async, so if ANY exception throws inside it
  // (updateOrderSyncStatus / finalizeReject / emitIncomingWebOrder),
  // the setTimeout callback would otherwise produce an unhandled promise
  // rejection that bubbles up to Node.js processTicksAndRejections crash.
  // We swallow and log here so the NODE SERVER STAYS ALIVE no matter what.
  stateEntry.watchdogTimer = setTimeout(() => {
    Promise.resolve()
      .then(() => onWatchdogTimeout(
        stateEntry, submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId,
      ))
      .catch((err) => {
        try {
          const ts = new Date().toISOString();
          // eslint-disable-next-line no-console
          console.error(
            `[${ts}] [posOrderQueue::startWatchdog] CAUGHT UNHANDLED EXCEPTION inside onWatchdogTimeout (server continue alive). ` +
            `submissionId=${submissionId} tenant=${tenantId} branch=${branchId} errName=${err?.name || '-'} errMsg=${err?.message || err} errStack=${(err?.stack || '').split('\n').slice(0, 4).join(' | ')}`,
          );
        } catch (_) { /* double safety: logger error never kills watchdog either */ }
      });
  }, timeoutSec * 1000);
};

const onWatchdogTimeout = async (stateEntry, submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId) => {
  try {
    if (stateEntry.resolved) return;
    stateEntry.watchdogTimer = null;
    const MAX_WATCHDOG_RETRIES = 2;
    const retriesSoFar = Number(stateEntry.retries || 0);

    if (retriesSoFar < MAX_WATCHDOG_RETRIES) {
      stateEntry.retries = retriesSoFar + 1;
      const upstreamSafe = Boolean(stateEntry.upstreamSavedQueuedAt && stateEntry.upstreamSavedQueuedAt > 0);
      //#region debug-point web-order-bugs-watchdog-retry
      try {
        const ts = new Date().toISOString();
        // eslint-disable-next-line no-console
        console.error(
          `[${ts}] [DEBUG-WEB-ORDER] [WATCHDOG-RETRY] submissionId=${submissionId} retriesCount=${stateEntry.retries} max=${MAX_WATCHDOG_RETRIES} tenant=${tenantId} branch=${branchId} beforeBroadcastDevice=${String(deviceUuid || '(empty=broadcast)')} upstreamSavedQueuedAt=${String(stateEntry.upstreamSavedQueuedAt || 0)} upstreamSafe=${upstreamSafe} resolved=${!!stateEntry.resolved} processing=${!!stateEntry.processing}`,
        );
      } catch (_dbg) { /* noop */ }
      //#endregion
      // 🔴 CRITICAL FIX GHOST NOTIF: HANYA emit socket JIKA upstream DB SUDAH confirmed
      //    (upstreamSavedQueuedAt > 0). Kalau upstream belum confirmed → jangan spam POS.
      if (upstreamSafe) {
        const envelope = buildOrderEnvelope(submissionId, tenantId, branchId, '', orderPayload, transactionId, salesRecordId);
        try {
          const emitResult = emitIncomingWebOrder('', branchId, envelope);
        } catch (_) { /* emit failure must NEVER kill watchdog */ }
        try {
          const branchKey = `__branch:${branchId}`;
          const branchBucket = getLongPollBucket(branchKey);
          branchBucket.push({ ...envelope, queuedAt: Date.now() });
          if (branchBucket.length > 200) branchBucket.splice(0, branchBucket.length - 200);
          try { notifyLongPollWaiters(branchKey); } catch (_) {}
        } catch (_) { /* longpoll push failure never kills flow */ }
      }
      startWatchdog(stateEntry, submissionId, tenantId, branchId, '', orderPayload, transactionId, salesRecordId);
      return;
    }

    const upstreamSafe = Boolean(stateEntry.upstreamSavedQueuedAt && stateEntry.upstreamSavedQueuedAt > 0);
    if (upstreamSafe) {
      try {
        await updateOrderSyncStatus(submissionId, tenantId, 'SYNC_DELAYED', {
          resolvedTargetDeviceUuid: deviceUuid,
          warning: 'POS_ACK_FALLBACK: Order disimpan di DB & akan di-pull POS otomatis dalam 1-3 menit.',
          transactionId,
          salesRecordId,
        });
      } catch (_) {}
      finalizeResolve(stateEntry, submissionId, {
        ok: true,
        code: 'SYNC_DELAYED',
        status: 'SYNC_DELAYED',
        ackStatus: 'SYNC_DELAYED',
        retryAvailable: false,
        warning: 'POS tidak merespon dalam batas waktu. Pesanan TETAP AMAN di database & POS akan mengambil otomatis (Auto-Pull 3 menit).',
        submissionId,
        transactionId: transactionId || null,
        orderId: salesRecordId || null,
        resolvedAt: Date.now(),
        _autoResolved: true,
      });
      return;
    }

    try {
      await updateOrderSyncStatus(submissionId, tenantId, 'FAILED_DELIVERY', {
        resolvedTargetDeviceUuid: deviceUuid,
        failureReason: 'POS_ACK_TIMEOUT',
        transactionId,
        salesRecordId,
      });
    } catch (_) { /* core notification failure -> ignore, continue finalize */ }

    // 🔴 SAFE FINALIZE (never throw). Instead of propagating a promise
    // rejection that no caller is awaiting (watchdog callbacks have no
    // caller awaiting the returned promise), finalizeReject below:
    //   (a) resolves the user-facing stateEntry.promise with the error
    //       (so poll/ack callers who AWAIT that promise get a proper result)
    //   (b) but NEVER allows that Error object to bubble up to
    //       processTicksAndRejections as an "unhandledRejection".
    finalizeReject(stateEntry, submissionId, {
      statusCode: 504,
      code: 'POS_ACK_TIMEOUT',
      message: `Perangkat POS tidak merespon dalam ${getAckTimeoutSeconds()} detik`,
      retryAvailable: true,
      submissionId,
      _fromWatchdog: true,
    });
  } catch (topLevelErr) {
    // ⚠️ 100% final safety net for onWatchdogTimeout body.
    // If anything above (envelope build, retries check, state checks, ...)
    // somehow throws, we log it and swallow. NO RE-THROW here.
    try {
      const ts = new Date().toISOString();
      // eslint-disable-next-line no-console
      console.error(
        `[${ts}] [posOrderQueue::onWatchdogTimeout] TOP-LEVEL catch (server continue alive). ` +
        `submissionId=${submissionId} tenant=${tenantId} branch=${branchId} err=${topLevelErr?.message || topLevelErr}`,
      );
    } catch (_) { /* logger error double guard */ }
  }
};

const normalizeTopLevelOrderEnvelopeFields = (orderPayloadRaw, ctx = {}) => {
  // 🔴 [FIX MISSING TABLE/CUSTOMER METADATA BRIDGE SIDE]
  //    Panggil injector AGGRESSIVE PERTAMA KALI sebelum normalisasi, supaya SEMUA
  //    key variants sudah TERISI PAYLOAD NYA (nested object auto-scan 3 level).
  try { injectMissingOrderMetadata(orderPayloadRaw, ctx); } catch (_) { /* noop */ }
  const payload = (orderPayloadRaw && typeof orderPayloadRaw === 'object') ? orderPayloadRaw : {};
  const fallback = Object.create(null);
  Object.assign(fallback, {
    tableId: payload.tableId || payload.table_id || ctx.tableId || ctx.table_id || '',
    table_id: payload.table_id || payload.tableId || ctx.table_id || ctx.tableId || '',
    tableNumber: payload.tableNumber || payload.table_number || payload.tableName || payload.table_name || payload.tableLabel || payload.table_label || payload.tableLabel || ((payload.table && (payload.table.tableNumber || payload.table.table_number || payload.table.name || payload.table.label || '')) || '') || ctx.tableNumber || ctx.table_number || '',
    table_number: payload.table_number || payload.tableNumber || ((payload.table && (payload.table.table_number || payload.table.tableNumber || payload.table.name || payload.table.label || '')) || '') || ctx.table_number || ctx.tableNumber || '',
    tableName: payload.tableName || payload.table_name || payload.tableNumber || payload.table_number || payload.tableLabel || payload.table_label || ((payload.table && (payload.table.tableNumber || payload.table.table_number || payload.table.name || payload.table.label || '')) || '') || ctx.tableNumber || ctx.table_number || '',
    table_name: payload.table_name || payload.tableName || ((payload.table && (payload.table.table_number || payload.table.tableNumber || payload.table.name || payload.table.label || '')) || '') || ctx.table_number || ctx.tableNumber || '',
    items: Array.isArray(payload.items) ? payload.items : (Array.isArray(payload.items_json) ? payload.items_json : (Array.isArray(payload.line_items) ? payload.line_items : (Array.isArray(payload.transaction_items) ? payload.transaction_items : []))),
    items_json: Array.isArray(payload.items_json) ? payload.items_json : (Array.isArray(payload.items) ? payload.items : (Array.isArray(payload.line_items) ? payload.line_items : (Array.isArray(payload.transaction_items) ? payload.transaction_items : []))),
    transactionId: payload.transactionId || payload.transaction_id || payload.receiptNumber || payload.receipt_number || ctx.transactionId || '',
    transaction_id: payload.transaction_id || payload.transactionId || payload.receipt_number || payload.receiptNumber || ctx.transactionId || '',
    orderId: payload.orderId || payload.order_id || payload.id || payload.salesRecordId || payload.sales_record_id || ctx.salesRecordId || '',
    order_id: payload.order_id || payload.orderId || payload.id || payload.sales_record_id || payload.salesRecordId || ctx.salesRecordId || '',
    receiptNumber: payload.receiptNumber || payload.receipt_number || payload.invoiceNumber || payload.invoice_number || payload.transactionId || payload.transaction_id || '',
    receipt_number: payload.receipt_number || payload.receiptNumber || payload.invoice_number || payload.invoiceNumber || payload.transaction_id || payload.transactionId || '',
    invoiceNumber: payload.invoiceNumber || payload.invoice_number || payload.receiptNumber || payload.receipt_number || '',
    invoice_number: payload.invoice_number || payload.invoiceNumber || payload.receipt_number || payload.receiptNumber || '',
    totalAmount: Number.isFinite(payload.totalAmount) ? payload.totalAmount : (Number.isFinite(payload.total_amount) ? payload.total_amount : (Number.isFinite(payload.grandTotal) ? payload.grandTotal : (Number.isFinite(payload.grand_total) ? payload.grand_total : (Number.isFinite(payload.totalPrice) ? payload.totalPrice : (Number.isFinite(payload.total_price) ? payload.total_price : 0))))),
    total_amount: Number.isFinite(payload.total_amount) ? payload.total_amount : (Number.isFinite(payload.totalAmount) ? payload.totalAmount : (Number.isFinite(payload.grand_total) ? payload.grand_total : (Number.isFinite(payload.grandTotal) ? payload.grandTotal : (Number.isFinite(payload.total_price) ? payload.total_price : (Number.isFinite(payload.totalPrice) ? payload.totalPrice : 0))))),
    grandTotal: Number.isFinite(payload.grandTotal) ? payload.grandTotal : (Number.isFinite(payload.grand_total) ? payload.grand_total : (Number.isFinite(payload.totalAmount) ? payload.totalAmount : (Number.isFinite(payload.total_amount) ? payload.total_amount : 0))),
    grand_total: Number.isFinite(payload.grand_total) ? payload.grand_total : (Number.isFinite(payload.grandTotal) ? payload.grandTotal : (Number.isFinite(payload.total_amount) ? payload.total_amount : (Number.isFinite(payload.totalAmount) ? payload.totalAmount : 0))),
    pax: Number.isFinite(payload.pax) ? payload.pax : (Number.isFinite(payload.guestCount) ? payload.guestCount : (Number.isFinite(payload.guests) ? payload.guests : (Number.isFinite(payload.customer_count) ? payload.customer_count : 1))),
    guestCount: Number.isFinite(payload.guestCount) ? payload.guestCount : (Number.isFinite(payload.pax) ? payload.pax : (Number.isFinite(payload.guests) ? payload.guests : (Number.isFinite(payload.customer_count) ? payload.customer_count : 1))),
    customer_count: Number.isFinite(payload.customer_count) ? payload.customer_count : (Number.isFinite(payload.pax) ? payload.pax : (Number.isFinite(payload.guests) ? payload.guests : (Number.isFinite(payload.guestCount) ? payload.guestCount : 1))),
    customerName: String(payload.customerName || payload.customer_name || payload.customer || payload.guest || payload.guestName || '').trim(),
    customer_name: String(payload.customer_name || payload.customerName || payload.customer || payload.guest || payload.guestName || '').trim(),
    customer: String(payload.customer || payload.customerName || payload.customer_name || payload.guest || payload.guestName || '').trim(),
    guest: String(payload.guest || payload.guestName || payload.customerName || payload.customer_name || payload.customer || '').trim(),
    orderNote: String(payload.orderNote || payload.order_note || payload.notes || payload.specialInstruction || payload.special_instruction || payload.specialNote || payload.special_note || payload.note || payload.remarks || '').trim(),
    order_note: String(payload.order_note || payload.orderNote || payload.notes || payload.special_note || payload.specialNote || payload.note || payload.remarks || '').trim(),
    order_notes: String(payload.order_notes || payload.notes || payload.order_note || payload.orderNote || payload.special_note || payload.specialNote || payload.note || '').trim(),
    notes: String(payload.notes || payload.orderNote || payload.order_note || payload.order_notes || payload.specialNote || payload.special_note || payload.specialInstruction || payload.special_instruction || payload.note || payload.remarks || '').trim(),
    note: String(payload.note || payload.notes || payload.orderNote || payload.order_note || payload.specialNote || payload.special_note || payload.remarks || '').trim(),
    specialNote: String(payload.specialNote || payload.special_note || payload.orderNote || payload.order_note || payload.notes || payload.note || payload.remarks || '').trim(),
    special_note: String(payload.special_note || payload.specialNote || payload.order_note || payload.orderNote || payload.notes || payload.note || payload.remarks || '').trim(),
    specialInstruction: String(payload.specialInstruction || payload.special_instruction || payload.orderNote || payload.order_note || payload.notes || payload.special_note || payload.specialNote || '').trim(),
    special_instruction: String(payload.special_instruction || payload.specialInstruction || payload.order_note || payload.orderNote || payload.notes || payload.special_note || payload.specialNote || '').trim(),
    remarks: String(payload.remarks || payload.notes || payload.orderNote || payload.order_note || payload.special_note || payload.specialNote || '').trim(),
    paymentMethod: String(payload.paymentMethod || payload.payment_method || '').trim(),
    payment_method: String(payload.payment_method || payload.paymentMethod || '').trim(),
    orderType: String(payload.orderType || payload.order_type || payload.ordertype || 'QR_ORDER').toUpperCase(),
    order_type: String(payload.order_type || payload.orderType || payload.ordertype || 'QR_ORDER').toUpperCase(),
    branchId: String(payload.branchId || payload.branch_id || ctx.branchId || '').trim(),
    branch_id: String(payload.branch_id || payload.branchId || ctx.branchId || '').trim(),
    tenantId: String(payload.tenantId || payload.tenant_id || ctx.tenantId || '').trim(),
    tenant_id: String(payload.tenant_id || payload.tenantId || ctx.tenantId || '').trim(),
  });
  return fallback;
};

// 🔴 CRITICAL FIX: ITEM QUANTITY FALLBACK AGGRESSIVE (user report: Receipt 0 x Rp 10.000 padahal total benar)
//    Web ordering / Admin Core kirim item field nama BEDA-BEDA: qty, quantity, qty_ordered, item_qty, order_qty, count, amount.
//    TANPA FALLBACK INI: POS Flutter extract quantity dari field yang SALAH → default 0 → "0 x Rp 10.000" di cetakan.
//    DEFAULT MINIMUM 1 BUKAN 0 (jika SEMUA field tidak ada = setidaknya 1 item, bukan 0).
const normalizeEachItemWithAllFieldFallbacks = (itemsRaw) => {
  if (!Array.isArray(itemsRaw)) return [];
  return itemsRaw.map((itRaw, idx) => {
    const it = (itRaw && typeof itRaw === 'object') ? itRaw : {};
    // 🔴 QUANTITY PRIORITY FALLBACK: default MIN = 1 BUKAN 0!
    const qtyCandidates = [
      it.quantity, it.qty, it.item_qty, it.itemQuantity, it.item_quantity,
      it.qty_ordered, it.order_qty, it.orderedQty, it.ordered_qty,
      it.count, it.qtyValue, it.qty_value, it.amount, it.number,
      it.quantityValue, it.quantity_value, it['qty-ordered'], it['quantity-ordered'],
      (it.product && it.product.qty), (it.product && it.product.quantity),
    ];
    let resolvedQty = NaN;
    for (const qv of qtyCandidates) {
      if (qv === null || qv === undefined || qv === '') continue;
      const num = Number(qv);
      if (Number.isFinite(num)) { resolvedQty = num; break; }
    }
    // MINIMUM 1 BUKAN 0. 0 = tidak masuk akal (user pasti pesan setidaknya 1).
    if (!Number.isFinite(resolvedQty) || resolvedQty <= 0) resolvedQty = 1;
    // 🔴 PRICE FALLBACK
    const priceCandidates = [
      it.unitPrice, it.unit_price, it.pricePerUnit, it.price_per_unit, it.basePrice, it.base_price,
      it.sellPrice, it.sell_price, it.finalUnitPrice, it.final_unit_price,
      it.price, it.productPrice, it.product_price, it.amount, it.unit_amount,
      (it.product && (it.product.price || it.product.sellPrice || it.product.unitPrice)),
    ];
    let resolvedUnitPrice = NaN;
    for (const pv of priceCandidates) {
      if (pv === null || pv === undefined || pv === '') continue;
      const num = Number(pv);
      if (Number.isFinite(num)) { resolvedUnitPrice = num; break; }
    }
    if (!Number.isFinite(resolvedUnitPrice) || resolvedUnitPrice < 0) resolvedUnitPrice = 0;
    // 🔴 SUBTOTAL FALLBACK
    const subtotalCandidates = [
      it.subtotal, it.sub_total, it.lineTotal, it.line_total, it.totalPrice, it.total_price,
      it.lineAmount, it.line_amount, it.amount, it.rowTotal, it.row_total,
      (Number.isFinite(resolvedQty) && Number.isFinite(resolvedUnitPrice) ? resolvedQty * resolvedUnitPrice : NaN),
    ];
    let resolvedSubtotal = NaN;
    for (const sv of subtotalCandidates) {
      if (sv === null || sv === undefined || sv === '') continue;
      const num = Number(sv);
      if (Number.isFinite(num)) { resolvedSubtotal = num; break; }
    }
    if (!Number.isFinite(resolvedSubtotal) || resolvedSubtotal < 0) resolvedSubtotal = resolvedQty * resolvedUnitPrice;
    // 🔴 NAME / PRODUCT FALLBACK
    const nameCandidates = [
      it.productName, it.product_name, it.name, it.itemName, it.item_name,
      it.title, it.label, it.displayName, it.display_name, it.description,
      (it.product && (it.product.name || it.product.productName || it.product.title || it.product.label)),
    ];
    let resolvedName = '';
    for (const nv of nameCandidates) {
      const ns = (nv !== null && nv !== undefined) ? String(nv).trim() : '';
      if (ns.length > 0) { resolvedName = ns; break; }
    }
    if (resolvedName === '') resolvedName = `Item ${idx + 1}`;
    // 🔴 ID FALLBACK
    const idCandidates = [
      it.productId, it.product_id, it.itemId, it.item_id, it.id, it.sku, it.SKU, it.code,
      (it.product && (it.product.id || it.product.productId || it.product.sku)),
    ];
    let resolvedId = '';
    for (const iv of idCandidates) {
      const is = (iv !== null && iv !== undefined) ? String(iv).trim() : '';
      if (is.length > 0) { resolvedId = is; break; }
    }
    // NOTES FALLBACK
    const notesCandidates = [
      it.notes, it.variantNotes, it.variant_notes, it.itemNote, it.item_note,
      it.customization, it.customizations, it.comment, it.remark, it.variant,
    ];
    let resolvedNotes = '';
    for (const nsv of notesCandidates) {
      const s = (nsv !== null && nsv !== undefined) ? String(nsv).trim() : '';
      if (s.length > 0) { resolvedNotes = s; break; }
    }
    return {
      ...it,
      // WRITE BACK BANYAK KEY VARIANTS AGAR BAGaimanapun POS Flutter extractor ambil → DAPAT!
      quantity: resolvedQty,
      qty: resolvedQty,
      item_qty: resolvedQty,
      itemQuantity: resolvedQty,
      item_quantity: resolvedQty,
      qty_ordered: resolvedQty,
      order_qty: resolvedQty,
      count: resolvedQty,
      unitPrice: resolvedUnitPrice,
      unit_price: resolvedUnitPrice,
      price: resolvedUnitPrice,
      pricePerUnit: resolvedUnitPrice,
      price_per_unit: resolvedUnitPrice,
      sellPrice: resolvedUnitPrice,
      lineTotal: resolvedSubtotal,
      line_total: resolvedSubtotal,
      subtotal: resolvedSubtotal,
      sub_total: resolvedSubtotal,
      totalPrice: resolvedSubtotal,
      total_price: resolvedSubtotal,
      lineAmount: resolvedSubtotal,
      productName: resolvedName,
      product_name: resolvedName,
      name: resolvedName,
      itemName: resolvedName,
      item_name: resolvedName,
      productId: resolvedId,
      product_id: resolvedId,
      itemId: resolvedId,
      item_id: resolvedId,
      notes: resolvedNotes,
      itemNotes: resolvedNotes,
      item_notes: resolvedNotes,
      variantNotes: resolvedNotes,
      index: idx,
    };
  });
};

const buildOrderEnvelope = (submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId, extraMeta) => {
  const normalizedTop = normalizeTopLevelOrderEnvelopeFields(orderPayload, { transactionId, salesRecordId, tenantId, branchId });
  // 🔴 APPLY ITEM NORMALIZATION SEKARANG: quantity / unitPrice / name / id / subtotal fallback semua variants.
  const normalizedItemsResolved = normalizeEachItemWithAllFieldFallbacks(normalizedTop.items);
  normalizedTop.items = normalizedItemsResolved;
  normalizedTop.items_json = normalizedItemsResolved;
  // 🔴 2-STEP QRIS FLOW: Detect print_type dari payload (dari Admin Core socket payload).
  //    - print_type = 'CHECKER_ONLY' (Step 1 QRIS): POS HANYA cetak Kitchen Checker, JANGAN cetak Struk Sales Receipt.
  //    - print_type = 'RECEIPT_ONLY' (Step 2 QRIS PAID): POS HANYA cetak Struk Sales Receipt, JANGAN cetak Checker lagi.
  //    - print_type = 'FULL' atau undefined (normal CASHIER flow): POS cetak keduanya.
  const rawPay = (orderPayload && typeof orderPayload === 'object') ? orderPayload : {};
  const rawTop = normalizedTop || {};
  const detectedPrintTypeRaw =
    rawTop.print_type || rawTop.printType ||
    rawPay.print_type || rawPay.printType ||
    (rawTop.__qris_unpaid_checker_only === true || rawPay.__qris_unpaid_checker_only === true ? 'CHECKER_ONLY' : '') ||
    (rawTop.order_stage === 'CHECKER_PRINT' || rawPay.order_stage === 'CHECKER_PRINT' ? 'CHECKER_ONLY' : '') ||
    (rawTop.order_stage === 'SALES_RECEIPT_PRINT' || rawPay.order_stage === 'SALES_RECEIPT_PRINT' ? 'RECEIPT_ONLY' : '') ||
    '';
  const normalizedPrintType =
    detectedPrintTypeRaw === 'CHECKER_ONLY' ? 'CHECKER_ONLY' :
    detectedPrintTypeRaw === 'RECEIPT_ONLY' ? 'RECEIPT_ONLY' : 'FULL';
  const extra = extraMeta && typeof extraMeta === 'object' ? extraMeta : {};
  const queuePosition = Number(extra.queuePosition || 0) || 0;
  const queueTotal = Number(extra.queueTotal || 0) || 0;
  const queueConcurrency = Number(extra.queueConcurrency || CONCURRENCY_PER_BRANCH) || CONCURRENCY_PER_BRANCH;
  const queueInFlight = Number(extra.queueInFlight || 0) || 0;
  return {
    submissionId,
    tenantId: normalizedTop.tenantId || tenantId,
    tenant_id: normalizedTop.tenant_id || tenantId,
    branchId: normalizedTop.branchId || branchId,
    branch_id: normalizedTop.branch_id || branchId,
    targetDeviceUuid: deviceUuid,
    orderPayload: { ...(orderPayload || {}), ...normalizedTop },
    envelope: normalizedTop,
    print_type: normalizedPrintType,
    printType: normalizedPrintType,
    order_stage:
      normalizedPrintType === 'CHECKER_ONLY' ? 'CHECKER_PRINT' :
      normalizedPrintType === 'RECEIPT_ONLY' ? 'SALES_RECEIPT_PRINT' : 'FULL_PRINT',
    orderStage:
      normalizedPrintType === 'CHECKER_ONLY' ? 'CHECKER_PRINT' :
      normalizedPrintType === 'RECEIPT_ONLY' ? 'SALES_RECEIPT_PRINT' : 'FULL_PRINT',
    __qris_unpaid_checker_only: normalizedPrintType === 'CHECKER_ONLY',
    transactionId: normalizedTop.transactionId || transactionId || null,
    transaction_id: normalizedTop.transaction_id || transactionId || null,
    salesRecordId: salesRecordId || normalizedTop.orderId || null,
    sales_record_id: salesRecordId || normalizedTop.order_id || null,
    orderId: normalizedTop.orderId,
    order_id: normalizedTop.order_id,
    tableId: normalizedTop.tableId,
    table_id: normalizedTop.table_id,
    tableNumber: normalizedTop.tableNumber,
    table_number: normalizedTop.table_number,
    tableName: normalizedTop.tableName,
    table_name: normalizedTop.table_name,
    items: normalizedTop.items,
    items_json: normalizedTop.items_json,
    pax: normalizedTop.pax,
    guestCount: normalizedTop.guestCount,
    customer_count: normalizedTop.customer_count,
    customerName: normalizedTop.customerName,
    customer_name: normalizedTop.customer_name,
    customer: normalizedTop.customer,
    guest: normalizedTop.guest,
    orderNote: normalizedTop.orderNote,
    order_note: normalizedTop.order_note,
    order_notes: normalizedTop.order_notes,
    notes: normalizedTop.notes,
    note: normalizedTop.note,
    specialNote: normalizedTop.specialNote,
    special_note: normalizedTop.special_note,
    specialInstruction: normalizedTop.specialInstruction,
    special_instruction: normalizedTop.special_instruction,
    remarks: normalizedTop.remarks,
    totalAmount: normalizedTop.totalAmount,
    total_amount: normalizedTop.total_amount,
    grandTotal: normalizedTop.grandTotal,
    grand_total: normalizedTop.grand_total,
    subtotal: normalizedTop.grandTotal,
    paymentMethod: normalizedTop.paymentMethod,
    payment_method: normalizedTop.payment_method,
    orderType: normalizedTop.orderType,
    order_type: normalizedTop.order_type,
    receiptNumber: normalizedTop.receiptNumber,
    receipt_number: normalizedTop.receipt_number,
    invoiceNumber: normalizedTop.invoiceNumber,
    invoice_number: normalizedTop.invoice_number,
    referenceId: normalizedTop.orderId,
    serverTs: Date.now(),
    // P4 BULLETPROOF (Queue Position Meta):
    // POS Flutter bisa menampilkan badge "Antrian ke X/Y" ke kasir jika ramai
    // dan juga printQueue semaphore bisa menyesuaikan display.
    queuePosition,
    queueTotal,
    queueConcurrency,
    queueInFlight,
  };
};

const processQueueJob = async (jobData) => {
  const {
    submissionId,
    tenantId,
    branchId,
    targetDeviceUuid,
    orderPayload,
    transactionId,
    salesRecordId,
  } = jobData || {};

  const stateEntry = submissionStates.get(submissionId);
  if (stateEntry && stateEntry.resolved) return;

  // P4 BULLETPROOF: Acquire concurrency slot per-branch supaya printer buffer
  // tidak kebanjiran. Kalau 10 order bersamaan → 3 jalan, 7 nunggu.
  const queueSnap = _getQueueSnapshotForBranch(branchId);
  const queueMetaExtra = {
    queuePosition: (queueSnap.inFlight + queueSnap.waiting + queueSnap.inMemoryPending + 1),
    queueTotal: (queueSnap.inFlight + queueSnap.waiting + queueSnap.inMemoryPending + 1),
    queueConcurrency: queueSnap.concurrency,
    queueInFlight: queueSnap.inFlight,
  };
  const releaseSlotFn = await _acquireBranchSlot(branchId);
  let released = false;
  const releaseOnce = () => {
    if (released) return;
    released = true;
    try { if (typeof releaseSlotFn === 'function') releaseSlotFn(); } catch (_) {}
  };
  // Safety auto-release setelah 30s supaya tidak deadlock kalau emit stuck.
  const autoReleaseTimer = setTimeout(() => releaseOnce(), 30000);
  const killAutoRelease = () => { try { clearTimeout(autoReleaseTimer); } catch (_) {} };
  try {
    const envelope = buildOrderEnvelope(submissionId, tenantId, branchId, '', orderPayload, transactionId, salesRecordId, queueMetaExtra);

    const upstreamSafe = Boolean(stateEntry && stateEntry.upstreamSavedQueuedAt && stateEntry.upstreamSavedQueuedAt > 0);
    if (upstreamSafe) {
      const branchKey = `__branch:${branchId}`;
      const branchBucket = getLongPollBucket(branchKey);
      branchBucket.push({ ...envelope, queuedAt: Date.now() });
      if (branchBucket.length > 200) branchBucket.splice(0, branchBucket.length - 200);
      notifyLongPollWaiters(branchKey);
      const emitResult = emitIncomingWebOrder('', branchId, envelope);
      await new Promise((r) => setTimeout(r, 6000));
      return emitResult;
    }
    // 🔴 SAFETY: upstream NOT confirmed safe → JANGAN emit socket ghost ke POS.
    //    Biarkan client polling / retry mechanism handle re-emit NANTI setelah upstream confirmed.
    try {
      const branchKey = `__branch:${branchId}`;
      const branchBucket = getLongPollBucket(branchKey);
      branchBucket.push({ ...envelope, queuedAt: Date.now(), _pendingUpstream: true });
      if (branchBucket.length > 200) branchBucket.splice(0, branchBucket.length - 200);
      notifyLongPollWaiters(branchKey);
    } catch (_) {}
    await new Promise((r) => setTimeout(r, 3000));
    return null;
  } finally {
    killAutoRelease();
    releaseOnce();
  }
};

const finalizeResolve = (stateEntry, submissionId, result) => {
  if (!stateEntry || stateEntry.resolved) return;
  stateEntry.resolved = true;
  stateEntry.processing = false;
  if (stateEntry.watchdogTimer) {
    clearTimeout(stateEntry.watchdogTimer);
    stateEntry.watchdogTimer = null;
  }
  stateEntry.result = result;
  if (stateEntry.resolvePromise) {
    try { stateEntry.resolvePromise(result); } catch (_) {}
  }
  setTimeout(() => {
    submissionStates.delete(submissionId);
  }, 300000);
};

const finalizeReject = (stateEntry, submissionId, errorObj) => {
  if (!stateEntry || stateEntry.resolved) return;
  stateEntry.resolved = true;
  stateEntry.processing = false;
  if (stateEntry.watchdogTimer) {
    clearTimeout(stateEntry.watchdogTimer);
    stateEntry.watchdogTimer = null;
  }
  stateEntry.result = { error: errorObj };
  if (stateEntry.rejectPromise) {
    const err = Object.assign(new Error(errorObj.message || 'Queue error'), errorObj);
    try { stateEntry.rejectPromise(err); } catch (_) {}
  }
  setTimeout(() => {
    submissionStates.delete(submissionId);
  }, 300000);
};

const resolveOrderAcknowledgement = async ({
  submissionId,
  ackStatus,
  ackPayload,
  deviceUuid,
  printedAt,
  tenantId,
  branchId,
}) => {
  const cleanSubmissionId = (submissionId || '').toString().trim();
  if (!cleanSubmissionId) {
    return { ok: false, reason: 'MISSING_SUBMISSION_ID' };
  }

  let stateEntry = submissionStates.get(cleanSubmissionId);
  //#region debug-point web-order-bugs-ack
  try {
    const ts = new Date().toISOString();
    const curResolved = stateEntry ? !!stateEntry.resolved : null;
    const curStatus = stateEntry ? (stateEntry.status || stateEntry.ackStatus || '') : '';
    const curRetries = stateEntry ? (stateEntry.retries || 0) : -1;
    // eslint-disable-next-line no-console
    console.error(
      `[${ts}] [DEBUG-WEB-ORDER] [ACK] submissionId=${cleanSubmissionId} ackStatus=${String(ackStatus || 'POS_PRINTED')} state.resolved_before=${curResolved} state.status_before=${curStatus} retries=${curRetries} deviceUuid=${String(deviceUuid || '')} tenant=${String(tenantId || '')} branch=${String(branchId || '')}`,
    );
  } catch (_dbgA) { /* noop */ }
  //#endregion
  const resolvedAckStatus = (ackStatus || 'POS_PRINTED').toString().trim();
  const resolvedPrintedAt = (printedAt || new Date().toISOString()).toString();
  const resolvedDeviceUuid = (deviceUuid || (stateEntry?.resolvedTargetDeviceUuid) || '').toString();

  if (stateEntry && !stateEntry.resolved) {
    finalizeResolve(stateEntry, cleanSubmissionId, {
      ok: true,
      ackStatus: resolvedAckStatus,
      resolvedDeviceUuid: stateEntry.resolvedTargetDeviceUuid || resolvedDeviceUuid,
      acknowledgedAt: resolvedPrintedAt,
      deviceUuid: resolvedDeviceUuid,
      ackPayload: ackPayload || {},
    });
  } else if (!stateEntry) {
    const now = Date.now();
    stateEntry = {
      submissionId: cleanSubmissionId,
      tenantId: tenantId || (ackPayload?.tenantId || ackPayload?.tenant_id || '').toString() || null,
      branchId: branchId || (ackPayload?.branchId || ackPayload?.branch_id || '').toString() || null,
      targetDeviceUuid: resolvedDeviceUuid || null,
      resolvedTargetDeviceUuid: resolvedDeviceUuid || null,
      status: resolvedAckStatus,
      resolved: true,
      processing: false,
      result: {
        ok: true,
        ackStatus: resolvedAckStatus,
        resolvedDeviceUuid: resolvedDeviceUuid,
        acknowledgedAt: resolvedPrintedAt,
        deviceUuid: resolvedDeviceUuid,
        ackPayload: ackPayload || {},
        _createdByManualAck: true,
      },
      retries: 0,
      watchdogTimer: null,
      resolvedAt: resolvedPrintedAt,
      createdAt: now,
      enqueuedAt: now,
      promise: null,
      resolvePromise: null,
      rejectPromise: null,
    };
    submissionStates.set(cleanSubmissionId, stateEntry);
    setTimeout(() => {
      if (submissionStates.get(cleanSubmissionId) === stateEntry) submissionStates.delete(cleanSubmissionId);
    }, 300000);
  } else if (stateEntry && stateEntry.resolved && stateEntry.result && !stateEntry.result.ok) {
    stateEntry.result = {
      ok: true,
      ackStatus: resolvedAckStatus,
      resolvedDeviceUuid: stateEntry.resolvedTargetDeviceUuid || resolvedDeviceUuid,
      acknowledgedAt: resolvedPrintedAt,
      deviceUuid: resolvedDeviceUuid,
      ackPayload: ackPayload || {},
      _upgradedFromError: true,
    };
    stateEntry.status = resolvedAckStatus;
    stateEntry.resolvedAt = resolvedPrintedAt;
  }

  const finalTenantId = (tenantId || ackPayload?.tenantId || ackPayload?.tenant_id || stateEntry?.tenantId || stateEntry?.result?.tenantId || '').toString();
  try {
    await notifyAdminCoreAck(cleanSubmissionId, finalTenantId, resolvedAckStatus, {
      ...(ackPayload || {}),
      deviceUuid: resolvedDeviceUuid,
      printedAt: resolvedPrintedAt,
    });
  } catch (_) {}

  return {
    ok: true,
    submissionId: cleanSubmissionId,
    ackStatus: resolvedAckStatus,
    resolvedDeviceUuid: resolvedDeviceUuid,
    acknowledgedAt: resolvedPrintedAt,
    _createdByManualAck: !stateEntry || stateEntry.result?._createdByManualAck,
  };
};

const pollIncomingOrders = async ({ deviceUuid, tenantId, branchId, sinceTs }) => {
  const normalizeSinceTsMs = (raw) => {
    if (raw === undefined || raw === null || raw === '') return 0;
    if (typeof raw === 'number' && Number.isFinite(raw)) return Math.max(0, Math.floor(raw));
    const asStr = String(raw).trim();
    if (!asStr) return 0;
    const asNum = Number(asStr);
    if (Number.isFinite(asNum) && asStr.length >= 10 && !asStr.includes('-') && !asStr.includes('T')) {
      return Math.max(0, Math.floor(asNum));
    }
    try {
      const asDate = new Date(asStr);
      if (asDate instanceof Date && !Number.isNaN(asDate.valueOf())) {
        return Math.max(0, Math.floor(asDate.valueOf()));
      }
    } catch (_isoErr) {}
    try {
      const n = Number(asStr);
      if (Number.isFinite(n)) return Math.max(0, Math.floor(n));
    } catch (_) {}
    return 0;
  };
  const orders = [];
  const since = normalizeSinceTsMs(sinceTs);
  const tenant = (tenantId || '').toString().trim();
  const branch = (branchId || '').toString().trim();
  const device = (deviceUuid || '').toString().trim();
  if (device) {
    const bucket = getLongPollBucket(device);
    for (const item of bucket) {
      if (!since || (item.queuedAt || 0) > since) {
        orders.push(item);
      }
    }
  }
  if (branch) {
    const branchKey = `__branch:${branch}`;
    const bucket = getLongPollBucket(branchKey);
    for (const item of bucket) {
      if (!since || (item.queuedAt || 0) > since) {
        if (!orders.find((x) => x.submissionId === item.submissionId)) {
          orders.push(item);
        }
      }
    }
  }
  if (tenant) {
    const tenantKey = `__tenant:${tenant}`;
    const bucket = getLongPollBucket(tenantKey);
    for (const item of bucket) {
      if (!since || (item.queuedAt || 0) > since) {
        if (!orders.find((x) => x.submissionId === item.submissionId)) {
          orders.push(item);
        }
      }
    }
  }
  if (branch) {
    const replayFromStates = getUnprocessedPendingOrdersByBranch(branch, since || 0);
    for (const r of replayFromStates) {
      if (!orders.find((x) => x.submissionId === r.submissionId)) {
        orders.push(r);
      }
    }
  }
  if (orders.length > 0) {
    return { orders, returnedAt: Date.now() };
  }

  const waiters = [];
  const keysToWait = [];
  if (device) keysToWait.push(device);
  if (branch) keysToWait.push(`__branch:${branch}`);
  if (tenant) keysToWait.push(`__tenant:${tenant}`);

  const MAX_WAIT_MS = 15000;
  return new Promise((resolve) => {
    let resolved = false;
    const doResolve = () => {
      if (resolved) return;
      resolved = true;
      for (const k of keysToWait) {
        const list = longPollWaiters.get(k) || [];
        const idx = list.indexOf(trigger);
        if (idx >= 0) list.splice(idx, 1);
      }
      const finalOrders = [];
      if (device) {
        const bucket = getLongPollBucket(device);
        for (const item of bucket) {
          if (!since || (item.queuedAt || 0) > since) {
            finalOrders.push(item);
          }
        }
      }
      if (branch) {
        const branchKey = `__branch:${branch}`;
        const bucket = getLongPollBucket(branchKey);
        for (const item of bucket) {
          if (!since || (item.queuedAt || 0) > since) {
            if (!finalOrders.find((x) => x.submissionId === item.submissionId)) {
              finalOrders.push(item);
            }
          }
        }
      }
      if (tenant) {
        const tenantKey = `__tenant:${tenant}`;
        const bucket = getLongPollBucket(tenantKey);
        for (const item of bucket) {
          if (!since || (item.queuedAt || 0) > since) {
            if (!finalOrders.find((x) => x.submissionId === item.submissionId)) {
              finalOrders.push(item);
            }
          }
        }
      }
      if (branch) {
        const replayFromStates = getUnprocessedPendingOrdersByBranch(branch, since || 0);
        for (const r of replayFromStates) {
          if (!finalOrders.find((x) => x.submissionId === r.submissionId)) {
            finalOrders.push(r);
          }
        }
      }
      resolve({ orders: finalOrders, returnedAt: Date.now() });
    };
    const trigger = () => doResolve();
    for (const k of keysToWait) {
      if (!longPollWaiters.has(k)) longPollWaiters.set(k, []);
      longPollWaiters.get(k).push(trigger);
    }
    setTimeout(doResolve, MAX_WAIT_MS);
  });
};

const initBullMqOrFallback = () => {
  try {
    const bullmq = require('bullmq');
    Queue = bullmq.Queue;
    Worker = bullmq.Worker;
    QueueEvents = bullmq.QueueEvents;
    const redisUrl = process.env.REDIS_URL || process.env.REDIS_CONNECTION_STRING;
    const redisHost = process.env.REDIS_HOST || '127.0.0.1';
    const redisPort = Number.parseInt(process.env.REDIS_PORT || '6379', 10) || 6379;

    const connection = redisUrl
      ? redisUrl
      : { host: redisHost, port: redisPort };

    posOrderQueueInstance = new Queue('pos-web-orders', { connection });
    posOrderWorker = new Worker('pos-web-orders', async (job) => processQueueJob(job.data), { connection });
    posOrderWorker.on('failed', (job, err) => {
      const submissionId = job?.data?.submissionId || '';
      console.warn('[posOrderQueue] BullMQ job failed:', submissionId, err?.message || err);
      const stateEntry = submissionStates.get(submissionId);
      if (stateEntry && !stateEntry.resolved) {
        finalizeReject(stateEntry, submissionId, {
          statusCode: 500,
          code: 'QUEUE_JOB_FAILED',
          message: err?.message || 'Queue job gagal',
          retryAvailable: true,
          submissionId,
        });
      }
    });
    posOrderQueueEvents = new QueueEvents('pos-web-orders', { connection });
    useBullMq = true;
    console.info('[posOrderQueue] menggunakan BullMQ (Redis) sebagai queue engine');
  } catch (err) {
    useBullMq = false;
    posOrderQueueInstance = null;
    posOrderWorker = null;
    posOrderQueueEvents = null;
    Queue = null;
    Worker = null;
    QueueEvents = null;
    console.info('[posOrderQueue] BullMQ/Redis tidak tersedia, fallback ke IN-MEMORY QUEUE:', err?.message || err);
  }
};

const getOrderSubmissionState = (submissionId) => {
  const key = String(submissionId || "").trim();
  if (!key) return null;
  const s = submissionStates.get(key);
  if (!s) return null;
  const finalAckStatus =
    (s.result && s.result.ackStatus) ||
    (s.result && s.result.status) ||
    s.status ||
    (s.result && s.result.ok ? "POS_ACKNOWLEDGED" : (s.result?.error?.code || s.result?.error?.error || "UNKNOWN"));
  const finalDeviceUuid =
    (s.result && s.result.resolvedDeviceUuid) ||
    s.resolvedDeviceUuid ||
    s.resolvedTargetDeviceUuid ||
    null;
  const finalAcknowledgedAt =
    (s.result && s.result.acknowledgedAt) ||
    s.resolvedAt ||
    null;
  return {
    submissionId: key,
    status: finalAckStatus,
    ackStatus: finalAckStatus,
    resolvedDeviceUuid: finalDeviceUuid,
    resolvedAt: finalAcknowledgedAt,
    createdAt: s.createdAt || null,
    branchId: s.branchId || null,
    tenantId: s.tenantId || null,
    targetDeviceUuid: s.targetDeviceUuid || s.resolvedTargetDeviceUuid || null,
    ackPayload: s.result && s.result.ackPayload ? s.result.ackPayload : s.ackPayload || null,
    retryAvailable: s.retryAvailable === undefined ? (!s.resolved || !s.result || !s.result.ok) : s.retryAvailable,
    resolved: !!s.resolved,
    _fromCache: true,
    _createdByManualAck: !!(s.result && s.result._createdByManualAck),
    _upgradedFromError: !!(s.result && s.result._upgradedFromError),
  };
};

initBullMqOrFallback();

module.exports = {
  enqueueWebOrderForPrinting,
  resolveOrderAcknowledgement,
  pollIncomingOrders,
  getQueuePendingCount,
  getQueueLengthPerBranch,
  estimateEtaSeconds,
  estimateEtaSecondsSync,
  submissionStates,
  getOrderSubmissionState,
  adminCoreFetch,
  getUnprocessedPendingOrdersByBranch,
  injectMissingOrderMetadata,
};
