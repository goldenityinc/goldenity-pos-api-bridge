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

const adminCoreFetch = async (path, options = {}) => {
  // 🔴 CRITICAL REGRESSION FIX HR2: URL Normalize double slashes (selain protocol https://)
  //    Masalah: ADMIN_CORE_URL = https://admin.example.com/api (DIAKHIRI /api tanpa slash),
  //    atau env var Railway USER setup TRAILING SLASH → https://admin.example.com/,
  //    atau path param CONTAIN double // sebelum di encode → url fetch INVALID.
  //    Node.js native fetch() (undici) strict terhadap URL format: double slashes
  //    setelah origin = path salah, kadang ECONNREFUSED / ENOTFOUND "fetch failed".
  const rawUrl = `${ADMIN_CORE_URL}/${path.replace(/^\//, '')}`; // selalu inject 1 slash antara base + path
  const url = rawUrl.replace(/([^:])\/{2,}/g, '$1/'); // replace 2+ slashes jadi 1 slash, KECUALI setelah protocol colon (:) biarkan https:// tetap.
  // 🔴 [Instrument HR2-HR3 pre-fix evidence] SELALU log actual fetch URL ke console (Railway logs capture):
  console.error('[dbg HR2-HR3 deviceFetch] ADMIN_CORE_ENV_SET:', {
    hasADMIN_CORE_API_BASE_URL: Boolean(process.env.ADMIN_CORE_API_BASE_URL),
    hasADMIN_CORE_API_URL: Boolean(process.env.ADMIN_CORE_API_URL),
    ADMIN_CORE_URL_RAW_len: String(ADMIN_CORE_URL_RAW || '').length,
    ADMIN_CORE_URL_resolved: ADMIN_CORE_URL,
    rawUrlBeforeNormalize: rawUrl,
    finalUrlAfterNormalize: url,
    doubleSlashesAfterProtocol: (url.match(/:\/\/[^/]+\/(.*)$/) || [])[1]?.split('//').length - 1 || 0,
    path_input: path,
    method: (options.method || 'GET').toUpperCase(),
  });
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
    return { ok: res.ok, status: res.status, data };
  } catch (err) {
    clearTimeout(timeoutId);
    return { ok: false, status: 0, data: null, error: err.message || String(err) };
  }
};

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
    return adminCoreFetch(`/api/v1/orders/${encodeURIComponent(submissionId)}/ack`, {
      method: 'POST',
      body: JSON.stringify({ ackStatus, ackPayload }),
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

const estimateEtaSeconds = async () => {
  const pending = await getQueuePendingCount();
  const perJob = 1.5;
  return Math.max(0, Math.round(pending * perJob));
};

const enqueueWebOrderForPrinting = async ({
  tenantId,
  branchId,
  targetDeviceUuid,
  orderPayload,
  submissionId,
  transactionId,
  salesRecordId,
}) => {
  const cleanSubmissionId = (submissionId || '').toString().trim();
  if (!cleanSubmissionId) {
    throw Object.assign(new Error('submissionId wajib disediakan'), { statusCode: 400, code: 'MISSING_SUBMISSION_ID' });
  }

  const existing = submissionStates.get(cleanSubmissionId);
  if (existing) {
    if (existing.resolved) {
      return { ...existing.result, _fromCache: true };
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

    try {
      await updateOrderSyncStatus(cleanSubmissionId, tenantId, 'QUEUED_FOR_POS', {
        resolvedTargetDeviceUuid,
        transactionId,
        salesRecordId,
      });
    } catch (_) {}

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
  stateEntry.watchdogTimer = setTimeout(() => {
    onWatchdogTimeout(stateEntry, submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId);
  }, timeoutSec * 1000);
};

const onWatchdogTimeout = async (stateEntry, submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId) => {
  if (stateEntry.resolved) return;
  stateEntry.watchdogTimer = null;

  if (stateEntry.retries <= 0) {
    stateEntry.retries += 1;
    // 🔴 CRITICAL FIX - BROADCAST RETRY KE BRANCH ROOM, BUKAN 1 device:
    //    Pass deviceUuid = '' supaya emitIncomingWebOrder SELALU broadcast BRANCH.
    const envelope = buildOrderEnvelope(submissionId, tenantId, branchId, '', orderPayload, transactionId, salesRecordId);
    const emitResult = emitIncomingWebOrder('', branchId, envelope);
    // Selalu append ke long-poll branch-level juga (jika ada longpoll clients)
    const branchKey = `__branch:${branchId}`;
    const branchBucket = getLongPollBucket(branchKey);
    branchBucket.push({ ...envelope, queuedAt: Date.now() });
    if (branchBucket.length > 200) branchBucket.splice(0, branchBucket.length - 200);
    notifyLongPollWaiters(branchKey);
    startWatchdog(stateEntry, submissionId, tenantId, branchId, '', orderPayload, transactionId, salesRecordId);
    return;
  }

  try {
    await updateOrderSyncStatus(submissionId, tenantId, 'FAILED_DELIVERY', {
      resolvedTargetDeviceUuid: deviceUuid,
      failureReason: 'POS_ACK_TIMEOUT',
      transactionId,
      salesRecordId,
    });
  } catch (_) {}

  finalizeReject(stateEntry, submissionId, {
    statusCode: 504,
    code: 'POS_ACK_TIMEOUT',
    message: `Perangkat POS tidak merespon dalam ${getAckTimeoutSeconds()} detik`,
    retryAvailable: true,
    submissionId,
  });
};

const normalizeTopLevelOrderEnvelopeFields = (orderPayloadRaw, ctx = {}) => {
  const payload = (orderPayloadRaw && typeof orderPayloadRaw === 'object') ? orderPayloadRaw : {};
  const fallback = Object.create(null);
  Object.assign(fallback, {
    tableId: payload.tableId || payload.table_id || ctx.tableId || ctx.table_id || '',
    table_id: payload.table_id || payload.tableId || ctx.table_id || ctx.tableId || '',
    tableNumber: payload.tableNumber || payload.table_number || payload.tableName || payload.table_name || payload.tableLabel || payload.table_label || payload.tableLabel || ((payload.table && (payload.table.tableNumber || payload.table.table_number || payload.table.name || payload.table.label || '')) || '') || ctx.tableNumber || ctx.table_number || '',
    table_number: payload.table_number || payload.tableNumber || ((payload.table && (payload.table.table_number || payload.table.tableNumber || payload.table.name || payload.table.label || '')) || '') || ctx.table_number || ctx.tableNumber || '',
    items: Array.isArray(payload.items) ? payload.items : (Array.isArray(payload.items_json) ? payload.items_json : (Array.isArray(payload.line_items) ? payload.line_items : (Array.isArray(payload.transaction_items) ? payload.transaction_items : []))),
    transactionId: payload.transactionId || payload.transaction_id || payload.receiptNumber || payload.receipt_number || ctx.transactionId || '',
    orderId: payload.orderId || payload.order_id || payload.id || payload.salesRecordId || payload.sales_record_id || ctx.salesRecordId || '',
    receiptNumber: payload.receiptNumber || payload.receipt_number || '',
    totalAmount: Number.isFinite(payload.totalAmount) ? payload.totalAmount : (Number.isFinite(payload.total_amount) ? payload.total_amount : (Number.isFinite(payload.grandTotal) ? payload.grandTotal : (Number.isFinite(payload.grand_total) ? payload.grand_total : (Number.isFinite(payload.totalPrice) ? payload.totalPrice : (Number.isFinite(payload.total_price) ? payload.total_price : 0))))),
    grandTotal: Number.isFinite(payload.grandTotal) ? payload.grandTotal : (Number.isFinite(payload.grand_total) ? payload.grand_total : (Number.isFinite(payload.totalAmount) ? payload.totalAmount : (Number.isFinite(payload.total_amount) ? payload.total_amount : 0))),
    pax: Number.isFinite(payload.pax) ? payload.pax : (Number.isFinite(payload.guestCount) ? payload.guestCount : (Number.isFinite(payload.guests) ? payload.guests : (Number.isFinite(payload.customer_count) ? payload.customer_count : 1))),
    customerName: String(payload.customerName || payload.customer_name || payload.customer || '').trim(),
    orderNote: String(payload.orderNote || payload.order_note || payload.notes || payload.specialInstruction || payload.special_instruction || payload.specialNote || payload.special_note || '').trim(),
    paymentMethod: String(payload.paymentMethod || payload.payment_method || '').trim(),
    orderType: String(payload.orderType || payload.order_type || payload.ordertype || 'QR_ORDER').toUpperCase(),
    branchId: String(payload.branchId || payload.branch_id || ctx.branchId || '').trim(),
    tenantId: String(payload.tenantId || payload.tenant_id || ctx.tenantId || '').trim(),
  });
  return fallback;
};

const buildOrderEnvelope = (submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId) => {
  const normalizedTop = normalizeTopLevelOrderEnvelopeFields(orderPayload, { transactionId, salesRecordId, tenantId, branchId });
  return {
    submissionId,
    tenantId: normalizedTop.tenantId || tenantId,
    branchId: normalizedTop.branchId || branchId,
    targetDeviceUuid: deviceUuid,
    orderPayload: { ...(orderPayload || {}), ...normalizedTop },
    envelope: normalizedTop,
    transactionId: normalizedTop.transactionId || transactionId || null,
    salesRecordId: salesRecordId || normalizedTop.orderId || null,
    tableId: normalizedTop.tableId,
    tableNumber: normalizedTop.tableNumber,
    table_number: normalizedTop.table_number,
    items: normalizedTop.items,
    pax: normalizedTop.pax,
    customerName: normalizedTop.customerName,
    orderNote: normalizedTop.orderNote,
    totalAmount: normalizedTop.totalAmount,
    grandTotal: normalizedTop.grandTotal,
    subtotal: normalizedTop.grandTotal,
    paymentMethod: normalizedTop.paymentMethod,
    orderType: normalizedTop.orderType,
    receiptNumber: normalizedTop.receiptNumber,
    referenceId: normalizedTop.orderId,
    serverTs: Date.now(),
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

  const envelope = buildOrderEnvelope(submissionId, tenantId, branchId, '', orderPayload, transactionId, salesRecordId);

  // Selalu append branch-level long-poll (fallback kalau socket mati)
  const branchKey = `__branch:${branchId}`;
  const branchBucket = getLongPollBucket(branchKey);
  branchBucket.push({ ...envelope, queuedAt: Date.now() });
  if (branchBucket.length > 200) branchBucket.splice(0, branchBucket.length - 200);
  notifyLongPollWaiters(branchKey);

  // 🔴 CRITICAL FIX - SELALU emit dengan targetDeviceUuid='' agar BROADCAST BRANCH ROOM DULU.
  //    Jangan pernah finalize reject "device offline" karena kita sudah BLAST ke seluruh room.
  const emitResult = emitIncomingWebOrder('', branchId, envelope);
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
  submissionStates,
  getOrderSubmissionState,
  adminCoreFetch,
  getUnprocessedPendingOrdersByBranch,
};
