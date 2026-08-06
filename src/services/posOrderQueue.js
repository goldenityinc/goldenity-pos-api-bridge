const { emitIncomingWebOrder, isDeviceOnline, connectedDevices } = require('./socketServer');

const DEFAULT_ACK_TIMEOUT_SECONDS = 30;
const getAckTimeoutSeconds = () => {
  const parsed = Number.parseInt(process.env.POS_ACK_TIMEOUT_SECONDS || '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_ACK_TIMEOUT_SECONDS;
};

const ADMIN_CORE_URL = (process.env.ADMIN_CORE_API_BASE_URL || 'http://localhost:5000').replace(/\/$/, '');
const ADMIN_CORE_INTERNAL_TOKEN = process.env.ADMIN_CORE_INTERNAL_TOKEN || '';
const ADMIN_CORE_TIMEOUT_MS = Number.parseInt(process.env.ADMIN_CORE_API_TIMEOUT_MS || '5000', 10) || 5000;

const submissionStates = new Map();

const longPollBuckets = new Map();

const getLongPollBucket = (deviceUuid) => {
  if (!longPollBuckets.has(deviceUuid)) {
    longPollBuckets.set(deviceUuid, []);
  }
  return longPollBuckets.get(deviceUuid);
};

const appendLongPollOrder = (deviceUuid, branchId, orderEnvelope) => {
  const bucket = getLongPollBucket(deviceUuid);
  bucket.push({
    ...orderEnvelope,
    queuedAt: Date.now(),
  });
  if (bucket.length > 100) {
    bucket.splice(0, bucket.length - 100);
  }
  const branchKey = `__branch:${branchId}`;
  const branchBucket = getLongPollBucket(branchKey);
  branchBucket.push({
    ...orderEnvelope,
    queuedAt: Date.now(),
  });
  if (branchBucket.length > 200) {
    branchBucket.splice(0, branchBucket.length - 200);
  }
  notifyLongPollWaiters(deviceUuid);
  notifyLongPollWaiters(branchKey);
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
  const url = `${ADMIN_CORE_URL}${path}`;
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
    let resolvedTargetDeviceUuid = (targetDeviceUuid || '').toString().trim();
    if (!resolvedTargetDeviceUuid) {
      try {
        resolvedTargetDeviceUuid = (await resolveDefaultPrinterDevice(branchId, tenantId)) || '';
      } catch (_) {
        resolvedTargetDeviceUuid = '';
      }
    }
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
    const envelope = buildOrderEnvelope(submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId);
    const emitResult = emitIncomingWebOrder(deviceUuid, branchId, envelope);
    if (deviceUuid) {
      appendLongPollOrder(deviceUuid, branchId, envelope);
    }
    startWatchdog(stateEntry, submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId);
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

const buildOrderEnvelope = (submissionId, tenantId, branchId, deviceUuid, orderPayload, transactionId, salesRecordId) => ({
  submissionId,
  tenantId,
  branchId,
  targetDeviceUuid: deviceUuid,
  orderPayload,
  transactionId: transactionId || null,
  salesRecordId: salesRecordId || null,
  serverTs: Date.now(),
});

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

  const envelope = buildOrderEnvelope(submissionId, tenantId, branchId, targetDeviceUuid, orderPayload, transactionId, salesRecordId);

  if (targetDeviceUuid) {
    appendLongPollOrder(targetDeviceUuid, branchId, envelope);
  }

  const emitResult = emitIncomingWebOrder(targetDeviceUuid, branchId, envelope);
  if (!emitResult.emitted && targetDeviceUuid && !isDeviceOnline(targetDeviceUuid)) {
    if (stateEntry && !stateEntry.resolved) {
      finalizeReject(stateEntry, submissionId, {
        statusCode: 503,
        code: 'POS_DEVICE_OFFLINE',
        message: 'Perangkat printer target tidak online',
        retryAvailable: true,
        submissionId,
      });
    }
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
  const orders = [];
  const since = Number(sinceTs) || 0;
  if (deviceUuid) {
    const bucket = getLongPollBucket(deviceUuid);
    for (const item of bucket) {
      if (!since || (item.queuedAt || 0) > since) {
        orders.push(item);
      }
    }
  }
  if (branchId) {
    const branchKey = `__branch:${branchId}`;
    const bucket = getLongPollBucket(branchKey);
    for (const item of bucket) {
      if (!since || (item.queuedAt || 0) > since) {
        if (!orders.find((x) => x.submissionId === item.submissionId)) {
          orders.push(item);
        }
      }
    }
  }

  if (orders.length > 0) {
    return { orders, returnedAt: Date.now() };
  }

  const waiters = [];
  const keysToWait = [];
  if (deviceUuid) keysToWait.push(deviceUuid);
  if (branchId) keysToWait.push(`__branch:${branchId}`);

  const MAX_WAIT_MS = 25000;
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
      if (deviceUuid) {
        const bucket = getLongPollBucket(deviceUuid);
        for (const item of bucket) {
          if (!since || (item.queuedAt || 0) > since) {
            finalOrders.push(item);
          }
        }
      }
      if (branchId) {
        const branchKey = `__branch:${branchId}`;
        const bucket = getLongPollBucket(branchKey);
        for (const item of bucket) {
          if (!since || (item.queuedAt || 0) > since) {
            if (!finalOrders.find((x) => x.submissionId === item.submissionId)) {
              finalOrders.push(item);
            }
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
};
