const {
  enqueueWebOrderForPrinting,
  resolveOrderAcknowledgement,
  pollIncomingOrders,
  getQueuePendingCount,
  getQueueLengthPerBranch,
  estimateEtaSeconds,
  submissionStates,
  getOrderSubmissionState,
} = require('../services/posOrderQueue');
const {
  getConnectedDevicesCount,
  getPerDeviceStatus,
} = require('../services/socketServer');

const resolveTenantId = (req) => {
  const fromHeader = (req.headers['x-tenant-id'] || '').toString().trim();
  if (fromHeader) return fromHeader;
  const fromUser = (req.user?.tenantId || req.user?.tenant_id || '').toString().trim();
  if (fromUser) return fromUser;
  const fromTenant = (req.tenant?.tenantId || req.tenant?.tenant_id || '').toString().trim();
  if (fromTenant) return fromTenant;
  const fromBody = (req.body?.tenantId || req.body?.tenant_id || '').toString().trim();
  if (fromBody) return fromBody;
  const fromQuery = (req.query?.tenantId || req.query?.tenant_id || '').toString().trim();
  return fromQuery;
};

const submitWebOrderToPos = async (req, res) => {
  try {
    const body = req.body || {};
    const submissionId = (body.submissionId || '').toString().trim();
    const tenantId = resolveTenantId(req) || (body.tenantId || '').toString().trim();
    const branchId = (body.branchId || '').toString().trim();

    if (!submissionId) {
      return res.status(400).json({
        ok: false,
        error: 'MISSING_SUBMISSION_ID',
        message: 'submissionId wajib disediakan untuk idempotency',
        retryAvailable: false,
      });
    }

    if (!tenantId) {
      return res.status(400).json({
        ok: false,
        error: 'MISSING_TENANT_ID',
        message: 'tenantId wajib disediakan',
        retryAvailable: false,
        submissionId,
      });
    }

    if (!branchId) {
      return res.status(400).json({
        ok: false,
        error: 'MISSING_BRANCH_ID',
        message: 'branchId wajib disediakan',
        retryAvailable: false,
        submissionId,
      });
    }

    // 🔴 [FIX STUCK 15% WEB ORDERING - SUPER AGGRESSIVE NON-BLOCKING PATTERN]
    //    Previous blocking `await enqueueWebOrderForPrinting` caused HTTP PENDING FOREVER 30s+.
    //    Strategy pattern yang TIDAK BISA GAGAL:
    //    (A) SINKRON - IDEMPOTENCY CHECK + RESPONSE 202 DIKIRIM TERLEBIH DAHULU.
    //    (B) ASINKRON - Setelah response END, jalankan enqueue di background event loop.
    //    Gunakan `setTimeout(fn, 0)` (LEBIH KUAT detach dari setImmediate) +
    //    process.nextTick() double guard agar tidak blokir HTTP response stream flush.
    const etaPromise = estimateEtaSeconds();

    // 1️⃣ IDEMPOTENCY CHECK DULU (SYNCHRONOUS, sebelum response):
    let existingState = null;
    try { existingState = getOrderSubmissionState(submissionId) || submissionStates.get(submissionId) || null; } catch(_) { existingState = null; }
    if (existingState) {
      const cachedStatus = String(
        existingState.status ||
        existingState.ackStatus ||
        existingState.ack_status ||
        existingState.state ||
        'PENDING_ACK',
      ).toUpperCase();
      let etaCached = 0;
      try { etaCached = Number(await etaPromise.catch(() => 0)) || 0; } catch(_) { etaCached = 0; }
      try { res.setHeader('X-Queue-Eta', String(etaCached)); } catch(_) {}
      const alreadyTerminal =
        cachedStatus === 'POS_PRINTED' ||
        cachedStatus === 'POS_ACKNOWLEDGED' ||
        cachedStatus === 'FAILED_DELIVERY' ||
        cachedStatus === 'TIMEOUT';
      if (alreadyTerminal) {
        return res.status(200).json({
          ok: true,
          _fromCache: true,
          ackStatus: cachedStatus,
          resolvedDeviceUuid: existingState.resolvedDeviceUuid || existingState.targetDeviceUuid || null,
          acknowledgedAt: existingState.resolvedAt || existingState.ackedAt || existingState.printedAt || null,
          failedDeliveryAt: existingState.failedAt || null,
          submissionId,
          etaNextQueue: etaCached,
          detail: existingState,
        });
      }
      return res.status(202).json({
        ok: true,
        _fromCache: true,
        ackStatus: cachedStatus,
        message: 'Pesanan sedang diantrikan di POS (cache idempotency). Silakan poll ack-status untuk update.',
        submissionId,
        etaNextQueue: etaCached,
        pollHintUrl: `/api/v1/relay/orders/by-submission/${encodeURIComponent(submissionId)}/ack-status`,
      });
    }

    // 🔴 2️⃣ [PENTING!] SEMENTARA TULIS submissionId PENDING ke SUBMISSION STATES AGAR IDEMPOTENCY
    //    TIDAK RACE CONDITION (setelah response dikirim tapi enqueue belum jalan user Retry).
    const enqueueArgs = Object.freeze({
      tenantId,
      branchId,
      targetDeviceUuid: body.targetDeviceUuid ? String(body.targetDeviceUuid).trim() : undefined,
      orderPayload: body.orderPayload || body.order_payload || body,
      submissionId,
      transactionId: body.transactionId || body.transaction_id || null,
      salesRecordId: body.salesRecordId || body.sales_record_id || null,
    });
    try {
      submissionStates.set(submissionId, Object.freeze({
        status: 'PENDING_ACK',
        ackStatus: 'PENDING_ACK',
        submissionId,
        transactionId: enqueueArgs.transactionId,
        tenantId,
        branchId,
        targetDeviceUuid: enqueueArgs.targetDeviceUuid || null,
        envelope: enqueueArgs.orderPayload,
        queuedAt: Date.now(),
        _stubBeforeEnqueue: true,
      }));
    } catch(_submissionCacheErr) {
      // Fail open: cache gagal tulis → lanjut response saja, tidak fatal.
    }

    // 🔴 3️⃣ [WAJIB!] KIRIM HTTP RESPONSE SEKARANG - SEBELUM BACKGROUND ENQUEUE.
    //    Ini menjamin Network Tab browser TIDAK LAGI PENDING FOREVER.
    let etaAccepted = 0;
    try { etaAccepted = Number(await etaPromise.catch(() => 0)) || 0; } catch(_) { etaAccepted = 0; }
    try { res.setHeader('X-Queue-Eta', String(etaAccepted)); } catch(_) {}
    let connectedCount = 0;
    let perDeviceSnap = Object.create(null);
    try { connectedCount = Number(getConnectedDevicesCount() || 0); } catch(_) { connectedCount = 0; }
    try { perDeviceSnap = getPerDeviceStatus() || Object.create(null); } catch(_) { perDeviceSnap = Object.create(null); }
    // 🔴 Send response to client NOW:
    res.status(202).json({
      ok: true,
      ackStatus: 'PENDING_ACK',
      message: 'Pesanan berhasil masuk antrian Bridge POS. Silakan polling status ack-status setiap 2 detik.',
      submissionId,
      transactionId: enqueueArgs.transactionId,
      etaNextQueue: etaAccepted,
      pollIntervalMs: 2000,
      pollHintUrls: {
        bySubmissionId: `/api/v1/relay/orders/by-submission/${encodeURIComponent(submissionId)}/ack-status`,
        byTransactionId: `/api/v1/relay/orders/by-transaction/${encodeURIComponent(
          enqueueArgs.transactionId || String(body.transactionId || ''),
        )}`,
      },
      connectedDevicesOnline: connectedCount,
      perDeviceStatus: perDeviceSnap,
    });

    // 🔴 4️⃣ BARU SETELAH RESPONSE TERKIRIM: Jalankan enqueue DI BACKGROUND.
    //    2x async detachment: process.nextTick() di dalam setTimeout(fn, 0)
    //    untuk pastikan HTTP socket buffer FLUSH duluan sebelum CPU enqueue.
    setTimeout(() => {
      process.nextTick(async () => {
        try {
          await enqueueWebOrderForPrinting(enqueueArgs);
        } catch (_enqueueErr) {
          try {
            submissionStates.set(submissionId, Object.freeze({
              status: 'FAILED_DELIVERY',
              ackStatus: 'FAILED_DELIVERY',
              submissionId,
              transactionId: enqueueArgs.transactionId,
              tenantId,
              branchId,
              targetDeviceUuid: enqueueArgs.targetDeviceUuid || null,
              failedAt: new Date().toISOString(),
              failedReason:
                (_enqueueErr && (_enqueueErr.code || _enqueueErr.message))
                  ? String((_enqueueErr.code ? _enqueueErr.code + ': ' : '') + (_enqueueErr.message || '')).slice(0, 400)
                  : 'ENQUEUE_BACKGROUND_ERROR',
              retryAvailable: true,
              envelope: enqueueArgs.orderPayload,
            }));
          } catch(_cacheWriteErr) { /* fail open */ }
          try {
            console.error(
              '[posRelayController] background enqueue ERROR (visible via ack-status poll FAILED_DELIVERY):',
              _enqueueErr && _enqueueErr.stack ? String(_enqueueErr.stack).slice(0, 1200) : String(_enqueueErr || ''),
              { submissionId, tenantId, branchId },
            );
          } catch(_logErr) { /* fail open, never crash event loop */ }
        }
      });
    }, 0);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: 'INTERNAL_ERROR',
      message: err.message || 'Internal server error',
      retryAvailable: true,
    });
  }
};

const acknowledgeOrderFromPos = async (req, res) => {
  try {
    const submissionId = (req.params?.submissionId || '').toString().trim();
    const body = req.body || {};

    if (!submissionId) {
      return res.status(400).json({
        ok: false,
        error: 'MISSING_SUBMISSION_ID',
        message: 'submissionId wajib disediakan di path parameter',
      });
    }

    const ackStatus = (body.ackStatus || body.ack_status || 'POS_PRINTED').toString().trim();
    const deviceUuid = (body.deviceUuid || body.device_uuid || '').toString().trim();
    const printedAt = (body.printedAt || body.printed_at || new Date().toISOString()).toString();
    const ackPayload = body.ackPayload || body.ack_payload || (Object.keys(body).length ? { ...body } : undefined);
    const tenantIdArg = resolveTenantId(req) || (body.tenantId || body.tenant_id || '').toString().trim() || undefined;
    const branchIdArg = (body.branchId || body.branch_id || '').toString().trim() || undefined;

    const result = await resolveOrderAcknowledgement({
      submissionId,
      ackStatus,
      ackPayload,
      deviceUuid,
      printedAt,
      tenantId: tenantIdArg,
      branchId: branchIdArg,
    });

    return res.status(200).json({ ok: true, submissionId, ackStatus, ...(result && typeof result === 'object' ? { resolvedDeviceUuid: result.resolvedDeviceUuid, acknowledgedAt: result.acknowledgedAt, _createdByManualAck: result._createdByManualAck } : {}) });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: 'INTERNAL_ERROR',
      message: err.message || 'Internal server error',
    });
  }
};

const pollIncomingOrdersHandler = async (req, res) => {
  try {
    const query = req.query || {};
    const deviceUuid = (query.deviceUuid || query.device_uuid || '').toString().trim();
    const tenantId = resolveTenantId(req) || (query.tenantId || query.tenant_id || '').toString().trim();
    const branchId = (query.branchId || query.branch_id || '').toString().trim();
    const sinceTs = query.sinceTs || query.since_ts || 0;

    if (!deviceUuid && !branchId) {
      return res.status(400).json({
        ok: false,
        error: 'MISSING_DEVICE_OR_BRANCH',
        message: 'Minimal deviceUuid atau branchId wajib disediakan',
        orders: [],
      });
    }

    const result = await pollIncomingOrders({
      deviceUuid,
      tenantId,
      branchId,
      sinceTs,
    });

    return res.status(200).json({
      ok: true,
      orders: result?.orders || [],
      returnedAt: result?.returnedAt || Date.now(),
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: 'INTERNAL_ERROR',
      message: err.message || 'Internal server error',
      orders: [],
    });
  }
};

const getQueueStatus = async (req, res) => {
  try {
    const pendingCount = await getQueuePendingCount().catch(() => 0);
    const connectedDevices = getConnectedDevicesCount();
    const perDevice = getPerDeviceStatus();
    const queueLengthPerBranch = getQueueLengthPerBranch();

    return res.status(200).json({
      ok: true,
      pendingCount,
      connectedDevices,
      perDevice,
      queueLengthPerBranch,
      sampledAt: new Date().toISOString(),
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: 'INTERNAL_ERROR',
      message: err.message || 'Internal server error',
    });
  }
};

module.exports = {
  submitWebOrderToPos,
  acknowledgeOrderFromPos,
  pollIncomingOrdersHandler,
  getQueueStatus,
};
