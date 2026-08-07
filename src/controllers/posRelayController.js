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

    // 🔴 [FIX STUCK 15% WEB ORDERING]
    //    Blocking async await enqueueWebOrderForPrinting MENYEBABKAN HTTP response TIDAK DIKIRIM
    //    SAMPAI POS ACK atau timeout 30 detik penuh → browser menampilkan "Mengirim ke kasir..."
    //    stuck 15% FOREVER (HTTP PENDING dari Network Tab screenshot user).
    //    SOLUSI QUEUE PATTERN YANG BENAR:
    //    1) Check IDEMPOTENCY CACHE dulu (jika submissionId SUDAH ADA result → return SEGERA).
    //    2) ENQUEUE di BACKGROUND (TANPA await = fire & forget async) dengan .catch handler.
    //    3) KEMBALIKAN RESPONSE 202 ACCEPTED SEGERA (PENDING_ACK) ke client dalam < 500ms.
    //    4) Client polling ack-status 2s interval untuk detect POS_PRINTED (sudah implement).
    const etaPromise = estimateEtaSeconds();

    // 1️⃣ IDEMPOTENCY CHECK DULU: Jika submissionId SUDAH ADA hasil → return result TANPA enqueue ulang.
    const existingState = getOrderSubmissionState(submissionId) || submissionStates.get(submissionId);
    if (existingState) {
      const cachedStatus = String(
        existingState.status ||
        existingState.ackStatus ||
        existingState.ack_status ||
        existingState.state ||
        'PENDING_ACK',
      ).toUpperCase();
      const etaCached = await etaPromise.catch(() => 0);
      res.setHeader('X-Queue-Eta', String(etaCached));
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
      // Non-terminal cached (PENDING_ACK) -> return 202 polling hint.
      return res.status(202).json({
        ok: true,
        _fromCache: true,
        ackStatus: cachedStatus,
        message: 'Pesanan sedang diantrikan di POS (cache idempotency). Silakan poll /api/v1/relay/orders/*/ack-status untuk update status realtime.',
        submissionId,
        etaNextQueue: etaCached,
        pollHintUrl: `/api/v1/relay/orders/by-submission/${encodeURIComponent(submissionId)}/ack-status`,
      });
    }

    // 2️⃣ FIRE & FORGET ENQUEUE (TIDAK DI AWAIT → HTTP response cepat < 500ms).
    //    Hasil POS ACK akan di-sync ke submissionStates cache, client akan detect via polling.
    const enqueueArgs = Object.freeze({
      tenantId,
      branchId,
      targetDeviceUuid: body.targetDeviceUuid ? String(body.targetDeviceUuid).trim() : undefined,
      orderPayload: body.orderPayload || body.order_payload || body,
      submissionId,
      transactionId: body.transactionId || body.transaction_id || null,
      salesRecordId: body.salesRecordId || body.sales_record_id || null,
    });
    setImmediate(async () => {
      try {
        await enqueueWebOrderForPrinting(enqueueArgs);
      } catch (_enqueueErr) {
        // Error enqueue akan otomatis masuk submissionStates cache FAILED_DELIVERY via internal queue,
        // yang bisa di-poll oleh client ack-status endpoint (return FAILED_DELIVERY + retryAvailable).
        // Tidak throw ke global event loop.
        console.error(
          '[posRelayController] background enqueue error (will be visible via ack-status poll):',
          _enqueueErr && _enqueueErr.stack ? _enqueueErr.stack : _enqueueErr,
          { submissionId, tenantId },
        );
      }
    });

    // 3️⃣ INSTANT HTTP 202 RESPONSE (PENDING_ACK status, polling hint).
    const etaAccepted = await etaPromise.catch(() => 0);
    res.setHeader('X-Queue-Eta', String(etaAccepted));
    return res.status(202).json({
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
      connectedDevicesOnline: getConnectedDevicesCount(),
      perDeviceStatus: getPerDeviceStatus(),
    });
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
