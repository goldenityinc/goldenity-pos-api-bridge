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

    // =====================================================
    // 🔥🔥🔥 [FIX ABSOLUTE FIRE-AND-FORGET - CRITICAL] 🔥🔥🔥
    // User Report: POST /web-order MASIH PENDING FOREVER (stuck 15%).
    // Root Cause: `estimateEtaSeconds()` and `await etaPromise` BEFORE 202 response
    //             causes BullMQ/Redis connection to BLOCK the HTTP response stream!
    //             Even `submissionStates` lookup can trigger Queue/Bull init.
    // Solution EXACTLY per user spec:
    //   1. SEND RESPONSE 202 IMMEDIATELY AT THE VERY TOP (right after validation!)
    //   2. ZERO awaits for queue/BullMQ/Redis/enqueue BEFORE response.
    //   3. All background work via `setTimeout(..., 0)` ABSOLUTE detach.
    // =====================================================

    // 1️⃣ ✅ SEND HTTP RESPONSE FIRST. IMMEDIATELY. NO AWAITS ABOVE THIS LINE.
    //    User Network Tab: POST web-order status = 202 Accepted < 500ms.
    //    No queue operations, no Redis, no BullMQ touched before this.
    const transactionId = body.transactionId || body.transaction_id || null;
    const pollHintByTx =
      transactionId
        ? `/api/v1/relay/orders/by-transaction/${encodeURIComponent(String(transactionId))}`
        : '';
    res.status(202).json({
      ok: true,
      ackStatus: 'PENDING_ACK',
      message:
        'Pesanan diterima Bridge POS. Jalur enqueue berjalan di background (fire-and-forget absolute). Silakan polling ack-status setiap 2 detik.',
      submissionId,
      transactionId,
      etaNextQueue: 0,
      pollIntervalMs: 2000,
      pollHintUrls: {
        bySubmissionId: `/api/v1/relay/orders/by-submission/${encodeURIComponent(submissionId)}/ack-status`,
        byTransactionId: pollHintByTx,
      },
    });

    // 2️⃣ 💻 BACKGROUND PROCESSING - ABSOLUTE DETACH.
    //    - setTimeout(fn, 0): pastikan response socket flush DULU sebelum CPU queue.
    //    - No await wrapping, no return, no catch propagate to request.
    setTimeout(() => {
      _submitWebOrderBackgroundWorker({
        submissionId,
        tenantId,
        branchId,
        transactionId,
        targetDeviceUuid: body.targetDeviceUuid ? String(body.targetDeviceUuid).trim() : undefined,
        orderPayload: body.orderPayload || body.order_payload || body,
        salesRecordId: body.salesRecordId || body.sales_record_id || null,
        rawBody: body,
      });
    }, 0);
  } catch (err) {
    try {
      if (!res.headersSent) {
        return res.status(500).json({
          ok: false,
          error: 'INTERNAL_ERROR',
          message: err && err.message ? err.message : 'Internal server error',
          retryAvailable: true,
        });
      }
    } catch (_) { /* fail open */ }
    console.error('[posRelayController] submitWebOrderToPos outer catch (after response sent unlikely):', err && err.stack ? err.stack : err);
  }
};

// ==========================================================
// 🧰 BACKGROUND WORKER (ABSOLUTE DETACH FROM HTTP REQUEST)
// Semua code yang berhubungan dengan Queue, BullMQ, Redis,
// submissionStates cache, estimateEta, enqueueWebOrderForPrinting
// PINDAH KESINI SEMUA. TIDAK BOLEH blocking request lifecycle!
// ==========================================================
const _submitWebOrderBackgroundWorker = async (args) => {
  const { submissionId, tenantId, branchId, transactionId, targetDeviceUuid, orderPayload, salesRecordId } = args;
  try {
    // (A) Idempotency cache check (BACKGROUND ONLY):
    let existingState = null;
    try {
      existingState =
        (typeof getOrderSubmissionState === 'function' ? getOrderSubmissionState(submissionId) : null) ||
        (submissionStates instanceof Map ? submissionStates.get(submissionId) : null) ||
        null;
    } catch (_) { existingState = null; }

    // Jika SUDAH terminal state → tidak perlu enqueue ulang.
    if (existingState) {
      const cachedStatus = String(
        existingState.status ||
        existingState.ackStatus ||
        existingState.ack_status ||
        existingState.state ||
        'PENDING_ACK',
      ).toUpperCase();
      const alreadyTerminal =
        cachedStatus === 'POS_PRINTED' ||
        cachedStatus === 'POS_ACKNOWLEDGED' ||
        cachedStatus === 'FAILED_DELIVERY' ||
        cachedStatus === 'TIMEOUT';
      if (alreadyTerminal) return;
    }

    // (B) Tulis stub idempotency PENDING_ACK ke cache:
    const enqueueArgs = Object.freeze({
      tenantId,
      branchId,
      targetDeviceUuid,
      orderPayload,
      submissionId,
      transactionId,
      salesRecordId,
    });
    try {
      if (submissionStates instanceof Map) {
        const normalizedTopFields = (() => {
          try {
            const payload = (orderPayload && typeof orderPayload === 'object') ? orderPayload : (rawBody || {});
            const table = payload.table && typeof payload.table === 'object' ? payload.table : {};
            return Object.freeze({
              tableId: String(payload.tableId || payload.table_id || table.id || table.tableId || table.table_id || rawBody.tableId || rawBody.table_id || '').trim(),
              table_id: String(payload.table_id || payload.tableId || table.id || table.table_id || table.tableId || rawBody.table_id || rawBody.tableId || '').trim(),
              tableNumber: String(payload.tableNumber || payload.table_number || payload.tableName || payload.table_name || payload.tableLabel || table.tableNumber || table.table_number || table.name || table.label || rawBody.tableNumber || rawBody.table || '').trim(),
              table_number: String(payload.table_number || payload.tableNumber || table.table_number || table.tableNumber || table.name || table.label || rawBody.table || '').trim(),
            });
          } catch (_e) {
            return Object.freeze({ tableId: '', table_id: '', tableNumber: '', table_number: '' });
          }
        })();
        submissionStates.set(submissionId, Object.assign(Object.create(null), {
          status: 'PENDING_ACK',
          ackStatus: 'PENDING_ACK',
          submissionId,
          transactionId,
          tenantId,
          branchId,
          targetDeviceUuid: targetDeviceUuid || null,
          orderPayload: orderPayload || null,
          envelope: orderPayload,
          tableId: normalizedTopFields.tableId,
          table_id: normalizedTopFields.table_id,
          tableNumber: normalizedTopFields.tableNumber,
          table_number: normalizedTopFields.table_number,
          createdAt: Date.now(),
          queuedAt: Date.now(),
          _stubBeforeEnqueue: true,
        }));
      }
    } catch (_submissionCacheErr) {
      // Fail open: cache gagal tulis → lanjut enqueue.
    }

    // (C) Enqueue untuk POS print (ABSOLUTE FIRE-AND-FORGET):
    try {
      await enqueueWebOrderForPrinting(enqueueArgs);
    } catch (_enqueueErr) {
      // Tulis status FAILED_DELIVERY ke cache untuk ack-status polling.
      try {
        if (submissionStates instanceof Map) {
          submissionStates.set(submissionId, Object.assign(Object.create(null), {
            status: 'FAILED_DELIVERY',
            ackStatus: 'FAILED_DELIVERY',
            submissionId,
            transactionId,
            tenantId,
            branchId,
            targetDeviceUuid: targetDeviceUuid || null,
            orderPayload: orderPayload || null,
            createdAt: Date.now(),
            failedAt: Date.now(),
            failedReason:
              (_enqueueErr && (_enqueueErr.code || _enqueueErr.message))
                ? String((_enqueueErr.code ? _enqueueErr.code + ': ' : '') + (_enqueueErr.message || '')).slice(0, 400)
                : 'ENQUEUE_BACKGROUND_ERROR',
            retryAvailable: true,
            envelope: orderPayload,
          }));
        }
      } catch (_cacheWriteErr) { /* fail open */ }
      try {
        console.error(
          '[posRelayController] _submitWebOrderBackgroundWorker enqueue ERROR (visible via ack-status poll FAILED_DELIVERY):',
          _enqueueErr && _enqueueErr.stack ? String(_enqueueErr.stack).slice(0, 1500) : String(_enqueueErr || ''),
          { submissionId, tenantId, branchId },
        );
      } catch (_logErr) { /* fail open, never crash event loop */ }
    }
  } catch (_workerOuterErr) {
    try {
      console.error(
        '[posRelayController] _submitWebOrderBackgroundWorker OUTER CATCH (unknown worker fatal, swallow):',
        _workerOuterErr && _workerOuterErr.stack ? String(_workerOuterErr.stack).slice(0, 2000) : String(_workerOuterErr || ''),
        { submissionId },
      );
    } catch (_) { /* fail open */ }
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
