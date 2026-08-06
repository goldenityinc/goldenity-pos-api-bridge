const {
  enqueueWebOrderForPrinting,
  resolveOrderAcknowledgement,
  pollIncomingOrders,
  getQueuePendingCount,
  getQueueLengthPerBranch,
  estimateEtaSeconds,
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

    const etaPromise = estimateEtaSeconds();

    try {
      const result = await enqueueWebOrderForPrinting({
        tenantId,
        branchId,
        targetDeviceUuid: body.targetDeviceUuid ? String(body.targetDeviceUuid).trim() : undefined,
        orderPayload: body.orderPayload || body.order_payload || body,
        submissionId,
        transactionId: body.transactionId || body.transaction_id || null,
        salesRecordId: body.salesRecordId || body.sales_record_id || null,
      });

      const etaNextQueue = await etaPromise.catch(() => 0);
      res.setHeader('X-Queue-Eta', String(etaNextQueue));

      if (result && result.ok) {
        return res.status(200).json({
          ok: true,
          ackStatus: result.ackStatus || 'POS_PRINTED',
          resolvedDeviceUuid: result.resolvedDeviceUuid,
          acknowledgedAt: result.acknowledgedAt,
          etaNextQueue,
          deviceUuid: result.deviceUuid,
          submissionId,
          _fromCache: result._fromCache === true,
        });
      }

      const etaNextQueueFallback = await etaPromise.catch(() => 0);
      res.setHeader('X-Queue-Eta', String(etaNextQueueFallback));
      return res.status(502).json({
        ok: false,
        error: 'QUEUE_RESOLVE_UNKNOWN',
        message: 'Queue selesai tanpa status jelas',
        retryAvailable: true,
        submissionId,
        etaNextQueue: etaNextQueueFallback,
      });
    } catch (queueErr) {
      const statusCode = Number(queueErr.statusCode) || 500;
      const errorCode = queueErr.code || 'QUEUE_ERROR';
      const message = queueErr.message || 'Terjadi kesalahan pada queue relay';
      const retryAvailable = queueErr.retryAvailable !== false;

      const etaNextQueue = await etaPromise.catch(() => 0);
      res.setHeader('X-Queue-Eta', String(etaNextQueue));

      if (statusCode === 504) {
        return res.status(504).json({
          ok: false,
          error: 'POS_ACK_TIMEOUT',
          message: message || 'Perangkat POS tidak merespon dalam waktu timeout',
          retryAvailable,
          submissionId,
          etaNextQueue,
        });
      }

      if (statusCode === 503) {
        return res.status(503).json({
          ok: false,
          error: 'POS_DEVICE_OFFLINE',
          message: message || 'Perangkat printer target tidak online',
          retryAvailable,
          submissionId,
          etaNextQueue,
        });
      }

      return res.status(statusCode >= 400 && statusCode < 600 ? statusCode : 500).json({
        ok: false,
        error: errorCode,
        message,
        retryAvailable,
        submissionId,
        etaNextQueue,
      });
    }
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
