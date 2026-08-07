const express = require('express');
const {
  submitWebOrderToPos,
  acknowledgeOrderFromPos,
  pollIncomingOrdersHandler,
  getQueueStatus,
} = require('../controllers/posRelayController');
// 🔴 IMPORT submissionStates & adminCoreFetch untuk:
//    (1) ack-status wildcard segment resolver
//    (2) by-transaction lookup endpoint (home page client polling list order)
const {
  submissionStates,
  getOrderSubmissionState,
  adminCoreFetch,
} = require('../services/posOrderQueue');

const router = express.Router();

router.post('/web-order', submitWebOrderToPos);
router.post('/orders/:submissionId/ack', acknowledgeOrderFromPos);
router.post('/orders/by-submission/:submissionId/ack', acknowledgeOrderFromPos);

// 🔴 [FIX 404 POLLING ACK] Web Ordering mengirim dynamic path segment:
//    bisa "/orders/{orderId}/ack-status" (raw numeric) ATAU
//    "/orders/by-submission/{uuid}/ack-status" (nested by-submission) ATAU
//    format lain. Karena Express route pattern HANYA bisa match 1 level,
//    kita TAMBAHKAN 2 route penutup berantai:
//    a) /orders/by-submission/:submissionId/ack-status (EXPLICIT)
//    b) /orders/:rawSegmentBase/ack-status + /orders/:rawSegmentBase/:rawSegmentNested/ack-status (WILDCARD)
//    Lalu didalam handler RESOLVE submissionId nya manual (detect by-submission prefix).
const normalizeAndResolveSubmissionFromSegment = (req) => {
  const base = (req.params.rawSegmentBase || req.params.submissionId || '').toString().trim();
  const nested = (req.params.rawSegmentNested || '').toString().trim();
  if (base.toLowerCase() === 'by-submission' && nested) {
    return { submissionId: nested, resolvedStyle: 'by-submission-wildcard' };
  }
  if (base) {
    // 🔴 Coba cari raw segment sebagai submissionId di cache terlebih dahulu
    if (submissionStates.has(base)) {
      return { submissionId: base, resolvedStyle: 'raw-order-id-matched-cache' };
    }
    // 🔴 Fallback: mungkin ini adalah salesRecordId / orderId numeric.
    //    Scan seluruh submissionStates cari state yang punya orderId/transactionId = base.
    for (const [storedSubId, st] of submissionStates.entries()) {
      const envelope = st && st.envelope ? st.envelope : (st || {});
      const storedOrderId = String(envelope.orderId || envelope.salesRecordId || envelope.id || st.orderId || '').trim();
      const storedTxId = String(envelope.transactionId || st.transactionId || '').trim();
      if (storedOrderId && storedOrderId === base) return { submissionId: storedSubId, resolvedStyle: 'scan-orderId' };
      if (storedTxId && storedTxId === base) return { submissionId: storedSubId, resolvedStyle: 'scan-transactionId' };
    }
    return { submissionId: base, resolvedStyle: 'raw-fallback-unknown' };
  }
  return { submissionId: '', resolvedStyle: 'empty-segment' };
};

const buildAckStatusResponseFromState = (submissionId, resolvedStyle) => {
  const s = getOrderSubmissionState(submissionId);
  if (!s) {
    const st = submissionStates.get(submissionId);
    if (!st) return { found: false, resolvedStyle, statusCode: 404, payload: { ok:false, message:`submissionId ${submissionId} tidak ada di Bridge queue (belum di-proses / sudah dihapus). Jika order baru submit: tunggu 2-3 detik lalu coba lagi.`, submissionId } };
    const status = String(st.status || st.ackStatus || st.state || 'PENDING_ACK').toUpperCase();
    return { found: true, resolvedStyle, statusCode: 200, payload: { ok:true, submissionId, ackStatus: status, resolvedDeviceUuid: st.resolvedDeviceUuid || st.targetDeviceUuid || null, resolvedAt: st.resolvedAt || st.ackedAt || null, posPrintedAt: st.printedAt || null, failedDeliveryAt: st.failedAt || null, detail: st } };
  }
  const status = String(s.status || s.ackStatus || 'PENDING_ACK').toUpperCase();
  return { found: true, resolvedStyle, statusCode: 200, payload: { ok:true, submissionId, ackStatus: status, resolvedDeviceUuid: s.resolvedDeviceUuid || s.targetDeviceUuid || null, resolvedAt: s.resolvedAt || s.ackedAt || null, posPrintedAt: s.printedAt || null, failedDeliveryAt: s.failedAt || null, detail: s } };
};

// a) Explicit route untuk pattern EXACT by-submission/:submissionId/ack-status
router.get('/orders/by-submission/:submissionId/ack-status', (req, res) => {
  try {
    const subId = (req.params.submissionId || '').toString().trim();
    const r = buildAckStatusResponseFromState(subId, 'exact-by-submission');
    return res.status(r.statusCode).json(r.payload);
  } catch (e) {
    return res.status(500).json({ ok:false, message: e.message || String(e) });
  }
});

// b) Wildcard 2 level segment: /orders/{by-submission}/{uuid}/ack-status
router.get('/orders/:rawSegmentBase/:rawSegmentNested/ack-status', (req, res) => {
  try {
    const resolved = normalizeAndResolveSubmissionFromSegment(req);
    if (!resolved.submissionId) return res.status(400).json({ ok:false, message:'submissionId tidak bisa di-resolve dari URL.' });
    const r = buildAckStatusResponseFromState(resolved.submissionId, resolved.resolvedStyle);
    return res.status(r.statusCode).json(r.payload);
  } catch (e) {
    return res.status(500).json({ ok:false, message: e.message || String(e) });
  }
});

// c) Wildcard 1 level segment: /orders/{orderIdOrSubmissionId}/ack-status
router.get('/orders/:rawSegmentBase/ack-status', (req, res) => {
  try {
    const resolved = normalizeAndResolveSubmissionFromSegment(req);
    if (!resolved.submissionId) return res.status(400).json({ ok:false, message:'submissionId tidak bisa di-resolve dari URL.' });
    const r = buildAckStatusResponseFromState(resolved.submissionId, resolved.resolvedStyle);
    return res.status(r.statusCode).json(r.payload);
  } catch (e) {
    return res.status(500).json({ ok:false, message: e.message || String(e) });
  }
});

// 🔴 [FIX 404 by-transaction POLLING HOME PAGE CLIENT]
//    Route ini dipakai oleh Web Ordering home-page-client setiap 2 detik (useEffect interval by-txId).
//    Jika local submissionStates TIDAK ADA entry, forward ke Admin Core (jika Admin Core deploy sudah aktif).
//    Jika local ADA → return langsung dari cache (cepat, tidak perlu wait core).
router.get('/orders/by-transaction/:txId', async (req, res) => {
  try {
    const txId = (req.params.txId || '').toString().trim();
    const tenantId = (req.tenant && (req.tenant.id || req.tenant.tenantId || req.tenant.tenant_id))
      ? String(req.tenant.id || req.tenant.tenantId || req.tenant.tenant_id)
      : ((req.query.tenantId || req.headers['x-tenant-id'] || req.headers['tenant-id'] || '')).toString().trim();
    const results = [];
    // 🔴 1. Cek LOCAL submissionStates (Bridge queue cache) -> PERTAMA (lebih cepat & fresh)
    for (const [subId, st] of submissionStates.entries()) {
      const envelope = st && st.envelope ? st.envelope : (st || {});
      const storedTxId = String(envelope.transactionId || st.transactionId || envelope.txId || st.txId || '').trim();
      if (storedTxId === txId) {
        const ackStatus = String(st.status || st.ackStatus || envelope.ackStatus || envelope.status || 'PENDING_ACK').toUpperCase();
        results.push({
          submissionId: subId,
          orderId: envelope.orderId || envelope.salesRecordId || envelope.id || st.orderId || null,
          transactionId: storedTxId || null,
          ackStatus,
          tableNumber: (envelope.tableNumber || envelope.table || st.tableNumber || st.table || null),
          tableId: (envelope.tableId || st.tableId || null),
          totalAmount: (envelope.orderPayload && envelope.orderPayload.totalAmount ? envelope.orderPayload.totalAmount : (st.totalAmount || envelope.totalAmount || null)),
          paymentMethod: (envelope.orderPayload && envelope.orderPayload.paymentMethod ? envelope.orderPayload.paymentMethod : (st.paymentMethod || envelope.paymentMethod || null)),
          items: (envelope.orderPayload && Array.isArray(envelope.orderPayload.items) ? envelope.orderPayload.items : (st.items || [])),
          customerName: (envelope.orderPayload && envelope.orderPayload.customerName ? envelope.orderPayload.customerName : (st.customerName || envelope.customerName || null)),
          resolvedDeviceUuid: st.resolvedDeviceUuid || st.targetDeviceUuid || envelope.resolvedDeviceUuid || null,
          createdAt: st.queuedAt || st.createdAt || envelope.createdAt || new Date().toISOString(),
          resolvedAt: st.resolvedAt || st.ackedAt || envelope.ackedAt || null,
          posPrintedAt: st.printedAt || envelope.printedAt || null,
          failedDeliveryAt: st.failedAt || envelope.failedAt || null,
        });
      }
    }
    // 🔴 2. Fallback: jika TIDAK DITEMUKAN di local, forward ke Admin Core endpoint (jika Admin Core sudah deploy SHA a36d842)
    if (results.length === 0 && typeof adminCoreFetch === 'function') {
      try {
        const qsTenant = tenantId ? `?tenantId=${encodeURIComponent(tenantId)}` : '';
        const upstream = await adminCoreFetch(`/api/v1/relay/orders/by-transaction/${encodeURIComponent(txId)}${qsTenant}`, {
          method: 'GET',
          headers: { 'X-Bridge-Proxy': '1', 'X-Internal-Relay': '1' },
        }).catch(() => null);
        if (upstream && upstream.ok && upstream.data) {
          const arr = Array.isArray(upstream.data) ? upstream.data : (upstream.data && Array.isArray(upstream.data.data) ? upstream.data.data : null);
          if (Array.isArray(arr)) return res.status(200).json(arr);
        }
      } catch (_adminCoreFallbackErr) { /* ignore jika Admin Core belum deploy */ }
    }
    // Return empty array 200 OK (supaya polling client tidak throw error exception catch catch)
    return res.status(200).json(results);
  } catch (e) {
    console.error('[posRelayRoutes] by-transaction error:', e && e.stack ? e.stack : e);
    return res.status(500).json({ ok:false, message: e.message || String(e) });
  }
});

router.get('/poll', pollIncomingOrdersHandler);
router.get('/queue-status', getQueueStatus);

module.exports = router;
