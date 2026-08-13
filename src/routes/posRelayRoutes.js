const express = require('express');
const multer = require('multer');
const {
  submitWebOrderToPos,
  acknowledgeOrderFromPos,
  pollIncomingOrdersHandler,
  getQueueStatus,
  relayUploadQrOrderPayment,
  replayTransactionWebOrderSocket,
} = require('../controllers/posRelayController');

const paymentProofUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 8 * 1024 * 1024,
  },
});
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

// 🔴🔴 POLLING CAP 5x: mencegah infinite polling loop menghabiskan Railway CPU.
//    Web Order frontend normal polling tiap 2 detik sampai dapat ack.
//    Jika lebih dari 5 kali berturut-turut masih PENDING_ACK tanpa perubahan →
//    RETURN SYNC_DELAYED (frontend akan berhenti aggressive polling).
const __DEBUG_ACK_POLL_VERBOSE = false;
const MAX_ACK_POLLS_BEFORE_THROTTLE = 5;
const THROTTLE_WINDOW_MS = 60000; // 60 detik window
const ackPollTracker = new Map(); // submissionId -> { count, windowStart }

function applyAckPollCap(submissionId, curAckStatusRaw, builtResponse) {
  if (!submissionId) return builtResponse;
  const now = Date.now();
  let tracker = ackPollTracker.get(submissionId);
  if (!tracker || (now - tracker.windowStart) > THROTTLE_WINDOW_MS) {
    tracker = { count: 0, windowStart: now, lastSeenStatus: '' };
    ackPollTracker.set(submissionId, tracker);
  }
  const status = String(curAckStatusRaw || '').toUpperCase();
  if (tracker.lastSeenStatus === status) {
    tracker.count += 1;
  } else {
    tracker.count = 1;
    tracker.lastSeenStatus = status;
    tracker.windowStart = now;
  }
  // Cap: status PENDING / PENDING_ACK / POS_ACKNOWLEDGED TANPA perubahan > MAX →
  //      override jadi SYNC_DELAYED supaya frontend STOP aggressive polling (diamond problem).
  const stuckStatuses = new Set(['', 'PENDING', 'PENDING_ACK', 'POS_ACKNOWLEDGED', 'QUEUED_FOR_POS', 'QUEUED']);
  if (tracker.count > MAX_ACK_POLLS_BEFORE_THROTTLE && stuckStatuses.has(status)) {
    const payloadOverride = {
      ...(builtResponse.payload || {}),
      ackStatus: 'SYNC_DELAYED',
      ok: true,
      pollsThisWindow: tracker.count,
      throttleUntil: new Date(now + THROTTLE_WINDOW_MS).toISOString(),
      message: `Polling cap ${MAX_ACK_POLLS_BEFORE_THROTTLE}x reached. Status frozen -> SYNC_DELAYED. Pull dari POS sync queue auto fallback (3 menit).`,
    };
    return { ...builtResponse, found: true, statusCode: 200, payload: payloadOverride };
  }
  return builtResponse;
}

const buildAckStatusResponseFromState = (submissionId, resolvedStyle) => {
  const s = getOrderSubmissionState(submissionId);
  if (!s) {
    const st = submissionStates.get(submissionId);
    if (!st) return applyAckPollCap(submissionId, '', { found: false, resolvedStyle, statusCode: 404, payload: { ok:false, message:`submissionId ${submissionId} tidak ada di Bridge queue (belum di-proses / sudah dihapus). Jika order baru submit: tunggu 2-3 detik lalu coba lagi.`, submissionId } });
    const status = String(st.status || st.ackStatus || st.state || 'PENDING_ACK').toUpperCase();
    const resp = { found: true, resolvedStyle, statusCode: 200, payload: { ok:true, submissionId, ackStatus: status, resolvedDeviceUuid: st.resolvedDeviceUuid || st.targetDeviceUuid || null, resolvedAt: st.resolvedAt || st.ackedAt || null, posPrintedAt: st.printedAt || null, failedDeliveryAt: st.failedAt || null, detail: st } };
    return applyAckPollCap(submissionId, status, resp);
  }
  const status = String(s.status || s.ackStatus || 'PENDING_ACK').toUpperCase();
  const resp = { found: true, resolvedStyle, statusCode: 200, payload: { ok:true, submissionId, ackStatus: status, resolvedDeviceUuid: s.resolvedDeviceUuid || s.targetDeviceUuid || null, resolvedAt: s.resolvedAt || s.ackedAt || null, posPrintedAt: s.printedAt || null, failedDeliveryAt: s.failedAt || null, detail: s } };
  return applyAckPollCap(submissionId, status, resp);
};

// GC ackPollTracker tiap 30 detik supaya Map tidak membesar tanpa batas
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of ackPollTracker) {
    if ((now - v.windowStart) > (THROTTLE_WINDOW_MS * 2)) ackPollTracker.delete(k);
  }
}, 30000);

// a) Explicit route untuk pattern EXACT by-submission/:submissionId/ack-status
router.get('/orders/by-submission/:submissionId/ack-status', (req, res) => {
  try {
    const subId = (req.params.submissionId || '').toString().trim();
    //#region debug-point web-order-bugs-ackpoll 🔴 DISABLED for Railway CPU save
    if (__DEBUG_ACK_POLL_VERBOSE) {
      try {
        const ts = new Date().toISOString();
        const st = submissionStates.get(subId);
        const sstatus = st ? (st.status || st.ackStatus || '') : '';
        const sresolved = st ? !!st.resolved : false;
        console.error(
          `[${ts}] [DEBUG-WEB-ORDER] [ACK-POLL-by-submission] submissionId=${subId} resolved=${sresolved} curStatus=${sstatus} resolvedStyle=exact-by-submission reqIp=${String(req.ip || '')}`,
        );
      } catch (_dbg) { /* noop */ }
    }
    //#endregion
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
    //#region debug-point web-order-bugs-ackpoll 🔴 DISABLED
    if (__DEBUG_ACK_POLL_VERBOSE) {
      try {
        const ts = new Date().toISOString();
        const st = submissionStates.get(resolved.submissionId);
        const sstatus = st ? (st.status || st.ackStatus || '') : '';
        const sresolved = st ? !!st.resolved : false;
        console.error(
          `[${ts}] [DEBUG-WEB-ORDER] [ACK-POLL-wild2] submissionId=${resolved.submissionId} resolvedStyle=${resolved.resolvedStyle} resolved=${sresolved} curStatus=${sstatus}`,
        );
      } catch (_dbg) { /* noop */ }
    }
    //#endregion
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
    //#region debug-point web-order-bugs-ackpoll 🔴 DISABLED
    if (__DEBUG_ACK_POLL_VERBOSE) {
      try {
        const ts = new Date().toISOString();
        const st = submissionStates.get(resolved.submissionId);
        const sstatus = st ? (st.status || st.ackStatus || '') : '';
        const sresolved = st ? !!st.resolved : false;
        console.error(
          `[${ts}] [DEBUG-WEB-ORDER] [ACK-POLL-wild1] submissionId=${resolved.submissionId} resolvedStyle=${resolved.resolvedStyle} resolved=${sresolved} curStatus=${sstatus}`,
        );
      } catch (_dbg) { /* noop */ }
    }
    //#endregion
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
    // 🔴 FIX 2 TABLE ISOLATION (BRIDGE SIDE): ambil tableId / branchId dari query
    //    Agar LOCAL FILTER + UPSTREAM FORWARD keduanya ISOLATED per meja:
    const tableId = ((req.query.tableId || req.query.table_id || req.headers['x-table-id'] || '')).toString().trim();
    const branchId = ((req.query.branchId || req.query.branch_id || req.headers['x-branch-id'] || '')).toString().trim();
    const results = [];
    // 🔴 1. Cek LOCAL submissionStates (Bridge queue cache) -> PERTAMA (lebih cepat & fresh)
    //    ✅ ISOLASI: tableId diset -> FILTER hasil hanya MILIK MEJA TERSEBUT (tidak bocor meja lain!)
    for (const [subId, st] of submissionStates.entries()) {
      const envelope = st && st.envelope ? st.envelope : (st || {});
      const storedTxId = String(envelope.transactionId || st.transactionId || envelope.txId || st.txId || '').trim();
      if (storedTxId !== txId) continue;
      // 🔴 LOCAL ISOLATION FILTER:
      if (tableId) {
        const storedTableId = String(envelope.tableId || st.tableId || '').trim();
        if (storedTableId && storedTableId !== tableId) continue;
      }
      const ackStatus = String(st.status || st.ackStatus || envelope.ackStatus || envelope.status || 'PENDING_ACK').toUpperCase();
      results.push({
        submissionId: subId,
        orderId: envelope.orderId || envelope.salesRecordId || envelope.id || st.orderId || null,
        transactionId: storedTxId || null,
        ackStatus,
        tableNumber: (envelope.tableNumber || envelope.table || st.tableNumber || st.table || null),
        tableId: (envelope.tableId || st.tableId || null),
        branchId: (envelope.branchId || st.branchId || null),
        totalAmount: (envelope.orderPayload && envelope.orderPayload.totalAmount ? envelope.orderPayload.totalAmount : (st.totalAmount || envelope.totalAmount || null)),
        subtotal: (envelope.orderPayload && envelope.orderPayload.totalAmount ? envelope.orderPayload.totalAmount : (st.totalAmount || envelope.totalAmount || null)),
        grandTotal: (envelope.orderPayload && envelope.orderPayload.totalAmount ? envelope.orderPayload.totalAmount : (st.totalAmount || envelope.totalAmount || null)),
        paymentMethod: (envelope.orderPayload && envelope.orderPayload.paymentMethod ? envelope.orderPayload.paymentMethod : (st.paymentMethod || envelope.paymentMethod || null)),
        items: (envelope.orderPayload && Array.isArray(envelope.orderPayload.items) ? envelope.orderPayload.items : (st.items || [])),
        customerName: (envelope.orderPayload && envelope.orderPayload.customerName ? envelope.orderPayload.customerName : (st.customerName || envelope.customerName || null)),
        pax: (envelope.orderPayload && envelope.orderPayload.pax ? envelope.orderPayload.pax : (st.pax || envelope.pax || null)),
        orderNote: (envelope.orderPayload && envelope.orderPayload.orderNote ? envelope.orderPayload.orderNote : (st.orderNote || envelope.orderNote || null)),
        receiptNumber: (envelope.receiptNumber || st.receiptNumber || envelope.receipt_number || st.receipt_number || null),
        referenceId: (envelope.referenceId || st.referenceId || null),
        resolvedDeviceUuid: st.resolvedDeviceUuid || st.targetDeviceUuid || envelope.resolvedDeviceUuid || null,
        createdAt: st.queuedAt || st.createdAt || envelope.createdAt || new Date().toISOString(),
        updatedAt: st.ackedAt || st.printedAt || envelope.updatedAt || st.resolvedAt || null,
        resolvedAt: st.resolvedAt || st.ackedAt || envelope.ackedAt || null,
        posPrintedAt: st.printedAt || envelope.printedAt || null,
        failedDeliveryAt: st.failedAt || envelope.failedAt || null,
      });
    }
    // 🔴 2. Fallback: jika TIDAK DITEMUKAN di local, forward ke Admin Core endpoint (jika Admin Core sudah deploy SHA a36d842)
    //    ✅ ISOLATION: forward JUGA tableId & branchId query string ke upstream agar CORE WHERE table_id filter!
    //    ✅ SPEED GUARANTEE (FIX 3 SLOW): upstream timeout HANYA 350ms. Kalau core lambat / cold start → return LOCAL results (empty array jika tidak ada).
    //       Web Ordering frontend TIDAK HANG / stuck polling <500ms guaranteed.
    if (results.length === 0 && typeof adminCoreFetch === 'function') {
      try {
        const query = new URLSearchParams();
        if (tenantId) query.set('tenantId', tenantId);
        if (branchId) query.set('branchId', branchId);
        if (tableId) query.set('tableId', tableId);
        const qs = query.toString() ? `?${query.toString()}` : '';
        let upstream = null;
        if (typeof Promise.race === 'function') {
          let timeoutCancel = null;
          const timeoutPromise = new Promise((resolve) => {
            timeoutCancel = setTimeout(() => resolve(null), 350);
            try { (timeoutCancel && typeof timeoutCancel.unref === 'function') ? timeoutCancel.unref() : null; } catch (_e) {}
          });
          const fetchPromise = adminCoreFetch(`/api/v1/relay/orders/by-transaction/${encodeURIComponent(txId)}${qs}`, {
            method: 'GET',
            headers: { 'X-Bridge-Proxy': '1', 'X-Internal-Relay': '1' },
          }).catch(() => null);
          upstream = await Promise.race([fetchPromise, timeoutPromise]);
          if (timeoutCancel !== null) clearTimeout(timeoutCancel);
        } else {
          upstream = await adminCoreFetch(`/api/v1/relay/orders/by-transaction/${encodeURIComponent(txId)}${qs}`, {
            method: 'GET',
            headers: { 'X-Bridge-Proxy': '1', 'X-Internal-Relay': '1' },
          }).catch(() => null);
        }
        if (upstream && upstream.ok && upstream.data) {
          const arr = Array.isArray(upstream.data) ? upstream.data : (upstream.data && Array.isArray(upstream.data.data) ? upstream.data.data : null);
          if (Array.isArray(arr)) return res.status(200).json(arr);
        }
      } catch (_adminCoreFallbackErr) { /* ignore jika Admin Core belum deploy / timeout */ }
    }
    // Return empty array 200 OK (supaya polling client tidak throw error exception catch catch)
    return res.status(200).json(results);
  } catch (e) {
    console.error('[posRelayRoutes] by-transaction error:', e && e.stack ? e.stack : e);
    return res.status(500).json({ ok:false, message: e.message || String(e) });
  }
});

// 🔴 [NEW ENDPOINT] Bridge forward: Active Orders PER MEJA (Web Ordering Order List Tab)
//    Route: GET /api/v1/relay/orders/active?tenantId=X&branchId=Y&tableId=Z
//    WAJIB tableId param (ISOLATION TOTAL): HANYA return order ACTIVE MILIK MEJA INI SAJA.
router.get('/orders/active', async (req, res) => {
  try {
    const tenantId = (req.tenant && (req.tenant.id || req.tenant.tenantId || req.tenant.tenant_id))
      ? String(req.tenant.id || req.tenant.tenantId || req.tenant.tenant_id)
      : ((req.query.tenantId || req.headers['x-tenant-id'] || req.headers['tenant-id'] || '')).toString().trim();
    const tableId = ((req.query.tableId || req.query.table_id || req.headers['x-table-id'] || '')).toString().trim();
    const branchId = ((req.query.branchId || req.query.branch_id || req.headers['x-branch-id'] || '')).toString().trim();
    // ✅ ISOLATION ENFORCEMENT: tableId WAJIB! Jika tidak ada → return INSTANT [].
    if (!tenantId || !tableId) return res.status(200).json([]);
    const results = [];
    const activeStatuses = new Set(['PENDING','PREPARING','READY_FOR_PICKUP','PENDING_PAYMENT','PENDING_ACK','POS_ACKNOWLEDGED','PARTIAL','NEW']);
    // 1. FAST PATH LOCAL submissionStates cache:
    //    REGULAR cutoff = 120 detik (CASHIER / PREPARING dll)
    //    QRIS PENDING_PAYMENT cutoff = 900 detik (15 menit)
    const cutoffAgoRegular = Date.now() - 120_000;
    const cutoffAgoQris = Date.now() - 900_000;
    for (const [subId, st] of submissionStates.entries()) {
      const envelope = st && st.envelope ? st.envelope : (st || {});
      const storedTableId = String(envelope.tableId || st.tableId || '').trim();
      if (storedTableId !== tableId) continue;
      if (branchId) {
        const stBr = String(envelope.branchId || st.branchId || '').trim();
        if (stBr && stBr !== branchId) continue;
      }
      const status = String(st.status || st.ackStatus || envelope.ackStatus || envelope.status || 'PENDING_ACK').toUpperCase();
      const paymentStatusRaw = String(st.paymentStatus || envelope.paymentStatus || envelope.payment_status || st.payment_status || '').toUpperCase();
      const paymentMethodRaw = String(envelope.orderPayload && envelope.orderPayload.paymentMethod ? envelope.orderPayload.paymentMethod : (st.paymentMethod || envelope.paymentMethod || '')).toUpperCase();
      const isQrisPendingPayment =
        status === 'PENDING_PAYMENT' ||
        paymentStatusRaw === 'PENDING_PAYMENT' ||
        paymentMethodRaw.includes('QRIS');
      if (!activeStatuses.has(status)) continue;
      const tCreated = st.queuedAt || st.createdAt || envelope.createdAt;
      const ts = tCreated ? new Date(tCreated).getTime() : Date.now();
      const applicableCutoff = isQrisPendingPayment ? cutoffAgoQris : cutoffAgoRegular;
      if (Number.isFinite(ts) && ts < applicableCutoff) continue;
      results.push({
        submissionId: subId,
        orderId: envelope.orderId || envelope.salesRecordId || envelope.id || st.orderId || null,
        transactionId: (envelope.transactionId || st.transactionId || subId),
        ackStatus: status,
        tableNumber: (envelope.tableNumber || envelope.table || st.tableNumber || st.table || null),
        tableId: storedTableId || tableId,
        branchId: (envelope.branchId || st.branchId || branchId || null),
        totalAmount: (envelope.orderPayload && envelope.orderPayload.totalAmount ? envelope.orderPayload.totalAmount : (st.totalAmount || envelope.totalAmount || 0)),
        subtotal: (envelope.orderPayload && envelope.orderPayload.totalAmount ? envelope.orderPayload.totalAmount : (st.totalAmount || envelope.totalAmount || 0)),
        grandTotal: (envelope.orderPayload && envelope.orderPayload.totalAmount ? envelope.orderPayload.totalAmount : (st.totalAmount || envelope.totalAmount || 0)),
        paymentMethod: (envelope.orderPayload && envelope.orderPayload.paymentMethod ? envelope.orderPayload.paymentMethod : (st.paymentMethod || envelope.paymentMethod || null)),
        items: (envelope.orderPayload && Array.isArray(envelope.orderPayload.items) ? envelope.orderPayload.items : (st.items || [])),
        customerName: (envelope.orderPayload && envelope.orderPayload.customerName ? envelope.orderPayload.customerName : (st.customerName || envelope.customerName || null)),
        pax: (envelope.orderPayload && envelope.orderPayload.pax ? envelope.orderPayload.pax : (st.pax || envelope.pax || null)),
        orderNote: (envelope.orderPayload && envelope.orderPayload.orderNote ? envelope.orderPayload.orderNote : (st.orderNote || envelope.orderNote || null)),
        receiptNumber: (envelope.receiptNumber || st.receiptNumber || envelope.receipt_number || st.receipt_number || null),
        resolvedDeviceUuid: st.resolvedDeviceUuid || st.targetDeviceUuid || envelope.resolvedDeviceUuid || null,
        createdAt: tCreated || new Date().toISOString(),
        resolvedAt: st.resolvedAt || st.ackedAt || envelope.ackedAt || null,
        posPrintedAt: st.printedAt || envelope.printedAt || null,
        failedDeliveryAt: st.failedAt || envelope.failedAt || null,
      });
    }
    if (results.length > 0) {
      results.sort((a,b) => {
        const at = (a.createdAt && typeof a.createdAt === 'string') ? new Date(a.createdAt).getTime() : 0;
        const bt = (b.createdAt && typeof b.createdAt === 'string') ? new Date(b.createdAt).getTime() : 0;
        return bt - at;
      });
      return res.status(200).json(results);
    }
    // 2. Fallback upstream Admin Core /active endpoint (dengan timeout 350ms instant fallback):
    if (typeof adminCoreFetch === 'function') {
      try {
        const query = new URLSearchParams();
        query.set('tenantId', tenantId);
        query.set('tableId', tableId);
        if (branchId) query.set('branchId', branchId);
        let upstream = null;
        const timeoutPromise = new Promise((resolve) => {
          const t = setTimeout(() => resolve(null), 350);
          try { (t.unref && typeof t.unref === 'function') ? t.unref() : null; } catch (_e) {}
        });
        const fetchPromise = adminCoreFetch(`/api/v1/relay/orders/active?${query.toString()}`, {
          method: 'GET',
          headers: { 'X-Bridge-Proxy': '1', 'X-Internal-Relay': '1' },
        }).catch(() => null);
        upstream = await Promise.race([fetchPromise, timeoutPromise]);
        if (upstream && upstream.ok && upstream.data) {
          const arr = Array.isArray(upstream.data) ? upstream.data : (upstream.data && Array.isArray(upstream.data.data) ? upstream.data.data : null);
          if (Array.isArray(arr)) return res.status(200).json(arr);
        }
      } catch (_e) { /* ignore */ }
    }
    return res.status(200).json(results);
  } catch (e) {
    console.error('[posRelayRoutes] orders/active error:', e && e.stack ? e.stack : e);
    return res.status(500).json({ ok:false, message: e.message || String(e) });
  }
});

router.get('/poll', pollIncomingOrdersHandler);
router.get('/queue-status', getQueueStatus);

// 🔴 NEW: /replay endpoint untuk RESEND socket event ke POS jika POS OFFLINE saat submit order
//    Skenario: user submit order → POS OFFLINE → POS kembali online → order TIDAK KELUAR di POS / Meja kosong.
//    User klik Refresh di Order List → frontend call endpoint ini per transactionId → EMIT ULANG socket events
//    (new_web_order / checker_only / paid_receipt) ke POS + mark meja OCCUPIED (seolah baru diterima).
//    Pattern: POST /api/v1/relay/replay (body)  or POST /replay/by-transaction/:txId or POST /replay/by-submission/:submissionId
router.post('/replay/by-transaction/:txId', replayTransactionWebOrderSocket);
router.post('/replay/by-submission/:submissionId', replayTransactionWebOrderSocket);
router.post('/replay', replayTransactionWebOrderSocket);

// 🔴 2-STEP QRIS FLOW Step 2: Upload payment proof untuk QR order yang sudah dibuat (Step 1 = checker only)
//    PUT /api/v1/relay/qr-orders/:id/payment
//    Request: multipart/form-data (payment_proof file) atau JSON (payment_proof_url string)
//    Relay ke Admin Core PUT /api/v1/qr-orders/:id/payment untuk update status PAID & trigger web_order_paid socket event.
//    paymentProofUpload.any(): menerima nama field apapun (payment_proof, paymentProofFile, dll) sebagai file buffer.
router.put('/qr-orders/:id/payment', paymentProofUpload.any(), relayUploadQrOrderPayment);
router.put('/qr-orders/:orderId/payment', paymentProofUpload.any(), relayUploadQrOrderPayment);
router.put('/qr-orders/by-transaction/:txId/payment', paymentProofUpload.any(), relayUploadQrOrderPayment);

module.exports = router;
