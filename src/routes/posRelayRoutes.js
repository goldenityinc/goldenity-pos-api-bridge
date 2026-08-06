const express = require('express');
const {
  submitWebOrderToPos,
  acknowledgeOrderFromPos,
  pollIncomingOrdersHandler,
  getQueueStatus,
} = require('../controllers/posRelayController');

const router = express.Router();

router.post('/web-order', submitWebOrderToPos);
router.post('/orders/:submissionId/ack', acknowledgeOrderFromPos);
router.post('/orders/by-submission/:submissionId/ack', acknowledgeOrderFromPos);
router.get('/orders/by-submission/:submissionId/ack-status', (req, res) => {
  const { getOrderSubmissionState } = require('../services/posOrderQueue');
  try {
    const subId = (req.params.submissionId || '').toString().trim();
    const state = getOrderSubmissionState(subId);
    if (!state) return res.status(404).json({ ok:false, message:`submissionId ${subId} tidak ada di queue` });
    return res.status(200).json({ ok:true, submissionId: subId, ackStatus: state.status, resolvedDeviceUuid: state.resolvedDeviceUuid, resolvedAt: state.resolvedAt || null, detail: state });
  } catch (e) { return res.status(500).json({ ok:false, message:e.message }); }
});
router.get('/poll', pollIncomingOrdersHandler);
router.get('/queue-status', getQueueStatus);

module.exports = router;
