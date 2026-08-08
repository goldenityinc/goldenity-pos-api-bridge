const { Server } = require('socket.io');
const jwt = require('jsonwebtoken');

let ioInstance = null;

const buildTenantRoom = (tenantId) => `tenant:${tenantId}`;
const buildDeviceRoom = (deviceUuid) => `device:${deviceUuid}`;
const buildBranchRoom = (branchId) => `branch:${branchId}`;

const connectedDevices = new Map();

const getConnectedDevices = () => connectedDevices;

const isDeviceOnline = (deviceUuid) => {
  const entry = connectedDevices.get(deviceUuid);
  if (!entry) return false;
  const now = Date.now();
  const staleThreshold = 120000;
  return (now - (entry.lastPing || entry.connectedAt || 0)) <= staleThreshold;
};

const getConnectedDevicesCount = () => {
  let online = 0;
  for (const [, entry] of connectedDevices.entries()) {
    if (isDeviceOnlineByEntry(entry)) online += 1;
  }
  return online;
};

const isDeviceOnlineByEntry = (entry) => {
  if (!entry) return false;
  const now = Date.now();
  const staleThreshold = 120000;
  return (now - (entry.lastPing || entry.connectedAt || 0)) <= staleThreshold;
};

const getPerDeviceStatus = () => {
  const result = {};
  for (const [deviceUuid, entry] of connectedDevices.entries()) {
    result[deviceUuid] = isDeviceOnlineByEntry(entry) ? 'online' : 'offline';
  }
  return result;
};

const emitIncomingWebOrder = (targetDeviceUuid, branchId, orderPayload) => {
  if (!ioInstance) return { emitted: false, reason: 'IO_NOT_INITIALIZED', fallback: null };
  const posNamespace = ioInstance.of('/pos-relay');
  const tenantIdFromPayload =
    (orderPayload && typeof orderPayload === 'object' && (orderPayload.tenantId || orderPayload.tenant_id)) ||
    (targetDeviceUuid && connectedDevices.get(targetDeviceUuid)?.tenantId) ||
    '';

  // 🔴 CRITICAL FIX 1b: EMIT MULTIPLE EVENT NAMES SEKALIGUS (COMPAT DENGAN SEMUA LISTENER VERSION)
  //    POS Flutter lama listen "incoming-web-order".
  //    POS Flutter baru / Admin Web listen "incoming_qr_order" atau "new_web_order".
  //    SOLUSI: EMIT KETIGA EVENT NAME DENGAN PAYLOAD YANG SAMA → pastikan tertangkap apapun listener nya.
  const emitMultiEvent = (targetObj, payload) => {
    try { targetObj.emit('incoming-web-order', payload); } catch (_) {}
    try { targetObj.emit('incoming_qr_order', payload); } catch (_) {}
    try { targetObj.emit('new_web_order', payload); } catch (_) {}
  };

  // 🔴 CRITICAL FIX 1 — SOCKET BROADCAST:
  //    SEBELUMNYA: Hanya emit ke 1 targetDeviceUuid room, LALU RETURN!
  //    Akibatnya device lain (Tablet Printer, PC Kasir Kedua) di BRANCH YANG SAMA
  //    TIDAK PERNAH menerima event incoming-web-order kalo 1 device default printer online.
  //    SEKARANG: SELALU BROADCAST KE BRANCH ROOM DULU (ALL devices connected to that branch),
  //    BARU emit tambahan ke targetDeviceUuid (guaranteed delivery ke printer default).
  //    JANGAN ada early-return! Semua level room harus di-emit.
  const stats = {
    branchEmitted: false,
    branchSockets: 0,
    deviceEmitted: false,
    deviceSockets: 0,
    tenantEmitted: false,
    tenantSockets: 0,
    manualEmitted: false,
    manualSockets: 0,
  };
  let fallbackReason = '';

  // STEP 1 (PRIMARY) — BROADCAST TO ALL DEVICES IN BRANCH ROOM
  if (branchId) {
    const branchRoom = buildBranchRoom(branchId);
    const socketsInBranchRoom = posNamespace.adapter?.rooms?.get(branchRoom);
    if (socketsInBranchRoom && socketsInBranchRoom.size > 0) {
      emitMultiEvent(posNamespace.to(branchRoom), orderPayload);
      stats.branchEmitted = true;
      stats.branchSockets = socketsInBranchRoom.size;
    } else {
      fallbackReason = fallbackReason || 'BRANCH_ROOM_EMPTY';
    }
  } else {
    fallbackReason = fallbackReason || 'NO_BRANCH_ID';
  }

  // STEP 2 (TARGETED DELIVERY) — EMIT DIRECTLY TO TARGET DEVICE ROOM (printer default)
  //    Jangan RETURN! Hanya tambahan emit guarantee, tidak menggantikan branch broadcast.
  if (targetDeviceUuid) {
    const deviceRoom = buildDeviceRoom(targetDeviceUuid);
    const socketsInDeviceRoom = posNamespace.adapter?.rooms?.get(deviceRoom);
    if (socketsInDeviceRoom && socketsInDeviceRoom.size > 0) {
      emitMultiEvent(posNamespace.to(deviceRoom), orderPayload);
      stats.deviceEmitted = true;
      stats.deviceSockets = socketsInDeviceRoom.size;
    } else {
      fallbackReason = fallbackReason || 'DEVICE_ROOM_EMPTY';
    }
  }

  // STEP 3 (FALLBACK TENANT) — Jika branch room kosong, coba broadcast ke tenant room
  if (!stats.branchEmitted && tenantIdFromPayload) {
    const tenantRoom = buildTenantRoom(String(tenantIdFromPayload).trim());
    const socketsInTenantRoom = posNamespace.adapter?.rooms?.get(tenantRoom);
    if (socketsInTenantRoom && socketsInTenantRoom.size > 0) {
      emitMultiEvent(posNamespace.to(tenantRoom), orderPayload);
      stats.tenantEmitted = true;
      stats.tenantSockets = socketsInTenantRoom.size;
    } else {
      fallbackReason = fallbackReason || 'DEVICE_BRANCH_TENANT_ROOMS_EMPTY';
    }
  }

  // STEP 4 (FALLBACK MANUAL ITERATE) — Jika SEMUA room strategy kosong, iterate manual
  const emittedAny = stats.branchEmitted || stats.deviceEmitted || stats.tenantEmitted;
  if (!emittedAny) {
    const allConnected = Array.from(posNamespace.sockets?.values?.() || []).filter(s => {
      const d = s.data?.posDevice || {};
      if (!d.branchId || !d.tenantId) return false;
      if (branchId && String(d.branchId) === String(branchId)) return true;
      if (tenantIdFromPayload && String(d.tenantId) === String(tenantIdFromPayload)) return true;
      return false;
    });
    if (allConnected.length > 0) {
      for (const s of allConnected) {
        try { emitMultiEvent(s, orderPayload); } catch (_) {}
      }
      stats.manualEmitted = true;
      stats.manualSockets = allConnected.length;
    } else {
      fallbackReason = fallbackReason || 'DEVICE_AND_BRANCH_OFFLINE';
    }
  }

  const finalEmitted = stats.branchEmitted || stats.deviceEmitted || stats.tenantEmitted || stats.manualEmitted;

  if (finalEmitted) {
    return {
      emitted: true,
      broadcastTo: {
        branch: stats.branchEmitted ? { count: stats.branchSockets } : null,
        device: stats.deviceEmitted ? { uuid: targetDeviceUuid || null, count: stats.deviceSockets } : null,
        tenant: stats.tenantEmitted ? { count: stats.tenantSockets } : null,
        manual: stats.manualEmitted ? { count: stats.manualSockets } : null,
      },
      fallbackReason: fallbackReason || null,
      targetDeviceUuid: targetDeviceUuid || null,
      branchId: branchId || null,
      tenantId: tenantIdFromPayload || null,
    };
  }

  return {
    emitted: false,
    reason: fallbackReason || 'DEVICE_AND_BRANCH_OFFLINE',
    debug: stats,
    debugConnectedDevices: connectedDevices.size,
    debugTargetDeviceUuid: targetDeviceUuid || null,
    debugBranchId: branchId || null,
    debugTenantId: tenantIdFromPayload || null,
  };
};

const extractHandshakeToken = (socket) => {
  const authToken = socket.handshake.auth?.token;
  if (typeof authToken === 'string' && authToken.trim().length > 0) {
    return authToken.trim().replace(/^Bearer\s+/i, '');
  }

  const headerToken = socket.handshake.headers?.authorization;
  if (typeof headerToken === 'string' && headerToken.startsWith('Bearer ')) {
    return headerToken.slice(7).trim();
  }

  return '';
};

const resolveSocketTenantId = (payload = {}) => {
  return (payload.tenantId ?? payload.tenant_id ?? '').toString().trim();
};

const initializeSocketServer = (server) => {
  if (ioInstance) {
    return ioInstance;
  }

  ioInstance = new Server(server, {
    cors: {
      origin: '*',
      methods: ['GET', 'POST'],
    },
    transports: ['websocket', 'polling'],
  });

  ioInstance.use((socket, next) => {
    try {
      const jwtSecret = process.env.JWT_SECRET;
      if (!jwtSecret) {
        return next(new Error('JWT_SECRET belum dikonfigurasi'));
      }

      const token = extractHandshakeToken(socket);
      if (!token) {
        return next(new Error('Token socket tidak ditemukan'));
      }

      const payload = jwt.verify(token, jwtSecret);
      const tenantId = resolveSocketTenantId(payload);
      if (!tenantId) {
        return next(new Error('tenantId tidak ditemukan pada token socket'));
      }

      socket.data.auth = payload;
      socket.data.tenantId = tenantId;
      return next();
    } catch (error) {
      return next(new Error(error.message || 'Autentikasi socket gagal'));
    }
  });

  ioInstance.on('connection', (socket) => {
    const tenantId = (socket.data.tenantId ?? '').toString().trim();
    if (tenantId) {
      socket.join(buildTenantRoom(tenantId));
    }

    socket.on('join_tenant', (payload = {}) => {
      const requestedTenantId = (payload.tenantId ?? payload.tenant_id ?? '')
        .toString()
        .trim();
      if (!requestedTenantId || requestedTenantId !== tenantId) {
        socket.emit('socket_error', {
          message: 'Tenant room tidak valid',
        });
        return;
      }

      socket.join(buildTenantRoom(requestedTenantId));
      socket.emit('tenant_joined', {
        tenantId: requestedTenantId,
        joinedAt: new Date().toISOString(),
      });
    });
  });

  const posRelayNamespace = ioInstance.of('/pos-relay');

  posRelayNamespace.use((socket, next) => {
    try {
      const auth = socket.handshake.auth || {};
      const deviceUuid = (auth.deviceUuid ?? '').toString().trim();
      const tenantId = (auth.tenantId ?? '').toString().trim();
      const branchId = (auth.branchId ?? '').toString().trim();

      if (!deviceUuid || !tenantId || !branchId) {
        return next(new Error('Handshake auth harus menyertakan deviceUuid, tenantId, branchId'));
      }

      socket.data.posDevice = { deviceUuid, tenantId, branchId };
      return next();
    } catch (error) {
      return next(new Error(error.message || 'Autentikasi POS relay gagal'));
    }
  });

  posRelayNamespace.on('connection', (socket) => {
    const { deviceUuid, tenantId, branchId } = socket.data.posDevice || {};
    const now = Date.now();

    socket.join(buildDeviceRoom(deviceUuid));
    socket.join(buildBranchRoom(branchId));
    socket.join(buildTenantRoom(tenantId));

    connectedDevices.set(deviceUuid, {
      socketId: socket.id,
      connectedAt: now,
      lastPing: now,
      tenantId,
      branchId,
    });

    socket.on('pos-ping', () => {
      const entry = connectedDevices.get(deviceUuid);
      if (entry) {
        entry.lastPing = Date.now();
        socket.emit('pos-pong', { serverTs: Date.now() });
      }
    });

    socket.on('order-acknowledged', (ackPayload = {}) => {
      const { resolveOrderAcknowledgement } = require('./posOrderQueue');
      resolveOrderAcknowledgement({
        submissionId: ackPayload.submissionId,
        ackStatus: ackPayload.ackStatus || 'POS_PRINTED',
        ackPayload: ackPayload.ackPayload || ackPayload,
        deviceUuid,
        printedAt: ackPayload.printedAt || new Date().toISOString(),
      }).catch(() => {});
    });

    socket.on('disconnect', () => {
      const entry = connectedDevices.get(deviceUuid);
      if (entry && entry.socketId === socket.id) {
        entry.lastPing = 0;
      }
    });
  });

  return ioInstance;
};

const emitToTenant = (tenantId, eventName, payload) => {
  if (!ioInstance) {
    return;
  }

  const normalizedTenantId = (tenantId ?? '').toString().trim();
  if (!normalizedTenantId) {
    return;
  }

  ioInstance.to(buildTenantRoom(normalizedTenantId)).emit(eventName, payload);
};

module.exports = {
  initializeSocketServer,
  emitToTenant,
  buildTenantRoom,
  buildDeviceRoom,
  buildBranchRoom,
  connectedDevices,
  getConnectedDevices,
  isDeviceOnline,
  getConnectedDevicesCount,
  getPerDeviceStatus,
  emitIncomingWebOrder,
};