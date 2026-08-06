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
  if (!ioInstance) return { emitted: false, reason: 'IO_NOT_INITIALIZED' };
  const posNamespace = ioInstance.of('/pos-relay');
  const deviceRoom = buildDeviceRoom(targetDeviceUuid);
  const socketsInRoom = posNamespace.adapter?.rooms?.get(deviceRoom);
  if (socketsInRoom && socketsInRoom.size > 0) {
    posNamespace.to(deviceRoom).emit('incoming-web-order', orderPayload);
    return { emitted: true, target: 'device', room: deviceRoom };
  }
  if (branchId) {
    const branchRoom = buildBranchRoom(branchId);
    const branchSockets = posNamespace.adapter?.rooms?.get(branchRoom);
    if (branchSockets && branchSockets.size > 0) {
      posNamespace.to(branchRoom).emit('incoming-web-order', orderPayload);
      return { emitted: true, target: 'branch-fallback', room: branchRoom };
    }
  }
  return { emitted: false, reason: 'DEVICE_AND_BRANCH_OFFLINE' };
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