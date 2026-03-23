const { Server } = require('socket.io');
const jwt = require('jsonwebtoken');

let ioInstance = null;

const buildTenantRoom = (tenantId) => `tenant:${tenantId}`;

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
};