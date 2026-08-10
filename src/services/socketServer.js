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

  const relayNs = ioInstance.of('/pos-relay');
  const defaultNs = ioInstance.of('/');

  const tenantIdFromPayload =
    (orderPayload && typeof orderPayload === 'object' && (orderPayload.tenantId || orderPayload.tenant_id)) ||
    (targetDeviceUuid && connectedDevices.get(targetDeviceUuid)?.tenantId) ||
    '';

  // 🔴 CRITICAL FIX 1b + 2-STEP QRIS FLOW: EMIT MULTIPLE EVENT NAMES SEKALIGUS
  //    POS Flutter lama listen "incoming-web-order".
  //    POS Flutter baru / Admin Web listen "incoming_qr_order" atau "new_web_order".
  //    2-STEP QRIS BARU: "new_web_order_checker" (Step 1: checker only), "web_order_paid" (Step 2: receipt paid)
  //    SOLUSI: EMIT SEMUA EVENT NAME RELEVAN DENGAN PAYLOAD YANG SAMA → pastikan tertangkap apapun listener nya.
  const emitMultiEvent = (targetObj, payload) => {
    try { targetObj.emit('incoming-web-order', payload); } catch (_) {}
    try { targetObj.emit('incoming_qr_order', payload); } catch (_) {}
    try { targetObj.emit('new_web_order', payload); } catch (_) {}
    try { targetObj.emit('new_web_order_checker', payload); } catch (_) {}
    try { targetObj.emit('web_order_paid', payload); } catch (_) {}
    try { targetObj.emit('web_order_paid_receipt', payload); } catch (_) {}
  };

  // Fungsi khusus 2-step: emit HANYA event CHECKER (Step 1) — dipanggil manual dari queue jika payload print_type=CHECKER_ONLY
  const emitCheckerOnly = (targetObj, payload) => {
    try { targetObj.emit('new_web_order_checker', payload); } catch (_) {}
    try { targetObj.emit('incoming_qr_order', payload); } catch (_) {}
    try { targetObj.emit('incoming-web-order', payload); } catch (_) {}
  };

  // Fungsi khusus 2-step: emit HANYA event PAID RECEIPT (Step 2) — untuk relay dari Admin Core socket
  const emitPaidReceiptOnly = (targetObj, payload) => {
    try { targetObj.emit('web_order_paid', payload); } catch (_) {}
    try { targetObj.emit('web_order_paid_receipt', payload); } catch (_) {}
    try { targetObj.emit('qris_paid', payload); } catch (_) {}
    try { targetObj.emit('payment_success', payload); } catch (_) {}
  };

  // 🔴 CRITICAL FIX 1 — SOCKET BROADCAST (DUAL-NAMESPACE DELIVERY):
  //    SEBELUMNYA: Hanya emit ke /pos-relay namespace.
  //    POS Flutter saat ini konek ke DEFAULT / namespace → events TIDAK PERNAH SAMPAI.
  //    SOLUSI: BROADCAST KE DUA NAMESPACE SEKALIGUS — /pos-relay DAN default / —
  //    PASTIKAN SEMUA ROOM STRATEGY (branch, device, tenant, manual) JALAN DI KEDUA NAMESPACE.
  //    SELALU BROADCAST KE BRANCH ROOM DULU (ALL devices connected to that branch),
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
    defaultNsEmitted: false,
    relayNsEmitted: false,
  };
  let fallbackReason = '';

  const namespaces = [
    { key: 'posRelay', ns: relayNs, tag: '/pos-relay' },
    { key: 'default', ns: defaultNs, tag: '/' },
  ];

  const emitToNs = (ns, tag) => {
    const nsStats = { branch: 0, device: 0, tenant: 0, manual: 0 };
    let nsGotAny = false;

    // STEP 1 (PRIMARY) — BROADCAST TO ALL DEVICES IN BRANCH ROOM
    if (branchId) {
      const branchRoom = buildBranchRoom(branchId);
      const socketsInBranchRoom = ns.adapter?.rooms?.get(branchRoom);
      if (socketsInBranchRoom && socketsInBranchRoom.size > 0) {
        emitMultiEvent(ns.to(branchRoom), orderPayload);
        stats.branchEmitted = true;
        nsStats.branch = socketsInBranchRoom.size;
        stats.branchSockets = Math.max(stats.branchSockets, socketsInBranchRoom.size);
        nsGotAny = true;
      }
    }

    // STEP 2 (TARGETED DELIVERY) — EMIT DIRECTLY TO TARGET DEVICE ROOM (printer default)
    if (targetDeviceUuid) {
      const deviceRoom = buildDeviceRoom(targetDeviceUuid);
      const socketsInDeviceRoom = ns.adapter?.rooms?.get(deviceRoom);
      if (socketsInDeviceRoom && socketsInDeviceRoom.size > 0) {
        emitMultiEvent(ns.to(deviceRoom), orderPayload);
        stats.deviceEmitted = true;
        nsStats.device = socketsInDeviceRoom.size;
        stats.deviceSockets = Math.max(stats.deviceSockets, socketsInDeviceRoom.size);
        nsGotAny = true;
      }
    }

    // STEP 3 (FALLBACK TENANT) — Jika branch room kosong, coba broadcast ke tenant room
    if (!nsGotAny && tenantIdFromPayload) {
      const tenantRoom = buildTenantRoom(String(tenantIdFromPayload).trim());
      const socketsInTenantRoom = ns.adapter?.rooms?.get(tenantRoom);
      if (socketsInTenantRoom && socketsInTenantRoom.size > 0) {
        emitMultiEvent(ns.to(tenantRoom), orderPayload);
        stats.tenantEmitted = true;
        nsStats.tenant = socketsInTenantRoom.size;
        stats.tenantSockets = Math.max(stats.tenantSockets, socketsInTenantRoom.size);
        nsGotAny = true;
      }
    }

    // STEP 4 (FALLBACK MANUAL ITERATE) — Jika SEMUA room strategy kosong, iterate manual
    if (!nsGotAny) {
      const allConnected = Array.from(ns.sockets?.values?.() || []).filter(s => {
        const d = (s.data?.posDevice || (s.data?.auth && s.data) || {});
        const sBranch = String(d.branchId ?? d.branch_id ?? '').trim();
        const sTenant = String(d.tenantId ?? d.tenant_id ?? '').trim();
        if (!sBranch && !sTenant) return false;
        if (branchId && sBranch === String(branchId)) return true;
        if (tenantIdFromPayload && sTenant === String(tenantIdFromPayload).trim()) return true;
        return false;
      });
      if (allConnected.length > 0) {
        for (const s of allConnected) {
          try { emitMultiEvent(s, orderPayload); } catch (_) {}
        }
        stats.manualEmitted = true;
        nsStats.manual = allConnected.length;
        stats.manualSockets = Math.max(stats.manualSockets, allConnected.length);
        nsGotAny = true;
      }
    }

    if (tag === '/pos-relay' && nsGotAny) stats.relayNsEmitted = true;
    if (tag === '/' && nsGotAny) stats.defaultNsEmitted = true;
    return nsGotAny;
  };

  for (const { ns, tag } of namespaces) {
    try { emitToNs(ns, tag); } catch (err) {
      console.warn('[socketServer] emitToNs failed for ns=' + tag + ' err=' + (err && err.message));
    }
  }

  if (!stats.branchEmitted) {
    fallbackReason = fallbackReason ||
      (branchId ? 'BRANCH_ROOM_EMPTY' : 'NO_BRANCH_ID');
  }
  if (!stats.deviceEmitted && targetDeviceUuid) {
    fallbackReason = fallbackReason || 'DEVICE_ROOM_EMPTY';
  }
  if (!stats.tenantEmitted && tenantIdFromPayload) {
    fallbackReason = fallbackReason || 'TENANT_ROOM_EMPTY';
  }
  if (!stats.manualEmitted && !(stats.branchEmitted || stats.deviceEmitted || stats.tenantEmitted)) {
    fallbackReason = fallbackReason || 'DEVICE_BRANCH_TENANT_ROOMS_EMPTY';
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
      origin: (origin, callback) => {
        if (!origin) return callback(null, true);
        const lower = String(origin).toLowerCase();
        const explicitAllowed = new Set([
          'https://pos-web-ordering-sttaging.up.railway.app',
          'https://pos-web-ordering-production.up.railway.app',
          'https://pos-web-ordering.up.railway.app',
          'http://localhost:3000',
          'http://127.0.0.1:3000',
          'http://localhost:3001',
          'http://127.0.0.1:3001',
          'http://localhost:5173',
          'http://127.0.0.1:5173',
          'http://localhost:5174',
          'http://127.0.0.1:5174',
          'http://localhost:8080',
          'http://127.0.0.1:8080',
        ]);
        if (explicitAllowed.has(origin) ||
            lower.endsWith('.up.railway.app') ||
            lower.startsWith('http://localhost:') ||
            lower.startsWith('http://127.0.0.1:') ||
            lower.startsWith('capacitor://') ||
            lower.startsWith('file://')) {
          return callback(null, true);
        }
        return callback(null, origin);
      },
      methods: ['GET', 'HEAD', 'PUT', 'PATCH', 'POST', 'DELETE', 'OPTIONS'],
      credentials: true,
      allowedHeaders: [
        'Origin', 'X-Requested-With', 'Content-Type', 'Accept',
        'Authorization', 'X-Tenant-ID', 'X-Branch-ID', 'X-Device-ID',
        'X-Device-Id', 'x-tenant-id', 'x-branch-id', 'x-device-id',
        // 🔴 FIX: match index.js Express CORS — frontend relay & polling
        // requests can send custom header 'x-internal-relay' / 'X-Internal-Relay'
        // without triggering Socket.IO preflight failure.
        'X-Internal-Relay',
        'x-internal-relay',
        'Cache-Control',
      ],
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

    // 🔴 CRITICAL BACKWARD COMPATIBILITY HANDLERS for DEFAULT / namespace POS Flutter:
    //    POS Flutter saat ini konek ke default / dan EMIT events join_branch / branch:join / join_room
    //    (bukan connect ke /pos-relay). Handler dibawah ini memastikan POS Flutter default /
    //    bisa JOIN rooms branch:X & device:X & tenant:X SECARA MANUAL, lalu receive broadcast events.
    //    Juga menerima POS device register heartbeat untuk populate connectedDevices Map routing.
    const defaultNsResolveAckDevice = (socket, payload) => {
      const auth = socket.handshake.auth || {};
      const deviceUuid = String(
        payload.deviceUuid ?? payload.deviceId ?? payload.device_id ?? auth.deviceUuid ?? auth.deviceId ?? auth.device_id ?? ''
      ).trim();
      const tId = String(
        payload.tenantId ?? payload.tenant_id ?? tenantId ?? auth.tenantId ?? auth.tenant_id ?? ''
      ).trim();
      const bId = String(
        payload.branchId ?? payload.branch_id ?? auth.branchId ?? auth.branch_id ?? ''
      ).trim();
      return { deviceUuid, tenantId: tId, branchId: bId };
    };

    const ackResolver = (ackPayload = {}, fallbackDevice = {}) => {
      const { resolveOrderAcknowledgement } = require('./posOrderQueue');
      const submissionId = String(
        ackPayload.submissionId ?? ackPayload.submission_id ?? ackPayload.orderId ?? ackPayload.order_id ?? ''
      ).trim();
      const ackStatus = String(
        ackPayload.ackStatus ?? ackPayload.ack_status ?? ackPayload.status ?? 'POS_ACKNOWLEDGED'
      ).trim();
      const deviceUuid = String(
        ackPayload.deviceUuid ?? ackPayload.resolvedDeviceUuid ?? ackPayload.deviceId ?? ackPayload.device_id ?? fallbackDevice.deviceUuid ?? ''
      ).trim();
      const printedAt = String(
        ackPayload.printedAt ?? ackPayload.printed_at ?? ackPayload.processedAt ?? ackPayload.acknowledgedAt ?? ''
      ).trim() || new Date().toISOString();
      if (!submissionId) return;
      resolveOrderAcknowledgement({
        submissionId,
        ackStatus,
        ackPayload: ackPayload.ackPayload || ackPayload,
        deviceUuid,
        printedAt,
      }).catch(() => {});
    };

    // Join branch (manual POS Flutter emit)
    socket.on('join_branch', (payload = {}) => {
      try {
        const { deviceUuid, tenantId: tId, branchId: bId } = defaultNsResolveAckDevice(socket, payload);
        if (bId) { socket.join(buildBranchRoom(bId)); }
        if (tId) { socket.join(buildTenantRoom(tId)); }
        if (deviceUuid) {
          socket.join(buildDeviceRoom(deviceUuid));
          if (tId) {
            connectedDevices.set(deviceUuid, {
              socketId: socket.id,
              connectedAt: Date.now(),
              lastPing: Date.now(),
              tenantId: tId,
              branchId: bId || connectedDevices.get(deviceUuid)?.branchId || '',
            });
          }
        }
        socket.emit('branch_joined', {
          tenantId: tId, branchId: bId, deviceUuid,
          joinedAt: new Date().toISOString(),
        });
      } catch (e) {
        try { socket.emit('socket_error', { message: 'join_branch failed: ' + (e.message || String(e)) }); } catch (_) {}
      }
    });

    socket.on('branch:join', (payload = {}) => {
      try {
        const { deviceUuid, tenantId: tId, branchId: bId } = defaultNsResolveAckDevice(socket, payload);
        if (bId) { socket.join(buildBranchRoom(bId)); }
        if (tId) { socket.join(buildTenantRoom(tId)); }
        if (deviceUuid) {
          socket.join(buildDeviceRoom(deviceUuid));
          if (tId) {
            connectedDevices.set(deviceUuid, {
              socketId: socket.id,
              connectedAt: Date.now(),
              lastPing: Date.now(),
              tenantId: tId,
              branchId: bId || connectedDevices.get(deviceUuid)?.branchId || '',
            });
          }
        }
        socket.emit('branch:joined', {
          tenantId: tId, branchId: bId, deviceUuid,
          joinedAt: new Date().toISOString(),
        });
      } catch (_) {}
    });

    socket.on('join_room', (payload = {}) => {
      try {
        const r = payload.room || payload.roomName || payload.name;
        if (r) socket.join(String(r));
        socket.emit('room_joined', { room: r });
      } catch (_) {}
    });

    socket.on('join_device', (payload = {}) => {
      try {
        const { deviceUuid, tenantId: tId, branchId: bId } = defaultNsResolveAckDevice(socket, payload);
        if (deviceUuid) {
          socket.join(buildDeviceRoom(deviceUuid));
          if (tId) {
            connectedDevices.set(deviceUuid, {
              socketId: socket.id,
              connectedAt: Date.now(),
              lastPing: Date.now(),
              tenantId: tId,
              branchId: bId || connectedDevices.get(deviceUuid)?.branchId || '',
            });
          }
        }
        socket.emit('device_joined', {
          deviceUuid, joinedAt: new Date().toISOString(),
        });
      } catch (_) {}
    });

    socket.on('device:register', (payload = {}) => {
      try {
        const { deviceUuid, tenantId: tId, branchId: bId } = defaultNsResolveAckDevice(socket, payload);
        if (deviceUuid && tId) {
          connectedDevices.set(deviceUuid, {
            socketId: socket.id,
            connectedAt: Date.now(),
            lastPing: Date.now(),
            tenantId: tId,
            branchId: bId || connectedDevices.get(deviceUuid)?.branchId || '',
          });
        }
        socket.emit('device:registered', { deviceUuid, tenantId: tId, branchId: bId });
      } catch (_) {}
    });
    socket.on('pos_device_register', (payload = {}) => {
      try {
        const { deviceUuid, tenantId: tId, branchId: bId } = defaultNsResolveAckDevice(socket, payload);
        if (deviceUuid && tId) {
          connectedDevices.set(deviceUuid, {
            socketId: socket.id,
            connectedAt: Date.now(),
            lastPing: Date.now(),
            tenantId: tId,
            branchId: bId || connectedDevices.get(deviceUuid)?.branchId || '',
          });
        }
      } catch (_) {}
    });
    socket.on('device_register', (payload = {}) => {
      try {
        const { deviceUuid, tenantId: tId, branchId: bId } = defaultNsResolveAckDevice(socket, payload);
        if (deviceUuid && tId) {
          connectedDevices.set(deviceUuid, {
            socketId: socket.id,
            connectedAt: Date.now(),
            lastPing: Date.now(),
            tenantId: tId,
            branchId: bId || connectedDevices.get(deviceUuid)?.branchId || '',
          });
        }
      } catch (_) {}
    });

    socket.on('pos-ping', () => {
      try {
        const auth = socket.handshake.auth || {};
        const du = String(auth.deviceUuid ?? auth.deviceId ?? auth.device_id ?? '').trim();
        if (du && connectedDevices.has(du)) {
          const entry = connectedDevices.get(du);
          entry.lastPing = Date.now();
          socket.emit('pos-pong', { serverTs: Date.now() });
        } else {
          socket.emit('pos-pong', { serverTs: Date.now() });
        }
      } catch (_) {
        try { socket.emit('pos-pong', { serverTs: Date.now() }); } catch (_) {}
      }
    });

    // 🔴 DUAL-NAMESPACE ACK LISTENER: POS Flutter emit ACK via default / namespace
    //    (event name variants web_order_acknowledged / pos_web_order_ack / order-acknowledged)
    const ackHandlers = [
      'order-acknowledged',
      'web_order_acknowledged',
      'pos_web_order_ack',
      'pos_order_ack',
    ];
    for (const ev of ackHandlers) {
      try {
        socket.on(ev, (ackPayload = {}) => {
          try {
            const auth = socket.handshake.auth || {};
            ackResolver(ackPayload, {
              deviceUuid: String(
                ackPayload.deviceUuid ?? ackPayload.resolvedDeviceUuid ?? ackPayload.deviceId ?? ackPayload.device_id ?? auth.deviceUuid ?? auth.deviceId ?? auth.device_id ?? ''
              ).trim(),
            });
          } catch (_) {}
        });
      } catch (_) {}
    }
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