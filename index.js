const http = require('http');
const express = require('express');
const cors = require('cors');
require('dotenv').config();

const publicRoutes = require('./src/routes/publicRoutes');
const protectedRoutes = require('./src/routes/protectedRoutes');
const { tenantResolver } = require('./src/middlewares/tenantResolver');
const { initializeSocketServer } = require('./src/services/socketServer');

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const server = http.createServer(app);

const corsOptions = {
  origin: '*',
};

app.use(cors(corsOptions));
app.options(/(.*)/, cors(corsOptions));
app.use(express.json({ limit: '10mb' }));

app.use(publicRoutes);
app.use(tenantResolver);
app.use(protectedRoutes);

initializeSocketServer(server);

server.listen(PORT, () => {
  console.log(`Goldenity Dynamic Bridge API running on port ${PORT}`);
});
