const { getObjectStorageClient, getStorageConfig } = require('../services/objectStorageService');
const { GetObjectCommand } = require('@aws-sdk/client-s3');

/**
 * Image Proxy Controller
 * Streams images from S3-compatible storage to client
 * Solves 403 Forbidden issue when bucket is private
 */

const mimeTypeMap = {
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.png': 'image/png',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml',
};

const getMimeType = (key) => {
  const ext = key.toLowerCase().substring(key.lastIndexOf('.'));
  return mimeTypeMap[ext] || 'application/octet-stream';
};

const serveImage = async (req, res) => {
  try {
    const { encodedKey } = req.params;
    if (!encodedKey) {
      return res.status(400).json({ error: 'Key is required' });
    }

    // Decode the key (URL-safe base64 or URI component)
    let key;
    try {
      key = decodeURIComponent(encodedKey);
    } catch (e) {
      return res.status(400).json({ error: 'Invalid key encoding' });
    }

    // Security: Prevent path traversal
    if (key.includes('..') || key.startsWith('/')) {
      return res.status(400).json({ error: 'Invalid key format' });
    }

    const client = getObjectStorageClient();
    if (!client) {
      return res.status(500).json({ error: 'Storage not configured' });
    }

    const config = getStorageConfig();
    const command = new GetObjectCommand({
      Bucket: config.bucket,
      Key: key,
    });

    const response = await client.send(command);

    // Set cache headers (1 hour for logos, 24 hours for product images)
    const cacheTime = key.includes('logo') ? 3600 : 86400;
    res.set({
      'Content-Type': getMimeType(key),
      'Cache-Control': `public, max-age=${cacheTime}`,
      'Content-Length': response.ContentLength || '',
      'ETag': response.ETag || '',
    });

    // Pipe stream directly to response
    response.Body.pipe(res);

    response.Body.on('error', (error) => {
      console.error('Stream error for key', key, ':', error.message);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Failed to stream image' });
      }
    });
  } catch (error) {
    console.error('Image proxy error:', error.message, error.code);

    if (error.Code === 'NoSuchKey' || error.name === 'NoSuchKey') {
      return res.status(404).json({ error: 'Image not found' });
    }

    if (error.Code === 'AccessDenied' || error.name === 'AccessDenied') {
      return res.status(403).json({ error: 'Access denied' });
    }

    if (!res.headersSent) {
      res.status(500).json({ error: 'Failed to serve image' });
    }
  }
};

module.exports = {
  serveImage,
};

module.exports = {
  serveImage,
};
