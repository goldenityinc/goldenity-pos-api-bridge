/**
 * Image URL Transform Utility
 * Converts S3 URLs to Image Proxy URLs for private buckets
 */

const PROXY_BASE = process.env.IMAGE_PROXY_BASE || '/images';

/**
 * Extract storage key from S3 URL or return original if already a proxy URL
 * 
 * Examples:
 *   'https://l3.storageapi.dev/orderly-carrier-abc/logos/logo_123.png' 
 *     -> 'logos/logo_123.png'
 *   'https://storage.amazonaws.com/bucket/path/to/file.jpg' 
 *     -> 'path/to/file.jpg'
 *   '/images/logo_123.png' (already proxy)
 *     -> '/images/logo_123.png'
 */
const extractS3KeyFromUrl = (url) => {
  if (!url || typeof url !== 'string') {
    return null;
  }

  // Already a proxy URL or relative path
  if (url.startsWith('/images/')) {
    return url; // Already transformed, return as-is
  }

  // Extract key from full S3/HTTP URL
  try {
    const urlObj = new URL(url);
    const pathname = urlObj.pathname; // /bucket-name/key/path or /key/path

    // Remove leading bucket name if present
    // Format: /bucketname/objects/key or /orderly-carrier-abc/logos/file.png
    let key = pathname;
    const parts = pathname.split('/').filter(Boolean); // Remove empty parts

    if (parts.length > 1) {
      // Assume first part is bucket, rest is key
      key = '/' + parts.slice(1).join('/');
    } else if (parts.length === 1) {
      key = '/' + parts[0];
    }

    return key.startsWith('/') ? key.substring(1) : key; // Return without leading slash
  } catch (e) {
    // If URL parsing fails, try regex extraction
    const match = url.match(/\/([^/]+?)\/(.+)$/); // Last two path segments
    if (match) {
      return match[2]; // Return the key part (after bucket)
    }
    return url; // Fallback to original
  }
};

/**
 * Convert S3 URL to Proxy URL
 * Examples:
 *   'https://l3.storageapi.dev/orderly-carrier-abc/logos/logo_123.png'
 *     -> '/images/logos/logo_123.png'
 *   'https://bucket.s3.amazonaws.com/product/img.jpg'
 *     -> '/images/product/img.jpg'
 *   '/images/logo.png' (already proxy)
 *     -> '/images/logo.png'
 *   null
 *     -> null
 */
const toProxyUrl = (s3Url) => {
  if (!s3Url) {
    return null;
  }

  // Already a proxy URL
  if (s3Url.startsWith('/images/')) {
    return s3Url;
  }

  const key = extractS3KeyFromUrl(s3Url);
  if (!key) {
    return s3Url; // Fallback to original if can't extract
  }

  // Encode key for URL safety, but preserve path separators
  const encoded = key
    .split('/')
    .map(part => encodeURIComponent(part))
    .join('%2F'); // URL-encode slashes to %2F

  return `${PROXY_BASE}/${encoded}`;
};

/**
 * Transform object URLs in data structure (used for API responses)
 * Recursively finds imageUrl, logo_url, image_url, photo_url fields and converts them
 */
const transformImageUrlsInObject = (obj, imageFields = ['imageUrl', 'image_url', 'logo_url', 'photo_url']) => {
  if (!obj || typeof obj !== 'object') {
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map(item => transformImageUrlsInObject(item, imageFields));
  }

  const transformed = { ...obj };

  imageFields.forEach(field => {
    if (transformed[field] && typeof transformed[field] === 'string') {
      transformed[field] = toProxyUrl(transformed[field]);
    }
  });

  return transformed;
};

module.exports = {
  toProxyUrl,
  transformImageUrlsInObject,
  extractS3KeyFromUrl,
  PROXY_BASE,
};
