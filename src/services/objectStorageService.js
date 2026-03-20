const {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

function joinUrl(base, path) {
  return `${base.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}

function getStorageConfig() {
  const bucket =
    process.env.STORAGE_BUCKET?.trim() ||
    process.env.STORAGE_BUCKET_NAME?.trim() ||
    process.env.S3_BUCKET?.trim() ||
    process.env.S3_BUCKET_NAME?.trim() ||
    process.env.R2_BUCKET?.trim() ||
    process.env.BUCKET?.trim() ||
    process.env.BUCKET_NAME?.trim() ||
    process.env.AWS_S3_BUCKET?.trim() ||
    process.env.AWS_BUCKET?.trim();
  const region =
    process.env.STORAGE_REGION?.trim() || process.env.S3_REGION?.trim() || 'auto';
  const endpoint =
    process.env.STORAGE_ENDPOINT?.trim() || process.env.S3_ENDPOINT?.trim() || undefined;
  const accessKeyId =
    process.env.STORAGE_ACCESS_KEY_ID?.trim() ||
    process.env.S3_ACCESS_KEY_ID?.trim();
  const secretAccessKey =
    process.env.STORAGE_SECRET_ACCESS_KEY?.trim() ||
    process.env.S3_SECRET_ACCESS_KEY?.trim();
  const publicBaseUrl =
    process.env.STORAGE_PUBLIC_BASE_URL?.trim() ||
    process.env.S3_PUBLIC_BASE_URL?.trim() ||
    undefined;

  if (!bucket) {
    throw new Error(
      'Storage bucket belum dikonfigurasi. Set STORAGE_BUCKET atau S3_BUCKET.',
    );
  }

  if (!accessKeyId || !secretAccessKey) {
    throw new Error(
      'Storage credential belum dikonfigurasi (STORAGE_ACCESS_KEY_ID/SECRET).',
    );
  }

  return {
    bucket,
    region,
    endpoint,
    accessKeyId,
    secretAccessKey,
    publicBaseUrl,
  };
}

function createS3Client(config) {
  return new S3Client({
    region: config.region,
    forcePathStyle: true,
    endpoint: config.endpoint,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
  });
}

function sanitizeBucketPrefix(bucket) {
  const value = (bucket || '').toString().trim().toLowerCase();
  if (!value) {
    throw new Error('bucket wajib diisi');
  }
  if (!/^[a-z0-9][a-z0-9_-]{1,62}$/.test(value)) {
    throw new Error('bucket hanya boleh huruf kecil, angka, underscore, atau strip');
  }
  return value;
}

function sanitizeFileName(fileName) {
  const value = (fileName || '').toString().trim();
  if (!value) {
    throw new Error('fileName wajib diisi');
  }
  if (value.includes('..') || value.includes('/') || value.includes('\\')) {
    throw new Error('fileName tidak valid');
  }
  return value;
}

function buildObjectKey(bucket, fileName) {
  return `${sanitizeBucketPrefix(bucket)}/${sanitizeFileName(fileName)}`;
}

function buildPublicUrl(config, key) {
  if (config.publicBaseUrl) {
    return joinUrl(config.publicBaseUrl, key);
  }

  if (config.endpoint) {
    return joinUrl(config.endpoint, `${config.bucket}/${key}`);
  }

  return `https://${config.bucket}.s3.${config.region}.amazonaws.com/${key}`;
}

async function uploadBase64Object({ bucket, fileName, base64, contentType }) {
  const cleanedBase64 = (base64 || '')
    .toString()
    .replace(/^data:[^;]+;base64,/, '')
    .trim();
  if (!cleanedBase64) {
    throw new Error('base64 wajib diisi');
  }

  const config = getStorageConfig();
  const key = buildObjectKey(bucket, fileName);
  const body = Buffer.from(cleanedBase64, 'base64');

  if (!body.length) {
    throw new Error('Konten file kosong');
  }

  const client = createS3Client(config);
  await client.send(
    new PutObjectCommand({
      Bucket: config.bucket,
      Key: key,
      Body: body,
      ContentType: (contentType || 'application/octet-stream').toString(),
    }),
  );

  return {
    bucket,
    fileName,
    key,
    url: buildPublicUrl(config, key),
  };
}

async function deleteObject({ bucket, fileName }) {
  const config = getStorageConfig();
  const key = buildObjectKey(bucket, fileName);
  const client = createS3Client(config);

  await client.send(
    new DeleteObjectCommand({
      Bucket: config.bucket,
      Key: key,
    }),
  );

  return {
    bucket,
    fileName,
    key,
  };
}

module.exports = {
  uploadBase64Object,
  deleteObject,
};