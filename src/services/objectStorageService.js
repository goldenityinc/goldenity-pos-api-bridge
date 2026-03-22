const {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

function joinUrl(base, path) {
  return `${base.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}

let hasLoggedStorageConfig = false;

function sanitizeEnvValue(raw) {
  if (typeof raw !== 'string') {
    return undefined;
  }

  const cleaned = raw.replace(/[\u200B-\u200D\uFEFF]/g, '').trim();
  return cleaned.length > 0 ? cleaned : undefined;
}

function firstEnvValue(keys) {
  for (const key of keys) {
    const value = sanitizeEnvValue(process.env[key]);
    if (value) {
      return value;
    }
  }
  return undefined;
}

function getStorageConfig(runtimeBucket) {
  const bucket = firstEnvValue([
    'STORAGE_BUCKET',
    'AWS_S3_BUCKET_NAME',
    'AWS_BUCKET_NAME',
    'BUCKET_NAME',
    'S3_BUCKET',
    'STORAGE_BUCKET_NAME',
    'S3_BUCKET_NAME',
    'R2_BUCKET',
    'BUCKET',
    'AWS_S3_BUCKET',
    'AWS_BUCKET',
  ]) || sanitizeEnvValue(runtimeBucket);
  const region = firstEnvValue(['STORAGE_REGION', 'AWS_REGION', 'S3_REGION']) || 'auto';
  const endpoint = firstEnvValue([
    'STORAGE_ENDPOINT',
    'AWS_S3_ENDPOINT',
    'S3_ENDPOINT',
    'AWS_ENDPOINT',
    'R2_ENDPOINT',
    'S3_URL',
  ]);
  const accessKeyId = firstEnvValue([
    'STORAGE_ACCESS_KEY',
    'STORAGE_ACCESS_KEY_ID',
    'AWS_ACCESS_KEY_ID',
    'S3_ACCESS_KEY_ID',
    'AWS_S3_ACCESS_KEY_ID',
    'AWS_S3_ACCESS_KEY',
    'ACCESS_KEY',
  ]);
  const secretAccessKey = firstEnvValue([
    'STORAGE_SECRET_KEY',
    'STORAGE_SECRET_ACCESS_KEY',
    'AWS_SECRET_ACCESS_KEY',
    'S3_SECRET_ACCESS_KEY',
    'AWS_S3_SECRET_ACCESS_KEY',
    'AWS_S3_SECRET_KEY',
    'SECRET_KEY',
  ]);
  const publicBaseUrl = firstEnvValue([
    'STORAGE_PUBLIC_BASE_URL',
    'S3_PUBLIC_BASE_URL',
    'AWS_S3_PUBLIC_BASE_URL',
  ]);

  if (!bucket) {
    throw new Error(
      'Storage bucket belum dikonfigurasi. Set STORAGE_BUCKET atau S3_BUCKET.',
    );
  }

  if (!accessKeyId || !secretAccessKey) {
    throw new Error(
      'Storage credential belum dikonfigurasi (STORAGE_ACCESS_KEY / STORAGE_SECRET_KEY).',
    );
  }

  if (!endpoint) {
    throw new Error(
      'Storage endpoint belum dikonfigurasi. Set STORAGE_ENDPOINT untuk S3-compatible storage.',
    );
  }

  if (!hasLoggedStorageConfig) {
    hasLoggedStorageConfig = true;
    console.log(
      `[ObjectStorage] bucket="${bucket}" endpoint="${endpoint}" region="${region}"`,
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

  const config = getStorageConfig(bucket);
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
  const config = getStorageConfig(bucket);
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