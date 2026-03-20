const { jsonOk, jsonError } = require('../utils/http');
const {
  uploadBase64Object,
  deleteObject,
} = require('../services/objectStorageService');

const uploadBase64 = async (req, res) => {
  try {
    const bucket = req.body?.bucket;
    const fileName = req.body?.fileName;
    const base64 = req.body?.base64;
    const contentType = req.body?.contentType;

    if (!bucket || !fileName || !base64) {
      return jsonError(
        res,
        400,
        'bucket, fileName, dan base64 wajib diisi',
      );
    }

    const uploaded = await uploadBase64Object({
      bucket,
      fileName,
      base64,
      contentType,
    });

    return jsonOk(
      res,
      {
        ...uploaded,
        contentType: (contentType || 'application/octet-stream').toString(),
      },
      'Upload berhasil',
      201,
    );
  } catch (error) {
    const message = error?.message || 'Storage upload gagal';
    const statusCode =
      message.includes('wajib diisi') || message.includes('tidak valid')
        ? 400
        : message.includes('belum dikonfigurasi')
        ? 503
        : 500;
    return jsonError(res, statusCode, message, message);
  }
};

const deleteStoredObject = async (req, res) => {
  try {
    const bucket = req.params?.bucket;
    const fileName = req.params?.fileName;
    if (!bucket || !fileName) {
      return jsonError(res, 400, 'bucket dan fileName wajib diisi');
    }

    const deleted = await deleteObject({ bucket, fileName });
    return jsonOk(res, deleted, 'File berhasil dihapus');
  } catch (error) {
    const message = error?.message || 'Storage delete gagal';
    const statusCode =
      message.includes('wajib diisi') || message.includes('tidak valid')
        ? 400
        : message.includes('belum dikonfigurasi')
        ? 503
        : 500;
    return jsonError(res, statusCode, message, message);
  }
};

module.exports = {
  uploadBase64,
  deleteStoredObject,
};
