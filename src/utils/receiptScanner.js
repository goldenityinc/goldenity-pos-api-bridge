const defaultInvalidResult = {
  isValid: false,
  reason: 'Gagal memvalidasi gambar.',
  transferredAmount: null,
};

const parseModelJson = (text) => {
  const raw = (text || '').toString().trim();
  if (!raw) {
    return null;
  }

  const withoutFence = raw
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/\s*```$/i, '')
    .trim();

  const start = withoutFence.indexOf('{');
  const end = withoutFence.lastIndexOf('}');
  if (start < 0 || end < start) {
    return null;
  }

  try {
    return JSON.parse(withoutFence.slice(start, end + 1));
  } catch (_) {
    return null;
  }
};

const validatePaymentProof = async (base64Image, mimeType, expectedAmount) => {
  try {
    // 🔴 GEMINI OCR DIMATIKAN SECARA PERMANEN (user request):
    //    "tolong ini dimatikan saja dan dihapus tidak perlu pakai OCR lagi supaya aman dan tidak rusak"
    //    Sebelumnya: import @google/generative-ai → call gemini-1.5-flash model analyze image.
    //    Sekarang: TIDAK PERNAH panggil AI / external service APAPUN.
    //    Return isValid=true SEHINGGA user bisa lanjutkan pembayaran TANPA verifikasi AI
    //    (admin POS manual cek bukti transfer = aman).
    const cleanedBase64 = (base64Image || '')
      .toString()
      .replace(/^data:[^;]+;base64,/, '')
      .trim();

    const hasImagePayload = cleanedBase64.length > 0;

    return {
      isValid: true,
      reason: hasImagePayload
        ? 'Bukti transfer diterima (verifikasi AI dimatikan, cek manual oleh kasir).'
        : 'Pembayaran tanpa bukti (verifikasi AI dimatikan, cek manual oleh kasir).',
      transferredAmount: Number(expectedAmount) || 0,
    };
  } catch (_) {
    return {
      isValid: true,
      reason: 'Fallback sistem OCR dimatikan, lanjut verifikasi manual kasir.',
      transferredAmount: Number(expectedAmount) || 0,
    };
  }
};

module.exports = {
  validatePaymentProof,
};
