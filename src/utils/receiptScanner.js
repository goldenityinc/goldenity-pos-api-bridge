const { GoogleGenerativeAI } = require('@google/generative-ai');

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
    const apiKey = (process.env.GEMINI_API_KEY || '').toString().trim();
    if (!apiKey) {
      return {
        isValid: false,
        reason: 'GEMINI_API_KEY belum dikonfigurasi.',
      };
    }

    const cleanedBase64 = (base64Image || '')
      .toString()
      .replace(/^data:[^;]+;base64,/, '')
      .trim();

    if (!cleanedBase64) {
      return {
        isValid: false,
        reason: 'Data gambar bukti pembayaran kosong.',
      };
    }

    const normalizedMimeType = (mimeType || 'image/jpeg').toString().trim().toLowerCase();
    const normalizedAmount = Number(expectedAmount) || 0;

    const client = new GoogleGenerativeAI(apiKey);
    const model = client.getGenerativeModel({ model: 'gemini-1.5-flash' });

    const prompt = `Anda adalah sistem verifikasi pembayaran otomatis. Analisis gambar ini. 1. Apakah ini benar gambar struk transfer bank atau e-wallet? 2. Apakah statusnya BERHASIL? 3. Ekstrak nominal transfer bersih sebagai angka bulat Rupiah tanpa titik/koma (contoh 40000). 4. Nyatakan valid hanya jika nominal transfer >= Rp ${normalizedAmount}. Kembalikan HANYA format JSON tanpa markdown: { "isValid": true/false, "reason": "penjelasan singkat kenapa valid/tidak", "transferredAmount": number|null }.`;

    const result = await model.generateContent([
      { text: prompt },
      {
        inlineData: {
          data: cleanedBase64,
          mimeType: normalizedMimeType,
        },
      },
    ]);

    const responseText = result?.response?.text?.() || '';
    const parsed = parseModelJson(responseText);
    if (!parsed || typeof parsed !== 'object') {
      return defaultInvalidResult;
    }

    const transferredAmount = Number(parsed.transferredAmount);
    return {
      isValid: parsed.isValid === true,
      reason: (parsed.reason || '').toString().trim() || 'Tidak ada alasan dari AI.',
      transferredAmount: Number.isFinite(transferredAmount) ? transferredAmount : null,
    };
  } catch (_) {
    return defaultInvalidResult;
  }
};

module.exports = {
  validatePaymentProof,
};
