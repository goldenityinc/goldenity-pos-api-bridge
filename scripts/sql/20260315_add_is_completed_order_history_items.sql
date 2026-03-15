-- Ganti konsep arsip (is_archived) menjadi status selesai (is_completed).
-- Kolom is_archived mungkin sudah ditambahkan sebelumnya; kita hapus dan
-- ganti dengan is_completed yang lebih sesuai kebutuhan bisnis.

-- 1. Tambah kolom is_completed (idempotent)
ALTER TABLE order_history_items
ADD COLUMN IF NOT EXISTS is_completed BOOLEAN NOT NULL DEFAULT FALSE;

-- 2. Buat index untuk filter cepat
CREATE INDEX IF NOT EXISTS idx_order_history_items_is_completed
ON order_history_items (is_completed);

-- 3. Hapus kolom is_archived jika ada dari migrasi sebelumnya
ALTER TABLE order_history_items
DROP COLUMN IF EXISTS is_archived;
