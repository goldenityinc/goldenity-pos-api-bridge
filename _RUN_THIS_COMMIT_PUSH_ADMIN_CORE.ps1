# GOLDENITY ADMIN-CORE COMMIT + PUSH SCRIPT (PowerShell 5+ Compatible)
# Fix: Nested cart product.id FIRST priority extraction (solve id=1784xx timestamp bug)
# HOW TO RUN:
#   1. Right click this file → "Run with PowerShell"
#   OR
#   2. Open VS Code Terminal tab (BUKAN spawned oleh Trae) → cd pos-api-bridge folder → .\_RUN_THIS_COMMIT_PUSH_ADMIN_CORE.ps1
#   OR
#   3. Copy paste semua lines ke VS Code Integrated Terminal lalu ENTER.

$ErrorActionPreference = 'Continue'
$AdminCoreDir = 'E:\Goldenity\goldenity-admin-core-api'

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "   GOLDENITY ADMIN-CORE COMMIT + PUSH (PowerShell)" -ForegroundColor Cyan
Write-Host "   Fix: Nested cart product.id FIRST priority extraction" -ForegroundColor Cyan
Write-Host "   (Bug: id=1784812xx timestamp vs real product.id)" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Test-Path (Join-Path $AdminCoreDir 'src/services/salesService.ts'))) {
  Write-Host "[ERROR ] Folder admin-core tidak ditemukan di: $AdminCoreDir" -ForegroundColor Red
  Read-Host "ENTER untuk keluar"; exit 1
}

Set-Location $AdminCoreDir
Write-Host "[STEP 1 ✅] Masuk folder: $(Get-Location)" -ForegroundColor Green
Write-Host ""

$env:GIT_PAGER = 'cat'
Write-Host "[STEP 2 🔍] Cek status git (file yang berubah):" -ForegroundColor Yellow
git --no-pager status --short
Write-Host ""

Write-Host "[STEP 3 ✍️ ]  git add src/services/salesService.ts src/controllers/publicQrController.ts" -ForegroundColor Yellow
git add src/services/salesService.ts src/controllers/publicQrController.ts
if ($LASTEXITCODE -ne 0) {
  Write-Host "[ERROR ❌] GAGAL git add! Cek error di atas." -ForegroundColor Red
  Read-Host "ENTER untuk keluar"; exit 2
}
Write-Host "Selesai add files." -ForegroundColor Gray
Write-Host ""

$commitMsg = "fix: nested cart product.id FIRST priority extraction + normalize at createSale entry (solve id=1784xx timestamp bug)"
Write-Host "[STEP 4 📝] git commit: `"$commitMsg`"" -ForegroundColor Yellow
git --no-pager commit -m $commitMsg
$commitExit = $LASTEXITCODE
if ($commitExit -ne 0) {
  Write-Host "[WARNING ⚠️] Commit exit code $commitExit (mungkin nothing to commit = sudah di-commit sebelumnya, LANJUT KE PUSH)" -ForegroundColor Magenta
} else {
  Write-Host "Commit BERHASIL ✅" -ForegroundColor Green
}
Write-Host ""

Write-Host "[STEP 5 ☁️ ]  git push origin main --verbose" -ForegroundColor Yellow
git push origin main --verbose
if ($LASTEXITCODE -ne 0) {
  Write-Host ""
  Write-Host "[ERROR ❌] GAGAL PUSH ke GitHub origin/main!" -ForegroundColor Red
  Write-Host "Kemungkinan penyebab:"
  Write-Host "  1. Belum login akun GitHub (git credential manager / gh auth login)"
  Write-Host "  2. Permission ke repo goldenity-admin-core-api belum WRITE access"
  Write-Host "  3. Jaringan / VPN aktif. Matikan VPN dulu atau ganti koneksi."
  Write-Host ""
  Write-Host "Cek manual dengan perintah: git remote -v"
  Read-Host "ENTER untuk keluar"; exit 5
}
Write-Host "Push BERHASIL ✅" -ForegroundColor Green
Write-Host ""

Write-Host "[STEP 6 ✅]  Verifikasi HEAD LOKAL == origin/main GITHUB:" -ForegroundColor Yellow
git fetch origin main | Out-Null
Write-Host ""

$localHash  = git rev-parse HEAD
$remoteHash = git rev-parse origin/main

Write-Host "  Lokal  (HEAD)        : $localHash"
Write-Host "  GitHub (origin/main) : $remoteHash"
Write-Host ""

if ($localHash -eq $remoteHash) {
  Write-Host "================================================================" -ForegroundColor Green
  Write-Host "  ✅✅✅ SUKSES! KEDUA HASH COMMIT SAMA = PUSH BERHASIL! ✅✅✅" -ForegroundColor Green
  Write-Host "================================================================" -ForegroundColor Green
  Write-Host ""
  Write-Host "LANGKAH SELANJUTNYA di RAILWAY:" -ForegroundColor Yellow
  Write-Host ""
  Write-Host "1. Buka https://railway.app → project: goldenity-admin-core-backend"
  Write-Host "2. Tab Deployments → cari commit PALING ATAS dengan message:"
  Write-Host "   fix: nested cart product.id FIRST priority extraction + normalize at createSale entry ..."
  Write-Host "3. Klik tombol 3 dots ⋮ di sisi kanan commit itu → Redeploy with Latest Commit"
  Write-Host "4. TUNGGU sampai badge hijau `"Active`" PINDAH ke commit BARU (BUKAN 8379bd6d lagi!)"
  Write-Host "5. Setelah Active pindah → Buka Deploy Logs → Filter search: [SalesService.createSale ENTRY]"
  Write-Host "6. Coba checkout Teh Hijau qty 2 di POS (tombol hijau)."
  Write-Host "7. TIGA LOG INI PASTI MUNCUL PERTAMA (sebelum error apa pun):"
  Write-Host "   [SalesService.createSale ENTRY] payload summary= ... productId:15, nestedProductId:15"
  Write-Host "   [SalesService.createSale ENTRY] RAW ITEMS JSON= [{id:15, product:{id:15,name:`"Teh Hijau`"}, ...}]"
  Write-Host "   [SalesService.createSale NORMALIZED] ... product_id:`"15`", product_name:`"Teh Hijau`", qty:2"
  Write-Host "8. ✅ Jika product_id = 15 (bukan 178481xx) → CHECKOUT PASTI LOLOS!"
  Write-Host ""
} else {
  Write-Host "[ERROR ❌] HASH COMMIT TIDAK SAMA! Push gagal di STEP 5." -ForegroundColor Red
  Write-Host "Lokal  = $localHash"
  Write-Host "Remote = $remoteHash"
  Write-Host ""
}

Read-Host "ENTER untuk menutup"
exit 0
