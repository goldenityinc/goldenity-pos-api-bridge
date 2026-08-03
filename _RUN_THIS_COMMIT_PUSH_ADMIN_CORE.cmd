@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title = GOLDENITY ADMIN-CORE COMMIT + PUSH (FIX ID=1784XX TIMESTAMP BUG)
color 0A

echo.
echo ================================================================
echo    GOLDENITY ADMIN-CORE COMMIT + PUSH
echo    Fix: Nested cart product.id FIRST priority extraction
echo    (Bug: id=1784812xx timestamp vs real product.id)
echo ================================================================
echo.

set "ADMIN_CORE_DIR=E:\Goldenity\goldenity-admin-core-api"

if not exist "%ADMIN_CORE_DIR%\src\services\salesService.ts" (
    color 0C
    echo [ERROR ❌] Folder admin-core tidak ditemukan di: %ADMIN_CORE_DIR%
    echo Periksa path terlebih dahulu.
    echo.
    pause
    exit /B 1
)

cd /D "%ADMIN_CORE_DIR%"
echo [STEP 1 ✅] Masuk folder: %CD%
echo.

echo [STEP 2 🔍] Cek status git (file yang berubah):
set GIT_PAGER=cat
git --no-pager status --short
echo.

echo [STEP 3 ✍️]  git add src/services/salesService.ts src/controllers/publicQrController.ts
git add src/services/salesService.ts src/controllers/publicQrController.ts
if errorlevel 1 (
    color 0C
    echo.
    echo [ERROR ❌] GAGAL git add! Cek error di atas.
    pause
    exit /B 2
)
echo Selesai add files.
echo.

echo [STEP 4 📝] git commit: "fix: nested cart product.id FIRST priority extraction + normalize at createSale entry (solve id=1784xx timestamp bug)"
git --no-pager commit -m "fix: nested cart product.id FIRST priority extraction + normalize at createSale entry (solve id=1784xx timestamp bug)"
if errorlevel 1 (
    color 0C
    echo.
    echo [ERROR ❌] GAGAL commit! Mungkin sudah di-commit sebelumnya (nothing to commit) tidak apa-apa lanjut ke STEP 5.
    echo Jika error karena conflict / akun git belum login, selesaikan dulu.
    pause
    goto :push_step
)
echo Commit BERHASIL ✅
echo.

:push_step
echo [STEP 5 ☁️]  git push origin main --verbose
git push origin main --verbose
if errorlevel 1 (
    color 0C
    echo.
    echo [ERROR ❌] GAGAL PUSH ke GitHub origin/main!
    echo Kemungkinan penyebab:
    echo   1. Belum login akun GitHub (git credential / GitHub CLI)
    echo   2. Permission ke repo goldenity-admin-core-api belum write
    echo   3. Jaringan internet / VPN menyala → matikan / ganti koneksi
    echo.
    echo Jalankan perintah berikut manual di VS Code terminal untuk cek:
    echo   git remote -v
    pause
    exit /B 5
)
echo Push BERHASIL ✅
echo.

echo [STEP 6 ✅]  Verifikasi HEAD LOKAL == origin/main GITHUB:
git fetch origin main
echo.

for /f "usebackq delims=" %%A in (`git rev-parse HEAD`) do set "LOCAL=%%A"
for /f "usebackq delims=" %%A in (`git rev-parse origin/main`) do set "REMOTE=%%A"

echo   Lokal  (HEAD)        : %LOCAL%
echo   GitHub (origin/main) : %REMOTE%
echo.

if "%LOCAL%"=="%REMOTE%" (
    color 0A
    echo ================================================================
    echo   ✅✅✅ SUKSES! KEDUA HASH COMMIT SAMA = PUSH BERHASIL! ✅✅✅
    echo ================================================================
    echo.
    echo LANGKAH SELANJUTNYA di RAILWAY:
    echo.
    echo 1. Buka https://railway.app → project: goldenity-admin-core-backend
    echo 2. Tab Deployments → cari commit PALING ATAS dengan message:
    echo    fix: nested cart product.id FIRST priority extraction ...
    echo 3. Klik tombol 3 dots ⋮ di sisi kanan commit itu
    echo    → pilih Redeploy with Latest Commit
    echo 4. TUNGGU sampai badge hijau "Active" PINDAH ke commit BARU (bukan 8379bd6d!)
    echo 5. Setelah Active → Buka Deploy Logs → Filter search ketik: [SalesService.createSale ENTRY]
    echo 6. Coba checkout Teh Hijau qty 2 di POS (tombol hijau).
    echo 7. LOG INI PASTI MUNCUL PERTAMA KALI (sebelum error apa pun):
    echo    [SalesService.createSale ENTRY] payload summary= ... productId:15, nestedProductId:15
    echo    [SalesService.createSale NORMALIZED] ... product_id:"15"
    echo 8. ✅ Jika product_id sudah 15 (bukan 1784xx) = CHECKOUT PASTI LOLOS!
    echo.
) else (
    color 0C
    echo [ERROR ❌] HASH COMMIT TIDAK SAMA! Push gagal di STEP 5.
    echo Lokal  = %LOCAL%
    echo Remote = %REMOTE%
    echo.
)

echo.
pause
endlocal
exit /B 0
