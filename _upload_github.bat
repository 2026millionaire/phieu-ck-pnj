@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"

echo === PNJ 1305 - Upload GitHub ===
echo Thu muc: %CD%
echo Repo: 2026millionaire/phieu-ck-pnj
echo Branch: master
echo.

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
    echo Loi: Thu muc nay khong phai Git repo.
    pause
    exit /b 1
)

echo === Trang thai hien tai ===
git status --short
echo.

set /p MSG="Nhap noi dung commit: "
if "%MSG%"=="" (
    echo Da huy vi chua nhap noi dung commit.
    pause
    exit /b 0
)

echo.
echo === Them file vao commit ===
git add .
if errorlevel 1 (
    echo Loi khi git add.
    pause
    exit /b 1
)

echo.
echo === Tao commit ===
git commit -m "%MSG%"
if errorlevel 1 (
    echo Khong tao duoc commit. Co the khong co thay doi moi.
    pause
    exit /b 1
)

echo.
echo === Upload len GitHub? ===
set /p PUSH="Nhan Y de push origin master, Enter de bo qua: "
if /i "%PUSH%"=="Y" (
    git push origin master
    if errorlevel 1 (
        echo Loi khi push len GitHub.
        pause
        exit /b 1
    )
    echo Da push len GitHub thanh cong.
) else (
    echo Da commit local, chua push len GitHub.
)

echo.
echo Xong.
pause
