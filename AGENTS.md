# AGENTS.md - phieu-ck-pnj / dangkhoa.io.vn/bk

## Project

This repo is the PNJ 1305 transfer-confirmation web app.

- Production URL: `https://dangkhoa.io.vn/bk/`
- Verified on: `2026-05-14` - `/bk/` returns `302` to `/bk/login`
- GitHub repo: `2026millionaire/phieu-ck-pnj`
- Branch: `master`
- Local app repo path: `G:\#ClaudeCode\SAP_auto\phieu-ck-app`
- Parent workspace repo: `G:\#ClaudeCode\SAP_auto` on branch `main`; do not confuse it with this app repo
- Stack: Flask + Gunicorn + Nginx reverse proxy + SQLite
- Production app folder: `/opt/phieu-ck-app/`
- Production DB: `/opt/phieu-ck-app/phieuck.db`
- Service: `phieuck.service`
- App port: `5050`

## Current Local State

- `phieu-ck-app` is a nested git repository with remote `https://github.com/2026millionaire/phieu-ck-pnj.git`.
- Local branch is `master`.
- Latest local commits seen on `2026-05-14`:
  - `7102845 refactor: migrate auth sang shared DB /opt/pnj-shared/pnj-auth.db`
  - `cfbf2a4 fix: link 'Che do thu doi' bi nginx sub_filter prepend /bk/ 2 lan`
  - `0f534d6 feat: them menu 'Che do thu doi' vao sidebar, link sang /bk/thudoi/`
- `AGENTS.md` may be untracked in the app repo; keep it as local working context unless the user asks to commit project notes.
- PowerShell may display Vietnamese as mojibake depending on console encoding. Preserve files as UTF-8 and do not rewrite Vietnamese text through a lossy encoding path.

## Working Style

- Tell the user exactly which files changed and what to reload/test after each edit.
- Preserve Vietnamese text carefully. Avoid mojibake. Read/write files as UTF-8.
- The user uses EVKey instead of Windows Vietnamese IME. If automating Windows dialogs, support English dialog labels as fallback.
- Treat PNJ/customer data and server credentials as sensitive. Do not paste secrets into chat unless explicitly needed and approved.

## Local Run

```bash
cd "G:\#ClaudeCode\SAP_auto\phieu-ck-app"
python app.py
```

Or double-click `start.bat`; it opens `http://localhost:5050`.

Local auth behavior:

- `REQUIRE_LOGIN=0` by default, so local development can bypass login.
- On production, run with `REQUIRE_LOGIN=1`.
- Shared auth DB path is controlled by `PNJ_AUTH_DB_PATH`; production default is `/opt/pnj-shared/pnj-auth.db`.

## Important Production Notes

- Current public URL is `/bk/`. Old `/xnck/*` redirects to `/bk/*`.
- Branch is `master`, not `main`.
- SSH uses custom port `24700`, not `22`.
- Nginx config actually read by server is `/etc/nginx/sites-enabled/phieuck`.
- `sites-enabled/phieuck` is not a symlink. Editing only `sites-available/phieuck` may have no effect.
- Nginx for `/bk/` uses `proxy_pass 127.0.0.1:5050/` plus `sub_filter` URL rewriting. The Flask app itself does not know the `/bk/` prefix.
- Existing sibling app `/ctkm/` uses a better pattern: upstream keeps prefix, no `sub_filter`. Prefer this style for new apps.
- Available future app ports noted by Claude: `5052`, `5053+`.

## Deploy Pattern

Local:

```bash
cd "G:\#ClaudeCode\SAP_auto\phieu-ck-app"
git status
git add .
git commit -m "message"
git push origin master
```

Server:

```bash
cd /opt/phieu-ck-app
git pull origin master
systemctl restart phieuck
```

Use the original Claude memory for SSH credential details if needed. Keep credentials out of committed files and chat summaries.

## Routes

| URL | Template | Notes |
|---|---|---|
| `/` | `index.html` | Create CK confirmation form |
| `/login` | `login.html` | Login page when `REQUIRE_LOGIN=1` |
| `/logout` | none | Clears session |
| `/history` | `history.html` | History |
| `/eoffice` | `eoffice.html` | eOffice QT82 |
| `/eoffice/<phieu_id>` | `eoffice.html` | eOffice data for saved slip |
| `/bieu-mau` | `bieu_mau.html` | Combined BB Huy BK + F1 + F2; default F1 |
| `/bb-huy` | redirect to `/bieu-mau` | Old URL |
| `/doi-thongtin` | redirect to `/bieu-mau` | Old URL |
| `/settings` | `settings.html` | Admin-aware section |
| `/bb-huy/print` | `bb_huy_print.html` | A5 landscape |
| `/doi-thongtin/print-f1` | `doi_thongtin_print_f1.html` | A4 portrait, logo + QR |
| `/doi-thongtin/print-f2` | `doi_thongtin_print_f2.html` | A4 compact, one-page target |

## API Surface

- `/api/settings` GET/POST
- `/api/da-trinh/<phieu_id>` POST
- `/api/save` POST
- `/api/phieu/<phieu_id>` GET
- `/api/print/<phieu_id>` GET
- `/api/history` GET
- `/api/delete/<phieu_id>` DELETE
- `/api/parse-sap` POST
- `/api/calc-ngay-tt` POST
- `/api/qr-url` POST
- `/api/so-thanh-chu` POST
- `/api/bank-bins` GET
- `/api/lookup-account` POST
- `/api/ocr-bk` POST
- `/api/template-tt/<phieu_id>` GET
- `/api/banks` GET
- `/api/tvv` GET/POST
- `/api/tvv/<tvv_id>` DELETE
- `/api/lydo-huy` GET/POST
- `/api/lydo-huy/<ld_id>` DELETE

## SQLite Tables

- `users`: login accounts in shared auth DB
- `phieu`, `chung_tu`: saved confirmations, filtered by `user_id`
- `settings`: key/value settings including `bk_prefix`, CHT/KT names, plant, MBBank settings
- `tvv`: TVV list, migrated from Excel
- `lydo_huy`: BK cancellation reasons

## Validation Rules

- Mã KH: trim leading zeros. Accept 9 digits starting with `10`, or `E` plus 7 digits. Warn/confirm only, do not block.
- SĐT: 10 digits. Warn/confirm only, do not block.
- CCCD: 12 digits. Block if invalid.
- BK prefix setting defaults to `4403`; pre-fill first BK document row.
- SAP parser must preserve short prefixes such as `4403`. Do not overwrite them with SAP `doc_num`.

## Print/Layout References

- Reference PDF: `C:\Users\ASUS\OneDrive\Desktop\Mau giay DNTT.pdf`
- Main print layout: A5 portrait, Times New Roman.
- Header: PNJ logo/company line on left; Vietnam national heading/slogan on right.
- Voucher line format: `CH PNJ NEXT 27 Hà Nội, Huế - 1305_dd/mm/yyyy_HH:mm`
- Main title: `PHIẾU XÁC NHẬN THÔNG TIN THANH TOÁN CHUYỂN KHOẢN`
- Transaction table: columns `Loại`, `Số CT`, `Giá trị`, `Ngày giờ`; yellow header.
- Total payment row: yellow, bold, directly attached to table.
- `bb_huy_print.html`: A5 landscape; `.gach-ngang { width: 37%; }`.
- `doi_thongtin_print_f1.html`: A4, PNJ logo + QR; 3 numbered sections; "Kính gửi Quý khách hàng," not italic.
- `doi_thongtin_print_f2.html`: A4 compact, font about 12.5px, line-height 1.4, margin 12mm; "Kính gửi: Công ty CP VBĐQ PNJ" bold + centered, not italic.

## Static Assets

- `static/logo_pnj.webp`
- `static/qr_chinhsach.png`
- `static/template_tt.xlsx`
- Legacy data file: `phieu-ck-data.xlsx`

## UI Guidance

- Mobile-first for every UI change.
- Test 375px viewport.
- Inputs and buttons must be large enough for touch.
- Avoid wide tables on mobile. Use cards or horizontal scrolling where needed.
- Sidebar currently has: Tạo phiếu, Lịch sử, eOffice QT82, Biểu Mẫu, Chế độ thu đổi, Cài đặt.
- The "Chế độ thu đổi" sidebar link is intentionally `href="/thudoi/"` because `/bk/thudoi/` was previously doubled by Nginx `sub_filter`.
- When changing navigation or absolute links, test through both local root `/` and production prefix `/bk/`.

## Known Bugs / Lessons

- Nginx deploy bug: update the real `sites-enabled/phieuck`, not only `sites-available`.
- SAP parser bug: short BK prefix `4403` was overwritten by SAP `2500...`; preserve short prefix values.
- Placeholder uppercase bug: reset with `.muted-placeholder::placeholder { text-transform: none; }`.
- Mobile table width bug: use `table-layout: fixed` and `width: 100%` on inputs so `<col>` widths apply.
- SSH quoting/heredoc is fragile from Windows. For remote nginx config writes, prefer Paramiko SFTP `open().write()`.
- Default amount input bug: do not render `formatNum(0)` as `0`; use empty string to avoid sticky zero when typing.
