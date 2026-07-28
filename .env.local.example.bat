@echo off
rem Sao chep file nay thanh .env.local.bat va dien bien moi truong tai may local.
rem KHONG commit file .env.local.bat co chua credential that.

rem ERP/Fiori
set "PNJ_ERP_BASE_URL=https://erp.pnj.com.vn"
set "PNJ_ERP_USER="
set "PNJ_ERP_PASSWORD="

rem Fixture gia lap khi chua co credential/quyen ERP
rem set "PNJ_ERP_BP_FIXTURE_PATH=G:\path\to\business_partners.fixture.json"
rem set "PNJ_BILLING_FIXTURE_PATH=G:\path\to\billing.fixture.json"
rem set "PNJ_PURCHASE_ORDER_FIXTURE_PATH=G:\path\to\purchase_orders.fixture.json"

rem Tuy chon: override Cloudflare Turnstile local neu can
rem set "CUSTOMER_LOOKUP_TURNSTILE_SITEKEY=1x00000000000000000000AA"
rem set "CUSTOMER_LOOKUP_TURNSTILE_SECRET=1x0000000000000000000000000000000AA"
