@echo off
setlocal
set DJANGO_BASE=http://127.0.0.1:8000
set DJANGO_USER=admin
set DJANGO_PASS=admin123
set UQBAR_EMAIL=tommymv30@gmail.com
set UQBAR_PASSWORD=Uqbar281173!!
cd /d "C:\Users\TomasVermehrenMiragl\TaxadeRemunercaoCRI"
mkdir logs 2>NUL
"C:\Users\TomasVermehrenMiragl\TaxadeRemunercaoCRI\.venv\Scripts\python.exe" "C:\Users\TomasVermehrenMiragl\TaxadeRemunercaoCRI\TestClickButton.py" --headless --logfile "C:\Users\TomasVermehrenMiragl\TaxadeRemunercaoCRI\logs\uqbar_cron.log"
endlocal
