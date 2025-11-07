@echo off
ECHO Starting A.A.R.I.A. Frontend and Backend...

REM Opens a new terminal, navigates to the frontend, and runs the dev server
START "AARIA Frontend" cmd /k "cd C:\Users\gadia\Programming\AARIA_Project\frontend && npm run tauri dev"

REM Opens a new terminal, navigates to the backend, and runs the server.py
START "AARIA Backend" cmd /k "cd C:\Users\gadia\Programming\AARIA_Project\backend && pipenv run python server.py"

ECHO New terminal windows are opening...