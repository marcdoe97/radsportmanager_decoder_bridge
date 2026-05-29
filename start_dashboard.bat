@echo off
cd /d "%~dp0"
where py >nul 2>nul
if %errorlevel%==0 (
    py -3 -c "import streamlit" >nul 2>nul
    if errorlevel 1 (
        echo Streamlit ist nicht installiert.
        echo Bitte zuerst install_dependencies.bat ausfuehren.
        pause
        exit /b 1
    )
    py -3 -m streamlit run local_dashboard.py --server.port 8501
) else (
    python -c "import streamlit" >nul 2>nul
    if errorlevel 1 (
        echo Streamlit ist nicht installiert.
        echo Bitte zuerst install_dependencies.bat ausfuehren.
        pause
        exit /b 1
    )
    python -m streamlit run local_dashboard.py --server.port 8501
)
pause
