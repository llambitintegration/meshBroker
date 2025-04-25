@echo off
setlocal enabledelayedexpansion

:: Setup script for Meshtastic MQTT Bridge
:: This script installs and configures all the necessary components for Windows 11

:: Print colored messages
call :print_info "Starting Meshtastic MQTT Bridge setup..."

:: Check if running with administrator privileges
net session >nul 2>&1
if %errorlevel% neq 0 (
    call :print_error "Please run as administrator (right-click, Run as administrator)"
    exit /b 1
)

:: Activate conda environment
call :print_info "Initializing conda and activating environment..."
call "%USERPROFILE%\miniconda3\Scripts\activate.bat"
call conda activate monorepo-dev
if %errorlevel% neq 0 (
    call :print_error "Failed to activate conda environment. Please ensure conda is installed and the monorepo-dev environment exists."
    exit /b 1
)

:: Install Chocolatey package manager if not already installed
call :print_info "Checking for Chocolatey package manager..."
where choco >nul 2>&1
if %errorlevel% neq 0 (
    call :print_info "Installing Chocolatey package manager..."
    powershell -Command "Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))"
    :: Refresh environment variables
    call refreshenv
)

:: Install required packages using Chocolatey
call :print_info "Installing required packages..."
choco install -y mosquitto python nodejs npm

:: Create directories for logs
call :print_info "Creating log directories..."
if not exist "C:\ProgramData\mosquitto\log" mkdir "C:\ProgramData\mosquitto\log"

:: Create directory for mosquitto persistence
call :print_info "Creating persistence directory..."
if not exist "C:\ProgramData\mosquitto\data" mkdir "C:\ProgramData\mosquitto\data"

:: Copy Mosquitto configuration
call :print_info "Configuring Mosquitto..."
copy "mosquitto\mosquitto.conf" "C:\Program Files\mosquitto\mosquitto.conf"

:: Start Mosquitto service
call :print_info "Starting Mosquitto service..."
net start mosquitto
sc config mosquitto start= auto

goto :eof

:print_info
echo [INFO] %~1
goto :eof

:print_success
echo [SUCCESS] %~1
goto :eof

:print_error
echo [ERROR] %~1
goto :eof
