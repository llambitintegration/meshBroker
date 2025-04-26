@echo off
REM Run Meshtastic device tests on Windows
REM This batch file provides easy access to the test runner

echo Running Meshtastic device tests...

if "%1"=="--unit" (
    python run_device_tests.py --unit %2 %3 %4
) else if "%1"=="--hardware" (
    python run_device_tests.py --hardware %2 %3 %4
) else if "%1"=="--all" (
    python run_device_tests.py --all %2 %3 %4
) else (
    echo Usage:
    echo   run_tests.bat [--unit^|--hardware^|--all] [options]
    echo.
    echo Options:
    echo   --unit       Run unit tests with mocks
    echo   --hardware   Run hardware integration tests
    echo   --all        Run all tests
    echo   -v           Verbose mode
    echo   --timeout N  Set hardware timeout in seconds
    echo.
    echo Example:
    echo   run_tests.bat --unit -v
    echo   run_tests.bat --hardware --timeout 10
    
    python run_device_tests.py --unit
)

if %ERRORLEVEL% NEQ 0 (
    echo Tests failed with exit code %ERRORLEVEL%
) else (
    echo All tests passed!
) 