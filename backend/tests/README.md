# Meshtastic Device Tests

This directory contains various tests for the Meshtastic device integration.

## Test Types

1. **Unit Tests** (`test_direct_meshtastic.py`)
   - Mock-based tests that don't require hardware
   - Tests basic functionality of device interfaces and managers

2. **Hardware Integration Tests** (`test_direct_meshtastic_integration.py`)
   - Tests that require actual Meshtastic hardware to be connected
   - Verifies real-world device connection and communication

3. **Router API Tests** (`test_meshtastic_router.py`)
   - Tests the FastAPI router endpoints with mocked integration layers
   - Verifies API behavior and error handling

4. **Simulated Device Tests** (`test_meshtastic_router_simulation.py`)
   - Tests the router with simulated devices (no real hardware needed)
   - Verifies end-to-end functionality with mocked hardware

5. **Router Hardware Integration Tests** (`test_meshtastic_router_integration.py`)
   - Tests the router with actual hardware connected
   - Verifies real-world API behavior with physical devices

## Running Tests

Use the `run_tests.bat` script in the parent directory to run these tests:

```
# Run unit tests
run_tests.bat --unit

# Run router API tests
run_tests.bat --router

# Run simulation tests
run_tests.bat --simulation

# Run hardware tests (requires connected devices)
run_tests.bat --hardware --timeout 30

# Run all tests
run_tests.bat --all
```

Add `-v` for verbose output:

```
run_tests.bat --unit -v
```

## Hardware Requirements

For the hardware integration tests (`--hardware`), you need:

1. At least one Meshtastic device connected to your computer
2. The device should be accessible via a serial port
3. The device should be in a working state

If no hardware is available, stick to the unit tests, router API tests, and simulation tests.

## Environment Variables

- `ENABLE_HARDWARE_TESTS`: Set to "true" to enable hardware tests
- `HARDWARE_TIMEOUT`: Timeout in seconds for hardware operations (default: 30)

These are automatically set by the run script when you use the `--hardware` option.

## Continuous Integration

When running in CI environments, you should typically only run:
- Unit tests
- Router API tests
- Simulation tests

Hardware integration tests should be run manually during development or in specialized test environments with actual devices connected.