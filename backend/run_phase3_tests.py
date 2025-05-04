import os
import subprocess
import sys

def run_phase3_tests():
    """Run the Phase 3 tests for Meshtastic MQTT CLI and Decoder"""
    # Set environment variables
    env = os.environ.copy()
    
    # Define the test files to run
    test_files = [
        "tests/test_meshtastic_decoder_enhanced.py",
        "tests/test_meshtastic_mqtt_cli_enhanced.py",
        "tests/test_integration_phase3.py"
    ]
    
    # Build the command
    cmd = [sys.executable, '-m', 'pytest'] + test_files + ['-v']
    
    print(f"Running Phase 3 tests:")
    print(f"Command: {' '.join(cmd)}")
    
    # Run the tests
    result = subprocess.run(cmd, env=env)
    return result.returncode

def run_simulation_tests():
    """Run the simulation tests that don't require actual Meshtastic library"""
    # Set environment variables
    env = os.environ.copy()
    
    # Define the test files to run
    test_files = [
        "tests/test_mqtt_client.py",
        "tests/test_meshtastic_mqtt_cli_enhanced.py"
    ]
    
    # Add markers to only run tests that don't require meshtastic
    cmd = [sys.executable, '-m', 'pytest'] + test_files + ['-v', '-k', 'not requires_meshtastic']
    
    print(f"Running Phase 3 simulation tests (without Meshtastic):")
    print(f"Command: {' '.join(cmd)}")
    
    # Run the tests
    result = subprocess.run(cmd, env=env)
    return result.returncode

if __name__ == '__main__':
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--simulation":
        print("Running simulation tests only (no Meshtastic required)...")
        sys.exit(run_simulation_tests())
    else:
        print("Running all Phase 3 tests...")
        sys.exit(run_phase3_tests())