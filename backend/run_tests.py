import os
import subprocess
import sys

def run_tests():
    """Run the previously skipped tests"""
    # Set environment variables to enable skipped tests
    env = os.environ.copy()
    env['RUN_SIMULATION_TESTS'] = 'true'
    env['SKIP_INTEGRATION_TESTS'] = 'false'
    
    # Run pytest with the environment variables set
    cmd = [sys.executable, '-m', 'pytest', 'backend/tests/test_meshtastic_simulation.py', '-v']
    
    print(f"Running command: {' '.join(cmd)}")
    print(f"With environment variables: RUN_SIMULATION_TESTS=true, SKIP_INTEGRATION_TESTS=false")
    
    result = subprocess.run(cmd, env=env)
    return result.returncode

if __name__ == '__main__':
    sys.exit(run_tests()) 