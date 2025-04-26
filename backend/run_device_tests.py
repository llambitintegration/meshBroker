#!/usr/bin/env python3
"""
Script to run Meshtastic device tests

This script provides options to run:
1. Mock-based unit tests
2. Hardware-based integration tests
3. Both types of tests
"""
import os
import sys
import argparse
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the device tests"""
    parser = argparse.ArgumentParser(description="Run Meshtastic device tests")
    parser.add_argument("--unit", action="store_true", help="Run mock-based unit tests")
    parser.add_argument("--hardware", action="store_true", help="Run hardware integration tests")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--router", action="store_true", help="Run router API tests with mocks")
    parser.add_argument("--simulation", action="store_true", help="Run simulated device integration tests")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--timeout", type=int, default=30, 
                        help="Timeout for hardware operations (seconds)")
    args = parser.parse_args()
    
    # If no options are specified, default to running unit tests
    if not (args.unit or args.hardware or args.all or args.router or args.simulation):
        args.unit = True
    
    # Set up environment variables for hardware tests
    if args.hardware or args.all:
        os.environ["ENABLE_HARDWARE_TESTS"] = "true"
        os.environ["HARDWARE_TIMEOUT"] = str(args.timeout)
    
    # Get the absolute path to the backend directory
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    tests_dir = os.path.join(backend_dir, "tests")
    
    # Build the pytest command
    cmd = ["pytest"]
    
    if args.verbose:
        cmd.append("-v")
    
    # Add asyncio configuration to prevent deprecation warning
    cmd.append("--asyncio-mode=strict")
    
    # Determine which tests to run with absolute paths
    if args.all:
        cmd.append(os.path.join(tests_dir, "test_direct_meshtastic.py"))
        cmd.append(os.path.join(tests_dir, "test_direct_meshtastic_integration.py"))
        cmd.append(os.path.join(tests_dir, "test_meshtastic_router.py"))
        cmd.append(os.path.join(tests_dir, "test_meshtastic_router_simulation.py"))
        cmd.append(os.path.join(tests_dir, "test_meshtastic_router_integration.py"))
    elif args.unit:
        cmd.append(os.path.join(tests_dir, "test_direct_meshtastic.py"))
    elif args.hardware:
        cmd.append(os.path.join(tests_dir, "test_direct_meshtastic_integration.py"))
        cmd.append(os.path.join(tests_dir, "test_meshtastic_router_integration.py"))
    elif args.router:
        cmd.append(os.path.join(tests_dir, "test_meshtastic_router.py"))
    elif args.simulation:
        cmd.append(os.path.join(tests_dir, "test_meshtastic_router_simulation.py"))
    
    # Use the rootdir option to specify exactly where to look for tests
    cmd.append(f"--rootdir={backend_dir}")
    
    # Run the tests
    logger.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main()) 