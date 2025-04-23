#!/usr/bin/env python3
"""
Script to create a password file for Mosquitto
Usage: python create_password.py username password
"""
import sys
import os
import subprocess
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def create_password_file(username, password, output_file="password_file"):
    """
    Create a password file for Mosquitto
    Args:
        username: MQTT username
        password: MQTT password
        output_file: Path to output password file
    """
    try:
        # Create empty password file if it doesn't exist
        if not os.path.exists(output_file):
            with open(output_file, 'w') as f:
                pass
            logger.info(f"Created new password file: {output_file}")
        
        # Use mosquitto_passwd to set the password
        result = subprocess.run(
            ["mosquitto_passwd", "-b", output_file, username, password],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully created user '{username}' in {output_file}")
            logger.info(f"You can use this file with Mosquitto by setting 'password_file {output_file}' in mosquitto.conf")
            return True
        else:
            logger.error(f"Error creating password: {result.stderr}")
            return False
    except FileNotFoundError:
        logger.error("Error: mosquitto_passwd command not found. Please install the Mosquitto utilities.")
        return False
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Create a password file for Mosquitto')
    parser.add_argument('username', help='MQTT username')
    parser.add_argument('password', help='MQTT password')
    parser.add_argument('--output', '-o', default='password_file', help='Output password file (default: password_file)')
    
    args = parser.parse_args()
    
    create_password_file(args.username, args.password, args.output)

if __name__ == "__main__":
    main() 