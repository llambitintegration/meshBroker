#!/usr/bin/env python3
"""
Script to create a password file for Mosquitto and toggle password authentication
Usage: python create_password.py username password
       python create_password.py --toggle on/off
"""
import sys
import os
import subprocess
import argparse
import logging
import re

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

def toggle_authentication(enable, config_file="mosquitto.conf"):
    """
    Toggle password authentication in mosquitto.conf
    Args:
        enable: True to enable authentication, False to disable
        config_file: Path to mosquitto.conf
    """
    try:
        if not os.path.exists(config_file):
            logger.error(f"Config file not found: {config_file}")
            return False
        
        # Read the current config
        with open(config_file, 'r') as f:
            config_lines = f.readlines()
        
        # Find and modify the allow_anonymous line
        anonymous_pattern = re.compile(r'^allow_anonymous\s+(true|false)')
        anonymous_found = False
        
        for i, line in enumerate(config_lines):
            match = anonymous_pattern.match(line.strip())
            if match:
                anonymous_found = True
                config_lines[i] = f"allow_anonymous {'false' if enable else 'true'}\n"
                break
        
        # If allow_anonymous not found, add it
        if not anonymous_found:
            config_lines.append(f"allow_anonymous {'false' if enable else 'true'}\n")
        
        # Write the updated config
        with open(config_file, 'w') as f:
            f.writelines(config_lines)
        
        status = "enabled" if enable else "disabled"
        logger.info(f"Authentication {status} in {config_file}")
        logger.info(f"allow_anonymous set to {'false' if enable else 'true'}")
        return True
        
    except Exception as e:
        logger.error(f"Error toggling authentication: {str(e)}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Create a password file for Mosquitto and toggle authentication')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Parser for creating password
    password_parser = subparsers.add_parser('create', help='Create password')
    password_parser.add_argument('username', help='MQTT username')
    password_parser.add_argument('password', help='MQTT password')
    password_parser.add_argument('--output', '-o', default='password_file', help='Output password file (default: password_file)')
    
    # Parser for toggling authentication
    toggle_parser = subparsers.add_parser('toggle', help='Toggle authentication')
    toggle_parser.add_argument('state', choices=['on', 'off'], help='Enable or disable authentication')
    toggle_parser.add_argument('--config', '-c', default='mosquitto.conf', help='Mosquitto config file (default: mosquitto.conf)')
    
    args = parser.parse_args()
    
    if args.command == 'create':
        create_password_file(args.username, args.password, args.output)
    elif args.command == 'toggle':
        toggle_authentication(args.state == 'on', args.config)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 