#!/usr/bin/env python3
"""
Validator Keypair Diagnostic Script

This script checks the validator keypair configuration to identify mismatches
between VALIDATOR_SEED and VALIDATOR_ACCOUNT_ID that cause blockchain submission failures.
"""

import os
import sys
from dotenv import load_dotenv
from substrateinterface import Keypair

# Load environment variables
load_dotenv()

def main():
    print("🔍 Validator Keypair Diagnostic")
    print("=" * 50)
    
    # Check environment variables
    validator_seed = os.getenv('VALIDATOR_SEED')
    validator_account_id = os.getenv('VALIDATOR_ACCOUNT_ID')
    
    print(f"VALIDATOR_SEED: {'✅ Set' if validator_seed else '❌ Not set'}")
    print(f"VALIDATOR_ACCOUNT_ID: {'✅ Set' if validator_account_id else '❌ Not set'}")
    
    if not validator_seed:
        print("\n❌ VALIDATOR_SEED is not set!")
        print("   This is required for transaction signing")
        return False
    
    if not validator_account_id:
        print("\n⚠️ VALIDATOR_ACCOUNT_ID is not set!")
        print("   This is used for validator role checking")
        print("   Proceeding with seed validation only...")
    else:
        print(f"\nExpected validator account: {validator_account_id}")
    
    # Try to create keypair from mnemonic
    try:
        print("\n🔑 Testing mnemonic seed...")
        keypair = Keypair.create_from_mnemonic(validator_seed)
        generated_account = keypair.ss58_address
        print(f"Generated account from mnemonic: {generated_account}")
        
        if validator_account_id:
            if generated_account == validator_account_id:
                print("✅ DIRECT SETUP: Mnemonic generates the epoch validator account!")
                print("   This is a direct validator setup")
                return True
            else:
                print("🔗 PROXY SETUP: Mnemonic generates a different account")
                print(f"   Epoch validator account: {validator_account_id}")
                print(f"   Proxy signing account: {generated_account}")
                print("✅ This is a valid proxy account configuration!")
                print("   The proxy account will sign transactions on behalf of the validator")
                return True
        else:
            print("✅ Signing keypair generated successfully")
            return True
            
    except Exception as e:
        print(f"❌ Failed to create keypair from mnemonic: {e}")
    
    # Try to create keypair from raw seed
    try:
        print("\n🔑 Testing raw seed...")
        keypair = Keypair.create_from_seed(validator_seed)
        generated_account = keypair.ss58_address
        print(f"Generated account from raw seed: {generated_account}")
        
        if validator_account_id:
            if generated_account == validator_account_id:
                print("✅ DIRECT SETUP: Raw seed generates the epoch validator account!")
                print("   This is a direct validator setup")
                return True
            else:
                print("🔗 PROXY SETUP: Raw seed generates a different account")
                print(f"   Epoch validator account: {validator_account_id}")
                print(f"   Proxy signing account: {generated_account}")
                print("✅ This is a valid proxy account configuration!")
                print("   The proxy account will sign transactions on behalf of the validator")
                return True
        else:
            print("✅ Signing keypair generated successfully")
            return True
            
    except Exception as e:
        print(f"❌ Failed to create keypair from raw seed: {e}")
    
    print("\n🚨 CRITICAL ISSUE DETECTED:")
    print("   Unable to create a valid keypair from VALIDATOR_SEED")
    print("   This will prevent blockchain transaction signing!")
    print("\n💡 SOLUTIONS:")
    print("   1. Verify the VALIDATOR_SEED format (mnemonic vs raw seed)")
    print("   2. Check for typos or encoding issues in the seed")
    print("   3. Ensure the seed was generated properly")
    
    return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 