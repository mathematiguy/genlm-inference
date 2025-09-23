#!/bin/bash
# Request password from the user
echo "Provide the password to your secrets.yaml file:"
read -s password

# Function to handle errors
handle_error() {
    echo "ERROR: $1" >&2
    unset password
    return 1
}

# Try to decrypt and export tokens, with error handling
if ! decrypt_output=$(openssl aes-256-cbc -d -a -pbkdf2 -in cluster/secrets.yaml.enc -pass pass:"$password" 2>/dev/null); then
    handle_error "Failed to decrypt secrets file. Please check your password and try again."
    exit 1
fi

# Parse the YAML and extract tokens
if ! parsed_yaml=$(echo "$decrypt_output" | python3 -c "import sys, yaml; print(yaml.safe_load(sys.stdin))" 2>/dev/null); then
    handle_error "Failed to parse YAML content. The file may be corrupted or in an invalid format."
    exit 1
fi

# Extract and export tokens
if ! GITHUB_TOKEN=$(echo "$decrypt_output" | python3 -c "import sys, yaml; print(yaml.safe_load(sys.stdin).get('GITHUB_TOKEN', ''))" 2>/dev/null); then
    handle_error "Failed to extract GITHUB_TOKEN from secrets."
    exit 1
fi

if ! HF_TOKEN=$(echo "$decrypt_output" | python3 -c "import sys, yaml; print(yaml.safe_load(sys.stdin).get('HF_TOKEN', ''))" 2>/dev/null); then
    handle_error "Failed to extract HF_TOKEN from secrets."
    exit 1
fi

# Validate tokens were retrieved
if [ -z "$GITHUB_TOKEN" ]; then
    handle_error "GITHUB_TOKEN not found in secrets file."
    exit 1
fi

if [ -z "$HF_TOKEN" ]; then
    handle_error "HF_TOKEN not found in secrets file."
    exit 1
fi

# Remove the password from the environment
unset password

# Export the tokens
export GITHUB_TOKEN
export HF_TOKEN

echo "Tokens successfully loaded!"
