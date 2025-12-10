#!/bin/bash

set -e

echo "============================================"
echo "Updating follower on darkwingdog"
echo "Adding BVN0 as known node"
echo "============================================"

# Create config update script
cat > /tmp/add-bvn0.sh << 'SCRIPT'
#!/bin/bash

# Find the follower config file
CONFIG_LOCATIONS=(
    ~/follower/config.yaml
    ~/follower/config.json
    ~/app-follower/config.yaml
    ~/app-follower/config.json
    ~/.follower/config.yaml
    ~/.follower/config.json
    /etc/follower/config.yaml
    /etc/follower/config.json
)

CONFIG_FILE=""
for loc in "${CONFIG_LOCATIONS[@]}"; do
    if [ -f "$loc" ]; then
        CONFIG_FILE="$loc"
        echo "Found config at: $CONFIG_FILE"
        break
    fi
done

if [ -z "$CONFIG_FILE" ]; then
    echo "No existing config found. Creating new config at ~/follower-config.yaml"
    CONFIG_FILE=~/follower-config.yaml
fi

# Backup existing config
if [ -f "$CONFIG_FILE" ]; then
    cp "$CONFIG_FILE" "${CONFIG_FILE}.backup.$(date +%Y%m%d-%H%M%S)"
    echo "Backed up existing config"
fi

# Create/update config with BVN0
if [[ "$CONFIG_FILE" == *.yaml ]] || [[ "$CONFIG_FILE" == *.yml ]]; then
    cat > "$CONFIG_FILE" << 'EOF'
# Follower configuration with BVN0 hardcoded
known_nodes:
  - name: BVN0
    address: 23.22.212.106:8080

bootstrap:
  enabled: false

peers:
  - 23.22.212.106:8080
EOF
    echo "Updated YAML config with BVN0"
else
    cat > "$CONFIG_FILE" << 'EOF'
{
  "known_nodes": [
    {
      "name": "BVN0",
      "address": "23.22.212.106:8080"
    }
  ],
  "bootstrap": {
    "enabled": false
  },
  "peers": [
    "23.22.212.106:8080"
  ]
}
EOF
    echo "Updated JSON config with BVN0"
fi

echo "Configuration updated at: $CONFIG_FILE"
echo "BVN0 added at 23.22.212.106:8080"

# Try to restart with accman
if command -v accman &> /dev/null; then
    echo "Restarting follower with accman..."
    accman restart app-follower 2>/dev/null || accman restart follower 2>/dev/null || echo "Please manually restart the follower"
else
    echo "Please manually restart the follower service"
fi
SCRIPT

# Copy script to darkwingdog and execute
echo "1. Copying update script to darkwingdog..."
scp /tmp/add-bvn0.sh paul@darkwingdog:/tmp/

echo "2. Executing on darkwingdog..."
ssh paul@darkwingdog "chmod +x /tmp/add-bvn0.sh && /tmp/add-bvn0.sh"

echo "3. Checking follower status..."
ssh paul@darkwingdog "accman status app-follower 2>/dev/null || accman status follower 2>/dev/null || echo 'Check follower status manually'"

# Clean up
rm /tmp/add-bvn0.sh

echo ""
echo "============================================"
echo "Update complete!"
echo "Follower should now connect to BVN0"
echo "============================================"