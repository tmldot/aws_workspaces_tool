#!/usr/bin/env bash
# Setup script to ensure inputs/ and logs/ directories exist with placeholders

set -e

# Create directories if they don't exist
mkdir -p inputs logs

# Create README placeholders if missing
if [ ! -f inputs/README.md ]; then
  cat > inputs/README.md << 'EOF'
# inputs/

This directory is used for input data files.  
The contents are **ignored by Git** (see root `.gitignore`).  
Only this `README.md` is tracked to preserve the folder structure.
EOF
  echo "Created inputs/README.md"
fi

if [ ! -f logs/README.md ]; then
  cat > logs/README.md << 'EOF'
# logs/

This directory is used for log output.  
The contents are **ignored by Git** (see root `.gitignore`).  
Only this `README.md` is tracked to preserve the folder structure.
EOF
  echo "Created logs/README.md"
fi

echo "✅ Directory structure setup complete."

