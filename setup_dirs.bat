@echo off
REM Setup script to ensure inputs/ and logs/ directories exist with placeholders

REM Create directories if they don't exist
if not exist inputs (
    mkdir inputs
)
if not exist logs (
    mkdir logs
)

REM Create inputs/README.md if missing
if not exist inputs\README.md (
    echo # inputs/ > inputs\README.md
    echo. >> inputs\README.md
    echo This directory is used for input data files. >> inputs\README.md
    echo The contents are **ignored by Git** (see root `.gitignore`). >> inputs\README.md
    echo Only this `README.md` is tracked to preserve the folder structure. >> inputs\README.md
    echo Created inputs\README.md
)

REM Create logs/README.md if missing
if not exist logs\README.md (
    echo # logs/ > logs\README.md
    echo. >> logs\README.md
    echo This directory is used for log output. >> logs\README.md
    echo The contents are **ignored by Git** (see root `.gitignore`). >> logs\README.md
    echo Only this `README.md` is tracked to preserve the folder structure. >> logs\README.md
    echo Created logs\README.md
)

echo ✅ Directory structure setup complete.

