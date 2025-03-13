#!/bin/sh
cd `dirname $0`

# Create a virtual environment to run our code
VENV_NAME="venv"
PYTHON="$VENV_NAME/bin/python"
ENV_ERROR="This module requires Python >=3.8, pip, and virtualenv to be installed."
LIBREOFFICE_CALC_ERROR="This module requires LibreOffice Calc for Excel formula recalculation."

# Check and create virtual environment
if ! python3 -m venv $VENV_NAME >/dev/null 2>&1; then
    echo "Failed to create virtualenv."
    if command -v apt-get >/dev/null; then
        echo "Detected Debian/Ubuntu, attempting to install python3-venv automatically."
        SUDO="sudo"
        if ! command -v $SUDO >/dev/null; then
            SUDO=""
        fi
        if ! apt info python3-venv >/dev/null 2>&1; then
            echo "Package info not found, trying apt update"
            $SUDO apt -qq update >/dev/null
        fi
        $SUDO apt install -qqy python3-venv >/dev/null 2>&1
        if ! python3 -m venv $VENV_NAME >/dev/null 2>&1; then
            echo $ENV_ERROR >&2
            exit 1
        fi
    else
        echo $ENV_ERROR >&2
        exit 1
    fi
fi

# Check and install LibreOffice Calc
if ! command -v libreoffice >/dev/null 2>&1; then
    echo "LibreOffice Calc not found, attempting to install."
    if command -v apt-get >/dev/null; then
        echo "Detected Debian/Ubuntu, installing LibreOffice Calc."
        SUDO="sudo"
        if ! command -v $SUDO >/dev/null; then
            SUDO=""
        fi
        # Ensure package list is up-to-date
        if ! apt info libreoffice-calc >/dev/null 2>&1; then
            echo "Package info not found, updating apt"
            $SUDO apt -qq update >/dev/null
        fi
        # Install LibreOffice Calc silently
        $SUDO apt install -qqy libreoffice-calc >/dev/null 2>&1
        if ! command -v libreoffice >/dev/null 2>&1; then
            echo $LIBREOFFICE_CALC_ERROR >&2
            exit 1
        else
            echo "LibreOffice Calc installed successfully."
        fi
    else
        echo $LIBREOFFICE_CALC_ERROR >&2
        echo "Please install LibreOffice Calc manually on your system." >&2
        exit 1
    fi
else
    echo "LibreOffice Calc already installed."
fi

# Install/upgrade Python packages
echo "Virtualenv found/created. Installing/upgrading Python packages..."
if ! [ -f .installed ]; then
    if ! $PYTHON -m pip install -r requirements.txt -Uqq; then
        exit 1
    else
        touch .installed
    fi
fi