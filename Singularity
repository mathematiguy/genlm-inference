################# Header: Define the base system you want to use ################
# Reference of the kind of base you want to use (e.g., docker, debootstrap, shub).
Bootstrap: docker
# Select the docker image you want to use (Here we choose tensorflow)
From: nvidia/cuda:12.4.1-cudnn-devel-ubuntu22.04

# Environment variables that should be sourced at runtime.
%environment
    # use bash as default shell
    SHELL=/bin/bash
    PYTHONPATH="/pkg/surya:/pkg/code:$PYTHONPATH"
    export PATH="/opt/venv/bin:$PATH"
    export VIRTUAL_ENV="/opt/venv"
    export SHELL

# Add files at build time
%files
    requirements.txt
    pkg /code/pkg
    setup.py /code/setup.py
    submodules/surya /pkg/surya

%post
    echo "Setting environment variables"
    export DEBIAN_FRONTEND=noninteractive
    
    echo "Installing system packages"
    apt-get update
    apt-get install -y \
        curl \
        wget \
        unzip \
        software-properties-common \
        git \
        python3.11 \
        python3.11-dev \
        python3.11-venv \
        python3-apt \
        apt-utils
    
    # Install uv
    echo "Installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    cp /root/.local/bin/uv /usr/local/bin/uv
    chmod 755 /usr/local/bin/uv
    
    # Install yq without PPA (simpler)
    echo "Installing yq"
    wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64
    chmod +x /usr/local/bin/yq
    
    # Create virtual environment
    echo "Creating virtual environment"
    uv venv /opt/venv --python python3.11
    
    # Set PATH to use virtual environment first
    export PATH="/opt/venv/bin:$PATH"
    export VIRTUAL_ENV="/opt/venv"
    
    # Install pip in the virtual environment first
    echo "Installing pip in virtual environment"
    uv pip install pip
    
    # Make the virtual environment Python the system default
    echo "Setting virtual environment as default Python"
    update-alternatives --install /usr/bin/python python /opt/venv/bin/python 100
    update-alternatives --install /usr/bin/python3 python3 /opt/venv/bin/python 100
    update-alternatives --install /usr/bin/pip pip /opt/venv/bin/pip 100
    update-alternatives --install /usr/bin/pip3 pip3 /opt/venv/bin/pip 100
    
    # Install packages in virtual environment
    echo "Installing Python packages"
    uv pip install -r requirements.txt
    uv pip install -e /pkg/surya
    uv pip install -e /code
