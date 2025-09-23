################# Header: Define the base system you want to use ################
# Reference of the kind of base you want to use (e.g., docker, debootstrap, shub).
Bootstrap: docker
# Select the docker image you want to use (Here we choose tensorflow)
From: jupyter/datascience-notebook

# Environment variables that should be sourced at runtime.
%environment
    # use bash as default shell
    SHELL=/bin/bash
    PYTHON_VERSION=3.11
    PATH="/pkg/code:$PATH" # ensure julia bin is in the path
    export SHELL PYTHON_VERSION PATH
    export JULIA_DEPOT_PATH="/tmp/:${JULIA_DEPOT_PATH}"

%files
    requirements.txt requirements.txt
    nga_niupepa /pkg/nga_niupepa
    setup.py /pkg

%post
    # Create the /code directory inside the container
    mkdir -p /code

    echo "Setting environment variables"
    export DEBIAN_FRONTEND=noninteractive

    echo "Installing Tools with apt-get"
    apt-get update
    apt-get install -y curl \
            wget \
            unzip \
            software-properties-common \
            git \
            entr
    apt-get clean

    # Install apt packages
    apt update
    apt install -y curl software-properties-common build-essential libgl1-mesa-glx rsync libopenjp2-7 python3-matplotlib libfreetype6-dev pkg-config
    add-apt-repository ppa:deadsnakes/ppa -y
    add-apt-repository ppa:rmescandon/yq -y

    # Install yq (yaml processing)
    apt install -y yq

    echo "Install basic requirements"
    pip${PYTHON_VERSION} install wheel pyct
    pip${PYTHON_VERSION} install --upgrade pip

    echo "Installing python requirements"
    pip${PYTHON_VERSION} install -r requirements.txt
    pip3 install -e /pkg
    # Install and precompile Julia packages
    julia -e 'using Pkg; Pkg.add("IJulia"); Pkg.add("PackageCompiler"); Pkg.add("DataFrames"); Pkg.add("EzXML"); Pkg.add("Arrow"); Pkg.add("JSON"); Pkg.add("Tables"), Pkg.add("ProgressMeter"); Pkg.add("ArgParse"); Pkg.add("Debugger"); Pkg.add("Parquet2");'

    chmod 755 -R /opt/julia