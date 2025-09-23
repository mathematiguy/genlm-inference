# This Makefile automates routine tasks for this Singularity-based project.
IMAGE ?= container.sif
RUN ?= singularity exec $(FLAGS) $(IMAGE)
SINGULARITY_ARGS ?=
DVC_CACHE_DIR ?= $(shell dvc cache dir)
FLAGS ?= --nv -B $$(pwd):/code --pwd /code -B $(DVC_CACHE_DIR)
VENV_PATH ?= venv

include cluster/makefile

.PHONY: show_logs trigger scratch archive repro predict start jupyter container push shell

run:
	$(RUN) bash run.sh

jupyter:
	sudo singularity exec $(FLAGS) sandbox.sif jupyter lab \
		--ip=0.0.0.0 \
		--no-browser \
		--port 8888 \
    --allow-root

init:
	dvc init && mkdir -p .dvc/cache

# Builds a Singularity container from the Singularity definition file.
# Note: This command requires sudo privileges.
container: $(IMAGE)
$(IMAGE): Singularity requirements.txt
	sudo singularity build --force $(IMAGE) $(SINGULARITY_ARGS) Singularity

# Starts a shell within the Singularity container, with the virtual environment activated.
shell:
	singularity shell $(FLAGS) $(IMAGE) $(SINGULARITY_ARGS) bash

sandbox: sandbox.sif
sandbox.sif: $(IMAGE)
	sudo singularity build --force --sandbox sandbox.sif $(IMAGE)

sandbox-shell: sandbox.sif
	sudo singularity shell --writable sandbox.sif
