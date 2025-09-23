REPO_NAME := $(shell basename `git rev-parse --show-toplevel` | tr '[:upper:]' '[:lower:]')
DOCKER_REGISTRY := mathematiguy
<<<<<<< Updated upstream
IMAGE := container.sif
RUN ?= singularity exec ${FLAGS} ${IMAGE}
FLAGS ?= --nv  -B $$(pwd):/code --pwd /code
SINGULARITY_ARGS ?=

.PHONY: sandbox container shell root-shell docker docker-push docker-pull enter enter-root

JUPYTER_PORT := 8000
jupyter: $(IMAGE)
	singularity exec $(FLAGS) container.sif jupyter lab \
		--ip=0.0.0.0 \
		--no-browser \
=======
IMAGE := $(DOCKER_REGISTRY)/$(REPO_NAME)
HAS_DOCKER ?= $(shell which docker)
RUN ?= $(if $(HAS_DOCKER), docker run $(DOCKER_ARGS) --rm -v $$(pwd):/home/kaimahi/$(REPO_NAME) -w /home/kaimahi/$(REPO_NAME) -u $(UID):$(GID) $(IMAGE))
UID ?= kaimahi
GID ?= kaimahi
DOCKER_ARGS ?=

.PHONY: docker docker-push docker-pull enter enter-root

include cluster/makefile

JUPYTER_PASSWORD ?= jupyter
JUPYTER_PORT ?= 8888
.PHONY: jupyter
jupyter: DOCKER_ARGS=-u $(UID):$(GID) --rm -it -p $(JUPYTER_PORT):$(JUPYTER_PORT) -e NB_USER=$$USER -e NB_UID=$(UID) -e NB_GID=$(GID)
jupyter:
	$(RUN) jupyter lab \
>>>>>>> Stashed changes
		--port $(JUPYTER_PORT) \
		--allow-root \
		--notebook-dir=/code

# Use this command to send the singularity container to a running remote session on the cluster
push: USER_NAME=caleb.moses
push: SERVER=cn-f001
push: OBJECT=$(IMAGE)
push: REMOTE=$(USER_NAME)@$(SERVER).server.mila.quebec
push: DEST=${REPO_NAME}/
push:
	rsync -ahP $(OBJECT) $(REMOTE):$(DEST)

${REPO_NAME}_sandbox:
	singularity build --sandbox ${REPO_NAME}_sandbox ${IMAGE}

sandbox: ${REPO_NAME}_sandbox
	sudo singularity shell --writable ${REPO_NAME}_sandbox

container: ${IMAGE}
${IMAGE}: Singularity requirements.txt
	sudo singularity build ${IMAGE} ${SINGULARITY_ARGS} Singularity

shell:
	singularity shell ${FLAGS} ${IMAGE} ${SINGULARITY_ARGS}

root-shell:
	sudo singularity shell ${FLAGS} ${IMAGE} ${SINGULARITY_ARGS}
