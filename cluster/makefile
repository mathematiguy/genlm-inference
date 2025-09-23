# This Makefile automates routine tasks for this Singularity-based project.
REPO_NAME := $(shell basename `git rev-parse --show-toplevel` | tr '[:upper:]' '[:lower:]')
IMAGE := container.sif
RUN ?= singularity exec $(FLAGS) -B $(DVC_CACHE_DIR) -B submodules/llama:/opt/llama $(IMAGE)
SINGULARITY_ARGS ?=
DVC_CACHE_DIR ?= $(shell dvc cache dir)
FLAGS ?=
VENV_PATH ?= venv
ARCHIVE ?= /network/archive/c/caleb.moses
SCRATCH ?= /network/scratch/c/caleb.moses

.PHONY: gpu_alloc show_logs set_secrets encrypt squeue trigger push dvc-setup

start_lab:
	mila serve lab --alloc -c 2 --mem=4G -t 2:00:00

start_local:
	salloc --gres=gpu:1 -c 16 --mem=32G -t 6:00:00

gpu_alloc:
	salloc --gres=gpu:1 -c 4 --mem=10G -t 1:00:00

show_logs:
	ls ${SCRATCH}/${REPO_NAME}/logs/*.log -t | head -n1 | xargs cat

set_secrets:
	bash -c "./cluster/set_secrets.sh && echo $$${GITHUB_TOKEN}"

encrypt: cluster/secrets.yaml.enc
cluster/secrets.yaml.enc: cluster/secrets.yaml
	openssl aes-256-cbc -a -salt -in $< -out $@ -pbkdf2

squeue:
	squeue -u caleb.moses

trigger:
	bash cluster/trigger.sh

SERVER ?= cn-f001
pull-cache:
	rsync -aHP --ignore-existing -e 'ssh -T -c aes128-gcm@openssh.com -o Compression=no' caleb.moses@$(SERVER).server.mila.quebec:/network/scratch/c/caleb.moses/nga-niupepa/dvc/* $$(dvc cache dir)

SERVER ?= cn-f001
push-cache:
	rsync -aHP --ignore-existing -e 'ssh -T -c aes128-gcm@openssh.com -o Compression=no' $$(dvc cache dir)/* caleb.moses@$(SERVER).server.mila.quebec:/network/scratch/c/caleb.moses/nga-niupepa/dvc

# Use this command to send the singularity container to a running remote session on the cluster
push: USER_NAME=caleb.moses
push: SERVER=cn-f001
push: OBJECT=$(IMAGE)
push: REMOTE=$(USER_NAME)@$(SERVER).server.mila.quebec
push: DEST=nga-niupepa/
push:
	rsync -ahP $(OBJECT) $(REMOTE):$(DEST)

dvc-setup: $(SCRATCH)/$(REPO_NAME)/logs \
	$(SCRATCH)/$(REPO_NAME)/dvc \
	$(ARCHIVE)/$(REPO_NAME)/dvc
	dvc config cache.type symlink
	dvc cache dir $(SCRATCH)/$(REPO_NAME)/dvc

$(SCRATCH)/$(REPO_NAME)/logs:
	mkdir -p $(SCRATCH)/$(REPO_NAME)/logs

$(SCRATCH)/$(REPO_NAME)/dvc:
	mkdir -p $(SCRATCH)/$(REPO_NAME)/dvc
	dvc cache dir --local $(SCRATCH)/$(REPO_NAME)/dvc

$(ARCHIVE)/$(REPO_NAME)/dvc:
	mkdir -p $(ARCHIVE)/$(REPO_NAME)/dvc
	dvc remote add -d archive $(ARCHIVE)/$(REPO_NAME)/dvc
