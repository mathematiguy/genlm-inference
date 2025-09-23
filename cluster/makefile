# This Makefile automates routine tasks for this Singularity-based project.
REPO_NAME := $(shell basename `git rev-parse --show-toplevel` | tr '[:upper:]' '[:lower:]')
IMAGE := container.sif
RUN ?= singularity exec $(FLAGS) -B $(DVC_CACHE_DIR) -B submodules/llama:/opt/llama $(IMAGE)
SINGULARITY_ARGS ?=
SLURM_ENV_VARS := SLURM_JOB_ID SLURM_JOB_NAME SLURM_JOB_PARTITION SLURM_JOB_QOS SLURM_JOB_ACCOUNT SLURM_JOB_USER SLURM_JOB_SUBMIT_TIME SLURM_JOB_START_TIME SLURM_JOB_TIME_LIMIT SLURM_JOB_NODELIST SLURM_JOB_NUM_NODES SLURM_NTASKS SLURM_CPUS_PER_TASK SLURM_CPUS_ON_NODE SLURM_MEM_PER_NODE SLURM_MEM_PER_CPU SLURM_GPUS SLURM_GPUS_PER_NODE SLURM_GPUS_PER_TASK SLURMD_NODENAME HOSTNAME
# Generate --env flags for each SLURM variable that exists
SLURM_ENV_FLAGS := $(foreach var,$(SLURM_ENV_VARS),--env $(var)="$($(var))")
CLUSTER_FLAGS ?= --nv \
    -B $$(pwd):/code --pwd /code \
    -B $(DVC_CACHE_DIR) \
    -B submodules/surya:/pkg/surya \
    -B submodules/reo-toolkit:/pkg/reo-toolkit \
    -B /network/scratch/c/caleb.moses/hf_cache \
    -B /network/weights \
    -B /cvmfs \
    --env PYTHONPATH=/pkg/code:/pkg/surya:/pkg/reo-toolkit:/code: \
    --env HF_HOME=/network/scratch/c/caleb.moses/hf_cache \
    --env MPLCONFIGDIR=/code/.matplotlib \
    --env NUMBA_CACHE_DIR=/code/.numba \
    --env HTTPSTAN_CACHE_DIR=/code/.httpstan_cache \
    --env TMPDIR=/code/.tmp \
    --env XDG_CACHE_HOME=/code \
    --env TF_CPP_MIN_LOG_LEVEL=1 \
    --env CUDA_VISIBLE_DEVICES=0 \
    --env TF_FORCE_GPU_ALLOW_GROWTH=true \
    --env CUDA_HOME=/cvmfs/ai.mila.quebec/apps/arch/common/cuda/12.4.1 \
    --env CUDA_PATH=/cvmfs/ai.mila.quebec/apps/arch/common/cuda/12.4.1 \
    --env PATH=/cvmfs/ai.mila.quebec/apps/arch/common/cuda/12.4.1/bin:/root/.local/bin:/opt/venv/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    --env LD_LIBRARY_PATH=/cvmfs/ai.mila.quebec/apps/arch/common/cuda/12.4.1/lib64:/usr/local/nvidia/lib:/usr/local/nvidia/lib64 \
    --env CUDA_CACHE_PATH=/code/.cuda_cache \
    --env FLASHINFER_JIT_DIR=/code/.flashinfer_cache \
    --env SGLANG_ATTENTION_BACKEND=triton \
    --env SGLANG_DISABLE_CUDA_GRAPH=1 \
    --env TORCH_COMPILE_DISABLE=1 \
    --env TORCHDYNAMO_DISABLE=1 \
    $(SLURM_ENV_FLAGS)
DVC_CACHE_DIR ?= $(shell dvc cache dir)
FLAGS ?=
VENV_PATH ?= venv
ARCHIVE ?= /network/archive/c/caleb.moses
SCRATCH ?= /network/scratch/c/caleb.moses
# Ensure we respect the CPU assignment for our cluster node
TASKSET_CPUS := $(shell grep Cpus_allowed_list /proc/self/status | awk '{print $$2}')

cluster-shell:
	@echo "Creating cache directories..."
	mkdir -p .httpstan_cache .tmp .matplotlib .numba .stan_cache
	@echo "Starting cluster shell with Stan cache fixes..."
	singularity exec $(CLUSTER_FLAGS) $(IMAGE) $(SINGULARITY_ARGS) bash -c "taskset -c $(TASKSET_CPUS) bash"

venv:
	python3 -m venv venv
	pip install dvc

monitor:
	bash cluster/monitor.sh

setup:
	source cluster/setup.sh

start_lab:
	mila serve lab --alloc -c 2 --mem=4G -t 2:00:00

start_local:
	salloc --partition main -c 6 --mem=32G -t 6:00:00

start_distributed:
	salloc --partition=long --nodes=1 --ntasks=1 --gpus-per-node=2 -c 8 --mem=80G -t 1:00:00

start_big_gpu:
	salloc --partition=long --nodes=1 --ntasks=1 --gres=gpu:80gb -c 8 --mem=80G -t 2:00:00

start_gpu:
	salloc --partition=main --nodes=1 --ntasks=1 --gpus-per-node=1 -c 8 --mem=48G -t 4:00:00

gpu_alloc:
	salloc --gres=gpu:1 -c 4 --mem=10G -t 1:00:00

JOB_ID ?= blank
show_logs:
	find ${SCRATCH}/lm-directed-beam-search-for-ocr/logs/jobs -type f -name '*.log' | grep $(JOB_ID) | xargs cat

list_logs:
	ls ${SCRATCH}/${REPO_NAME}/logs/*.log -t

set_secrets:
	bash -c "./cluster/set_secrets.sh && echo $$${GITHUB_TOKEN}"

encrypt: cluster/secrets.yaml.enc
cluster/secrets.yaml.enc: cluster/secrets.yaml
	openssl aes-256-cbc -a -salt -in $< -out $@ -pbkdf2

JUPYTER_PORT := 8000
jupyter-cluster: container.sif
	mkdir -p .tmp .cuda_cache .flashinfer_cache
	chmod 755 .tmp .cuda_cache .flashinfer_cache
	PATH="/cvmfs/ai.mila.quebec/apps/arch/common/cuda/12.4.1/bin:$$PATH" \
	LD_LIBRARY_PATH="/cvmfs/ai.mila.quebec/apps/arch/common/cuda/12.4.1/lib64:$$LD_LIBRARY_PATH" \
	singularity exec $(CLUSTER_FLAGS) container.sif jupyter lab \
		--ip=0.0.0.0 \
		--no-browser \
		--port $(JUPYTER_PORT) \
		--allow-root \
		--notebook-dir=/code

diagnose-cuda: container.sif
	singularity exec $(CLUSTER_FLAGS) container.sif bash -c " \
		echo '=== CUDA Libraries ===' && \
		find /usr -name '*cuda*' -type f 2>/dev/null | head -10 && \
		echo '=== cuDNN Libraries ===' && \
		find /usr -name '*cudnn*' -type f 2>/dev/null | head -10 && \
		echo '=== TensorFlow Location ===' && \
		python -c 'import tensorflow as tf; print(tf.__file__)' && \
		echo '=== LD_LIBRARY_PATH ===' && \
		echo \$$LD_LIBRARY_PATH && \
		echo '=== CUDA Version ===' && \
		nvcc --version 2>/dev/null || echo 'nvcc not found' && \
		echo '=== Available GPUs ===' && \
		nvidia-smi 2>/dev/null || echo 'nvidia-smi not found'"

cluster-shell:
	singularity exec $(CLUSTER_FLAGS) $(IMAGE) $(SINGULARITY_ARGS) bash -c "taskset -c $(TASKSET_CPUS) bash"

cluster-run:
	singularity exec $(CLUSTER_FLAGS) $(IMAGE) $(SINGULARITY_ARGS)  bash -c "taskset -c $(TASKSET_CPUS) bash run.sh"

squeue:
	squeue -u caleb.moses --format="%.8i %.8u %.9P %.30j %.2t %.10M %.10L %.6D %.6C %.10b %.20R" --sort=-M

sacct:
	sacct -u caleb.moses -S now-1week --format=JobID,User,Partition,JobName%30,State,Start,End,Elapsed

multi-trigger:
	bash cluster/multitrigger.sh

trigger:
	bash cluster/trigger.sh

SERVER ?= cn-f001
pull-cache:
	rsync -aHP --ignore-existing -e 'ssh -T -c aes128-gcm@openssh.com -o Compression=no' caleb.moses@$(SERVER).server.mila.quebec:/network/scratch/c/caleb.moses/${REPO_NAME}/dvc/* $$(dvc cache dir)

SERVER ?= cn-f001
push-cache:
	rsync -aHP --ignore-existing -e 'ssh -T -c aes128-gcm@openssh.com -o Compression=no' $$(dvc cache dir)/* caleb.moses@$(SERVER).server.mila.quebec:/network/scratch/c/caleb.moses/${REPO_NAME}/dvc

# Use this command to send the singularity container to a running remote session on the cluster
push: USER_NAME=caleb.moses
push: SERVER=cn-f001
push: OBJECT=$(IMAGE)
push: REMOTE=$(USER_NAME)@$(SERVER).server.mila.quebec
push: DEST=${REPO_NAME}/
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
