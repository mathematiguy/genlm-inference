#!/usr/bin/env python3
# submit_pipeline.py
"""
Simple pipeline job submission script.
Reads job configs from params.yaml and submits them via trigger.sh.
"""
import getpass
import os
import argparse
import subprocess
import sys
import yaml
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import fnmatch
import re


# Set up logging
logger = logging.getLogger(__name__)


@dataclass
class JobConfig:
    """Configuration for a single job."""

    stage: str
    step: str
    gpus: int = 1
    time_limit: str = "00:30:00"
    memory: str = "48g"
    cpus: int = 8
    partition: str = "long"
    nodes: int = 1
    ntasks: int = 1
    ntasks_per_node: int = 1


def format_time_duration(hours: float) -> str:
    """Format hours into a human-readable duration string."""
    if hours < 24:
        return f"{hours:.1f} hours"
    elif hours < 24 * 7:
        days = hours / 24
        return f"{days:.1f} days ({hours:.1f} hours)"
    elif hours < 24 * 7 * 52:
        weeks = hours / (24 * 7)
        days = hours / 24
        return f"{weeks:.1f} weeks ({days:.1f} days)"
    else:
        years = hours / (24 * 365)
        weeks = hours / (24 * 7)
        return f"{years:.1f} years ({weeks:.1f} weeks)"


def parse_time_limit(time_str: str) -> float:
    """Parse SLURM time format (HH:MM:SS or MM:SS) to hours as float."""
    try:
        parts = time_str.split(':')
        if len(parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = map(int, parts)
            return hours + minutes/60 + seconds/3600
        elif len(parts) == 2:  # MM:SS
            minutes, seconds = map(int, parts)
            return minutes/60 + seconds/3600
        elif len(parts) == 1:  # Just minutes
            return int(parts[0])/60
        else:
            logger.warning(f"Unknown time format: {time_str}, assuming 1 hour")
            return 1.0
    except (ValueError, AttributeError):
        logger.warning(f"Could not parse time limit '{time_str}', assuming 1 hour")
        return 1.0

def calculate_resource_totals(job_configs: Dict[str, JobConfig], jobs_to_run: Set[str]) -> Tuple[float, float]:
    """Calculate total GPU hours and CPU hours for the given jobs."""
    total_gpu_hours = 0.0
    total_cpu_hours = 0.0
    
    for job_key in jobs_to_run:
        if job_key in job_configs:
            config = job_configs[job_key]
            time_hours = parse_time_limit(config.time_limit)
            
            # Calculate resource hours
            gpu_hours = config.gpus * time_hours
            cpu_hours = config.cpus * time_hours
            
            total_gpu_hours += gpu_hours
            total_cpu_hours += cpu_hours
    
    return total_gpu_hours, total_cpu_hours


class PipelineSubmitter:
    """Simple pipeline job submitter."""
    
    def __init__(self, config_file: str = "params.yaml", dry_run: bool = False, run_group: str = None):
        self.config_file = Path(config_file)
        self.dry_run = dry_run
        self.user_run_group = run_group  # Store user-defined run group
        self.job_configs: Dict[str, JobConfig] = {}
        self.dependencies: Dict[str, List[str]] = {}
        self.submitted_jobs: Dict[str, str] = {}

    def load_job_configs(self):
        """Load job configurations from params.yaml."""
        logger.info("Loading job configurations...")

        with open(self.config_file, "r") as f:
            params = yaml.safe_load(f)

        stages = params.get("stages", {})
        for stage_name, stage_config in stages.items():
            for step_name, step_config in stage_config.items():
                job_key = f"{stage_name}:{step_name}"

                # Handle new format with resources section
                if "resources" in step_config:
                    resources = step_config["resources"]
                    self.job_configs[job_key] = JobConfig(
                        stage=stage_name,
                        step=step_name,
                        gpus=resources.get("gpus", 1),
                        time_limit=resources.get("time_limit", "00:30:00"),
                        memory=resources.get("memory", "48g"),
                        cpus=resources.get("cpus", 8),
                        partition=resources.get("partition", "long"),
                        nodes=resources.get("nodes", 1),
                        ntasks=resources.get("ntasks", 1),
                        ntasks_per_node=resources.get("ntasks_per_node", 1),
                    )
                else:
                    # Old format for backward compatibility
                    self.job_configs[job_key] = JobConfig(
                        stage=stage_name,
                        step=step_name,
                        gpus=step_config.get("gpus", 1),
                        time_limit=step_config.get("time_limit", "00:30:00"),
                        memory=step_config.get("memory", "48g"),
                        cpus=step_config.get("cpus", 8),
                        partition=step_config.get("partition", "long"),
                        nodes=step_config.get("nodes", 1),
                        ntasks=step_config.get("ntasks", 1),
                        ntasks_per_node=step_config.get("ntasks_per_node", 1),
                    )

        logger.info(f"Loaded {len(self.job_configs)} job configurations")

    def generate_run_group_id(self, user_group: str = None) -> str:
        """Generate a run group ID for this pipeline execution."""
        if hasattr(self, '_run_group_id'):
            return self._run_group_id
        
        if user_group:
            # User provided a meaningful name
            # Clean it up for git branch safety
            import re
            clean_group = re.sub(r'[^a-zA-Z0-9_.-]', '_', user_group)
            self._run_group_id = clean_group
            logger.info(f"Using user-defined run group: {self._run_group_id}")
        else:
            # Auto-generate using semantic versioning style: YYYYMMDD.HHMM.commit
            now = datetime.now()
            date_part = now.strftime("%Y%m%d")
            time_part = now.strftime("%H%M")
            
            try:
                result = subprocess.run(['git', 'rev-parse', '--short=6', 'HEAD'], 
                                      capture_output=True, text=True, check=True)
                commit_short = result.stdout.strip()
            except:
                commit_short = "unknown"
            
            # Semantic versioning style: date.time.commit
            self._run_group_id = f"{date_part}.{time_part}.{commit_short}"
            logger.info(f"Generated run group ID: {self._run_group_id}")
        
        return self._run_group_id
    
    def check_dvc_available(self):
        """Check if DVC is available and working. Fail fast if not."""
        try:
            result = subprocess.run(
                ["dvc", "--version"], 
                capture_output=True, 
                text=True, 
                check=True,
                timeout=10
            )
            logger.info(f"DVC available: {result.stdout.strip()}")
            return True
        except FileNotFoundError:
            logger.error("DVC command not found in PATH")
            logger.error("DVC is required for:")
            logger.error("  - Checking job dependencies")
            logger.error("  - Verifying which jobs need to run")
            logger.error("  - Ensuring pipeline integrity")
            logger.error("")
            logger.error("Please install DVC or activate your virtual environment:")
            logger.error("  pip install dvc")
            logger.error("  # or")
            logger.error("  source venv/bin/activate")
            return False
        except subprocess.CalledProcessError as e:
            logger.error(f"DVC command failed: {e}")
            logger.error("DVC may be corrupted or misconfigured")
            return False
        except subprocess.TimeoutExpired:
            logger.error("DVC command timed out")
            return False
        
    def load_dependencies_from_dvc(self):
        """Load dependencies from DVC using DOT format."""
        logger.info("Loading dependencies from DVC...")
    
        try:
            result = subprocess.run(
                ["dvc", "dag", "--dot"], capture_output=True, text=True, check=False
            )
    
            if result.returncode == 0 and result.stdout:
                logger.debug("Parsing DVC dependencies...")
                self._parse_dot_output(result.stdout)
                logger.info(
                    f"Loaded {len(self.dependencies)} dependency relationships from DVC"
                )
            else:
                logger.error(f"DVC dag --dot failed (return code {result.returncode})")
                if result.stderr:
                    logger.error(f"DVC error: {result.stderr.strip()}")
                raise RuntimeError("Failed to load DVC dependencies")
    
        except FileNotFoundError:
            raise RuntimeError(
                "DVC not found. DVC is required to safely determine job dependencies. "
                "Please install DVC or activate your virtual environment."
            )
    
    def load_secrets(self, password: str = None) -> Dict[str, str]:
        """Load secrets from encrypted file using OpenSSL AES (matching set_secrets.sh)."""
        secrets_env = {}
        
        if password is None:
            return secrets_env
        
        try:
            import yaml
            import subprocess
            
            # Look for the encrypted secrets file
            possible_paths = [
                Path("cluster/secrets.yaml.enc"),
                Path("secrets.yaml.enc"),
            ]
            
            secrets_file = None
            for path in possible_paths:
                if path.exists():
                    secrets_file = path
                    logger.debug(f"Found encrypted secrets file at: {secrets_file}")
                    break
            
            if secrets_file is None:
                logger.error("No encrypted secrets file found in any of these locations:")
                for path in possible_paths:
                    logger.error(f"  - {path.absolute()}")
                raise FileNotFoundError("secrets.yaml.enc not found")
            
            # Decrypt using OpenSSL (same method as set_secrets.sh)
            decrypt_cmd = [
                "openssl", "aes-256-cbc", "-d", "-a", "-pbkdf2",
                "-in", str(secrets_file),
                "-pass", f"pass:{password}"
            ]
            
            logger.debug("Decrypting secrets file...")
            result = subprocess.run(
                decrypt_cmd,
                capture_output=True,
                text=True,
                check=False  # Don't raise exception on non-zero exit
            )
            
            if result.returncode != 0:
                logger.error("Failed to decrypt secrets file")
                if result.stderr:
                    logger.error(f"OpenSSL error: {result.stderr.strip()}")
                raise ValueError("Decryption failed - check your password")
            
            # Parse the decrypted YAML
            try:
                secrets = yaml.safe_load(result.stdout)
                if not isinstance(secrets, dict):
                    raise ValueError("Decrypted content is not a valid YAML dictionary")
            except yaml.YAMLError as e:
                logger.error(f"Failed to parse decrypted YAML: {e}")
                raise ValueError("Invalid YAML format in decrypted content")
            
            # Extract tokens (same keys as set_secrets.sh)
            github_token = secrets.get('GITHUB_TOKEN', '').strip()
            hf_token = secrets.get('HF_TOKEN', '').strip()
            
            if not github_token:
                logger.error("GITHUB_TOKEN not found in secrets file")
                raise ValueError("GITHUB_TOKEN missing from secrets")
            
            if not hf_token:
                logger.warning("HF_TOKEN not found in secrets file")
                # Don't fail for missing HF_TOKEN, just warn
            
            # Build environment variables
            secrets_env['GITHUB_TOKEN'] = github_token
            if hf_token:
                secrets_env['HF_TOKEN'] = hf_token
            
            logger.info(f"Successfully loaded {len(secrets_env)} secrets")
            return secrets_env
            
        except subprocess.CalledProcessError as e:
            logger.error(f"OpenSSL decryption failed: {e}")
            raise ValueError("Failed to decrypt secrets file")
        except Exception as e:
            logger.error(f"Failed to load secrets: {e}")
            raise
    
    def prompt_for_secrets_password(self) -> str:
        """Prompt user for secrets password if needed."""
        # Check if GITHUB_TOKEN is already set
        if os.environ.get('GITHUB_TOKEN'):
            logger.info("GITHUB_TOKEN already set in environment")
            return None
        
        # Check if encrypted secrets file exists
        possible_paths = [
            Path("cluster/secrets.yaml.enc"),
            Path("secrets.yaml.enc"),
        ]
        
        secrets_file = None
        for path in possible_paths:
            if path.exists():
                secrets_file = path
                break
        
        if secrets_file is None:
            logger.error("No encrypted secrets file found in any of these locations:")
            for path in possible_paths:
                logger.error(f"  - {path.absolute()}")
            
            # Ask user what to do
            logger.info("\nNo secrets.yaml.enc file found, but GITHUB_TOKEN is required for jobs.")
            logger.info("Options:")
            logger.info("1. Set GITHUB_TOKEN environment variable manually")
            logger.info("2. Locate the secrets.yaml.enc file") 
            logger.info("3. Continue without token (jobs will likely fail)")
            
            response = input("Choose option (1/2/3): ").strip()
            
            if response == "1":
                token = getpass.getpass("Enter GITHUB_TOKEN manually: ")
                if token.strip():
                    os.environ['GITHUB_TOKEN'] = token.strip()
                    logger.info("GITHUB_TOKEN set manually")
                return None
            elif response == "2":
                custom_path = input("Enter path to secrets.yaml.enc file: ").strip()
                if Path(custom_path).exists():
                    logger.info(f"Found secrets file at: {custom_path}")
                    # Continue to prompt for password
                    secrets_file = Path(custom_path)
                else:
                    logger.error(f"File not found: {custom_path}")
                    raise FileNotFoundError(f"Secrets file not found: {custom_path}")
            else:
                logger.warning("Continuing without secrets - jobs will likely fail")
                return None
        
        logger.info(f"\nFound encrypted secrets file: {secrets_file}")
        logger.info("This file contains encrypted tokens needed for job execution.")
        
        password = getpass.getpass("Provide the password to your secrets.yaml file: ")
        if not password.strip():
            logger.error("Empty password provided")
            raise ValueError("Password cannot be empty")
        
        return password.strip()

    def _parse_dot_output(self, dot_text: str):
        """Parse DOT format output to extract dependencies."""
        import re

        dependencies_found = 0

        # Look for edges like: "stage_name" -> "other_stage"
        for line in dot_text.split("\n"):
            # Match quoted node names with arrows
            match = re.search(r'"([^"]+)"\s*->\s*"([^"]+)"', line)
            if match:
                dependency, dependent = match.groups()

                # Try to match DVC nodes to our job keys
                dep_jobs = self._find_matching_jobs(dependency)
                dependent_jobs = self._find_matching_jobs(dependent)

                # Create dependencies between all matching combinations
                for dep_job in dep_jobs:
                    for dependent_job in dependent_jobs:
                        if dependent_job not in self.dependencies:
                            self.dependencies[dependent_job] = []
                        if dep_job not in self.dependencies[dependent_job]:
                            self.dependencies[dependent_job].append(dep_job)
                            dependencies_found += 1
                            logger.debug(
                                f"Dependency: {dependent_job} depends on {dep_job}"
                            )

        logger.debug(f"Found {dependencies_found} valid dependencies from DOT format")

    def _find_matching_jobs(self, dvc_node: str) -> List[str]:
        """Find all jobs that match the DVC node name (handles stages without targets)."""
        # Clean the node name (remove quotes, spaces)
        cleaned = dvc_node.strip().strip("\"'")
        matches = []

        # Direct match
        if cleaned in self.job_configs:
            return [cleaned]

        # Try stage@step -> stage:step conversion (most common case)
        if "@" in cleaned:
            converted = cleaned.replace("@", ":")
            if converted in self.job_configs:
                return [converted]

        # Try stage_step -> stage:step conversion
        if "_" in cleaned and ":" not in cleaned:
            for job_key in self.job_configs:
                if job_key.replace(":", "_") == cleaned:
                    matches.append(job_key)

        # If no @ symbol, this might be a stage without a target
        # Find all jobs that belong to this stage
        if "@" not in cleaned and ":" not in cleaned:
            stage_matches = []
            for job_key in self.job_configs:
                stage, step = job_key.split(":")
                if stage == cleaned:
                    stage_matches.append(job_key)
            
            if stage_matches:
                logger.debug(f"DVC stage '{cleaned}' mapped to {len(stage_matches)} jobs: {stage_matches}")
                return stage_matches

        # Look for partial matches by stage or step name
        for job_key in self.job_configs:
            stage, step = job_key.split(":")
            if (
                cleaned == stage
                or cleaned == step
                or cleaned == f"{stage}_{step}"
                or cleaned.endswith(step)
                or step in cleaned
            ):
                matches.append(job_key)

        # Remove duplicates while preserving order
        seen = set()
        unique_matches = []
        for match in matches:
            if match not in seen:
                seen.add(match)
                unique_matches.append(match)

        return unique_matches
    
    def check_job_status(self) -> Dict[str, str]:
        """Check which jobs need to be run vs are already up-to-date."""
        logger.info("Checking job status with DVC (this may take a few minutes)...")
    
        try:
            import tempfile
            import os
    
            # Create a temporary file for JSON output
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".json", delete=False
            ) as temp_file:
                temp_filename = temp_file.name
    
            try:
                # Run DVC status with JSON output redirected to temp file
                logger.info("Running DVC status check...")
                logger.info("-" * 50)
    
                with open(temp_filename, "w") as json_file:
                    result = subprocess.run(
                        ["dvc", "status", "--json"],
                        stdout=json_file,  # JSON goes to file
                        stderr=None,  # Progress bars go to terminal
                        check=False,
                    )
    
                logger.info("-" * 50)
                logger.info("DVC status completed")
    
                if result.returncode != 0:
                    logger.error("DVC status failed")
                    raise RuntimeError("DVC status check failed - cannot safely determine which jobs to run")

                # Read the JSON from the temp file
                logger.debug("Reading status results...")

                with open(temp_filename, "r") as json_file:
                    json_content = json_file.read()

                if not json_content.strip():
                    logger.info("All stages are up-to-date according to DVC")
                    return {job_key: "cached" for job_key in self.job_configs.keys()}
                else:
                    logger.debug("Parsing DVC status results...")
                    return self._parse_dvc_status_json(json_content)
    
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_filename)
                except OSError:
                    pass
        except FileNotFoundError:
            raise RuntimeError(
                "DVC not found. DVC is required to safely check which jobs need to run. "
                "Please install DVC or activate your virtual environment."
            )
        except KeyboardInterrupt:
            logger.info("DVC status interrupted by user")
            raise
        except Exception as e:
            logger.error(f"Error running DVC status: {e}")
            logger.warning("Assuming all jobs need to run")
            return {job_key: "needs_run" for job_key in self.job_configs.keys()}

    def _parse_dvc_status_json(self, status_output: str) -> Dict[str, str]:
        """Parse dvc status --json output to determine which jobs need to run."""
        import json

        # The output might have progress messages before the JSON
        # Try to find the JSON part - look for the opening brace
        lines = status_output.split("\n")
        json_start = -1

        for i, line in enumerate(lines):
            if line.strip().startswith("{"):
                json_start = i
                break

        if json_start >= 0:
            json_lines = lines[json_start:]
            json_text = "\n".join(json_lines)
        else:
            json_text = status_output

        try:
            status_data = json.loads(json_text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse DVC status JSON: {e}")
            logger.debug("This might be due to very large output or incomplete JSON.")
            logger.debug(f"Raw output (last 300 chars): {repr(status_output[-300:])}")

            # Try to extract stage names from the raw output as fallback
            logger.debug("Attempting fallback parsing...")
            return self._fallback_parse_dvc_output(status_output)

        job_status = {}
        changed_dvc_stages = set(status_data.keys())

        logger.debug(f"DVC reports {len(changed_dvc_stages)} changed stages")

        # Convert DVC stage names to our job keys and mark as changed
        changed_job_keys = set()
        for dvc_stage in changed_dvc_stages:
            # Convert stage@step to stage:step or handle stage-only cases
            job_keys = self._dvc_stage_to_job_keys(dvc_stage)
            for job_key in job_keys:
                changed_job_keys.add(job_key)
                logger.debug(f"DVC stage mapping: {dvc_stage} -> {job_key} (needs run)")

        # Mark all jobs as cached or needs_run
        for job_key in self.job_configs.keys():
            if job_key in changed_job_keys:
                job_status[job_key] = "needs_run"
            else:
                job_status[job_key] = "cached"

        changed_count = len(changed_job_keys)
        cached_count = len(self.job_configs) - changed_count

        logger.info(
            f"Mapped to {changed_count} jobs that need to run, {cached_count} are cached"
        )
        return job_status

    def _fallback_parse_dvc_output(self, status_output: str) -> Dict[str, str]:
        """Fallback parsing when JSON fails - extract stage names from raw output."""
        import re

        # Look for stage names in various formats
        stage_patterns = [
            r'"([^"]+@[^"]+)":', # stage@step format
            r'"([^"]+)":', # stage-only format
        ]
        changed_stages = set()

        for pattern in stage_patterns:
            for match in re.finditer(pattern, status_output):
                stage_name = match.group(1)
                changed_stages.add(stage_name)

        logger.debug(f"Fallback parsing found {len(changed_stages)} changed stages")

        # Convert to job keys
        job_status = {}
        changed_job_keys = set()

        for dvc_stage in changed_stages:
            job_keys = self._dvc_stage_to_job_keys(dvc_stage)
            for job_key in job_keys:
                changed_job_keys.add(job_key)

        # Mark all jobs
        for job_key in self.job_configs.keys():
            if job_key in changed_job_keys:
                job_status[job_key] = "needs_run"
            else:
                job_status[job_key] = "cached"

        changed_count = len(changed_job_keys)
        cached_count = len(self.job_configs) - changed_count

        logger.debug(
            f"Fallback mapped to {changed_count} jobs that need to run, {cached_count} are cached"
        )
        return job_status

    def _dvc_stage_to_job_keys(self, dvc_stage: str) -> List[str]:
        """Convert DVC stage name to job key format(s). Handles stages without targets."""
        job_keys = []
        
        # DVC stages use stage@step format, we use stage:step
        if "@" in dvc_stage:
            job_key = dvc_stage.replace("@", ":")
            if job_key in self.job_configs:
                job_keys.append(job_key)
        else:
            # No @ symbol - this might be a stage without a target
            # Find all jobs that belong to this stage
            for job_key in self.job_configs.keys():
                stage, step = job_key.split(":")
                if dvc_stage == stage or dvc_stage == step:
                    job_keys.append(job_key)
        
        return job_keys

    def get_execution_order(self, jobs_to_include: Set[str] = None) -> List[List[str]]:
        """Calculate execution order using topological sort."""
        # Use specified jobs or all jobs
        if jobs_to_include is None:
            all_jobs = set(self.job_configs.keys())
        else:
            all_jobs = jobs_to_include

        # Calculate in-degrees (how many dependencies each job has)
        in_degree = {job: 0 for job in all_jobs}

        for job, deps in self.dependencies.items():
            if job in all_jobs:
                # Only count dependencies that are also in our job set
                valid_deps = [dep for dep in deps if dep in all_jobs]
                in_degree[job] = len(valid_deps)

        # Start with jobs that have no dependencies
        queue = deque([job for job in all_jobs if in_degree[job] == 0])
        execution_order = []

        while queue:
            # All jobs in current batch can run in parallel
            current_batch = list(queue)
            queue.clear()
            execution_order.append(current_batch)

            # Remove completed jobs and update dependents
            for completed_job in current_batch:
                # Find jobs that depend on this completed job
                for job, deps in self.dependencies.items():
                    if job in all_jobs and completed_job in deps:
                        in_degree[job] -= 1
                        if in_degree[job] == 0:
                            queue.append(job)

        return execution_order

    def get_target_jobs(self, target_pattern: str) -> Set[str]:
        """Get target jobs matching a pattern and all their dependencies."""
        # First, find matching jobs using pattern matching
        matching_jobs = self._find_target_jobs_by_pattern(target_pattern)
        
        if not matching_jobs:
            available_jobs = list(self.job_configs.keys())[:20]  # Show first 20
            available_stages = set()
            for job_key in self.job_configs.keys():
                stage, _ = job_key.split(":")
                available_stages.add(stage)
            
            raise ValueError(
                f"Pattern '{target_pattern}' matched no jobs. "
                f"Available job keys include: {available_jobs[:10]}... "
                f"Available stages include: {sorted(list(available_stages))[:10]}..."
            )

        # Find all dependencies recursively for all matching jobs
        target_jobs = set()

        def add_dependencies(job_key: str):
            """Recursively add a job and all its dependencies."""
            if job_key in target_jobs:
                return  # Already processed

            target_jobs.add(job_key)

            # Add all dependencies of this job
            deps = self.dependencies.get(job_key, [])
            for dep in deps:
                if dep in self.job_configs:  # Make sure dependency exists
                    add_dependencies(dep)

        # Add dependencies for all matching jobs
        for job_key in matching_jobs:
            add_dependencies(job_key)

        if len(matching_jobs) == 1:
            logger.info(
                f"Pattern '{target_pattern}' matched 1 job, "
                f"requiring {len(target_jobs)} jobs total (including dependencies)"
            )
        else:
            logger.info(
                f"Pattern '{target_pattern}' matched {len(matching_jobs)} jobs, "
                f"requiring {len(target_jobs)} total jobs (including dependencies)"
            )

        return target_jobs

    def _find_target_jobs_by_pattern(self, pattern: str) -> List[str]:
        """Find jobs that match the target pattern (supports wildcards and regex)."""
        matching_jobs = []
        
        # Check for exact job key match first
        if pattern in self.job_configs:
            return [pattern]
        
        # Check if pattern contains wildcards or regex characters
        has_wildcards = any(char in pattern for char in ['*', '?', '[', ']'])
        has_regex = any(char in pattern for char in ['^', '$', '(', ')', '|', '+', '{', '}'])
        
        if has_wildcards:
            # Use fnmatch for shell-style wildcards
            logger.debug(f"Using wildcard matching for pattern: {pattern}")
            for job_key in self.job_configs.keys():
                if fnmatch.fnmatch(job_key, pattern):
                    matching_jobs.append(job_key)
                    logger.debug(f"  Matched: {job_key}")
        elif has_regex:
            # Use regex matching
            logger.debug(f"Using regex matching for pattern: {pattern}")
            try:
                regex_pattern = re.compile(pattern)
                for job_key in self.job_configs.keys():
                    if regex_pattern.search(job_key):
                        matching_jobs.append(job_key)
                        logger.debug(f"  Matched: {job_key}")
            except re.error as e:
                logger.error(f"Invalid regex pattern '{pattern}': {e}")
                raise ValueError(f"Invalid regex pattern: {e}")
        else:
            # No wildcards or regex, try exact matching approaches
            # Check if it's a stage name (find all jobs in that stage)
            for job_key in self.job_configs.keys():
                stage, step = job_key.split(":")
                if stage == pattern:
                    matching_jobs.append(job_key)
            
            # If no stage matches, try substring matching
            if not matching_jobs:
                for job_key in self.job_configs.keys():
                    if pattern in job_key:
                        matching_jobs.append(job_key)
        
        # Sort for consistent output
        matching_jobs.sort()
        return matching_jobs

    def filter_jobs_by_status(
        self, include_cached: bool = False, target_pattern: str = None
    ) -> Set[str]:
        """Get the set of jobs that should be submitted."""
        job_status = self.check_job_status()

        # If target specified, filter to target and its dependencies first
        if target_pattern:
            target_jobs = self.get_target_jobs(target_pattern)
            logger.info(f"Filtering to target pattern '{target_pattern}' and dependencies")
        else:
            target_jobs = set(self.job_configs.keys())

        if include_cached:
            return target_jobs

        # Start with jobs that need to run (within our target set)
        jobs_to_run = {
            job_key
            for job_key, status in job_status.items()
            if status == "needs_run" and job_key in target_jobs
        }

        # Add jobs that depend on changed jobs (cascade effect, but only within target set)
        jobs_with_deps = set(jobs_to_run)

        def add_dependents(changed_job: str):
            """Recursively add jobs that depend on changed jobs."""
            for job_key, deps in self.dependencies.items():
                if (
                    changed_job in deps
                    and job_key not in jobs_with_deps
                    and job_key in target_jobs
                ):  # Only add if in target set
                    jobs_with_deps.add(job_key)
                    add_dependents(job_key)

        # Add all dependent jobs within target set
        for job in list(jobs_to_run):
            add_dependents(job)

        deps_added = len(jobs_with_deps) - len(jobs_to_run)
        if deps_added > 0:
            logger.info(f"Added {deps_added} jobs due to dependency cascading")

        return jobs_with_deps

    def preview_execution_plan(self, jobs_to_run: Set[str] = None):
        """Show what will be executed."""
        if jobs_to_run is None:
            jobs_to_run = set(self.job_configs.keys())

        execution_order = self.get_execution_order(jobs_to_run)

        logger.info("\n" + "=" * 60)
        logger.info("EXECUTION PLAN")
        logger.info("=" * 60)

        total_jobs = len(self.job_configs)
        jobs_to_submit = len(jobs_to_run)
        jobs_cached = total_jobs - jobs_to_submit

        if jobs_cached > 0:
            logger.info(
                f"STATUS: {jobs_to_submit} jobs to run, {jobs_cached} cached (will be skipped)"
            )
        else:
            logger.info(f"STATUS: Running all {jobs_to_submit} jobs")

        # Calculate and display resource totals
        total_gpu_hours, total_cpu_hours = calculate_resource_totals(self.job_configs, jobs_to_run)
        logger.info(f"RESOURCE REQUIREMENTS:")
        logger.info(f"  Total GPU hours: {total_gpu_hours:.1f}")
        logger.info(f"  Total CPU hours: {total_cpu_hours:.1f}")
        
        # Calculate cost estimates (adjust these rates for your cluster)
        gpu_cost_per_hour = 2.50  # Example: $2.50 per GPU hour
        cpu_cost_per_hour = 0.10  # Example: $0.10 per CPU hour
        estimated_cost = (total_gpu_hours * gpu_cost_per_hour) + (total_cpu_hours * cpu_cost_per_hour)
        logger.info(f"  Estimated cost: ${estimated_cost:.2f} (GPU: ${total_gpu_hours * gpu_cost_per_hour:.2f}, CPU: ${total_cpu_hours * cpu_cost_per_hour:.2f})")

        # Show skipped jobs in dry run mode
        if self.dry_run and jobs_cached > 0:
            skipped_jobs = set(self.job_configs.keys()) - jobs_to_run
            logger.info(f"\nSKIPPED JOBS (cached, {len(skipped_jobs)} total):")
            logger.info("-" * 40)

            # Group skipped jobs by stage
            skipped_by_stage = {}
            for job_key in skipped_jobs:
                stage, step = job_key.split(":")
                if stage not in skipped_by_stage:
                    skipped_by_stage[stage] = []
                skipped_by_stage[stage].append(step)

            for stage, steps in skipped_by_stage.items():
                logger.info(f"  {stage}: {len(steps)} jobs")
                if len(steps) <= 5:
                    for step in steps:
                        logger.info(f"    ✓ {step}")
                else:
                    for step in steps[:3]:
                        logger.info(f"    ✓ {step}")
                    logger.info(f"    ... and {len(steps)-3} more")

        if not execution_order:
            logger.info("\nNo jobs need to be run - everything is up to date!")
            return

        logger.info(f"\nJOBS TO RUN:")
        for i, batch in enumerate(execution_order, 1):
            # Calculate resources for this batch
            batch_gpu_total = 0
            batch_cpu_total = 0
            batch_max_time = 0
            
            for job_key in batch:
                config = self.job_configs[job_key]
                time_hours = parse_time_limit(config.time_limit)
                batch_gpu_total += config.gpus
                batch_cpu_total += config.cpus
                batch_max_time = max(batch_max_time, time_hours)
            
            logger.info(f"\nBATCH {i} ({len(batch)} jobs in parallel):")
            logger.info(f"  Peak resources: {batch_gpu_total} GPUs, {batch_cpu_total} CPUs")
            logger.info(f"  Max duration: {batch_max_time:.1f}h")
            logger.info("-" * 40)

            for job_key in batch:
                config = self.job_configs[job_key]
                deps = self.dependencies.get(job_key, [])
                time_hours = parse_time_limit(config.time_limit)
                job_gpu_hours = config.gpus * time_hours
                job_cpu_hours = config.cpus * time_hours

                logger.info(f"  {config.stage}@{config.step}")
                logger.info(f"    GPUs: {config.gpus}, Time: {config.time_limit} ({job_gpu_hours:.1f} GPU-h)")
                logger.info(f"    Memory: {config.memory}, CPUs: {config.cpus} ({job_cpu_hours:.1f} CPU-h)")
                if deps:
                    # Only show dependencies that are also being run
                    running_deps = [dep for dep in deps if dep in jobs_to_run]
                    if running_deps:
                        logger.info(f"    Depends on: {', '.join(running_deps)}")

        total_to_run = sum(len(batch) for batch in execution_order)
        logger.info(f"\nSUMMARY: {total_to_run} jobs in {len(execution_order)} batches")
        logger.info(f"TOTAL RESOURCES: {format_time_duration(total_gpu_hours)} GPU hours, {format_time_duration(total_cpu_hours)} CPU hours")
    
    def submit_job(self, job_key: str, extra_env: Dict[str, str] = None, jobs_to_run: Set[str] = None) -> str:
        """Submit a single job and return job ID."""
        config = self.job_configs[job_key]

        if self.dry_run:
            logger.info(f"DRY RUN: Would submit {config.stage}@{config.step}")
            return f"fake_{len(self.submitted_jobs)}"

        # Generate run group ID for this pipeline run
        run_group_id = self.generate_run_group_id(self.user_run_group)

        # Build dependency string - ONLY include dependencies that are being submitted in this run
        dependency_args = []
        deps = self.dependencies.get(job_key, [])
        if deps and jobs_to_run is not None:
            # Filter dependencies to only those being submitted in this run
            active_deps = [dep for dep in deps if dep in jobs_to_run and dep in self.submitted_jobs]
            if active_deps:
                dep_job_ids = [self.submitted_jobs[dep] for dep in active_deps]
                dependency_args = ["--dependency", f"afterok:{':'.join(dep_job_ids)}"]
                logger.debug(f"Job {job_key} depends on running jobs: {active_deps}")
            else:
                # Log that we're skipping dependencies (they're cached or not in scope)
                skipped_deps = [dep for dep in deps if dep not in jobs_to_run or dep not in self.submitted_jobs]
                if skipped_deps:
                    logger.debug(f"Job {job_key} skipping dependencies (cached/not running): {skipped_deps}")

        # Build trigger.sh command with run group
        cmd = [
            "bash",
            "cluster/trigger.sh",
            "--stage",
            f"{config.stage}@{config.step}",
            "--run-group",
            run_group_id,
            "--gpus",
            str(config.gpus),
            "--time",
            config.time_limit,
            "--memory",
            config.memory,
            "--cpus",
            str(config.cpus),
            "--partition",
            config.partition,
            "--nodes",
            str(config.nodes),
            "--ntasks",
            str(config.ntasks),
            "--ntasks-per-node",
            str(config.ntasks_per_node),
        ] + dependency_args

        logger.info(f"Submitting {config.stage}@{config.step}...")
        
        # DEBUG: Always log the exact command that will be run
        cmd_string = ' '.join(cmd)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("=" * 60)
            logger.debug("Executing:")
            logger.debug(cmd_string)
            logger.debug("=" * 60)

        # Prepare environment
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
            # Don't log the actual values for security
            logger.debug(f"Added {len(extra_env)} environment variables")

        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=True,
                env=env  # Pass the enhanced environment
            )

            # Extract job ID from output
            import re

            match = re.search(r"Submitted batch job (\d+)", result.stdout)
            if match:
                job_id = match.group(1)
                logger.info(f"  -> Job ID: {job_id}")
                return job_id
            else:
                logger.warning(f"  -> Warning: Could not extract job ID")
                logger.debug(f"  -> stdout: {result.stdout}")
                return f"unknown_{len(self.submitted_jobs)}"

        except subprocess.CalledProcessError as e:
            logger.error(f"  -> ERROR: trigger.sh failed with exit code {e.returncode}")
            if e.stdout:
                logger.error(f"  -> stdout: {e.stdout.strip()}")
            if e.stderr:
                logger.error(f"  -> stderr: {e.stderr.strip()}")
            logger.error(f"  -> Command: {cmd_string}")

            # Ask user what to do
            response = (
                input(f"\nJob submission failed. Continue with remaining jobs? (y/N): ")
                .strip()
                .lower()
            )
            if response not in ["y", "yes"]:
                logger.info("Stopping job submission.")
                raise
            else:
                logger.info("Continuing with remaining jobs...")
                return f"failed_{len(self.submitted_jobs)}"
    
    def run_pipeline(self, target_pattern: str = None):
        """Execute the full pipeline."""
        
        # CRITICAL: Check DVC availability first
        if not self.check_dvc_available():
            logger.error("Aborting pipeline submission due to missing DVC")
            sys.exit(1)
        
        # Load everything (only once)
        if not self.job_configs:
            self.load_job_configs()
            
        self.load_dependencies_from_dvc()  # Will now fail fast if DVC unavailable
    
        # Handle secrets loading
        secrets_password = self.prompt_for_secrets_password()
        secrets_env = self.load_secrets(secrets_password)
        
        # Clear password from memory immediately
        if secrets_password:
            secrets_password = None
            del secrets_password
    
        # Check which jobs need to run
        jobs_to_run = self.filter_jobs_by_status(
            include_cached=False, target_pattern=target_pattern
        )
    
        if not jobs_to_run:
            if target_pattern:
                logger.info(
                    f"Target pattern '{target_pattern}' and its dependencies are all up-to-date!"
                )
            else:
                logger.info("All jobs are up-to-date! Nothing to submit.")
            return True
    
        # Show plan
        self.preview_execution_plan(jobs_to_run)
    
        # Get confirmation
        if not self.dry_run:
            response = input("\nSubmit these jobs? (y/N): ").strip().lower()
            if response not in ["y", "yes"]:
                logger.info("Cancelled.")
                return False
    
        # Execute in order
        execution_order = self.get_execution_order(jobs_to_run)
        submitted_count = 0
        failed_count = 0
    
        for i, batch in enumerate(execution_order, 1):
            logger.info(f"{'='*40}")
            logger.info(f"EXECUTING BATCH {i}")
            logger.info("=" * 40)
    
            batch_failures = 0
            for job_key in batch:
                try:
                    # Pass secrets environment AND jobs_to_run to each job
                    job_id = self.submit_job(job_key, extra_env=secrets_env, jobs_to_run=jobs_to_run)
                    if job_id.startswith("failed_"):
                        failed_count += 1
                        batch_failures += 1
                    else:
                        self.submitted_jobs[job_key] = job_id
                        submitted_count += 1
                except KeyboardInterrupt:
                    logger.info("Job submission interrupted by user")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error submitting {job_key}: {e}")
                    failed_count += 1
                    batch_failures += 1
    
                    response = (
                        input(f"\nUnexpected error occurred. Continue? (y/N): ")
                        .strip()
                        .lower()
                    )
                    if response not in ["y", "yes"]:
                        logger.info("Stopping job submission due to error.")
                        break
    
            # If too many failures in this batch, ask if we should continue
            if batch_failures > len(batch) // 2:
                logger.warning(
                    f"More than half of batch {i} failed ({batch_failures}/{len(batch)})"
                )
                response = (
                    input(
                        f"\nMany jobs failed in this batch. Continue to next batch? (y/N): "
                    )
                    .strip()
                    .lower()
                )
                if response not in ["y", "yes"]:
                    logger.info("Stopping pipeline execution due to batch failures.")
                    break
    
        # Clear secrets from memory
        if secrets_env:
            secrets_env.clear()
    
        # Summary
        total_attempted = submitted_count + failed_count
        if failed_count > 0:
            logger.warning(
                f"Pipeline completed with issues: {submitted_count} jobs submitted, {failed_count} failed"
            )
        else:
            logger.info(
                f"Pipeline completed successfully: {submitted_count} jobs submitted"
            )
    
        return failed_count == 0


def setup_logging(log_level: str = "INFO"):
    """Set up logging configuration."""
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure logging
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    # Make DVC and other external libraries less verbose unless debug
    if numeric_level > logging.DEBUG:
        logging.getLogger("dvc").setLevel(logging.WARNING)
        logging.getLogger("subprocess").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description="Submit pipeline jobs")
    parser.add_argument("--config", default="params.yaml", help="Config file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument(
        "--run-group", 
        help="Custom run group name (e.g., 'experiment_v2', 'baseline_test'). "
             "If not provided, uses semantic versioning: YYYYMMDD.HHMM.commit"
    )
    parser.add_argument(
        "--debug-dvc", action="store_true", help="Show DVC debug output"
    )
    parser.add_argument(
        "--force-all", action="store_true", help="Run all jobs even if cached"
    )
    parser.add_argument(
        "--check-status",
        action="store_true",
        help="Only check job status, don't submit",
    )
    parser.add_argument(
        "--target", 
        help="Target job pattern to run (includes dependencies). "
             "Supports wildcards (*,?) and regex. Examples: "
             "'stage:target_*', 'train:*', 'eval.*', '^preprocess:.*'"
    )
    parser.add_argument(
        "--test-trigger",
        action="store_true",
        help="Test trigger.sh with a simple job to verify it works",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Set up logging
    setup_logging(args.log_level)

    # Pass run group to submitter
    submitter = PipelineSubmitter(args.config, args.dry_run, args.run_group)

    if args.debug_dvc:
        # Show DVC debug info
        submitter.load_job_configs()

        logger.info("\n" + "=" * 60)
        logger.info("DVC DEBUG OUTPUT")
        logger.info("=" * 60)

        logger.info(f"Sample job keys (first 5):")
        for job_key in list(submitter.job_configs.keys())[:5]:
            logger.info(f"  {job_key}")

        logger.info(f"\nTesting DVC status --json command:")
        try:
            result = subprocess.run(
                ["dvc", "status", "--json"], capture_output=True, text=True, check=False
            )
            logger.info(f"Return code: {result.returncode}")
            if result.stdout:
                import json

                try:
                    status_data = json.loads(result.stdout)
                    logger.info(f"Found {len(status_data)} changed stages:")
                    for stage_name in list(status_data.keys())[:5]:  # Show first 5
                        logger.info(f"  {stage_name}")
                    if len(status_data) > 5:
                        logger.info(f"  ... and {len(status_data)-5} more")
                except json.JSONDecodeError:
                    logger.info(f"Raw output: {result.stdout[:200]}...")
            else:
                logger.info("No output (all stages up-to-date)")
            if result.stderr:
                logger.info(f"Stderr: {result.stderr}")
        except FileNotFoundError:
            logger.info("DVC not found")

        logger.info(f"\nTesting DVC dag --dot command:")
        try:
            result = subprocess.run(
                ["dvc", "dag", "--dot"], capture_output=True, text=True, check=False
            )
            logger.info(f"Return code: {result.returncode}")
            if result.stdout:
                # Show just the edges (lines with ->)
                edges = [
                    line.strip()
                    for line in result.stdout.split("\n")
                    if "->" in line and '"' in line
                ]
                logger.info(f"Found {len(edges)} dependency edges:")
                for edge in edges[:5]:  # Show first 5
                    logger.info(f"  {edge}")
                if len(edges) > 5:
                    logger.info(f"  ... and {len(edges)-5} more")
            if result.stderr:
                logger.info(f"Stderr: {result.stderr}")
        except FileNotFoundError:
            logger.info("DVC not found")

        return

    if args.test_trigger:
        # Test trigger.sh with help flag
        submitter.load_job_configs()

        logger.info("\n" + "=" * 60)
        logger.info("TESTING TRIGGER.SH")
        logger.info("=" * 60)

        logger.info("Testing trigger.sh --help:")
        try:
            result = subprocess.run(
                ["bash", "cluster/trigger.sh", "--help"],
                capture_output=True,
                text=True,
                check=False,
            )
            logger.info(f"Return code: {result.returncode}")
            if result.stdout:
                logger.info("Help output:")
                logger.info(result.stdout)
            if result.stderr:
                logger.info("Stderr:")
                logger.info(result.stderr)
        except Exception as e:
            logger.info(f"Error running trigger.sh: {e}")

        # Test with a sample job configuration
        if submitter.job_configs:
            sample_job_key = list(submitter.job_configs.keys())[0]
            sample_config = submitter.job_configs[sample_job_key]

            logger.info(f"\nTesting with sample job: {sample_job_key}")
            test_cmd = [
                "bash",
                "cluster/trigger.sh",
                "--stage",
                f"{sample_config.stage}@{sample_config.step}",
                "--gpus",
                str(sample_config.gpus),
                "--time",
                sample_config.time_limit,
                "--memory",
                sample_config.memory,
                "--cpus",
                str(sample_config.cpus),
                "--partition",
                sample_config.partition,
                "--nodes",
                str(sample_config.nodes),
                "--ntasks",
                str(sample_config.ntasks),
                "--ntasks-per-node",
                str(sample_config.ntasks_per_node),
                "--help",  # Add help flag to avoid actually submitting
            ]

            logger.info(f"Test command: {' '.join(test_cmd)}")
            try:
                result = subprocess.run(
                    test_cmd, capture_output=True, text=True, check=False
                )
                logger.info(f"Return code: {result.returncode}")
                if result.stdout:
                    logger.info("Output:")
                    logger.info(
                        result.stdout[:500]
                        + ("..." if len(result.stdout) > 500 else "")
                    )
                if result.stderr:
                    logger.info("Stderr:")
                    logger.info(
                        result.stderr[:500]
                        + ("..." if len(result.stderr) > 500 else "")
                    )
            except Exception as e:
                logger.info(f"Error testing trigger.sh: {e}")

        return

    if args.check_status:
        # Just show what would run
        submitter.load_job_configs()
        submitter.load_dependencies_from_dvc()

        job_status = submitter.check_job_status()
        jobs_to_run = submitter.filter_jobs_by_status(
            include_cached=False, target_pattern=args.target
        )

        logger.info("\n" + "=" * 60)
        logger.info("JOB STATUS CHECK")
        logger.info("=" * 60)

        if args.target:
            target_jobs = submitter.get_target_jobs(args.target)
            all_jobs_in_scope = target_jobs
            logger.info(
                f"Scope: Target pattern '{args.target}' and its {len(target_jobs)} dependencies"
            )
        else:
            all_jobs_in_scope = set(submitter.job_configs.keys())
            logger.info(f"Scope: All {len(all_jobs_in_scope)} jobs in pipeline")

        cached_jobs = [
            job
            for job, status in job_status.items()
            if status == "cached" and job in all_jobs_in_scope
        ]
        needs_run_jobs = [
            job
            for job, status in job_status.items()
            if status == "needs_run" and job in all_jobs_in_scope
        ]

        if cached_jobs:
            logger.info(f"\nCACHED JOBS ({len(cached_jobs)}):")
            for job in cached_jobs[:10]:  # Show first 10
                logger.info(f"  ✓ {job}")
            if len(cached_jobs) > 10:
                logger.info(f"  ... and {len(cached_jobs)-10} more")

        if needs_run_jobs:
            logger.info(f"\nJOBS THAT NEED TO RUN ({len(needs_run_jobs)}):")
            for job in needs_run_jobs[:10]:  # Show first 10
                logger.info(f"  → {job}")
            if len(needs_run_jobs) > 10:
                logger.info(f"  ... and {len(needs_run_jobs)-10} more")

        deps_added = len(jobs_to_run) - len(needs_run_jobs)
        if deps_added > 0:
            logger.info(f"\nADDITIONAL JOBS DUE TO DEPENDENCIES ({deps_added}):")
            additional_jobs = jobs_to_run - set(needs_run_jobs)
            for job in list(additional_jobs)[:10]:
                logger.info(f"  ↳ {job}")
            if len(additional_jobs) > 10:
                logger.info(f"  ... and {len(additional_jobs)-10} more")

        # Calculate resource totals for status check
        total_gpu_hours, total_cpu_hours = calculate_resource_totals(submitter.job_configs, jobs_to_run)
        
        logger.info(
            f"\nSUMMARY: {len(jobs_to_run)} jobs would be submitted, {len(cached_jobs)} are cached"
        )
        logger.info(f"RESOURCE REQUIREMENTS: {total_gpu_hours:.1f} GPU hours, {total_cpu_hours:.1f} CPU hours")
        return

    # Validate target pattern if specified
    if args.target:
        # Load configs once to validate target
        if not hasattr(submitter, "job_configs") or not submitter.job_configs:
            submitter.load_job_configs()

        # Check if target pattern matches any jobs
        try:
            matching_jobs = submitter._find_target_jobs_by_pattern(args.target)
            if not matching_jobs:
                logger.error(f"Target pattern '{args.target}' matched no jobs")
                
                # Show available job examples
                available_jobs = list(submitter.job_configs.keys())
                logger.info(f"Available job examples:")
                for job in available_jobs[:10]:  # Show first 10
                    logger.info(f"  {job}")
                if len(available_jobs) > 10:
                    logger.info(f"  ... and {len(available_jobs)-10} more")
                
                # Show available stages
                available_stages = set()
                for job_key in submitter.job_configs.keys():
                    stage, _ = job_key.split(":")
                    available_stages.add(stage)
                
                logger.info(f"\nAvailable stages:")
                for stage in sorted(list(available_stages))[:20]:  # Show first 20
                    logger.info(f"  {stage}")
                if len(available_stages) > 20:
                    logger.info(f"  ... and {len(available_stages)-20} more")
                
                logger.info(f"\nPattern examples:")
                logger.info(f"  --target 'train:*'        # All jobs in train stage")
                logger.info(f"  --target 'stage:step_*'   # All steps starting with 'step_'")
                logger.info(f"  --target 'eval.*'         # All jobs starting with 'eval'")
                logger.info(f"  --target '^preprocess:.*' # Regex: preprocess stage")
                
                sys.exit(1)
            else:
                logger.info(f"Target pattern '{args.target}' matched {len(matching_jobs)} jobs")
        except ValueError as e:
            logger.error(f"Invalid target pattern: {e}")
            sys.exit(1)

    # Modify behavior for force-all
    if args.force_all:
        original_filter = submitter.filter_jobs_by_status
        submitter.filter_jobs_by_status = (
            lambda include_cached=False, target_pattern=None: (
                submitter.get_target_jobs(target_pattern)
                if target_pattern
                else set(submitter.job_configs.keys())
            )
        )

    success = submitter.run_pipeline(target_pattern=args.target)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()