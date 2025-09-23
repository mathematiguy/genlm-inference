#!/usr/bin/env python3
"""
Safe DVC Lock Merger

Safely merges dvc.lock files by:
1. Saving current dvc.lock to temp
2. Checking out each experiment branch
3. Loading both current and branch dvc.lock
4. Merging them in memory
5. Saving result and committing to work branch
"""

import yaml
import git
import shutil
import tempfile
from pathlib import Path
import argparse
import sys

class SafeDVCMerger:
    def __init__(self, repo_path="."):
        try:
            self.repo = git.Repo(repo_path)
        except git.InvalidGitRepositoryError:
            print(f"Error: {repo_path} is not a git repository")
            sys.exit(1)
        
        self.original_branch = self.repo.active_branch.name
        self.temp_dir = None
        
    def save_current_lock(self):
        """Save current dvc.lock to a temporary file."""
        self.temp_dir = tempfile.mkdtemp()
        current_lock_path = Path("dvc.lock")
        
        if current_lock_path.exists():
            temp_lock_path = Path(self.temp_dir) / "current_dvc.lock"
            shutil.copy2(current_lock_path, temp_lock_path)
            print(f"Saved current dvc.lock to {temp_lock_path}")
            return temp_lock_path
        else:
            print("No current dvc.lock found, starting fresh")
            return None
    
    def load_yaml_file(self, file_path):
        """Safely load a YAML file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Warning: Could not load {file_path}: {e}")
            return {}
    
    def merge_locks(self, base_lock, branch_lock):
        """Merge two dvc.lock dictionaries."""
        # Start with base
        merged = base_lock.copy()
        if "stages" not in merged:
            merged["stages"] = {}
        
        # Add stages from branch
        branch_stages = branch_lock.get("stages", {})
        stats = {"added": 0, "updated": 0, "skipped": 0}
        
        for stage_name, stage_data in branch_stages.items():
            if stage_name not in merged["stages"]:
                merged["stages"][stage_name] = stage_data
                stats["added"] += 1
                print(f"    + Added: {stage_name}")
            else:
                # Check if different by comparing output hashes
                existing_outs = self._get_output_hashes(merged["stages"][stage_name])
                new_outs = self._get_output_hashes(stage_data)
                
                if existing_outs != new_outs:
                    merged["stages"][stage_name] = stage_data
                    stats["updated"] += 1
                    print(f"    ~ Updated: {stage_name}")
                else:
                    stats["skipped"] += 1
                    print(f"    = Skipped: {stage_name} (unchanged)")
        
        return merged, stats
    
    def _get_output_hashes(self, stage_data):
        """Extract output path -> hash mapping."""
        outputs = {}
        for output in stage_data.get("outs", []):
            path = output.get("path", "")
            md5_hash = output.get("md5", "")
            outputs[path] = md5_hash
        return outputs
    
    def write_yaml_file(self, data, file_path):
        """Write YAML file with proper Unicode handling."""
        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, 
                     default_flow_style=False, 
                     sort_keys=False,
                     allow_unicode=True)
    
    def get_experiment_branches(self, pattern="unknown_"):
        """Get list of experiment branches."""
        branches = []
        for ref in self.repo.remote().refs:
            branch_name = ref.name.split('/')[-1]
            if branch_name.startswith(pattern):
                branches.append(branch_name)
        return sorted(branches)
    
    def merge_from_branches(self, branches, work_branch="merged-dvc-lock", dry_run=False):
        """Main merge process."""
        print(f"=== Safe DVC Lock Merger ===")
        print(f"Original branch: {self.original_branch}")
        print(f"Work branch: {work_branch}")
        print(f"Branches to merge: {len(branches)}")
        print()
        
        if dry_run:
            print("DRY RUN MODE - No changes will be made")
            print()
        
        # Step 1: Save current dvc.lock
        current_lock_path = self.save_current_lock()
        
        # Load current lock
        if current_lock_path:
            merged_lock = self.load_yaml_file(current_lock_path)
        else:
            merged_lock = {"stages": {}}
        
        base_stage_count = len(merged_lock.get("stages", {}))
        print(f"Starting with {base_stage_count} stages")
        print()
        
        # Step 2: Process each branch
        total_stats = {"added": 0, "updated": 0, "skipped": 0}
        
        for i, branch in enumerate(branches, 1):
            print(f"[{i}/{len(branches)}] Processing: {branch}")
            
            try:
                # Checkout the branch
                self.repo.git.checkout(f"origin/{branch}")
                
                # Load the branch's dvc.lock
                if Path("dvc.lock").exists():
                    branch_lock = self.load_yaml_file("dvc.lock")
                    
                    # Merge it
                    merged_lock, stats = self.merge_locks(merged_lock, branch_lock)
                    
                    # Update totals
                    for key in total_stats:
                        total_stats[key] += stats[key]
                    
                    print(f"    Branch summary: +{stats['added']} ~{stats['updated']} ={stats['skipped']}")
                else:
                    print(f"    No dvc.lock found in {branch}")
                
            except Exception as e:
                print(f"    Error processing {branch}: {e}")
            
            print()
        
        # Step 3: Return to original branch
        self.repo.git.checkout(self.original_branch)
        
        # Step 4: Create work branch and save result
        final_stage_count = len(merged_lock.get("stages", {}))
        print(f"Merge complete!")
        print(f"Final stages: {final_stage_count} (was {base_stage_count})")
        print(f"Total: +{total_stats['added']} ~{total_stats['updated']} ={total_stats['skipped']}")
        print()
        
        if not dry_run and final_stage_count > 0:
            # Create work branch
            try:
                work_head = self.repo.create_head(work_branch, force=True)
                work_head.checkout()
                
                # Write merged dvc.lock
                self.write_yaml_file(merged_lock, "dvc.lock")
                
                # Commit
                self.repo.index.add(["dvc.lock"])
                commit_msg = f"Merge dvc.lock from {len(branches)} experiment branches\n\nStats: +{total_stats['added']} added, ~{total_stats['updated']} updated, ={total_stats['skipped']} skipped\nTotal stages: {final_stage_count}"
                self.repo.index.commit(commit_msg)
                
                print(f"✓ Created work branch '{work_branch}' with merged dvc.lock")
                print(f"✓ Committed merged result")
                
                # Return to original branch
                self.repo.git.checkout(self.original_branch)
                print(f"✓ Returned to original branch: {self.original_branch}")
                
            except Exception as e:
                print(f"Error creating work branch: {e}")
                self.repo.git.checkout(self.original_branch)
        
        # Cleanup
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)
        
        print()
        print("Next steps:")
        if not dry_run:
            print(f"1. Review the merged dvc.lock: git show {work_branch}:dvc.lock | head -50")
            print(f"2. Switch to work branch: git checkout {work_branch}")
            print(f"3. Test with: dvc status")
            print(f"4. Restore outputs: dvc checkout")
            print(f"5. If satisfied: git checkout {self.original_branch} && git merge {work_branch}")
        else:
            print("Run without --dry-run to create the merged branch")


def main():
    parser = argparse.ArgumentParser(description="Safely merge DVC lock files from experiment branches")
    parser.add_argument("--pattern", default="unknown_", 
                       help="Branch pattern to match (default: unknown_)")
    parser.add_argument("--work-branch", default="merged-dvc-lock",
                       help="Name of work branch to create (default: merged-dvc-lock)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be done without making changes")
    parser.add_argument("--limit", type=int, default=None,
                       help="Limit number of branches to process (for testing)")
    
    args = parser.parse_args()
    
    try:
        merger = SafeDVCMerger()
        branches = merger.get_experiment_branches(args.pattern)
        
        if not branches:
            print(f"No branches found matching pattern: {args.pattern}")
            return
        
        if args.limit:
            branches = branches[:args.limit]
            print(f"Limited to first {args.limit} branches")
        
        merger.merge_from_branches(branches, args.work_branch, args.dry_run)
        
    except KeyboardInterrupt:
        print("\nAborted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
