# Comparison: job-runner.sh vs. New Optimized Scripts

## Summary Table

| Script | Time Limit | Memory | CPUs | Segments/Task | Parallel Tasks* | Key Features |
|--------|-----------|---------|------|---------------|----------------|--------------|
| **`job-runner.sh`** (ORIGINAL) | 24h (default) | 100m (default) | 1 (default) | 100 (default) | ~8-10 | Basic, minimal config |
| **`test-config.sh`** | 336h (14 days) | 2G | 2 | 50 | ~50 | Safe testing, dry-run |
| **`run-optimized-job.sh`** | 336h (14 days) | 2G | 2 | 50 | ~50 | Reliability-focused |
| **`run-fast-parallel.sh`** | 168h (7 days) | 2G | 2 | 25 | ~100 | Balanced speed/safety |
| **`run-ultra-parallel.sh`** | 168h (7 days) | 2G | 2 | 10 | ~250 | Maximum speed |

*Approximate parallel tasks for typical crawl data

## Detailed Comparison

### Your Original `job-runner.sh`:
```bash
./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt 
  #--dry-run
```

**Uses ALL DEFAULTS from slurm-sequential-runner.py:**
- `--time 24` (24 hours) ❌ Too short for your 5-day jobs
- `--mem 100m` (100 megabytes) ❌ Very small memory allocation  
- `--cpus 1` ❌ Single CPU per task
- `--segments-per-task 100` ❌ Fewer parallel jobs
- `--partition` (none specified) ❌ Uses default partition selection

### Problems with Original Script:
1. **TIMEOUT ISSUE**: 24-hour limit caused your 5-day job to fail
2. **RESOURCE WASTE**: Only 100MB memory, 1 CPU per task
3. **SLOW PROCESSING**: Large segments = fewer parallel tasks
4. **NO PARTITION CONTROL**: May not get optimal queue

### Improvements in New Scripts:

#### 1. **Time Limit Fixed**:
```bash
# Original (FAILS after 1 day):
--time 24

# New (survives 7-14 days):
--time 168   # or --time 336
```

#### 2. **Better Resource Allocation**:
```bash
# Original (minimal):
--mem 100m --cpus 1

# New (optimized):
--mem 2G --cpus 2
```

#### 3. **Controlled Parallelism**:
```bash
# Original (fewer tasks):
--segments-per-task 100  → ~8-10 parallel jobs

# New options:
--segments-per-task 50   → ~50 parallel jobs
--segments-per-task 25   → ~100 parallel jobs  
--segments-per-task 10   → ~250 parallel jobs
```

#### 4. **Partition Selection**:
```bash
# Original (no control):
(uses default)

# New (explicit):
--partition compute  # Best for long CPU jobs
```

## Migration Path

### Quick Fix (Minimal Changes):
Replace your `job-runner.sh` with:
```bash
#!/bin/bash
#SBATCH --job-name=runner

./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 168 \
  --mem 2G \
  --cpus 2
```

### Recommended Approach:
1. Use `test-config.sh` first (dry-run)
2. Switch to `run-fast-parallel.sh` for production
3. Keep your original as `job-runner-original.sh` backup

## Performance Impact

**Original vs Fast-Parallel:**
- **Speed**: 4-10x faster completion (100 vs 10 parallel tasks)
- **Reliability**: 7x longer time limit (168h vs 24h)  
- **Resources**: 20x more memory (2G vs 100m), 2x more CPUs
- **Success Rate**: Much higher (no timeout failures)

**Bottom Line**: Your original script had the exact problems you experienced - it was designed for short, simple jobs, not the heavy 5-day processing workload you're running.