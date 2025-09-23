==========================================
SLURM Job Status Report
==========================================
Generated: Mon Sep 22 15:38:19 BST 2025

📊 CURRENT JOB STATUS
----------------------------------------
🎯 Main orchestrator job: 13539493 (Runtime: 4:10:55)
🚀 Array tasks running: 47

📈 COMPLETION STATISTICS
----------------------------------------
✅ Completed tasks: 1125
🏃 Currently processing: 1172 tasks
⏰ Remaining in queue: 1471 tasks
📊 Overall progress: 44.3% complete

🖥️  RESOURCE UTILIZATION
----------------------------------------
🏗️  Compute nodes in use: 16
⚡ Total CPUs in use: 94

📍 NODE DISTRIBUTION
----------------------------------------
      6 bp1-compute050
      5 bp1-compute059
      5 bp1-compute052
      4 bp1-compute089
      4 bp1-compute072
      4 bp1-compute067
      4 bp1-compute051
      3 bp1-compute086
      2 bp1-compute103
      2 bp1-compute102

⏱️  RUNTIME ANALYSIS
----------------------------------------
📋 Current task runtimes:
     10 3:46
      1 9:52
      1 9:15
      1 8:41
      1 8:30

⏰ ESTIMATED COMPLETION
----------------------------------------
📈 Completion rate: 269.01 tasks/hour
⏳ Estimated time remaining: 5.4 hours (~.2 days)

🔍 DETAILED QUEUE STATUS
----------------------------------------
             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
13539494_[421-2559   compute crawl_jo   ismjml PD       0:00      1 (JobArrayTaskLimit)
          13539493   compute run-fast   ismjml  R    4:10:55      1 bp1-compute064
      13539494_411   compute crawl_jo   ismjml  R       3:46      1 bp1-compute050
      13539494_412   compute crawl_jo   ismjml  R       3:46      1 bp1-compute089
      13539494_413   compute crawl_jo   ismjml  R       3:46      1 bp1-compute052
      13539494_414   compute crawl_jo   ismjml  R       3:46      1 bp1-compute052
      13539494_415   compute crawl_jo   ismjml  R       3:46      1 bp1-compute071
      13539494_416   compute crawl_jo   ismjml  R       3:46      1 bp1-compute072
      13539494_417   compute crawl_jo   ismjml  R       3:46      1 bp1-compute102
      13539494_418   compute crawl_jo   ismjml  R       3:46      1 bp1-compute102
      13539494_419   compute crawl_jo   ismjml  R       3:46      1 bp1-compute103
      13539494_420   compute crawl_jo   ismjml  R       3:46      1 bp1-compute103
      13539494_410   compute crawl_jo   ismjml  R       8:30      1 bp1-compute050
      13539494_409   compute crawl_jo   ismjml  R       8:41      1 bp1-compute050
      13539494_408   compute crawl_jo   ismjml  R       9:15      1 bp1-compute051
      13539494_407   compute crawl_jo   ismjml  R       9:52      1 bp1-compute086
      13539494_405   compute crawl_jo   ismjml  R      12:51      1 bp1-compute062
      13539494_404   compute crawl_jo   ismjml  R      13:07      1 bp1-compute050
      13539494_403   compute crawl_jo   ismjml  R      13:39      1 bp1-compute052
      13539494_401   compute crawl_jo   ismjml  R      14:27      1 bp1-compute072
      13539494_400   compute crawl_jo   ismjml  R      14:51      1 bp1-compute067
      13539494_399   compute crawl_jo   ismjml  R      15:00      1 bp1-compute059
      13539494_398   compute crawl_jo   ismjml  R      16:27      1 bp1-compute089
      13539494_395   compute crawl_jo   ismjml  R      18:00      1 bp1-compute051
      13539494_394   compute crawl_jo   ismjml  R      20:36      1 bp1-compute086
      13539494_393   compute crawl_jo   ismjml  R      21:10      1 bp1-compute060
      13539494_391   compute crawl_jo   ismjml  R      21:39      1 bp1-compute072
      13539494_390   compute crawl_jo   ismjml  R      22:36      1 bp1-compute059
      13539494_389   compute crawl_jo   ismjml  R      24:49      1 bp1-compute064
      13539494_386   compute crawl_jo   ismjml  R      25:44      1 bp1-compute067
      13539494_385   compute crawl_jo   ismjml  R      26:01      1 bp1-compute072
      13539494_381   compute crawl_jo   ismjml  R      26:40      1 bp1-compute059
      13539494_378   compute crawl_jo   ismjml  R      28:04      1 bp1-compute050
      13539494_377   compute crawl_jo   ismjml  R      28:17      1 bp1-compute051
      13539494_375   compute crawl_jo   ismjml  R      29:06      1 bp1-compute067
      13539494_374   compute crawl_jo   ismjml  R      30:01      1 bp1-compute061
      13539494_372   compute crawl_jo   ismjml  R      32:53      1 bp1-compute059
      13539494_369   compute crawl_jo   ismjml  R      33:35      1 bp1-compute060
      13539494_368   compute crawl_jo   ismjml  R      33:52      1 bp1-compute059
      13539494_364   compute crawl_jo   ismjml  R      39:09      1 bp1-compute067
      13539494_360   compute crawl_jo   ismjml  R      41:44      1 bp1-compute050
      13539494_359   compute crawl_jo   ismjml  R      41:58      1 bp1-compute089
      13539494_352   compute crawl_jo   ismjml  R      46:36      1 bp1-compute051
      13539494_351   compute crawl_jo   ismjml  R      47:27      1 bp1-compute089
      13539494_350   compute crawl_jo   ismjml  R      47:37      1 bp1-compute052
      13539494_348   compute crawl_jo   ismjml  R      50:32      1 bp1-compute086
      13539494_335   compute crawl_jo   ismjml  R      58:56      1 bp1-compute056
      13539494_331   compute crawl_jo   ismjml  R    1:00:59      1 bp1-compute052

==========================================
💡 To refresh this report, run: ./job-status.sh
🛠️  To cancel jobs, run: scancel JOBID
📊 For detailed job info: scontrol show job JOBID
==========================================
