sudo nvme zns reset-zone /dev/nvme1n1 -a
# opt/cachelib/bin/cachebench --json_test_config cachelib/cachebench/test_configs/zone_hash.json --progress 60 --progress_stats_file 1.json
sudo opt/cachelib/bin/cachebench --json_test_config cachelib/cachebench/test_configs/zone_hash.json --progress 60 --progress_stats_file 1.json