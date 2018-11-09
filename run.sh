#!/bin/bash

# This script assume MScaleJoin.jar is in the same directory

# Trial id
trial=${1}
# Dataset path
data_path=${2}
# Dataset size (for each stream)
data_size=${3}
# Number of threads
n_threads="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18"
# Rates per second
rates="1000 3000 5000"
# Window size in milis
windows="60000 300000 600000"
# Output path
output_path="output"
# Output file name
output="${output_path}/output_${1}_${4}" # ${4} should be a date

# Experiments and its default path
# Assuming all this directories exist under data_path
declare -A experiments
# Band-join with 3 streams and mixed with equi-join
# experiments[Scenario1]="0;${data_path}/scenario1/${data_size}/"
# Band-join with 3 streams
# experiments[Scenario2]="1;${data_path}/scenario2/${data_size}/"
# Band-join with 4 streams
experiments[Scenario3]="2;${data_path}/scenario3/${data_size}/"
# Equi-join with 4 streams, join on common key and using NLJ
experiments[Scenario4a1]="3;${data_path}/scenario4_dist/${data_size}/"
# Equi-join with 4 streams, join on common key and using SHJ
experiments[Scenario4a2]="4;${data_path}/scenario4_dist/${data_size}/"
# Equi-join with 4 streams, join on distinct key and using NLJ
experiments[Scenario4b1]="5;${data_path}/scenario4/${data_size}/"
# Equi-join with 4 streams, join on distinct key and using SHJ
experiments[Scenario4b2]="6;${data_path}/scenario4/${data_size}/"

# Check dataset path
if [[ ! -d $data_path ]]; then
	exit 1
fi

# Create output directory if it doesn't exist
if [[ ! -d $output_path ]]; then
	mkdir $output_path
fi

# Run the experiments n_trials time
for id in "${!experiments[@]}"; do
	config=${experiments[$id]}
	arr_config=(${config//;/ })
	code=${arr_config[0]}
	local_data_path=${arr_config[1]}

	for n in $n_threads; do
		for r in $rates; do
			for w in $windows; do
				# [trial_id, experiment_id, threads, window_ms, rate_s, latency_ms, processed_s, processed_avg_s, output_s, comparison_s, comparison_avg_s]
				java -jar MScaleJoin.jar $code $n $r $w $local_data_path | while read line; do echo "$trial,$id,$line"; done >> $output
				sleep 2
			done
		done
	done
done

# Send mail as notification that the task is finished. 
# This assume that 'mail' is intalled and configured.
ip=`hostname -I`
echo "MScaleJoin experiment is done on $ip" | mail -s "MScaleJoin Experiment on $ip" habib.ryd@gmail.com
