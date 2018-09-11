#!/bin/bash

# This script assume MScaleJoin.jar is in the same directory

# Dataset path
data_path=${1}
# Dataset size (for each stream)
data_size=${2}
# Number of trials for each experiment
n_trials=${3}
# Number of threads
n_threads="1 2 4 6 8 10 12 14 16"
# Output path
output_path="output"
# Output file name
output="${output_path}/out_${4}" # ${4} should be a date

# Experiments and its default path
# Assuming all this directories exist under data_path
declare -A experiments
experiments[BandJoin]="1;${data_path}/band_join/${data_size}/"
experiments[EquiJoinCommonNlj]="2;${data_path}/equi_join_common/${data_size}/"
experiments[EquiJoinCommonShj]="3;${data_path}/equi_join_common/${data_size}/"
experiments[EquiJoinDistinctNlj]="4;${data_path}/equi_join_distinct/${data_size}/"
experiments[EquiJoinDistinctShj]="5;${data_path}/equi_join_distinct/${data_size}/"

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
		# Structure of the output file
		# [algorithm,experiment,cpus,elapsed_s,initial_response_ms,output_total,output_s,comparison_total,comparison_s]

		for (( i=0; i<$n_trials; i++ )); do
			echo -n "MScaleJoin,${id},${n}" >> 
			java -jar MScaleJoin.jar $code $n 4000000 $local_data_path >> $output
			sleep 2
		done
	done
done

# Send mail as notification that the task is finished. 
# This assume that 'mail' is intalled and configured.
ip=`hostname -I`
echo "MScaleJoin experiment is done on $ip" | mail -s "MScaleJoin Experiment on $ip" habib.ryd@gmail.com
