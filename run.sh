#!/bin/bash

# This script assume MScaleJoin.jar is in the same directory

# Date for output suffix
now=`date +%Y-%m-%d.%H:%M:%S`

# Dataset path
data_path=${1}
# Dataset size (for each stream)
data_size=${2}
# Output path
output_path="output"
# Output file name
output="$output_path/MScaleJoin_$now"
# Number of trials for each experiment
n_trials=${3}
# Number of threads
n_threads="2 4 6 8 10 12 14 16"

# Experiments and its default path
# Assuming all this directories exist under data_path
declare -A experiments
experiments[BandJoin]="1;$data_path/band_join/$data_size/"
experiments[EquiJoinCommonNlj]="2;$data_path/equi_join_common/$data_size/"
experiments[EquiJoinCommonShj]="3;$data_path/equi_join_common/$data_size/"
experiments[EquiJoinDistinctNlj]="4;$data_path/equi_join_distinct/$data_size/"
experiments[EquiJoinDistinctShj]="5;$data_path/equi_join_distinct/$data_size/"

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

	echo -e "EXPERIMENT $id BEGIN\n" >> $output
	
	for n in $n_threads; do
		echo -e "BEGIN WITH $n THREADS\n" >> $output
		for (( i=0; i<$n_trials; i++ )); do
			echo "TRIAL INSTANCE $i" >> $output
			java -jar MScaleJoin.jar $code $n 4000000 $local_data_path >> $output
			sleep 1
		done
		echo -e "FINISH WITH $n THREADS\n" >> $output
	done
	
	echo -e "EXPERIMENT $id FINISH\n" >> $output
done
