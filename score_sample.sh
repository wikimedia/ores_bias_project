#!/bin/bash
source ./bin/activate
python3 ores_archaeologist.py score_history --cutoff_revisions=ores_bias_data/cutoff_revisions_sample.csv --wrap=False --output=ores_bias_data/historically_scored_sample.csv
