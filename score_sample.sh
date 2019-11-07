#!/bin/bash
source ./bin/activate
python3 ores_archaeologist.py score_history --cutoff_revisions=data/arwiki_revisions.csv --wrap=False --output=data/rescored_arwiki_revisions.csv
#python3 ores_archaeologist.py score_history --cutoff_revisions=data/cutoff_revisions_sample.csv --wrap=False --output=data/historically_scored_sample.csv > output 2> error
