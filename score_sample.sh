#!/bin/bash
source ./bin/activate

python3 ores_archaeologist.py score_history --cutoff_revisions=data/enwiki_revisions_small.csv --wrap=False --output=data/enwiki_scored_small_sample.csv > output 2> error

python3 ores_archaeologist.py score_history --cutoff_revisions=data/arwiki_revisions.csv --wrap=False --output=data/rescored_arwiki_revisions.csv

