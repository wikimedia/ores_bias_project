#!/bin/bash
cd ~/ores_bias/ores_bias_project
source ../mediawiki-services-ores-deploy/bin/activate
python3 ores_archaeologist.py score_wiki_commit_revisions --commit=8c4ed2536dd668f909b48a75ff0688fdc481be09 --wiki_db=kowiki --all_revisions='ores_bias_data/test_revisions_sample.csv' --load-environment=False --wrap=True

python3 ores_archaeologist.py score_commit_revisions --commit=8c4ed2536dd668f909b48a75ff0688fdc481be09 --cutoff_revisions='ores_bias_data/test_revisions_sample.csv' --load_environment=False --wrap=True

python3 ores_archaeologist.py score_history --cutoff_revisions='ores_bias_data/test_revisions_sample.csv'  --wrap=True
# python3 ores_archaeologist.py get_threshhold --wiki_db=enwiki --date=2018-08-22 --threshhold_string="m
# aximum recall @ precision >= 0.99"
# python3 ores_archaeologist.py score_revisions --wiki_db=enwiki --uri=https://en.wikipedia.org --date=2018-08-22 --infile=test_revids.txt
