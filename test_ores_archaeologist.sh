#!/bin/bash
cd ~/ores_bias/ores_bias_project
source ../mediawiki-services-ores-deploy/bin/activate
python3 ores_archaeologist.py get_threshhold --wiki_db=enwiki --date=2018-08-07 --threshhold_string="maximum recall @ precision >= 0.99" # 

#python3 ores_archaeologist.py score_wiki_commit_revisions --commit=860c70b73de36d63584db019cccf841f943622e7 --wiki_db=kowiki --all_revisions='ores_bias_data/test_revisions_sample.csv' --load-environment=False --wrap=True

python3 ores_archaeologist.py score_commit_revisions --commit=860c70b73de36d63584db019cccf841f943622e7 --cutoff_revisions='ores_bias_data/test_revisions_sample.csv'  --wrap=True --load_environment=False

python3 ores_archaeologist.py score_history --cutoff_revisions='ores_bias_data/test_revisions_sample.csv'  --wrap=True
# python3 ores_archaeologist.py score_revisions --wiki_db=enwiki --uri=https://en.wikipedia.org --date=2018-08-22 --infile=test_revids.txt
