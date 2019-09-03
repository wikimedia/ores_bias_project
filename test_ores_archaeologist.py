#!/bin/bash
cd ~/ores_bias
source mediawiki-services-ores-deploy/bin/activate
python3 ores_archaeologist.py get_threshhold --wiki_db=enwiki --date=2018-08-22 --threshhold_string="maximum recall @ precision >= 0.99"
python3 ores_archaeologist.py score_revisions --wiki_db=enwiki --uri=https://en.wikipedia.org --date=2018-08-22 --infile=test_revids.txt
