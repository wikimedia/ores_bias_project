#!/usr/bin/env python3
# to be run on a wikimedia environment with spark access
import subprocess

from get_labels import load_wikis, load_makefile, grep_labelfile, download_labels
from move_labels_to_datalake import move_labels_to_datalake

ignore_wikis = ['test2wiki','euwiki','testwiki','mediawikiwiki','simplewiki']
wikis = [wiki for wiki in load_wikis() if wiki not in ignore_wikis]

makefile = load_makefile()
label_files = [lf for lf in map(lambda x: grep_labelfile(x, makefile), wikis) if lf is not None]

download_labels(label_files)

move_labels_to_datalake(label_files, wikis)

subprocess.call(["/usr/lib/spark2/bin/spark-submit", "--name","get ores label user histories", "--master", "yarn", "--conf", "spark.driver.memory=4g","--conf", "spark.executor.memory=8g","--conf", "spark.executor.memoryOverhead=2g",  "get_label_user_histories.py"])


subprocess.call(["/usr/lib/spark2/bin/spark-submit", "--name","lookup wikidata entries", "--master", "yarn", "--conf", "spark.driver.memory=4g","--conf", "spark.executor.memory=8g","--conf", "spark.executor.memoryOverhead=2g",  "lookup_wikidata_entries.py"])

subprocess.call(["/usr/lib/spark2/bin/spark-submit", "--name","get labeldata stage2", "--master", "yarn", "--conf", "spark.driver.memory=4g","--conf", "spark.executor.memory=8g","--conf", "spark.executor.memoryOverhead=2g",  "get_labeldata_stage2.py"])

subprocess.call("./evaluate_encoded_bias.py")
