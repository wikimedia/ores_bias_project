#!/usr/bin/env python3

# to be run on a wikimedia environment with spark access
from get_labels import load_wikis, load_makefile, grep_labelfile, download_labels
from score_labels import score_labels, get_thresholds
#from move_labels_to_datalake import move_labels_to_datalake
import subprocess

test_wikis = ['test2wiki']
wikis = [wiki for wiki in load_wikis() if wiki not in test_wikis]

makefile = load_makefile()
label_files = [lf for lf in map(lambda x: grep_labelfile(x, makefile), wikis) if lf is not None]
download_labels(label_files)

#label_files_to_csv(label_file_wikis)
scored_labels = score_labels(label_files, wikis)
scored_threshold_labels = get_thresholds(wikis, load_environment=False)

move_labels_to_datalake(label_files, wikis)

subprocess.call(["/usr/lib/spark2/bin/spark-submit", "--name","get ores label user histories", "--master", "yarn", "--conf", "spark.driver.memory=4g","--conf", "spark.executor.memory=8g","--conf", "spark.executor.memoryOverhead=2g",  "get_label_user_histories.py"])

subprocess.call("./evaluate_encoded_bias.py")
