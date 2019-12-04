#!/usr/bin/env python3

# to be run on a wikimedia environment with spark access
from get_labels import load_wikis, load_makefile, grep_labelfile, download_labels, label_files_to_csv
#from score_labels import score_labels
#from move_labels_to_datalake import move_labels_to_datalake
import subprocess

wikis = list(load_wikis())
makefile = load_makefile()
label_files = list(map(lambda x: grep_labelfile(x, makefile), wikis))
download_labels(label_files)

label_file_wikis = zip(label_files, wikis)
label_files_to_csv(label_file_wikis)

score_labels(label_files, wikis, overwrite=True)

move_labels_to_datalake(label_files, wikis)

subprocess.call(["/usr/lib/spark2/bin/spark-submit", "--name","get ores label user histories", "--master", "yarn", "--conf", "spark.driver.memory=4g","--conf", "spark.executor.memory=8g","--conf", "spark.executor.memoryOverhead=2g",  "get_label_user_histories.py"])

subprocess.call("./evaluate_encoded_bias.py")
