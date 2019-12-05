#!/usr/bin/env python3
# to be run on a wikimedia environment with spark access

from move_labels_to_datalake import move_labels_to_datalake
move_labels_to_datalake(label_files, wikis)

# subprocess.call(["/usr/lib/spark2/bin/spark-submit", "--name","get ores label user histories", "--master", "yarn", "--conf", "spark.driver.memory=4g","--conf", "spark.executor.memory=8g","--conf", "spark.executor.memoryOverhead=2g",  "get_label_user_histories.py"])

# subprocess.call("./evaluate_encoded_bias.py")
