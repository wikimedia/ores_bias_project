#!/usr/bin/env python3
import stat
import re
import glob 
import pandas as pd
import os
from os import path
import pyarrow
import subprocess
from concurrent.futures import ThreadPoolExecutor

wikis = pd.read_csv("ores_bias_data/rcfilters_enabled.csv")
wikis = set(wikis['Wiki'])

base_path = "/mnt/data/xmldatadumps/public/"

script_file = "get_reverts.sh"
of = open(script_file,'w')
of.write("#!/usr/bin/env bash\n")
filename_fmt = "{0}-reverts.json"

# what files are already on hadoop?

hdfs = pyarrow.hdfs.connect()

hdfs_files = hdfs.ls("/user/nathante/ores_bias/reverts", detail=True)

hdfs_files = {d["name"]:d['size'] for d in hdfs_files}

with ThreadPoolExecutor(max_workers=32) as executor:
    for wiki in wikis:
        regex = re.compile(".*/({0}-20190501)-pages-meta-history(.*).xml(.*).bz2".format(wiki))
        glob_string = path.join(base_path,wiki,"20190501","{0}-20190501-pages-meta-history*.xml*.bz2".format(wiki))
        files = glob.glob(glob_string)
        for filename in files:
            def process_file(filename):
                m  = regex.match(filename)
                outfilename = path.split(filename)[-1]
                outfilepath = path.join("ores_bias_data",outfilename)
                hadooppath = path.join("hdfs://analytics-hadoop","user","nathante","ores_bias","reverts",outfilename)
                if hadooppath in hdfs_files:
                    if hdfs_files[hadooppath] > 0:
                        return
                    
                call = ["mwreverts", "dump2reverts", filename, "--radius", "48",  "--output", "ores_bias_data"]
 
                subprocess.run(call)
                
                hdfs.upload(hadooppath,open(outfilepath,'rb'))
                os.remove(outfilepath)

            executor.submit(process_file, filename)
