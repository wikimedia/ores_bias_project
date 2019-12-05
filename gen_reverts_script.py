#!/usr/bin/env python3

import re
import glob 
import pandas as pd
import os
from os import path
wikis = pd.read_csv("ores_bias_data/rcfilters_enabled.csv")
wikis = set(wikis['Wiki'])

base_path = "/mnt/data/xmldatadumps/public/"

script_file = "get_reverts.sh"
of = open(script_file,'w')
of.write("#!/usr/bin/env bash/n")
call = "mwreverts dump2reverts {0} --window 2 --radius 48 --threads 6 > {1}-reverts.json/n"

for wiki in wikis:
    regex = re.compile(".*/({0}-20190501)-pages-meta-history(.*).xml(.*).bz2".format(wiki))
    glob_string = path.join(base_path,wiki,"20190501","{0}-20190501-pages-meta-history*.xml*.bz2".format(wiki))
    files = glob.glob(glob_string)
    for filename in files:
        m  = regex.match(filename)
        outfilename = "_".join(m)
        of.write(call.format(filename,outfilename))
of.close()
    
os.chmod(script_file,"+x")
