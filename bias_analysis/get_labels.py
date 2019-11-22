#!/usr/bin/env python3
import re
import time
import subprocess
import os
import shutil
import json
import itertools

def load_wikis():
    lines = open("rcfilters_enabled.csv",'r')
    wikis = [line.split(',')[0] for line in lines]
    wikis = [wiki for wiki in wikis if wiki != "Wiki"]

    # simple wiki uses the same models and labels as english wikipedia
    wikis = [wiki for wiki in wikis if wiki != "simplewiki"]
    return list(set(wikis))
    
def load_makefile():
    with open("../Makefile",'r') as makefile1:
        with open("../Makefile.manual",'r') as makefile2:
            makefile = makefile1.read() + '\n' + makefile2.read()
    return makefile
                           
def grep_labelfile(wiki, makefile):
    humanlabel_re_format = r"datasets/{0}\.human_labeled_revisions\.(.*)k_(.*)\.json:.*"
    # find candidate human labeled revisions
    humanlabel_re = re.compile(humanlabel_re_format.format(wiki))
    # choose the best match
    # choose the human labeled revisions with the largest N
    # and the most recent
    matches = list(humanlabel_re.finditer(makefile))
    if len(matches) == 0:
        print("found no matches for {0}".format(wiki))
        return None

    max_n = max(int(match.groups()[0]) for match in matches)
    max_n_match = [match for match in matches if int(match.groups()[0]) == max_n]

    latest_date = max(int(match.groups()[1]) for match in max_n_match)
    latest_date_match = [match for match in max_n_match if int(match.groups()[1]) == latest_date]
    if len(latest_date_match) > 1:
        print("too many matches {1} for {0}".format(wiki,latest_date_match))
        return None
    else:
        print("found match {0} for {1}".format(latest_date_match[0],wiki))
        match = latest_date_match[0]
        label_file = makefile[match.start():match.end()-1]
        return label_file
        
def _download_labels(label_file):
    os.chdir("..")
    try: 
        subprocess.call(["make",label_file])
    except Exception as e:
        print(e)
    os.chdir("bias_analysis")

def load_labels(label_file):
    return open("../{0}".format(label_file))

def download_labels(label_files):
    for label_file in label_files:
        _download_labels(label_file)

if __name__ == "__main__":
    wikis = load_wikis()
    makefile = load_makefile()
    label_files = map(lambda x: grep_labelfile(x, makefile), wikis)
    download_labels(label_files)
    
