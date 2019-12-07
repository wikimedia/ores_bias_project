#!/usr/bin/env python3
from get_labels import load_wikis, load_makefile, grep_labelfile, download_labels
from score_labels import score_labels, get_thresholds

import pickle
import subprocess

ignore_wikis = ['test2wiki','euwiki','testwiki','mediawikiwiki','simplewiki']
wikis = [wiki for wiki in load_wikis() if wiki not in ignore_wikis]

makefile = load_makefile()
label_files = [lf for lf in map(lambda x: grep_labelfile(x, makefile), wikis) if lf is not None]
download_labels(label_files)

scored_labels = score_labels(label_files, wikis)
scored_threshold_labels = get_thresholds(wikis, load_environment=False)

pickle.dump(scored_labels, open("data/scored_labels.pickle",'wb'))
pickle.dump(scored_threshold_labels, open("data/label_thresholds.pickle",'wb'))
