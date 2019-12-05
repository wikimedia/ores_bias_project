
import git
import json
import pandas as pd
import datetime

repo = git.Repo("../mediawiki-extensions-ORES")
repo.git.checkout("-f","master")
# we only use the last commit
commit = list(repo.iter_commits(paths="extension.json", until=datetime.datetime(2019,12,1)))[0]

repo.git.checkout('-f',commit)
config = json.load(open("../mediawiki-extensions-ORES/extension.json",'r'))
dmg_default_thresholds = config.get('config',{}).get('OresFiltersThresholds',{}).get("value",{}).get("damaging",{})
gf_default_thresholds = config.get('config',{}).get('OresFiltersThresholds',{}).get("value",{}).get("goodfaith",{})
commit_dt = commit.committed_datetime

dmg_likelygood_threshold = dmg_default_thresholds.get('likelygood',{})
dmg_maybebad_threshold = dmg_default_thresholds.get('maybebad',{})
dmg_likelybad_threshold = dmg_default_thresholds.get('likelybad',{})
dmg_verylikelybad_threshold = dmg_default_thresholds.get('verylikelybad',{})
gf_likelygood_threshold = gf_default_thresholds.get('likelygood',{})
gf_maybebad_threshold = gf_default_thresholds.get('maybebad',{})
gf_likelybad_threshold = gf_default_thresholds.get('likelybad',{})

## this value is "False"
gf_verylikelybad_threshold = gf_default_thresholds.get('verylikelybad',{})

defaults = {    "damaging_likelygood_min": dmg_likelygood_threshold['min'], 
    "damaging_likelygood_max": dmg_likelygood_threshold['max'],
    "damaging_maybebad_min": dmg_maybebad_threshold['min'],
    "damaging_maybebad_max": dmg_maybebad_threshold['max'],
    "damaging_likelybad_min": dmg_likelybad_threshold['min'],
    "damaging_likelybad_max": dmg_likelybad_threshold['max'],
    "damaging_verylikelybad_min": dmg_verylikelybad_threshold['min'],
    "damaging_verylikelybad_max": dmg_verylikelybad_threshold['max'],
    "goodfaith_likelygood_min": gf_likelygood_threshold['min'], 
    "goodfaith_likelygood_max": gf_likelygood_threshold['max'],
    "goodfaith_maybebad_min": gf_maybebad_threshold['min'],
    "goodfaith_maybebad_max": gf_maybebad_threshold['max'],
    "goodfaith_likelybad_min": gf_likelybad_threshold['min'],
    "goodfaith_likelybad_max": gf_likelybad_threshold['max'],
    "goodfaith_verylikelybad_min": None,
    "goodfaith_verylikelybad_max": None}

json.dump(defaults,open("./data/default_threshholds.json",'w'))
