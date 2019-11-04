import git
import json
import pandas as pd
repo = git.Repo("../mediawiki-extensions-ORES")
repo.git.checkout("-f","master")
commits = repo.iter_commits(paths="extension.json")
found_maybebad_thresholds = set()
found_likelybad_thresholds = set()
found_verylikelybad_thresholds = set()
dates = []
damaging_maybebad_min = []
damaging_maybebad_max = []
damaging_likelybad_min = []
damaging_likelybad_max = []
damaging_verylikelybad_min = []
damaging_verylikelybad_max = []
for commit in commits:
    repo.git.checkout('-f',commit)
    config = json.load(open("../mediawiki-extensions-ORES/extension.json",'r'))
    default_thresholds = config.get('config',{}).get('OresFiltersThresholds',{}).get("value",{}).get("damaging",{})
    commit_dt = commit.committed_datetime
    maybebad_threshold = default_thresholds.get('maybebad',{})
    maybebad_threshold_str = json.dumps(maybebad_threshold)
    likelybad_threshold = default_thresholds.get('likelybad',{})
    likelybad_threshold_str = json.dumps(likelybad_threshold)
    verylikelybad_threshold = default_thresholds.get('verylikelybad',{})
    verylikelybad_threshold_str = json.dumps(verylikelybad_threshold)
    if not all([maybebad_threshold_str in found_maybebad_thresholds,
               likelybad_threshold_str in found_likelybad_thresholds,
                verylikelybad_threshold_str in found_verylikelybad_thresholds]):

        found_maybebad_thresholds.add(maybebad_threshold_str)
        found_likelybad_thresholds.add(likelybad_threshold_str)
        found_verylikelybad_thresholds.add(verylikelybad_threshold_str)
        damaging_maybebad_min.append(maybebad_threshold.get('min',None))
        damaging_maybebad_max.append(maybebad_threshold.get('max',None))
        damaging_likelybad_min.append(likelybad_threshold.get('min',None))
        damaging_likelybad_max.append(likelybad_threshold.get('max',None))
        damaging_verylikelybad_min.append(verylikelybad_threshold.get('min',None))
        damaging_verylikelybad_max.append(verylikelybad_threshold.get('max',None))
        dates.append(commit_dt)

# damaging_maybebad_min damaging_maybebad_max damaging_likelybad_min damaging_likelybad_max damaging_verylikelybad_min damaging_verylikelybad_max

defaults = pd.DataFrame({"date":dates,
                         "damaging_maybebad_min":damaging_maybebad_min,
                         "damaging_maybebad_max":damaging_maybebad_max,
                         "damaging_likelybad_min":damaging_likelybad_min,
                         "damaging_likelybad_max":damaging_likelybad_max,
                         "damaging_verylikelybad_min":damaging_verylikelybad_min,
                         "damaging_verylikelybad_max":damaging_verylikelybad_max})

defaults.to_json("./data/deafult_threshholds.json")
