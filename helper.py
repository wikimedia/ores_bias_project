# functions in this file can be run under any environment.
# this essentially means no pandas
import time
import subprocess
import dateutil
import mwapi
from sortedcontainers import SortedDict
import sys
import datetime
import git
import re
import os
import pickle
import fire
import json

# python 3.5 doesn't have datetime.fromisoformat
from dateutil import parser
fromisoformat = parser.isoparse

api = mwapi.Session("https://en.wikipedia.org",'ores bias project by groceryheist <nathante@uw.edu>')

sys.path.append("../../mw_revert_tool_detector")
from mwcomments.siteList import SiteList

siteList = dict(SiteList.from_api())

tmpdir = os.path.join(os.path.abspath('.'),'tmp')

if not os.path.exists(tmpdir):
    os.mkdir(tmpdir)

os.environ['TMPDIR'] = os.path.join(os.path.abspath('.'),'tmp')
repo_path = "../mediawiki-services-ores-deploy/"
editquality_repo_path = '../editquality_groceryheist'
wheels_repo_path = '../wheels'

date_commits = SortedDict()
wiki_date_commits = {}

editquality_commits = {}
wheels_commits = {}
    
repo = git.Repo(repo_path)
repo.git.checkout("-f", 'master')

editquality_repo = git.Repo(editquality_repo_path)
editquality_repo.git.checkout("-f", 'master')

wheels_repo = git.Repo(wheels_repo_path)
wheels_repo.git.checkout("-f", 'master')

commits = repo.iter_commits(paths=["submodules/editquality"])

wheels_commits_path = "ores_bias_data/wheels_commits.pickle"
editquality_commits_path = "ores_bias_data/editquality_commits.pickle"
date_commits_path = "ores_bias_data/date_commits.pickle"
wiki_date_commits_path = "ores_bias_data/wiki_date_commits.pickle"
lfs_transition_date = fromisoformat("2018-08-10")

if os.path.exists(wiki_date_commits_path) and os.path.exists(date_commits_path):
    wiki_date_commits = pickle.load(open(wiki_date_commits_path,'rb'))
    date_commits = pickle.load(open(date_commits_path,'rb'))
    editquality_commits = pickle.load(open(editquality_commits_path,'rb'))
    wheels_commits = pickle.load(open(wheels_commits_path,'rb'))
else:
    for commit in commits:
        editquality_repo.git.checkout('-f', 'master')
        wheels_repo.git.checkout('-f', 'master')
        commit_datetime = datetime.datetime.fromtimestamp(commit.committed_datetime.timestamp())
        if commit_datetime > lfs_transition_date:
            models_path = os.path.join(repo_path,'submodules/editquality/models')
            repo.git.checkout('-f', commit)

            try:
                editquality_submodule = repo.submodule("submodules/editquality")
                if hasattr(editquality_submodule,'update'): 
                    editquality_submodule.update(init=True, force=True, recursive=True)
            except Exception as e:
                print(commit_datetime)
                print(e)

        # find the most recent commit in the editquality repo
        else:
            models_path = os.path.join(editquality_repo_path,'models')
            editquality_commit = next(editquality_repo.iter_commits(until=commit_datetime, max_count=1))
            editquality_commits[commit.hexsha] = editquality_commit.hexsha
            editquality_repo.git.checkout('-f', editquality_commit)
            time.sleep(5)
            # reset to master

        found_prior_commit = False
        found_next_commit = False
        try:

            wheels_commit = next(wheels_repo.iter_commits(until=commit_datetime, max_count=1))

            wheels_commit_time = datetime.datetime.fromtimestamp(wheels_commit.committed_datetime.timestamp())
            wheels_time_diff = commit_datetime - wheels_commit_time
            print("time from wheels commit {0} to deploy:{1}".format(wheels_commit.hexsha[0:10], wheels_time_diff))
            found_prior_commit = True

        except StopIteration as e:
            pass

        try:
            wheels_commit2 = next(wheels_repo.iter_commits(since=commit_datetime, max_count=1))
            wheels_commit_time2 = datetime.datetime.fromtimestamp(wheels_commit2.committed_datetime.timestamp())
            wheels_time_diff2 = wheels_commit_time2 - commit_datetime

            print("time from deploy to wheels commit {0}:{1}".format(wheels_commit2.hexsha[0:10], wheels_time_diff2))
            found_next_commit = True
        except StopIteration as e:
            pass
        

        if found_prior_commit and found_next_commit:
            if wheels_time_diff2 <= datetime.timedelta(days=1):
                use_commit = wheels_commit2.hexsha
            else:
                use_commit = wheels_commit.hexsha


        elif found_next_commit:
            use_commit = wheels_commit2.hexsha
        else:
            print("found no wheels commit for commit {0} at {1}".format(commit, comit_datetime))
            
        wheels_commits[commit.hexsha] = use_commit
        print("using commit {0}".format(use_commit))

        date_commits[commit_datetime] = commit.hexsha
        model_re = re.compile(r'(.*)\.damaging\..*\.model')
        files = os.listdir(models_path)
        for f in files:
            wiki_db = model_re.findall(f)
            if len(wiki_db) > 0:
                d = wiki_date_commits.get(wiki_db[0], SortedDict())
                d[commit_datetime] = commit.hexsha
                wiki_date_commits[wiki_db[0]] = d

    pickle.dump(wheels_commits, open(wheels_commits_path,'wb'))
    pickle.dump(wiki_date_commits, open(wiki_date_commits_path,'wb'))
    pickle.dump(date_commits, open(date_commits_path,'wb'))
    pickle.dump(editquality_commits, open(editquality_commits_path,'wb'))

repo.git.checkout("-f", "master")
editquality_repo.git.checkout('-f', "master")
wheels_repo.git.checkout("-f", "master")

def lookup_commit_from_wiki_date(wiki_db, date):
    return lookup_commit_from_date(date, wiki_date_commits[wiki_db])

def lookup_commit_from_date(date, sorted_dict):
    if isinstance(date, str):
        date = fromisoformat(date)

    idx = sorted_dict.bisect_right(date)

    if idx > 0:
        idx = idx - 1
    commited_datetime = date_commits.keys()[idx]
    commit = date_commits[commited_datetime]

    return commit

def find_model_file(wiki_db, commit, model_type='damaging'):
    
    if commit in editquality_commits:
        models_path = os.path.join(editquality_repo_path, 'models')
    else:
        models_path = os.path.join(repo_path, 'submodules/editquality/models')
    
    model_re = r'{0}\.{1}\..*\.model'.format(wiki_db, model_type)
    files = os.listdir(models_path)
    model_files = [f for f in files if re.match(model_re,f)]
    if len(model_files) > 0:
        return os.path.join(models_path, model_files[0])

def get_wheels_package_versions(path):
    name_version_re = re.compile(r"(.*?)-(.*?)-.*.whl")
    files = os.listdir(path)
    matches = [name_version_re.findall(f) for f in files]
    for matchlist in matches:
        if len(matchlist) > 0:
            match = matchlist[0]
            if len(match) == 2:
                yield (match[0],match[1])

# load the environment according to the deploy
def load_model_environment(date = None, commit=None, wiki_db=None):

    if isinstance(date, str):
        date = fromisoformat(date)

    if commit is None and (wiki_db is None or date is None):
        raise ValueError("Commit or (Wiki_db and date) required to load environment")

    if commit is None:
        commit = lookup_commit_from_wiki_date(date, wiki_db)

    print("date:{0}, commit:{1}".format(date, commit))

    repo.git.checkout('-f', commit)
    
    curdir = os.path.abspath(".")
    os.chdir(repo_path)
    subprocess.run("git submodule sync --recursive",shell=True)
    os.chdir(curdir)

    print("loading editquality")
    if commit in editquality_commits:

        print('checkout {0} from {1}'.format(editquality_commits[commit], editquality_repo.working_dir))
        editquality_repo.git.checkout('-f', editquality_commits[commit])
        editquality_path = editquality_repo_path
    else:
        editquality_path = os.path.join(repo.working_dir,"submodules/editquality")
        try: 
            editquality_submodule = repo.submodule("submodules/editquality")            
            if hasattr(editquality_submodule,'sync'): 
                editquality_submodule.sync(init=True, recursive=True, force=True)
            if hasattr(editquality_submodule,'update'): 
                editquality_submodule.update(init=True, recursive=True, force=True)

        except git.exc.GitCommandError as e:
            print(e)

        except AttributeError as e:
            print(e)

    print("loading wheels")
    wheels_path = wheels_repo.working_dir
    if commit in wheels_commits:
        print("updating wheels to {0}".format(wheels_commits[commit]))
        wheels_repo.git.checkout("-f", wheels_commits[commit])

    else:
        print("loading wheels submodule")
        wheels_path = os.path.join(repo.working_dir,"submodules/wheels")
        try:
            wheels_submodule = repo.submodule("submodules/wheels")
            if hasattr(wheels_submodule, 'update'):
                wheels_submodule.update(init=True, recursive=True, force=True)

        except git.exc.GitCommandError as e:
            print(e)

    # the order of dependency priorities: wheels > repo > editquality_repo
    wheels_package_versions = dict(get_wheels_package_versions(wheels_path))
    
    repo_package_versions = {s[0]:s[1] for s in [l.strip().replace("-","_").split('==') for l in open(os.path.join(repo.working_dir,'frozen-requirements.txt'))]}

    packages = {**repo_package_versions, **wheels_package_versions}
    requirements = ["{0}=={1}\n".format(name, version) for name, version in packages.items()]

    with open("temp_requirements.txt",'w') as reqfile:
        reqfile.writelines(requirements)


    call = "source {0}/bin/activate".format(repo.working_dir)

    call = call + " && pip3 download -r temp_requirements.txt -d deps && pip3 install -r temp_requirements.txt --find-links=deps"

    print(editquality_path)
    call = call + " && cd {0} && python3 setup.py install && cd ../../..".format(editquality_path)

    print(call)
    subprocess.run(call, shell=True, executable="/bin/bash")
