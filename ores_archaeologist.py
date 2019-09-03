import fire
import os
import re
from importlib import reload
import git
import pandas as pd
import datetime
import subprocess
import sys

import os
editquality = None
revscoring = None

tmpdir = os.path.join(os.path.abspath('.'),'tmp')

if not os.path.exists(tmpdir):
    os.mkdir(tmpdir)

os.environ['TMPDIR'] = os.path.join(os.path.abspath('.'),'tmp')
repo_path = "mediawiki-services-ores-deploy/"
def lookup_commit_from_date(date):

    repo = git.Repo(repo_path)
    repo.git.checkout("-f", 'master')
    commits = repo.iter_commits(paths=["submodules/editquality"])
    for commit in commits:
        commit_datetime = datetime.datetime.fromtimestamp(commit.committed_datetime.timestamp())
        print("date:{0}, commit:{1}".format(commit_datetime, commit))
        if commit_datetime < date:

            try: 
                repo.git.checkout('-f', commit)
                subprocess.run("cd {0} && git submodule sync --recursive && cd ..".format(repo_path), shell = True)
                editquality_submodule = repo.submodule("submodules/editquality")
                if hasattr(editquality_submodule,'update'): 
                    editquality_submodule.update(init=True, recursive=True,force=True,progress=True)
            except git.exc.GitCommandError as e:
                print(e)

            except AttributeError as e:
                print(e)

            return commit


def find_model_file(wiki_db, model_type='damaging'):
    path = 'mediawiki-services-ores-deploy/submodules/editquality/models'
    model_re = r'{0}\.{1}\..*\.model'.format(wiki_db, model_type)
    files = os.listdir(path)
    model_file = [f for f in files if re.match(model_re,f)][0]
    return os.path.join(repo_path,"submodules","editquality","models",model_file)

def load_model_environment(date, wiki_db, model_type='damaging'):
    commit = lookup_commit_from_date(date)

    subprocess.run("cd mediawiki-services-ores-deploy/submodules/editquality && python3 setup.py install && pip3 download -r requirements.txt -d deps && pip3 install -r requirements.txt --find-links=deps", shell=True)
    subprocess.run("cd ../../..",shell=True)
    global editquality
    global revscoring
    import editquality
    import revscoring
    reload(editquality)
    reload(revscoring)

def load_model(date, wiki_db, model_type='damaging'):
    load_model_environment(date = date, wiki_db=wiki_db, model_type=model_type)
    model_path = find_model_file(wiki_db=wiki_db, model_type=model_type)
    model =  revscoring.Model.load(open(model_path))
    return model


def get_threshhold(model, query):
    return model.info['statistics']['thresholds'][True][query]

class Ores_Archaeologist(object):
    def get_threshhold(self, wiki_db, date, threshhold_string, outfile = None, append=True):
        model_file = load_model(date = datetime.datetime.fromisoformat(date),
                           wiki_db = wiki_db,
                           model_type = "damaging"
        )
        threshhold = get_threshhold(model_file, threshhold_string)
        outline = '\t'.join(str(v) for v in [wiki_db, date, threshhold_string, threshhold])
        if outfile is None:
            print(outline)
        else:
            if append:
                flag = 'a'
            else:
                flag = 'w'
                of = open(outfile, flag)
                of.write(outline)
    
    def score_revisions(self, wiki_db, uri, date, model_type = 'damaging', infile = "<stdin>"):
        date = datetime.datetime.fromisoformat(date)
        load_model_environment(date, wiki_db, model_type)
        model_file = find_model_file(wiki_db, model_type)
        subprocess.call("revscoring score {0} --host={1} --rev-ids={2}".format(model_file, uri, infile), shell=True)


if __name__ == "__main__":
    fire.Fire(Ores_Archaeologist)
    os.rmdir(tmpdir)
