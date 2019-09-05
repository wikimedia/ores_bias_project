import json
import fire
import pickle
import os
import re
from importlib import reload
import git
import pandas as pd
import datetime
import subprocess
import sys
from sortedcontainers import SortedDict
import io
import mwapi
import dateutil
# python 3.5 doesn't have datetime.fromisoformat
fromisoformat = dateutil.parser.isoparse
api = mwapi.Session("https://en.wikipedia.org",'ores bias project by groceryheist <nathante@uw.edu>')

sys.path.append("../../mw_revert_tool_detector")
from mwcomments.siteList import SiteList

siteList = dict(SiteList.from_api())

editquality = None
revscoring = None

tmpdir = os.path.join(os.path.abspath('.'),'tmp')

if not os.path.exists(tmpdir):
    os.mkdir(tmpdir)

os.environ['TMPDIR'] = os.path.join(os.path.abspath('.'),'tmp')
repo_path = "../mediawiki-services-ores-deploy/"
editquality_repo_path = '../editquality_groceryheist'

date_commits = SortedDict()
wiki_date_commits = {}

editquality_commits = {}
    
repo = git.Repo(repo_path)
repo.git.checkout("-f", 'master')

editquality_repo = git.Repo(editquality_repo_path)
editquality_repo.git.checkout("-f", 'master')

commits = repo.iter_commits(paths=["submodules/editquality"])

editquality_commits_path = "ores_bias_data/editquality_commits.pickle"
date_commits_path = "ores_bias_data/date_commits.pickle"
wiki_date_commits_path = "ores_bias_data/wiki_date_commits.pickle"
lfs_transition_date = fromisoformat("2018-08-10")

if os.path.exists(wiki_date_commits_path) and os.path.exists(date_commits_path):
    print('unpickling commit history ')
    wiki_date_commits = pickle.load(open(wiki_date_commits_path,'rb'))
    date_commits = pickle.load(open(date_commits_path,'rb'))
    editquality_commits = pickle.load(open(editquality_commits_path,'rb'))
else:
    for commit in commits:
        commit_datetime = pd.to_datetime(datetime.datetime.fromtimestamp(commit.committed_datetime.timestamp()))
        if commit_datetime > lfs_transition_date:
            models_path = os.path.join(repo_path,'submodules/editquality/models')
            repo.git.checkout(commit)
            try:
                editquality_submodule = repo.submodule("submodules/editquality")
                if hasattr(editquality_submodule,'update'): 
                    editquality_submodule.update(init=True)
            except Exception as e:
                print(commit_datetime)
                print(e)

        # find the most recent commit in the editquality repo
        else:
            models_path = os.path.join(editquality_repo_path,'models')
            editquality_commit = next(editquality_repo.iter_commits(until=commit_datetime, max_count=1))
            editquality_commits[commit.hexsha] = editquality_commit.hexsha

        date_commits[commit_datetime] = commit.hexsha
        model_re = re.compile(r'(.*)\.damaging\..*\.model')
        files = os.listdir(models_path)
        for f in files:
            wiki_db = model_re.findall(f)
            if len(wiki_db) > 0:
                d = wiki_date_commits.get(wiki_db[0], SortedDict())
                d[commit_datetime] = commit.hexsha
                wiki_date_commits[wiki_db[0]] = d

    pickle.dump(wiki_date_commits, open(wiki_date_commits_path,'wb'))
    pickle.dump(date_commits, open(date_commits_path,'wb'))
    pickle.dump(editquality_commits, open(editquality_commits_path,'wb'))

def lookup_commit_from_wiki_date(wiki_db, date):
    return lookup_commit_from_date(date, wiki_date_commits[wiki_db])

def lookup_commit_from_date(date, sorted_dict = date_commits):
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

# we actually always want to load up the environment from the deploy module.
def load_model_environment(date = None, commit = None):
    if commit is None:
        commit = lookup_commit_from_date(date)

    print("date:{0}, commit:{1}".format(date, commit))

    repo.git.checkout('-f', commit)
    subprocess.run("cd {0} && git submodule sync --recursive && cd ..".format(repo_path), shell = True)
    if commit in editquality_commits:
        editquality_repo.git.checkout('-f', editquality_commits[commit])
        editquality_path = editquality_repo_path
    else:
        editquality_path = os.path.join(repo_path,"submodules/editquality")
        try: 
            editquality_submodule = repo.submodule("submodules/editquality")
            if hasattr(editquality_submodule,'update'): 
                editquality_submodule.update(init=True, recursive=True,force=True,progress=True)
        except git.exc.GitCommandError as e:
            print(e)

        except AttributeError as e:
            print(e)

    # install mediawiki-services-ores-deploy
    subprocess.run("cd {0} && make".format(repo_path), shell=True)

    subprocess.run("cd ../../..", shell=True)
    global editquality
    global revscoring
    import editquality
    import revscoring
    reload(editquality)
    reload(revscoring)

def load_model(date, wiki_db, model_type='damaging'):
    commit = lookup_commit_from_wiki_date(wiki_db, date)
    load_model_environment(date=date, commit=commit)
    model_path = find_model_file(wiki_db=wiki_db, commit=commit)
    model = revscoring.Model.load(open(model_path))
    return model

def get_threshhold(model, query):
    return model.info['statistics']['thresholds'][True][query]

class Ores_Archaeologist(object):
    def get_threshhold(self, wiki_db, date, threshhold_string, outfile = None, append=True):
        model = load_model(date = fromisoformat(date),
                           wiki_db = wiki_db,
                           model_type = "damaging"
        )
        threshhold = get_threshhold(model, threshhold_string)
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
    
    # some versions of revscoring don't handle errors properly so I need to hot-patch it.'
    # basically this will be the same functionality as in revscoring.score_processor but will handle errors instead of raising them.
    def score_revisions(self, wiki_db, uri, date=None, commit=None, load_environment = True, model_type = 'damaging', infile = "<stdin>"):

        if load_environment:
            load_model_environment(date=date, commit=commit)

        if commit is None:
            commit = lookup_commit_from_wiki_date(wiki_db, date)

        model_file = find_model_file(wiki_db, commit, model_type)

        run = subprocess.run(["revscoring", "score", model_file,"--host={0}".format(uri), '--rev-ids={0}'.format(infile)], shell=False, stdout=subprocess.PIPE)
        print(run.args)
        print(run.returncode)
        return run.stdout


    def score_history(self, cutoff_revisions, preprocess=True):
        if preprocess:
            cutoff_revisions = self.preprocess_cutoff_history(cutoff_revisions)

        parts = []
        for commit in set(cutoff_revisions.commit):
            scored_revisions = self.score_commit_revisions(commit, cutoff_revisions, preprocess=False)
            parts.append(scored_revisions)
        return pd.concat(parts)

    def score_commit_revisions(self, commit, cutoff_revisions, preprocess=True, load_environment=True):
        if preprocess:
            cutoff_revisions = self.preprocess_cutoff_history(cutoff_revisions)

        if load_environment:
            load_model_environment(commit=commit)

        all_revisions = cutoff_revisions.loc[cutoff_revisions.commit == commit]
        parts = []

        for wiki_db in set(all_revisions.wiki_db):
            scored_revisions = self.score_wiki_commit_revisions(commit, wiki_db, all_revisions, preprocess=False)
            parts.append(scored_revisions)

        return pd.concat(parts)

    def score_wiki_commit_revisions(self, commit, wiki_db, all_revisions, preprocess=True, load_environment=False):
        if preprocess:
            all_revisions = self.preprocess_cutoff_history(all_revisions)

        uri = siteList[wiki_db]
        wiki_db_revisions = all_revisions.loc[(all_revisions.wiki_db == wiki_db) & (all_revisions.commit==commit)]
        revids = list(wiki_db_revisions.revision_id)
        # write revids to a temporary file
        tmpfilename = "{0}_{1}_revids.tmp".format(commit[0:10], wiki_db)

        with open(tmpfilename,'w') as tempfile:
            tempfile.write("rev_id\n")
            tempfile.writelines([str(r) + '\n' for r in revids])
                    
        score_jsons = self.score_revisions(wiki_db, uri, commit=commit, load_environment=load_environment, model_type="damaging", infile=tmpfilename)

        score_jsons = score_jsons.decode()

        scores = []
        for line in score_jsons.split('\n'):
            if line == '':
                continue
            fields = line.split('\t')
            revid = fields[0]

            if len(fields) < 2:
                probability = None

            else:
                result = json.loads(fields[1])

                probability = result.get('probability', None)
            
                if probability is not None:
                    probability = probability['true']

            scores.append({"revision_id":int(revid), "prob_damaging":probability})


        scores = pd.DataFrame.from_records(scores)
        all_revisions = pd.merge(all_revisions, scores, on=['revision_id'], how='left')
        return all_revisions
        
    def preprocess_cutoff_history(self, cutoff_revisions):
        
        if isinstance(cutoff_revisions,str):
            cutoff_revisions = pd.read_csv(cutoff_revisions, sep=',',parse_dates=['event_timestamp','date','period_start','period_end'],quotechar='\"',infer_datetime_format=True,error_bad_lines=False,escapechar='\\')

        # cutoff_revisions.date = pd.to_datetime(cutoff_revisions.date)
        # cutoff_revisions.event_timestamp = pd.to_datetime(cutoff_revisions.event_timestamp)
        # cutoff_revisions.period_start = pd.to_datetime(cutoff_revisions.period_start)
        # cutoff_revisions.period_end = pd.to_datetime(cutoff_revisions.period_end)

        # we need to find the right model for each 
        # asssign commits to cutoff_revisions
        wikis_with_models = set(wiki_date_commits.keys())

        cutoff_revisions = cutoff_revisions.loc[cutoff_revisions.wiki_db.isin(wikis_with_models),:]

        commits = cutoff_revisions.apply(lambda row: lookup_commit_from_wiki_date(row.wiki_db, row.event_timestamp), axis=1)


        cutoff_revisions['commit'] = commits

        cutoff_revisions = cutoff_revisions.sort_values(by=['commit','wiki_db'],axis=0)

        return cutoff_revisions
      

class Ores_Archaeologist_Api(Ores_Archaeologist):

    def _wrap(self, res):
        buf = io.StringIO()
        res.to_csv(buf, index=False,quotechar='\"',escapechar="\\")
        return buf.getvalue()

    def score_wiki_commit_revisions(self, commit, wiki_db, all_revisions, preprocess=True, load_environment=False, wrap=False, output=None):
        res = super().score_wiki_commit_revisions(commit, wiki_db, all_revisions, preprocess, load_environment)
        csv = self._wrap(res)
        if output is not None:
            with open(output,'w') as of:
                of.write(csv)
        if wrap:
            return csv
        else:
            return res

    def score_commit_revisions(self, commit, cutoff_revisions, preprocess=True, load_environment=True, 
                               wrap=False,output=None):
        res = super().score_commit_revisions(commit, cutoff_revisions, preprocess, load_environment)
        csv = self._wrap(res)
        if output is not None:
            with open(output,'w') as of:
                of.write(csv)
        if wrap:
            return csv
        else:
            return res

    def score_history(self, cutoff_revisions, preprocess=True, wrap=False, output=None):
        res = super().score_history(cutoff_revisions, preprocess)
        csv = self._wrap(res)
        if output is not None:
            with open(output,'w') as of:
                of.write(csv)
        if wrap:
            return csv
        else:
            return res

if __name__ == "__main__":
    fire.Fire(Ores_Archaeologist_Api)
    os.rmdir(tmpdir)
