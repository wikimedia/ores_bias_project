import fire
import pickle
import os
import re
import pandas as pd
import subprocess
import sys
import io
from helper import *

class Ores_Archaeologist(object):
    def get_threshhold(self, wiki_db, date, threshhold_string, outfile = None, append=True, model_type='damaging'):

        if isinstance(date,str):
            date = fromisoformat(date)
        commit = lookup_commit_from_wiki_date(wiki_db,date)
        model_path = find_model_file(wiki_db, commit, model_type)
        load_model_environment(date=date, commit=commit)

        # make sure that we run using the right virtualenv
        threshhold_temp = "model_threshholds.txt"

        call = "source {0}/bin/activate && python3 get_model_threshhold.py --model_path={1} --query=\"{2}\" --outfile={3} --append=True --commit={4}".format(repo.working_dir, model_path, threshhold_string,threshhold_temp, commit)
        print(call)
        proc = subprocess.run(call, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
        out = proc.stdout
        with open(threshhold_temp,'r') as f:
            lines = f.readlines()
            
        return lines[-1]

    # some versions of revscoring don't handle errors properly so I need to hot-patch it.'
    # basically this will be the same functionality as in revscoring.score_processor but will handle errors instead of raising them.
    def score_revisions(self, wiki_db, uri, date=None, commit=None, load_environment=True, model_type='damaging', infile="<stdin>"):

        if load_environment:
            load_model_environment(date=date, commit=commit)

        if commit is None:
            commit = lookup_commit_from_wiki_date(wiki_db, date)

        model_file = find_model_file(wiki_db, commit, model_type)


#        call = "source {0}/bin/activate && python3 get_model_threshhold.py --model_path={1} --query=\"{2}\" --outfile={3} --append=True --commit={4}".format(repo.working_dir, model_path, threshhold_string,threshhold_temp, commit)

        proc = subprocess.run("source {0}/bin/activate".format(repo.working_dir) + " && {0}/bin/python3".format(repo.working_dir) + " score_revisions_shim.py " + model_file + " --host={0} --rev-ids={1}".format(uri, infile), shell=True, stdout=subprocess.PIPE, executable="/bin/bash")

        print(proc.args)
        print(proc.returncode)
        output = proc.stdout.decode()

        return output

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

        scores = []

        for line in score_jsons.split('\n'):
            error = None
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

                else:
                    error = line
                    
            scores.append({"revision_id":int(revid), "prob_damaging":probability, "revscoring_error":error})


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
      

## TODO: use a seperate environment and interpreter for running revscoring
## use nltk assets from wheels
## get package versions from wheels
## if pip fails or the model fails check for an update to wheels.

class Ores_Archaeologist_Api(Ores_Archaeologist):

    def _wrap(self, super_func, output, *args, **kwargs):
        res = super_func(*args, **kwargs)
        
        buf = io.StringIO()
        
        res.to_csv(buf, index=False,quotechar='\"',escapechar="\\")
        csv = buf.getvalue()
        if output is not None:
            with open(output,'w') as of:
                of.write(csv)
        return csv

    def score_wiki_commit_revisions(self, commit, wiki_db, all_revisions, preprocess=True, load_environment=False, wrap=False, output=None):

        return self._wrap(super().score_wiki_commit_revisions, None, commit, wiki_db, all_revisions, preprocess, load_environment)

    def score_commit_revisions(self, commit, cutoff_revisions, preprocess=True, load_environment=True, 
                               wrap=False,output=None):

        return self._wrap(super().score_commit_revisions, output, commit, cutoff_revisions, preprocess, load_environment)

    def score_history(self, cutoff_revisions, preprocess=True, wrap=False, output=None):

        return self._wrap(super().score_history, output, cutoff_revisions, preprocess)

if __name__ == "__main__":
    fire.Fire(Ores_Archaeologist_Api)
    os.rmdir(tmpdir)
