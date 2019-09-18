import fire
import pickle
import os
import re
import pandas as pd
import subprocess
import sys
import io
from helper import *
import numpy as np

call_log = "syscalls.sh"

class Ores_Archaeologist(object):

    def _call_and_retry(self, call, poll_interval = 60*5, max_retries=5):
        while max_retries > 0 :
            with subprocess.Popen(call, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash', universal_newlines=True) as proc:
                max_retries = max_retries - 1
                print("starting process:{0}".format(call))
                (results, errors) = proc.communicate()
                print(errors)
                if proc.returncode == 0:
                    return results
        return None
                    # try:
                    #     (results, errors) = proc.communicate(timeout=poll_interval)
                    #     if proc.returncode == 0:
                    #         return results
                    #     else:
                    #         print(errors)
                    #         return None
                    # # except subprocess.TimeoutExpired as e:
                    #     # check stderr
                    #     success = False
                    #     status = proc.poll()
                    #     if status is  None:
                    #         print("process may have stalled, trying to terminate")
                    #         # try to terminate it and then kill it
                    #         (results, errors) = proc.communicate()
                    #         term_tries = 0
                    #         while True:
                    #             success = False
                    #             if max_terminate_tries > 0:
                    #                 max_terminate_tries = max_terminate_tries - 1
                    #                 proc.terminate()
                    #                 try:
                    #                     proc.wait(10)
                    #                     print(errors)
                    #                     return results
                    #                 except subprocess.TimeoutExpired as e1:
                    #                     pass
                                    
                    # finally:
                    #     if (success is True) or (proc.returncode == 0):
                    #         print("success")

                    #     if proc.returncode != 0:
                    #         if proc.stderr:
                    #             print(proc.stderr.read())
                    #         if max_proc_tries < 0:
                    #             return None
            
    def get_threshhold(self, wiki_db, date, threshhold_string, outfile = None, append=True, model_type='damaging', load_model_environment=True):

        if isinstance(date,str):
            date = fromisoformat(date)

        commit = lookup_commit_from_wiki_date(wiki_db, date)
        model_path = find_model_file(wiki_db, commit, model_type)
        if load_model_environment: 
            load_model_environment(date=date, commit=commit)

        # make sure that we run using the right virtualenv
        threshhold_temp = "model_threshholds.txt"

        call = "source {0}/bin/activate && python3 get_model_threshhold.py --model_path={1} --query=\"{2}\" --outfile={3} --append=True --commit={4} && source ./bin/activate".format(repo.working_dir, model_path, threshhold_string,threshhold_temp, commit)
        
        with open(call_log,'a') as log:
            log.write(call + '\n')

        # poll every 5 minutes. If the proccess is dead restart it. 
        proc = self._call_and_retry(call)
        if proc is not None:
            with open(threshhold_temp,'r') as f:
                lines = f.readlines()
                return lines[-1]


    def get_all_threshholds(self, cutoffs, output = None):

        if isinstance(cutoffs, str):
            cutoffs = pd.read_csv(cutoffs)

        string_value_dict = {'damaging_likelybad_max':'damaging_likelybad_max_value',
                             'damaging_likelybad_min':'damaging_likelybad_min_value',
                             'damaging_likelygood_max':'damaging_likelygood_max_value',
                             'damaging_likelygood_min':'damaging_likelygood_min_value',
                             'damaging_maybebad_max':'damaging_maybebad_max_value',
                             'damaging_maybebad_min':'damaging_maybebad_min_value',
                             'damaging_verylikelybad_max':'damaging_verylikelybad_max_value',
                             'damaging_verylikelybad_min':'damaging_verylikelybad_min_value',
                             'goodfaith_bad_max':'goodfaith_bad_max_value',
                             'goodfaith_bad_min':'goodfaith_bad_min_value',
                             'goodfaith_good_max':'goodfaith_good_max_value',
                             'goodfaith_good_min':'goodfaith_good_min_value',
                             'goodfaith_likelybad_max':'goodfaith_likelybad_max_value',
                             'goodfaith_likelybad_min':'goodfaith_likelybad_min_value',
                             'goodfaith_likelygood_max':'goodfaith_likelygood_max_value',
                             'goodfaith_likelygood_min':'goodfaith_likelygood_min_value',
                             'goodfaith_maybebad_max':'goodfaith_maybebad_max_value',
                             'goodfaith_maybebad_min':'goodfaith_maybebad_min_value',
                             'goodfaith_verylikelybad_max':'goodfaith_verylikelybad_max_value',
                             'goodfaith_verylikelybad_min':'goodfaith_verylikelybad_min_value'
                             }
                             
        output_rows = []
        for k, row in cutoffs.iterrows():
            first = True
            for key in string_value_dict.keys():

                threshhold = row[key]
                try:
                    value = float(threshhold)
                except ValueError as e:
                    if len(threshhold) == 0 or pd.isna(threshhold):
                        value = pd.np.NaN
                    else:
                        if key.startswith('goodfaith'):
                            model_type = 'goodfaith'
                        else:
                            model_type = 'damaging'
                        res = self.get_threshhold(wiki_db = row.wiki_db, date=row.deploy_dt, threshhold_string = threshhold, model_type = model_type, load_model_environment=first)
                        value = res.split('\t')[1]
                        
                first = False
                row[string_value_dict[key]] = value
            output_rows.append(row)

        result = pd.from_records(output_rows)
        if output is not None:
            with open(output, 'w') as outfile:
                result.to_csv(outfile, index=False)
        return result

    # some versions of revscoring don't handle errors properly so I need to hot-patch it.'
    # basically this will be the same functionality as in revscoring.score_processor but will handle errors instead of raising them.
    def score_revisions(self, wiki_db, uri, date=None, commit=None, load_environment=True, model_type='damaging', infile="<stdin>"):

        if commit is None:
            commit = lookup_commit_from_wiki_date(wiki_db, date)

        if load_environment:
            load_model_environment(date=date, commit=commit, wiki_db=wiki_db)

        print(editquality_repo.git.status())

        model_file = find_model_file(wiki_db, commit, model_type)
            
        #        call = "source {0}/bin/activate && python3 get_model_threshhold.py --model_path={1} --query=\"{2}\" --outfile={3} --append=True --commit={4}".format(repo.working_dir, model_path, threshhold_string,threshhold_temp, commit)

        # if model_file is None:
        #     import pdb; pdb.set_trace()

        call = "source {0}/bin/activate".format(repo.working_dir) + " && {0}/bin/python3".format(repo.working_dir) + " revscoring_score_shim.py " + model_file + " --host={0} --rev-ids={1} && source ./bin/activate".format(uri, infile)

        with open(call_log,'a') as log:
            log.write(call + '\n')

        print(call)
        output = self._call_and_retry(call)
        print("--commit={0}".format(commit))
        return output

    def score_history(self, cutoff_revisions, preprocess=True):
        if preprocess:
            cutoff_revisions = self.preprocess_cutoff_history(cutoff_revisions)

        parts = []
        for commit in set(cutoff_revisions.commit):
            scored_revisions = self.score_commit_revisions(commit, cutoff_revisions, preprocess=False)
            parts.append(scored_revisions)
        return pd.concat(parts, sort=False)

    def score_commit_revisions(self, commit, cutoff_revisions, preprocess=True, load_environment=True):
        if preprocess:
            cutoff_revisions = self.preprocess_cutoff_history(cutoff_revisions)

        if load_environment:
            load_model_environment(commit=commit)

        all_revisions = cutoff_revisions.loc[cutoff_revisions.commit == commit]
        parts = []

        for wiki_db in set(all_revisions.wiki_db):
            scored_revisions = self.score_wiki_commit_revisions(commit, wiki_db, all_revisions, preprocess=False, load_environment=False)
            parts.append(scored_revisions)

        return pd.concat(parts, sort=False)

    def score_wiki_commit_revisions(self, commit, wiki_db, all_revisions, preprocess=True, load_environment=True):
        if preprocess:
            all_revisions = self.preprocess_cutoff_history(all_revisions)

        if load_environment:
            load_model_environment(commit=commit, wiki_db=wiki_db)

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
        if score_jsons is not None:
            for line in score_jsons.split('\n'):
                error = None
                if line == '':
                    continue
                fields = line.split('\t')
                revid = fields[0]

                if len(fields) < 2:
                    probability = np.NaN
                else:
                    result = json.loads(fields[1])

                    probability = result.get('probability', None)
                    if probability is not None:
                        probability = probability['true']

                    else:
                        error = line

                scores.append({"revision_id":int(revid), "prob_damaging":probability, "revscoring_error":error})

        if len(scores) > 0:
            scores = pd.DataFrame.from_records(scores)
            all_revisions = pd.merge(all_revisions, scores, on=['revision_id'], how='left')

        else:
            all_revisions.loc[:, 'prob_damaging'] = np.NaN
            all_revisions.loc[:, "revscoring_error"] = "Unknown error. Check log. Process died?"

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

class Ores_Archaeologist_Api():


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

        cls = Ores_Archaeologist()

        return self._wrap(cls.score_wiki_commit_revisions, None, commit, wiki_db, all_revisions, preprocess, load_environment)

    def score_commit_revisions(self, commit, cutoff_revisions, preprocess=True, load_environment=True, 
                               wrap=False,output=None):

        cls = Ores_Archaeologist()

        return self._wrap(cls.score_commit_revisions, output, commit, cutoff_revisions, preprocess, load_environment)

    def score_history(self, cutoff_revisions, preprocess=True, wrap=False, output=None):
        cls = Ores_Archaeologist()

        return self._wrap(cls.score_history, output, cutoff_revisions, preprocess)


    def score_revisions(self, *args, **kwargs):
        cls = Ores_Archaeologist()
        return cls.score_revisions(*args, **kwargs)

    def get_threshhold(self, *args, **kwargs):
        cls = Ores_Archaeologist()
        return cls.get_threshhold(*args, **kwargs)

    def get_all_threshholds(self, *args, **kwargs):
        cls = Ores_Archaeologist()
        return self._wrap(cls.get_all_threshholds(*args, **kwargs))

if __name__ == "__main__":
    fire.Fire(Ores_Archaeologist_Api)
    os.rmdir(tmpdir)
