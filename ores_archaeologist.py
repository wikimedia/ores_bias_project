#!/usr/bin python3.6
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
import shutil
from functools import partial
import psutil

call_log = "syscalls.sh"

# loop here sleep before we check for a hang.
def tryWaitKill(proc):
    try:
        proc.wait(5)
    except psutil.TimeoutExpired as e:
        
        if proc.status() == psutil.STATUS_ZOMBIE:
            return False
                        
        # check if all child processes are stuck
        children = proc.children(recursive=True)
        active = []
        for p in children:
            try:
                active.append(p.cpu_percent(0.2) > 0.2)
            except psutil.NoSuchProcess as e:
                active.append(True)

        if not any(active):
            return False

        else:
            tryWaitKill(proc)
    return True

def reap_children(proc, timeout=3):
    "Tries hard to terminate and ultimately kill all the children of this process."
    def on_terminate(proc):
        print("process {} terminated with exit code {}".format(proc, proc.returncode))

    procs = proc.children(recursive=True)
    # send SIGTERM
    for p in procs:
        try:
            p.terminate()
        except psutil.NoSuchProcess:
            pass
    gone, alive = psutil.wait_procs(procs, timeout=timeout, callback=on_terminate)
    if alive:
        # send SIGKILL
        for p in alive:
            print("process {} survived SIGTERM; trying SIGKILL".format(p))
            try:
                p.kill()
            except psutil.NoSuchProcess:
                pass
        gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
        if alive:
            # give up
            for p in alive:
                print("process {} survived SIGKILL; giving up".format(p))

def tryparsefloat(s):
    try:
        return float(s)
    except ValueError as e:
        return None
    
class Ores_Archaeologist(object):

    def __init__(self):
        self.cache_file = "data/revscoring_cache.pickle"

    def _call_and_retry(self, call, max_retries=5):
        while max_retries > 0 :
            with psutil.Popen(call, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash', universal_newlines=True) as proc:
                max_retries = max_retries - 1

                success = tryWaitKill(proc)
                print("starting process:{0}".format(call))
                

                if not success:
                    # send sigterm
                    reap_children(proc)
                    continue

                # then look at the process tree and see if the subprocess is stuck

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
            
    def get_threshold(self, wiki_db, date, threshold_string, outfile = None, append=True, model_type='damaging', load_environment=True, commit = None):

        if isinstance(date,str):
            date = fromisoformat(date)

        if commit is None:
            commit = lookup_commit_from_wiki_date(wiki_db, date)

        if load_environment is True: 
            load_model_environment(date=date, commit=commit)

        model_path = find_model_file(wiki_db, commit, model_type)
        set_revscoring_version(model_path, commit)

        # make sure that we run using the right virtualenv
        threshold_temp = "model_thresholds.txt"

        call = "source {0}/bin/activate && python3 get_model_threshold.py --model_path={1} --query=\"{2}\" --outfile={3} --append=True --commit={4} && source ./bin/activate".format(repo.working_dir, model_path, threshold_string, threshold_temp, commit)
        
        with open(call_log,'a') as log:
            log.write(call + '\n')

        # poll every 5 minutes. If the proccess is dead restart it. 
        proc = self._call_and_retry(call)
        if proc is not None:
            with open(threshold_temp,'r') as f:
                lines = f.readlines()
                return lines[-1]

    def get_all_thresholds(self, cutoffs):
        default_thresholds = json.load(open("data/default_thresholds.json",'r'))

        def lookup_threshold(key, threshold):
            value = tryparsefloat(threshold)
            if value is not None:
                return value
            if pd.isna(value) or len(threshold)==0:
                # pre_cutoff_thresholds = default_thresholds.loc[default_thresholds.date<=row.deploy_dt]
                # min_dt = pre_cutoff_thresholds.date.max()
                # threshold = list(pre_cutoff_thresholds.loc[pre_cutoff_thresholds.date==min_dt,key])[0]
                threshold = default_thresholds.get(key,np.nan)
                if isinstance(threshold, float):
                    return threshold

            if key.startswith('goodfaith'):
                model_type = 'goodfaith'
            else:
                model_type = 'damaging'

            res = self.get_threshold(wiki_db = row.wiki_db, date=row.deploy_dt, threshold_string = threshold, model_type = model_type, load_environment=first)
            if res is not None:
                value = res.split('\t')[1]
                return tryparsefloat(value)

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
                threshold = row[key]
                value = lookup_threshold(key, threshold)
                row[string_value_dict[key]] = value
            output_rows.append(row)

        result = pd.DataFrame.from_records(output_rows)
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
            
        #        call = "source {0}/bin/activate && python3 get_model_threshold.py --model_path={1} --query=\"{2}\" --outfile={3} --append=True --commit={4}".format(repo.working_dir, model_path, threshold_string,threshold_temp, commit)

        # if model_file is None:

        call = "source {0}/bin/activate".format(repo.working_dir)

        # sometimes the repo doesn't get loaded the first try.
        if model_file is None:
            load_model_environment(date=date, commit=commit, wiki_db=wiki_db)
            model_file = find_model_file(wiki_db, commit, model_type)

        if model_file is None:
            return None

        call = call + " && {0}/bin/python3".format(repo.working_dir) + " revscoring_score_shim.py " + model_file + " --host={0} --rev-ids={1} && source ./bin/activate".format(uri, infile)

        with open(call_log,'a') as log:
            log.write(call + '\n')

        print(call)

        output = self._call_and_retry(call)
        if output is None:
            call = call + " && {0}/bin/python3".format(repo.working_dir) + " revscoring_score_shim.py " + model_file + " --host={0} --rev-ids={1} --io-workers=1 --cpu-workers=1 && source ./bin/activate".format(uri, infile)
            
        print("--commit={0}".format(commit))
        return output

    def score_history(self, cutoff_revisions, preprocess=True, use_cache=True, add_thresholds=True):
        
        if preprocess:
            cutoff_revisions = self.preprocess_cutoff_history(cutoff_revisions)

        # for period 1 use the latest model
        period_1 = cutoff_revisions.loc[cutoff_revisions.period=='period1']
        period_2 = cutoff_revisions.loc[cutoff_revisions.period=='period2']
        time_last_commit = period_2.groupby('wiki_db').event_timestamp.max().reset_index()
        last_commit = pd.merge(period_2, time_last_commit, on=['wiki_db','event_timestamp']).reset_index()
        last_commit = last_commit.loc[:,['wiki_db','commit']]
        period_1 = period_1.drop('commit', 1)
        period_1 = pd.merge(period_1, last_commit, on=['wiki_db'])
        cutoff_revisions = pd.concat([period_1, period_2], sort=True)

        parts = []
        for commit in set(cutoff_revisions.commit):
            scored_revisions = self.score_commit_revisions(commit, cutoff_revisions, preprocess=False, load_environment=True, use_cache=use_cache, add_thresholds=add_thresholds)
            parts.append(scored_revisions)

        result =  pd.concat(parts,
                         sort=False,
                         ignore_index=True)
        
        scored_revids = result.loc[:,["wiki_db", "revision_id", "prob_damaging"]]
        scored_revids.to_pickle(self.cache_file)

        return result

    def score_commit_revisions(self, commit, cutoff_revisions, preprocess=True, load_environment=True, use_cache=True, add_thresholds = True):

        if 'pred_damaging' in cutoff_revisions.columns and not cutoff_revisions.pred_damaging.isna().any():
            return cutoff_revisions
        
        if preprocess:
            cutoff_revisions = self.preprocess_cutoff_history(cutoff_revisions)

        if load_environment:
            load_model_environment(commit=commit)
            
        commit_revisions = cutoff_revisions.loc[cutoff_revisions.commit == commit]
        parts = []

        for wiki_db in set(commit_revisions.wiki_db):

            wiki_commit_revisions = commit_revisions.loc[ (commit_revisions.wiki_db == wiki_db)]
            scored_revisions = self.score_wiki_commit_revisions(commit, wiki_db, wiki_commit_revisions, preprocess=False, load_environment=False, use_cache=use_cache, add_thresholds=add_thresholds)
            parts.append(scored_revisions)

        return pd.concat(parts,
                         sort=False,
                         ignore_index=True)
            

        

    def score_wiki_commit_revisions(self, commit, wiki_db, all_revisions, preprocess=True, load_environment=True, use_cache=True, add_thresholds = True):
        if preprocess:
            all_revisions = self.preprocess_cutoff_history(all_revisions)

        if load_environment:
            load_model_environment(commit=commit, wiki_db=wiki_db)


        if use_cache is True:
            if os.path.exists(self.cache_file):
                cached_scores = pd.read_pickle(self.cache_file)

        all_revisions = pd.merge(all_revisions,cached_scores, on=['wiki_db','revision_id'])

        # don't score revisions we have already scored
        if 'prob_damaging' in all_revisions.columns and not all_revisions.prob_damaging.isna().any():
            return all_revisions

        uri = siteList[wiki_db]

        if use_cache is False:
            wiki_db_revisions = all_revisions.loc[(all_revisions.wiki_db == wiki_db) & (all_revisions.commit==commit)]
        else:
            wiki_db_revisions = all_revisions.loc[(all_revisions.wiki_db == wiki_db) & (all_revisions.commit==commit) & (all_revisions.prob_damaging.isna())]

        revids = list(wiki_db_revisions.revision_id)
        # write revids to a temporary file
        
        tmpfilename = "temp_files/{0}_{1}_revids.tmp".format(commit[0:10], wiki_db)

        non_int_revids = []
        with open(tmpfilename,'w') as tempfile:
            tempfile.write("rev_id\n")
            for r in revids:
                try:
                   r = int(r)
                   tempfile.write(str(r) + '\n')
                except ValueError as e:
                    non_int_revids.append(r)
                    
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

                    if type(result) is str:
                        probability = None
                        error = result
                    else:
                        probability = result.get('probability', None)

                    if probability is not None:
                        probability = probability['true']

                    else:
                        error = line

                scores.append({"revision_id":str(revid), "prob_damaging":probability, "revscoring_error":error})

        for r in non_int_revids:
            scores.append({"revision_id":r, "prob_damaging":None, "revscoring_error":"revid is not an integer"})

        if len(scores) > 0:
            scores = pd.DataFrame.from_records(scores)
            all_revisions = pd.merge(all_revisions, scores, on=['revision_id'], how='left')

        else:
            all_revisions.loc[:, 'prob_damaging'] = np.NaN
            all_revisions.loc[:, "revscoring_error"] = "Unknown error. Check log. Process died?"


        if add_thresholds is True:
            all_revisions = self.lookup_revision_thresholds(all_revisions)
            
        return all_revisions
        
    # there's only ever one wikidb here
    # all the revisions must be from the same commit
    # the revscoring environment must be already built
    def lookup_revision_thresholds(self, revisions):

        # find the correct threshold strings for these revisions
        cutoffs = pd.read_csv("data/ores_rcfilters_cutoffs.csv", parse_dates=['deploy_dt'])

        wiki_db = revisions.wiki_db[0]
        cutoffs = cutoffs.loc[cutoffs.wiki_db == wiki_db]
        cutoffs = cutoffs.sort_values('deploy_dt')
        revisions = revisions.sort_values('event_timestamp')
        revisions = pd.merge_asof(revisions, cutoffs, left_on='event_timestamp', right_on='deploy_dt', by='wiki_db', direction='backward')

        deploy_dt = cutoffs.loc[cutoffs.deploy_dt <= revisions.event_timestamp.min(), 'deploy_dt'].max()

        if pd.isnull(deploy_dt):
            deploy_dt = cutoffs.deploy_dt.max()

        commit = revisions.commit[0]

        threshold_names =['damaging_likelybad_min', 
                          'damaging_likelybad_max'
                          'damaging_likelygood_max',
                          'damaging_likelygood_min',
                          'damaging_maybebad_max',
                          'damaging_maybebad_min',
                          'damaging_verylikelybad_max',
                          'damaging_verylikelybad_min',
                          'goodfaith_likelybad_max',
                          'goodfaith_likelybad_min',
                          'goodfaith_likelygood_max',
                          'goodfaith_likelygood_min',
                          'goodfaith_maybebad_max',
                          'goodfaith_maybebad_min',
                          'goodfaith_verylikelybad_max',
                          'goodfaith_verylikelybad_min']
                                        
        cutoffs = cutoffs.loc[cutoffs.deploy_dt == deploy_dt].reset_index()
                
        thresholds = self.get_all_thresholds(cutoffs)

        value_names = [s+'_value' for s in threshold_names]

        revisions = pd.concat([revisions, thresholds.loc[:,value_names + threshold_names]])
        return revisions
            
        
    # if we are pre-cutoff then use scores from the latest model
        
        # call get_thresholds
        

        # merge and return.
        

    def preprocess_cutoff_history(self, cutoff_revisions):
        
        if isinstance(cutoff_revisions,str):
            cutoff_revisions = pd.read_csv(cutoff_revisions, sep=',',parse_dates=['event_timestamp','period1_start','period2_end','date_first','date_last'],quotechar='\"',infer_datetime_format=True,error_bad_lines=False,escapechar='\\')


        cutoff_revisions.revision_id = cutoff_revisions.revision_id.astype(str)
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

    def get_threshold(self, *args, **kwargs):
        cls = Ores_Archaeologist()
        return cls.get_threshold(*args, **kwargs)

    def get_all_thresholds(self, cutoffs, output = None):
        cls = Ores_Archaeologist()
        return self._wrap(cls.get_all_thresholds, output, cutoffs)

if __name__ == "__main__":
    fire.Fire(Ores_Archaeologist_Api)
    shutil.rmtree(tmpdir)
