from functools import reduce
from itertools import chain
import pandas as pd
import git
import re
import json
import subprocess
import datetime
import os
from project_settings import *

def load_mwconfig():
    out = subprocess.run(["php","-f","wmf-config_to_json.php"], capture_output=True)
    try:
        jsonobj = json.loads(out.stdout)
        return jsonobj
    except json.JSONDecodeError as E:
        print(E)
        print(out.stdout)

def unpack_threshholds(wiki_db, models):
    line = {}
    if wiki_db == 'default':
        return {}

    for model, levels in models.items():
        for threshhold, values in levels.items():
            if isinstance(values,bool):
                continue
            for level, value in values.items():
                key = "{0}_{1}_{2}".format(model,threshhold,level)
                line[key] = value

    return line

def unpack_old_threshholds(wiki_db, levels):
    return {"damaging_{0}".format(name):value for name,value in levels.items()}


def process_commit(commit):

    date = commit.committed_datetime
    date = datetime.datetime.fromtimestamp(date.timestamp())

    print(date)
    repo.git.checkout('-f', commit)

    json_config = load_mwconfig()

    json_config = json_config['settings']

    threshholds = json_config.get('wgOresFiltersThresholds',{})

    # This made it opt-in before some point in time after which it was opt-out
    rcfilters_enabled = json_config.get("wgEnableRcFiltersBetaFeature", {})

    extension_status = json_config.get('wgOresExtensionStatus',{})

    # whether the extension is installed at all
    useOres = json_config.get("wmgUseORES",{})

    # Just a kill switch that shouldn't have been used in practice
    useOresUi = json_config.get("wgOresUiEnabled",{})

    oldDamagingThreshholds = json_config.get("wgOresDamagingThresholds",{})

    # makes it available on watchlists by default.
    rcfilters_watchlist_enabled = json_config.get("wgStructuredChangeFiltersOnWatchlist",{})

    wikis = reduce(lambda l, r: set(r.keys()).union(l),[threshholds, rcfilters_enabled, extension_status, useOres, useOresUi, rcfilters_watchlist_enabled],{})

    for wiki_db in wikis:
        line = {"wiki_db":wiki_db,
                'date':date,
                'commitsha':commit.hexsha,
                "rcfilters_enabled":rcfilters_enabled.get(wiki_db,None),
                'extension_status':extension_status.get(wiki_db,None),
                'useOres':useOres.get(wiki_db,None),
                'useOresUi':useOresUi.get(wiki_db,None),
                'rcfilters_watchlist_enabled':rcfilters_watchlist_enabled.get(wiki_db,None)}

        line = {** line, ** unpack_threshholds(wiki_db, threshholds.get(wiki_db,{}))}
        line = {** line, ** unpack_old_threshholds(wiki_db, oldDamagingThreshholds.get(wiki_db,{}))}

        yield line

full_table_pickle = os.path.join(data_dir, "full_config_history.pickle")

if not os.path.exists(full_table_pickle):
    repo = git.Repo(path="operations-mediawiki-config")
    repo.git.checkout('-f',"master")
    commits = repo.iter_commits(paths=['wmf-config/InitialiseSettings.php'],since="2016-06-21")

    # we only care about damaging for now
    # we only care about 'maybe bad' and 'likely bad'
    # goal schema:
    ## wiki | date | dmg_$threshhold_min |dmg_$threshhold_max | gf_$threshhold_min | gf_$threshhold_max

    lines = list(chain(*map(process_commit, commits)))
    table1 = pd.DataFrame.from_records(lines)
    table1.to_pickle(full_table_pickle)

else:
    table1 = pd.read_pickle(full_table_pickle)

fr = table1[table1.wiki_db == 'frwiki']

distinct_cols = [
    'extension_status',
    'useOres',
    'rcfilters_enabled',
    'rcfilters_watchlist_enabled',
    'useOresUi',
    'damaging_verylikelybad_min',
    'damaging_verylikelybad_max',
    'damaging_likelybad_min',
    'damaging_likelybad_max',
    'damaging_maybebad_min',
    'damaging_maybebad_max', 
    'damaging_likelygood_min',
    'damaging_likelygood_max',
    'damaging_soft',
    'damaging_softest',
    'damaging_hard',
    'goodfaith_verylikelybad_min',
    'goodfaith_verylikelybad_max', 
    'goodfaith_likelybad_min',
    'goodfaith_likelybad_max',
    'goodfaith_maybebad_min',
    'goodfaith_maybebad_max',
    'goodfaith_likelygood_min',
    'goodfaith_likelygood_max']


def dedup_chronological(df, distinct_cols):
    df = df.sort_values(['wiki_db','date'])
    bywiki = df.groupby('wiki_db')[distinct_cols]
    # d.shift(1) == d if the previous entry is the same as the current entry
    identical = bywiki.apply(lambda d: ((d.shift(1) == d) | (d.shift(1).isna() & d.isna()))).all(1)
    return (df[~identical])


rcfilters_watchlist_available_date = datetime.datetime.fromisoformat("2017-09-19")
rcfilters_watchlist_default_date = datetime.datetime.fromisoformat("2018-06-16")

table = dedup_chronological(table1, distinct_cols)

for wiki in set(table.wiki_db):
    prev_date =  table.loc[ (table.wiki_db == wiki) & (table.date <= rcfilters_watchlist_available_date), ['date']].max()
    prev_date = list(prev_date)[0]
    new_row = table.loc[ (table.wiki_db == wiki) & (table.date == prev_date)].to_dict('records')
    if len(new_row) > 0:
        new_row = new_row[0]
        new_row["date"] = rcfilters_watchlist_available_date
        new_row["rcfilters_enabled"] = True
        table = table.append(new_row, ignore_index = True)

    prev_date =  table.loc[ (table.wiki_db == wiki) & (table.date <= rcfilters_watchlist_default_date), ['date']].max()
    prev_date = list(prev_date)[0]
    new_row = table.loc[ (table.wiki_db == wiki) & (table.date == prev_date)].to_dict('records')
    if len(new_row) > 0:
        new_row = new_row[0]
        new_row["date"] = rcfilters_watchlist_default_date
        new_row["rcfilters_enabled"] = True
        new_row["rcfilters_watchlist_enabled"] = True
        table = table.append(new_row, ignore_index=True)

default = table.loc[table.wiki_db=='default',['date','extension_status','useOres','useOresUi','rcfilters_enabled','useOresUi','commitsha','rcfilters_watchlist_enabled']]

table = table[table.wiki_db !='default']

enwiki_status = table.loc[table.wiki_db == 'enwiki',['date','extension_status','useOres','rcfilters_enabled','useOresUi','commitsha']]
plwiki_status = table.loc[table.wiki_db == 'plwiki',['date','extension_status','useOres','rcfilters_enabled','useOresUi']]

table['has_ores'] = table.useOres | table.useOresUi

table = table.sort_values(by=['date','wiki_db'])
default = default.sort_values(by=['date'])

default_asof = pd.merge_asof(table,default,on="date",suffixes=['','_default'],direction='backward').loc[:,['wiki_db','date','rcfilters_enabled_default', 'extension_status_default','rcfilters_watchlist_enabled_default','commitsha_default']]

default_asof = default_asof.sort_values(by=['wiki_db','date'])
table = pd.merge(table,default_asof, on=['date','wiki_db'],how='left')
table = table.sort_values(by=['wiki_db','date'])

table['has_extension'] = (table.extension_status == 'on') | (table.extension_status_default == 'on')

table['has_beta_extension'] = (table.extension_status == 'beta') | ((table.extension_status_default == 'beta') & ~table.has_extension)

has_rcfilters = (table.has_ores == True) & ( (table.date >= max(default[default.rcfilters_enabled.isna()].date)) | ( (table.rcfilters_enabled_default == True) & (table.rcfilters_enabled != False)) | (table.rcfilters_enabled == True)) | (table.useOresUi == True)

table['has_rcfilters'] = has_rcfilters

table['has_rcfilters_watchlist'] = (table.has_ores == True) & ( (table.rcfilters_watchlist_enabled == True) | (( (table.rcfilters_watchlist_enabled_default == True) |  (table.date >= max(default[default.rcfilters_watchlist_enabled.isna()].date)) | (table.date >= rcfilters_watchlist_default_date)  & (table.rcfilters_watchlist_enabled != False))))


table.to_csv(os.path.join(data_dir, "mw_config_history.csv"),index=False)

#cutoffs = table.loc[:,['wiki_db','date','commitsha','has_ores','has_rcfilters','has_rcfilters_watchlist']]

cutoffs = dedup_chronological(table, ['has_ores','has_rcfilters','has_rcfilters_watchlist'])

# get more precise cutoffs from the server admin log on wikitech
sal_pages = ["Server_admin_log/Archive_29","Server_admin_log/Archive_30","Server_admin_log/Archive_31","Server_admin_log/Archive_32","Server_admin_log/Archive_33","Server_admin_log/Archive_34","Server_admin_log/Archive_35", "Server_admin_log/Archive_36","Server_admin_log/Archive_37","Server_Admin_Log"]

import mwapi

api = mwapi.Session('https://wikitech.wikimedia.org',user_agent='ores bias project by groceryheist')

query = api.get(action="query",titles=sal_pages,params ={"prop":['revisions'],"rvprop":['content']})
pages = query['query']['pages']

init_sync_re = re.compile(r"\*.*?(\d\d\:\d\d).*?(?:\ [\d\d:\d\d:\d\d]\ )?.*Synchronized wmf-config/InitialiseSettings.php.*")

init_sync_ts = []

for pageid in pages:
    data = api.get(action='parse', pageid=pageid, prop=['templates','text','sections','wikitext'])
    matches = init_sync_re.finditer(data['parse']['wikitext']['*'])
    section_offsets = [(d['byteoffset'],d['line']) for d in data['parse']['sections']]
    for match in matches:
        match_offset = match.span()[0]
        day = [(line,offset) for offset, line in section_offsets if offset is not None and offset <= match_offset]
        # the log is sorted
        day = day[-1][0]
        time = match.groups()[0]
        dt = datetime.datetime.fromisoformat(day + " " + time)
        init_sync_ts.append(dt)

init_sync_ts = pd.Series(init_sync_ts)

def find_deploy_time(commit_time):
    return min(init_sync_ts[init_sync_ts > commit_time])

#set the cutoff timestamp to the deployment immediately following the commit
cutoffs['commit_dt'] = cutoffs.date

cutoffs['deploy_dt'] = cutoffs.commit_dt.apply(find_deploy_time)
cutoffs['deploy_gap'] = cutoffs.deploy_dt - cutoffs.commit_dt  
cutoffs = cutoffs.drop('date',1)
cutoffs = cutoffs.reindex(columns=["wiki_db","commitsha","has_ores","has_rcfilters","has_rcfilters_watchlist","commit_dt","deploy_dt","deploy_gap"])
cutoffs.to_csv(os.path.join(data_dir,"ores_rcfilters_cutoffs.csv"), index=False)
