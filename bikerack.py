from revscoring.extractors import api
from revscoring.utilities.extract import extract, ConfiguredExtractor
import yamlconf
from revscoring.dependencies import Dependent
import editquality.feature_lists

# simple recursive function for finding a near-zero of a monotonic function
def bin_search_0(f, range, threshhold=0.001, max_depth=10):
    midpoint = (range[1] - range[0]) / 2
    score = f(midpoint)
    upper = f(range[1])
    lower = f(range[0])
    max_depth = max_depth - 1
    rg = np.array([range[0], midpoint, range[1]])
    print('\t'.join(list(map(str,rg))))
    print('\t'.join(list(map(str,map(f,rg)))))
    if max_depth == 0 or all([abs(score) >= abs(upper), (lower - upper) <= 1e-10], ):
        return rg[np.argmin(np.abs([lower, score, upper]))]
    elif abs(score) < threshhold:
        return midpoint

    ret = []
    if score != upper:
        ret.append(bin_search_0(f, [range[0], midpoint], threshhold, max_depth))
    if score != lower:
        ret.append(bin_search_0(f, [midpoint, range[1]], threshhold, max_depth))
        
    scores = list(map(f, ret))
    
    return ret[np.argmin(np.abs(scores))]

# get the features
def extract_features(label_file,context):
    rev_ids = [json.loads(label) for label in load_labels(label_file)]
    
    session = mwapi.Session(
        host= "https://{0}.wikipedia.org".format(
            context.replace("wiki","")),
        user_agent="Ores bias analysis project by Nate TeBlunthuis <groceryheist@uw.edu>")

    dependent_names = ["editquality.feature_lists.{0}.damaging".format(context),
                  "editquality.feature_lists.{0}.goodfaith".format(context)]
    dependents = []
    for dependent_path in dependent_names:
        dependent_or_list = yamlconf.import_path(dependent_path)
        if isinstance(dependent_or_list, Dependent):
            dependents.append(dependent_or_list)
        else:
            dependents.extend(dependent_or_list)

    extractor = api.Extractor(session)
    features = extract(dependents, rev_ids, extractor,extractors=os.cpu_count() - 1)
    return features

odef pred_wiki(df, wiki, model='damaging'):
    prob_col = df.loc[df.wiki == wiki, "prob_{0}".format(model)]
    true_col = df.loc[df.wiki == wiki, "true_{0}".format(model)]
    t, acc = choose_threshhold(true_col, prob_col)
    df.loc[df.wiki == wiki, 'pred_{0}'.format(model)] = (prob_col > t)
    return (df, acc)


def gen_preds(df, model='damaging'):
    wikis = set(df.wiki)
    accs = []
    for wiki in wikis:
        df, acc = pred_wiki(df, wiki, model)
        accs.append(acc)

    return (df, accs, wikis)


# just use grid search to choose a threshhold
# that maximizes accuracy
def choose_threshhold(true_col, prob_col, n=500):
    ts = np.arange(0, n + 1) / n

    def acc(t):
        tpr = np.mean((prob_col >= t) & (true_col == True))
        tnr = np.mean((prob_col < t) & (true_col == False))
        return (tpr + tnr)
    t = np.argmax(np.abs(list(map(acc, ts))))

    return (ts[t], acc(t))



import pandas as pd
import numpy as np
from qwikidata.sparql import return_sparql_query_results
from move_labels_to_datalake import grouper
import re

df = pd.read_pickle("labeled_newcomers_anons_wikidata_ids.pickle")
df.pp_value = df.pp_value.str.decode('utf-8')
df = df.reset_index()
endpoint = "https://query.wikidata.org/sparql"

# we want to get instance of, and sex or gender
base_query = """ SELECT ?s1 ?s2Label
            WHERE  {
                      {?s1 wdt:P31 wd:Q5}
                      {?s1 wdt:P21 ?s2}
            FILTER(?s1 IN (%s))
            SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                   }
"""

entities = ["wd:{0}".format(e) for e in set(df.pp_value)]

N = int(np.ceil(len(entities) / 300))
batches = list(grouper(entities, 300))

found_entities = []
genders = []
entityre = re.compile("http://www.wikidata.org/entity/(.*)")
i = 0 
for batch in batches:
    i = i + 1
    print("batch {0} / {1}".format(i,N))
    query = base_query % ','.join(batch)

    response  = return_sparql_query_results(query)

    bindings = response['results']['bindings']
    found_entities.extend(entityre.findall(b['s1']['value'])[0] for b in bindings)
    genders.extend(b['s2Label']['value'] for b in bindings)
    

df2 = pd.DataFrame({"entity": found_entities, "gender":genders})
df = pd.merge(df, df2, left_on='pp_value', right_on='entity', how='left')
df = df.drop("pp_sortkey",1)
df.to_pickle("labeled_pages_genders.pickle")
# unpack the jsons

# join back to the original tabl
#!/usr/bin/env python3
import pandas as pd
import glob
import dns.resolver
import pymysql as mysql

def get_mediawiki_section_dbname_mapping(mw_config_path, use_x1):
    db_mapping = {}
    if use_x1:
        dblist_section_paths = [mw_config_path.rstrip('/') + '/dblists/all.dblist']
    else:
        dblist_section_paths = glob.glob(mw_config_path.rstrip('/') + '/dblists/s[0-9]*.dblist')
    for dblist_section_path in dblist_section_paths:
        with open(dblist_section_path, 'r') as f:
            for db in f.readlines():
                db_mapping[db.strip()] = dblist_section_path.strip().rstrip('.dblist').split('/')[-1]
                
    return db_mapping


def get_dbstore_host_port(db_mapping, use_x1, dbname):
    if dbname == 'staging':
        shard = 'staging'
    elif use_x1:
        shard = 'x1'
    else:
        try:
            shard = db_mapping[dbname]
        except KeyError:
            raise RuntimeError("The database {} is not listed among the dblist files of the supported sections."
                               .format(dbname))
    answers = dns.resolver.query('_' + shard + '-analytics._tcp.eqiad.wmnet', 'SRV')
    host, port = str(answers[0].target), answers[0].port
    return (host,port)

def get_db_conn(dbname):
    mw_config_path = "/srv/mediawiki-config"
    use_x1 = False
    db_mapping = get_mediawiki_section_dbname_mapping(mw_config_path,use_x1)
    host,port = get_dbstore_host_port(db_mapping,use_x1,dbname='enwiki')

    conn = mysql.connect(host = host,
                         port = port,
                         database='enwiki',
                         read_default_file = "/etc/mysql/conf.d/analytics-research-client.cnf",
                         charset='utf8',
                         use_unicode=True,
                         autocommit=False)
    return conn

edit_data = pd.read_pickle("labeled_newcomers_anons.pickle")

wikis = set(edit_data.wiki)


base_query = "select pp_page, pp_value, pp_sortkey from page_props WHERE pp_page IN ({0}) AND pp_propname = 'wikibase_item'"

df_parts = []
for dbname in wikis:
    print(dbname)
    conn = get_db_conn(dbname)

    page_ids = set(edit_data.loc[edit_data.wiki == dbname].pageid)
    page_ids = ','.join([str(pid) for pid in page_ids])

    query = base_query.format(page_ids)
    df_part  = pd.read_sql_query(query,con=conn,index_col="pp_page")
    df_parts.append(df_part)

df = pd.concat(df_parts)

df.to_pickle("labeled_newcomers_anons_wikidata_ids.pickle")
