#!/usr/bin/env python3
import pandas as pd
from os import path

cutoffs = pd.read_csv("data/ores_rcfilters_cutoffs.csv")
wikis = set(cutoffs.wiki_db)
sets = []
for wiki_db in wikis:
    scores_file = "data/quarry_ores_scores/{0}_scores.csv".format(wiki_db)
    if path.exists(scores_file):
        scores = pd.read_csv(scores_file)
        ids = pd.read_csv("data/quarry_ores_scores/{0}_ids.csv".format(wiki_db))
        ids = ids.loc[ids.oresm_name == "b'damaging'"]
        
        ids = ids.set_index('oresm_id')
        scores = scores.set_index('oresc_model')
        # we only want scores from a damaging model
        scores = scores.join(ids, how='inner')
        scores = scores.rename({'oresc_probability':'prob_damaging', 'oresc_rev':'revision_id'}, axis=1)
        scores = scores.loc[:,['revision_id','prob_damaging']]
        scores['wiki_db'] = wiki_db
        sets.append(scores)

all_scores = pd.concat(sets)
all_scores.to_csv("data/scores_from_db.csv")
