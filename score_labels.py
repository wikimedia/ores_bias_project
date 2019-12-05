
import json
import os
from get_labels import load_labels
from ores_archaeologist import Ores_Archaeologist
from datetime import datetime
import pandas as pd
import pickle
siteList = dict(pickle.load(open("data/wikimedia_sites.pickle",'rb')))

default_mock_date = datetime(2019,12,1)
# instead of ores we want to use ores_archaeologist to score the revisions so we can them for a
# particular date for reproducibility. But frankly this should be a lower priority. 
def _score_labels(label_file_wikis, mock_date=default_mock_date):
    oa = Ores_Archaeologist()
    first = True

    for lf, wiki in label_file_wikis:
        labels = [json.loads(l) for l in load_labels(lf)]
        rows = []
        revids = [d.get('rev_id',None) for d in labels]

        tmpfilename = "temp_files/{0}_label_revids.tmp".format(wiki)
        with open(tmpfilename,'w') as of:
            of.write("rev_id\n")
            of.writelines('\n'.join(str(id) for id in revids[0:10]))

        wiki_db = wiki
        date = mock_date
        uri = siteList[wiki_db]

        damaging_scores = oa.score_revisions(wiki_db = wiki_db,
                                            uri = uri,
                                            date=mock_date,
                                            load_environment = first,
                                            model_type = "damaging",
                                            infile = tmpfilename
                                    )

        goodfaith_scores = oa.score_revisions(wiki_db = wiki_db,
                                            uri = uri,
                                            date=mock_date,
                                            load_environment = False,
                                            model_type = "goodfaith",
                                            infile = tmpfilename
                                    )
        first = False
        scored_labels = []
        it = zip(damaging_scores.split("\n"), labels)
        for score, label in it:
            fields = score.split("\t")
            if len(fields) == 2:
                rev_id = fields[0]
                labeled = {"damaging":fields[1],
                           "label":label}
                scored_labels.append(labeled)

        it = zip(goodfaith_scores.split("\n"), scored_labels)
        scored_labels = []

        for score, label in it:
            fields = score.split("\t")
            if len(fields) == 2: 
                rev_id = fields[0]
                labeled = {"damaging":label["damaging"],
                           "goodfaith":fields[1],
                           "label":label["label"]
                }
            scored_labels.append(labeled)
            
        yield (wiki_db, scored_labels)

def score_labels(label_files, wikis, overwrite=False):
    label_file_wikis = zip(label_files, wikis)
    return list(_score_labels(label_file_wikis))

def get_thresholds(wikis, mock_date = default_mock_date, load_environment = True):
    first = True
    oa = Ores_Archaeologist()
    threshold_dict = {}

    cutoffs = pd.read_csv("data/ores_rcfilters_cutoffs.csv", parse_dates = ['deploy_dt'])
    last_deploy = cutoffs[cutoffs.deploy_dt <= mock_date].groupby('wiki_db').deploy_dt.max()
    cutoffs = pd.merge(cutoffs, last_deploy, on=['wiki_db','deploy_dt'])
    
    load_environment = False
    thresholds = [oa.get_all_thresholds(cutoffs[cutoffs["wiki_db"] == wiki_db], wiki_db, mock_date, load_environment) for wiki_db in wikis]
    return pd.concat(thresholds, sort=True)


# def _score_labels(labels,context,label_file, overwrite = False):
#     if not os.path.exists("data/scored_labels"):
#         os.makedirs("data/scored_labels")

#     output_filename = "data/scored_labels/{0}".format(os.path.split(label_file)[1])

#     if os.path.exists(output_filename) and overwrite == False:
#         return

#     ores_host = "https://ores.wikimedia.org/"
#     user_agent = "Ores bias analysis project by Nate TeBlunthuis <groceryheist@uw.edu>"

#     output_file = open(output_filename,'w')

#     call_ores(ores_host,
#               user_agent,
#               context,
#               model_names = ["damaging","goodfaith"],
#               parallel_requests=4,
#               retries=2,
#               input=labels,
#               output=output_file,
#               batch_size=50,
#               verbose=True)

#     output_file.close()


