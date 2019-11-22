import os
from get_labels import load_labels
from ores.utilities.score_revisions import run as call_ores

def _score_labels(labels,context,label_file, overwrite = False):
    if not os.path.exists("../datasets/scored_labels"):
        os.makedirs("../datasets/scored_labels")

    output_filename = "../datasets/scored_labels/{0}".format(os.path.split(label_file)[1])

    if os.path.exists(output_filename) and overwrite == False:
        return

    ores_host = "https://ores.wikimedia.org/"
    user_agent = "Ores bias analysis project by Nate TeBlunthuis <groceryheist@uw.edu>"

    output_file = open(output_filename,'w')

    call_ores(ores_host,
              user_agent,
              context,
              model_names = ["damaging","goodfaith"],
              parallel_requests=4,
              retries=2,
              input=labels,
              output=output_file,
              batch_size=50,
              verbose=True)

    output_file.close()

def score_labels(label_files, wikis):
    for wiki, label_file in zip(wikis,label_files):
        _score_labels(load_labels(label_file),wiki,label_file)
