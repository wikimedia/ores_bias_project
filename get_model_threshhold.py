# functions in this file depend on specific versions of revscoring and editquality. 
import fire
import sys
from helper import *

import revscoring

def get_threshhold(model_path, query, outfile, commit = None, append = True):
    model = revscoring.Model.load(open(model_path))
    threshhold =  model.info['statistics']['thresholds'][True][query]
    outline = '\t'.join(str(v) for v in [query, threshhold, commit] if v is not None)
    if append: 
        flag = 'a'
    else:
        flag = 'w'

    # if not os.path.exists(outfile):
    #     o = open(outfile,'c')
    #     o.close() 

    with open(outfile,'a') as of:
        of.write(outline + '\n')
    return outline


fire.Fire(get_threshhold)
