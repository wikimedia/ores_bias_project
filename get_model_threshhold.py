# functions in this file depend on specific versions of revscoring and editquality. 
import fire
import sys

import revscoring

def get_threshhold(model_path, query, outfile, commit = None, append = True):
    if hasattr(revscoring, 'Model'):
        model = revscoring.Model.load(open(model_path))
    else:
        model = revscoring.ScorerModel.load(open(model_path))

    try: 
        threshhold =  model.info['statistics']['thresholds'][True][query]

    except (NotImplementedError, TypeError) as e:
        threshhold = 'model info not implemented in this version of revscoring'

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
