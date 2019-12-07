import pickle
from numpy import nan

class ORESConfidenceLevel(object):
    def __init__(self, name, threshholds, inverse = False):
        self.threshholds = threshholds
        self.name = name
        self.pred_col_name = "pred_{0}".format(self.name)
        self.fp_col_name = "fp_{0}".format(self.name)
        self.fn_col_name = "fn_{0}".format(self.name)
        self.inverse = inverse


    def in_level(self, wiki, scores):
        interval = self.threshholds[wiki]
        if interval is None or any([i is None for i in interval]):
            print(interval)
            return None

        if interval[0] > interval[1]:
            raise ValueError("interval is not ordered")

        if scores is None:
            return None

        return (scores > interval[0]) & (scores < interval[1])


    def gen_preds(self, df, model):
        wikis = set(df.wiki)
        true_col_name = "true_{0}".format(model)
        prob_col_name = "prob_{0}".format(model)
        for wiki in wikis:
            prob_col = df.loc[df.wiki == wiki, prob_col_name]
            true_col = df.loc[df.wiki == wiki, true_col_name]
            df.loc[df.wiki==wiki, self.pred_col_name] = self.in_level(wiki, prob_col)

        if not self.inverse:
            df[self.fp_col_name] = \
            ((df.loc[:,self.pred_col_name] == True) & (df.loc[:,true_col_name] == False)).astype("double")

            df[self.fn_col_name] = \
                ((df.loc[:,self.pred_col_name] == False) & (df.loc[:,true_col_name] == True)).astype("double")

        else: #self.inverse
            df[self.fp_col_name] = \
            ((df.loc[:,self.pred_col_name] == False) & (df.loc[:,true_col_name] == True)).astype("double")

            df[self.fn_col_name] = \
                ((df.loc[:,self.pred_col_name] == True) & (df.loc[:,true_col_name] == False)).astype("double")

            
        df.loc[df[self.pred_col_name].isna(),self.fp_col_name] = None
        df.loc[df[self.pred_col_name].isna(),self.fn_col_name] = None
        return df


# df is [wiki_db, min_value, max_value]
def pdquery_to_tuple(df):
    def _pdquery_to_tuple(df):
        for row in df.iterrows():
            t = row[1]
            wiki_db = t[0].replace("wiki","")
            minval = t[1]
            maxval = t[2]
            if minval is None:
                minval = nan

            if maxval is None:
                maxval = nan

            yield (wiki_db, (minval, maxval))

    return dict(_pdquery_to_tuple(df))
    
thresholds = pickle.load(open("data/label_thresholds.pickle",'rb'))

dmg_levels = []
gf_levels = []

dmg_unlikely_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','damaging_likelygood_min_value', 'damaging_likelygood_max_value']])

dmg_levels.append(ORESConfidenceLevel("dmg_unlikely", dmg_unlikely_threshholds, inverse=True))

dmg_maybe_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','damaging_maybebad_min_value', 'damaging_maybebad_max_value']])

dmg_levels.append(ORESConfidenceLevel("dmg_maybe", dmg_maybe_threshholds))

dmg_likely_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','damaging_likelybad_min_value', 'damaging_likelybad_max_value']])

dmg_levels.append(ORESConfidenceLevel("dmg_likely", dmg_likely_threshholds))

dmg_very_likely_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','damaging_verylikelybad_min_value', 'damaging_verylikelybad_max_value']])

dmg_levels.append(ORESConfidenceLevel("dmg_very_likely", dmg_very_likely_threshholds))

gf_very_likely_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','goodfaith_likelygood_min_value', 'goodfaith_likelygood_max_value']])

#goodfaith thresholds go very_likely > likely > unlikely > very_unlikely
gf_levels.append(ORESConfidenceLevel("gf_very_likely", gf_very_likely_threshholds, inverse=False))

gf_likely_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','goodfaith_maybebad_min_value', 'goodfaith_maybebad_max_value']])

gf_levels.append(ORESConfidenceLevel("gf_likely", gf_likely_threshholds, inverse=True))

gf_unlikely_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','goodfaith_likelybad_min_value', 'goodfaith_likelybad_max_value']])

gf_levels.append(ORESConfidenceLevel("gf_unlikely", gf_unlikely_threshholds,inverse=True))

gf_very_unlikely_threshholds = pdquery_to_tuple(thresholds.loc[:,['wiki_db','goodfaith_verylikleybad_min_value', 'goodfaith_verylikelybad_max_value']])

gf_levels.append(ORESConfidenceLevel("gf_very_unlikely", gf_very_unlikely_threshholds, inverse=True))

# old thresholds copied manually from wikis

# dmg_unlikely_threshholds = {'ar':(0,0.168), 'bs':(0,0.198), 'ca':(0,0.619), 'cs':(0,0.038),'data':(0,0.283),'en':(0,0.305), 'es':(0,0.208),'esbooks':(0,0.123), 'et':(0,0.231), 'fi':(0,0.00361), 'fr':(0,0.098), 'he':(0,0.006), 'hu':(0,0.068),'it':(0,0.151), 'ko':(0,0.283), 'lv':(0,0.148), 'nl':(0,0.573), 'pl':(0,0.038), 'pt':(0,0.253), 'ro':(0,0.246), 'ru':(0,0.484), 'sq':(0,0.087), 'sr':(0,0.157), 'sv':(0,0.518), 'tr':(0,0.148)}

# dmg_maybe_threshholds = {'ar':(0.153,1),'bs':(0.131,1), 'ca':(0.195,1), 'cs':(0.52,1),'data':None, 'en':(0.147,1), 'es':(0.527,1), 'esbooks':None, 'et':(0.151,1), 'fi':(0.01533,1), 'fr':(0.08,1), 'he':None, 'hu':(0.103,1), 'it':(0.15,1), 'ko':(0.153,1), 'lv':(0.281,1), 'nl':(0.378,1), 'pt':(0.229,1),'pl':None, 'ro':(0.296,1), 'ru':(0.365,1), 'sq':(0.121,1), 'sr':(0.058,1), 'sv':(0.172,1), 'tr':(0.194,1)}

# dmg_likely_threshholds = {'ar':(0.455,1),'bs':(0.346,1), 'ca':(0.779,1), 'cs':(0.623,1), 'data':(0.387,1),'en':(0.626,1), 'es':(0.85,1), 'esbooks':(0.697,1), 'et':(0.641,1), 'fi':(0.25513,1), 'fr':(0.774,1), 'he':(0.181,1), 'hu':(0.805,1), 'it':(0.67,1), 'ko':(0.699,1), 'lv':(0.801,1), 'nl':(0.795,1), 'pl':(0.941,1), 'pt':(0.819,1),'ro':(0.857,1), 'ru':(0.785,1), 'sq':(0.801,1), 'sr':(0.638,1), 'sv':(0.779,1), 'tr':(0.704,1)}

#dmg_very_likely_threshholds = {'ar':None,'bs':(0.549,1), 'ca':(0.924,1), 'cs':(0.908,1), 'data':(0.925,1), 'en':(0.927,1),'es':(0.961,1), 'esbooks':(0.947,1), 'et':(0.937,1), 'fi':(0.62696,1), 'fr':(0.859,1), 'he':None, 'hu':(0.87,1), 'it':(0.825,1), 'ko':(0.851,1), 'lv':(0.892,1), 'nl':(0.931,1), 'pl':(0.941,1), 'pt':(0.949,1), 'ro':(0.915,1), 'ru':(0.916,1), 'sq':(0.901,1), 'sr':(0.873,1), 'sv':(0.948,1), 'tr':(0.88,1)} 


# gf_very_likely_threshholds = {'ar':(0.999,1), 'bs':(0.999,1), 'ca':(0.999,1), 'cs':(0.747,1), 'data':(0.969,1), 'en':(0.787,1),'es':None, 'esbooks':(1,1), 'et':(0.682,1), 'fi':None, 'fr':(0.777,1), 'he':None, 'hu':(0.957,1), 'it':(0.87,1), 'ko':(0.617,1), 'lv':(0.997,1), 'nl':(0.596,1), 'pl':(0.912,1), 'pt':(0.866,1), 'ro':(0.895,1), 'ru':(0.762,1), 'sq':(0.919,1), 'sr':None, 'sv':(0.982,1), 'tr':(0.86,1)}

# gf_likely_threshholds = {'ar':(0,1),'bs':(0,0.999),'ca':(0,0.999), 'cs':(0,0.95),'data':None,'en':(0,0.933), 'es':None, 'esbooks':None, 'et':(0,0.898), 'fi':(0,1), 'fr':(0,0.962), 'he':None, 'hu':None, 'it':(0,0.865), 'ko':(0,0.606),'lv':(0,0.999), 'nl':(0,0.691), 'pt':(0,0.782), 'pl':None, 'ro':(0,0.793), 'ru':(0,0.769), 'sq':(0,0.942), 'sr':None, 'sv':(0,0.89), 'tr':(0,0.84)}
# gf_unlikely_threshholds = {'ar':None,'bs':(0,0.786), 'ca':(0,0.926), 'cs':(0,0.44),'data':(0,0.997),'en':(0,0.357), 'es':None, 'esbooks':(0,0.997),'et':(0,0.572), 'fi':None, 'fr':(0,0.281), 'he':(0,0.941), 'hu':(0,0.932), 'it':(0,0.343), 'ko':(0,0.216), 'lv':(0,0.901), 'nl':(0,0.319), 'pt':(0,0.206),'pl':None, 'ro':(0,0.154), 'ru':(0,0.244), 'sq':(0,0.265), 'sr':None, 'sv':(0,0.599), 'tr':(0,0.339)}

# gf_very_unlikely_threshholds = {'ar':None, 'bs':None, 'ca':(0,0.031), 'cs':(0,0.111), 'data':None, 'en':(0,0.071), 'es':(0,0.451), 'esbooks':(0,0.001), 'et':(0,0.186), 'fi':None, 'fr':(0,0.106), 'he':(0,0.006), 'hu':(0,0.181),'it':(0,0.151), 'ko':None, 'lv':(0,0.002), 'nl':(0,0.11), 'pl':(0,0.244), 'pt':(0,0.058), 'ro':(0,0.074), 'ru':None, 'sq':(0,0.057), 'sr':None, 'sv':(0,0.237), 'tr':(0,0.162)}
