#!/usr/bin/env python3
from plotnine import *
import pandas as pd
import itertools
from get_labels import load_labels, load_wikis, load_makefile, grep_labelfile
import json
import os
import numpy as np
import re
import subprocess
from confidence_levels import ORESConfidenceLevel, dmg_levels, gf_levels
import pickle

from pyRemembeR import Remember
remember = Remember("paper/evaluate_encoded_bias.RDS")

theme_set(theme_bw())

wikis = load_wikis()
makefile = load_makefile()

def load_scored_labels(label_file, context):
    missing_revs = open("missing_revisions.txt", 'w')
    wiki_scored_labels = scored_labels.get(context,None)

    if wiki_scored_labels is None:
        wiki_scored_labels = []

    for record in wiki_scored_labels:
        row = {}
        label = record['label']

        row['true_damaging'] = label.get('damaging', None)
        row['true_goodfaith'] = label.get('goodfaith', None)


        damaging = record.get('damaging',{})
        if isinstance(damaging,str):
            damaging = json.loads(damaging)

        goodfaith = record.get('goodfaith',{})
        if isinstance(goodfaith,str):
            try:
                goodfaith = json.loads(goodfaith)
            except json.JSONDecodeError as e:
                print(goodfaith)
                missing_revs.write(goodfaith+"\n")
                continue

        if isinstance(damaging,str):
            try:
                damaging = json.loads(damaging)
            except json.JSONDecodeError as e:
                print(damaging)
                missing_revs.write(damaging+"\n")
                continue

        row['prob_damaging'] = damaging.get('probability',{}).get('true',None)
        row['prob_goodfaith'] = goodfaith.get('probability',{}).get('true',None)

            # These are the labels based on the default threshholds
            # But for fair comparison between the different wikis
            # I chose to choose threshholds with fpr ~= fnr
            # row['pred_damaging'] = score['damaging']['score']['prediction']
            # row['pred_goodfaith'] =score['goodfaith']['score']['prediction']

        row['rev_id'] = label['rev_id']
        row['wiki'] = context

        yield row

    missing_revs.close()

label_files = list(map(lambda x: grep_labelfile(x, makefile), wikis))
scored_labels = dict(pickle.load(open("data/scored_labels.pickle",'rb')))

rows = itertools.chain(* [load_scored_labels(label_file, context)
                          for label_file, context in zip(label_files, wikis)])

rows = [r for r in rows if r is not None]

df_labels = pd.DataFrame(rows)
df_labels = df_labels.set_index("rev_id")

df_editors = pd.read_pickle(os.path.join('data',"labeled_newcomers_anons.pickle"))
df_editors2 = pd.read_pickle(os.path.join("data","label_edits_gender_geo.pickle"))

# at this point we have the data we need
df_editors2 = df_editors2.loc[:,['entityid','revid','wiki','title_namespace_localized','sexorgender','ishuman','latitude','longitude','country_code','name','economic_region','maxmind_continent']]

df_editors = pd.merge(df_editors, df_editors2, left_on=['revid','wiki'], right_on=['revid','wiki'], how='left',suffixes=('','_y'))

df_editors['rev_id'] = df_editors['revid']
df_editors = df_editors.drop('revid',axis=1)
#df_editors = df_editors.set_index("rev_id")

n_edits = df_editors.loc[df_editors.ns == 0,'rev_id'].count()
n_wikidataentities = df_editors.entityid.isna().sum()
print("found {0} Wikidata ids for {1} edits ({2}%)".format(n_edits, n_wikidataentities, n_wikidataentities/n_edits))

df_labels = pd.merge(df_labels, df_editors,left_on=['rev_id','wiki'],
                     right_on=["rev_id", "wiki"],
                     how='left')
#
df_labels['wiki'] = df_labels['wiki'].str.replace("wiki", "")
df = df_labels.loc[:, ["wiki",
                       "revid",
                       "is_anon",
                       "is_newcomer",
                       "pred_damaging",
                       "pred_goodfaith",
                       "prob_damaging",
                       "prob_goodfaith",
                       "true_damaging",
                       "true_goodfaith"]]

missing_scores = df.loc[df.prob_damaging.isna(), :]
df = df.loc[~df.prob_damaging.isna(), :]

df['group'] = 'other'
df.loc[df.is_anon == True, 'group'] = 'anon'
df.loc[df.is_newcomer == True, 'group'] = 'newcomer'

def build_rates(df, dmg_levels, gf_levels):
    for confidenceLevel in dmg_levels:
        df = confidenceLevel.gen_preds(df, "damaging")

    for confidenceLevel in gf_levels:
        df = confidenceLevel.gen_preds(df, "goodfaith")

    df['true_damaging'] = df['true_damaging'].astype("double")
    df['true_goodfaith'] = df['true_goodfaith'].astype("double")
    gb = df.groupby(["wiki", "group"])
    rates = gb.agg(['mean','std'])
    v = list(rates.columns.levels[0][1:].values)
    rates['count'] =  gb.wiki.count()
    rates.columns = rates.columns.to_flat_index()
    rates.columns = ['_'.join([s for s in t if s != '']) for t in rates.columns]

    rates['dmg_miscalibration_mean'] = (rates['prob_damaging_mean'] - rates['true_damaging_mean'])
    rates['dmg_miscalibration_std'] = np.sqrt(rates['prob_damaging_std'].pow(2) +  rates['true_damaging_std'].pow(2))
    rates['gf_miscalibration_mean'] = rates['prob_goodfaith_mean'] - rates['true_goodfaith_mean']
    rates['gf_miscalibration_std'] = np.sqrt((rates['prob_goodfaith_std']).pow(2) +  rates['true_goodfaith_std'].pow(2))

    v.append("dmg_miscalibration")
    v.append("gf_miscalibration")

    d = np.sqrt(rates.loc[:,"count"])
    for var in v:
        m = rates.loc[:,"{0}_mean".format(var)]
        s = rates.loc[:,"{0}_std".format(var)]
        rates["{0}_upper".format(var)] = m + 1.96*s / d
        rates["{0}_lower".format(var)] = m - 1.96*s / d

    rates = rates.reset_index()
    return rates

# re_damaging_uppers = r".*_dmg_.*_upper"
# re_damaging_lowers = r"*_dmg_.*_lowers"
# re_damaging_means = r".*_dmg_.*_mean"

# re_goodfaith_uppers = r".*_gf_.*_upper"434
# re_goodfaith_lowers = r".*_gf_.*_lowers"
# re_goodfaith_means = r".*_gf_.*_mean"

def build_plot_dataset(rates, prefix):

    rates_1 = rates.melt(id_vars = ['wiki','group'],value_vars = [col for col in list(rates.columns) if (re.match(r"{0}_.*_upper".format(prefix), col)) ], value_name ='upper')

    rates_1['variable'] = rates_1['variable'].str.replace("_upper","")

    rates_2 = rates.melt(id_vars = ['wiki','group'],value_vars = [col for col in list(rates.columns) if (re.match(r"{0}_.*_lower".format(prefix), col)) ], value_name = 'lower')

    rates_2['variable'] = rates_2['variable'].str.replace("_lower","")

    rates_3 = rates.melt(id_vars = ['wiki','group'],value_vars = [col for col in list(rates.columns) if (re.match(r"{0}_.*_mean".format(prefix), col)) ], value_name='mean')

    rates_3['variable'] = rates_3['variable'].str.replace("_mean","")

    plot_rates = pd.merge(rates_1, rates_2)
    plot_rates = pd.merge(plot_rates, rates_3)
    plot_rates['variable'] = plot_rates['variable'].astype('category')

    return plot_rates

def make_plots(rates, suffix1, suffix2):
    fp_rates_damaging = build_plot_dataset(rates, 'fp_dmg')
    remember(fp_rates_damaging,'fp_rates_damaging_{0}_{1}'.format(suffix1, suffix2))
    
    fp_rates_damaging.variable.cat.reorder_categories(['fp_dmg_unlikely', 'fp_dmg_maybe','fp_dmg_likely','fp_dmg_very_likely'],ordered=True,inplace=True)

    fp_rates_damaging.variable.cat.rename_categories(['Very likely good', 'May have problems','Likely have problems','Very likely have problems'],inplace=True)

    p = ggplot(fp_rates_damaging, aes(x='wiki', y='mean',ymin= 'lower', ymax = 'upper', group='group',color='group')) + geom_pointrange(position=position_dodge(width=0.5)) + facet_wrap(facets='variable', ncol=2, nrow=2, scales='free_y') 

    p = p + ylab("False positive rate (Damage predicted, but actually good)")
    p = p + ggtitle("Bias of damaging model against {0}".format(suffix1))
    p = p + theme(legend_position='right')
    p = p + theme(legend_title=element_blank())
    p.save("bias_plots/Damaging_fpr_{0}.png".format(suffix2),width=18, height=8,units='in')

    fn_rates_damaging = build_plot_dataset(rates, 'fn_dmg')

    remember(fn_rates_damaging,'fn_rates_damaging_{0}_{1}'.format(suffix1, suffix2))

    fn_rates_damaging.variable.cat.reorder_categories(['fn_dmg_unlikely', 'fn_dmg_maybe','fn_dmg_likely','fn_dmg_very_likely'],ordered=True,inplace=True)

    fn_rates_damaging.variable.cat.rename_categories(['Very likely good', 'May have problems','Likely have problems','Very likely have problems'],inplace=True)

    p = ggplot(fn_rates_damaging, aes(x='wiki', y='mean',ymin= 'lower', ymax = 'upper', group='group',color='group')) + geom_pointrange(position=position_dodge(width=0.5)) + facet_wrap(facets='variable', ncol=2, nrow=2, scales='free_y') 

    p = p + ylab("False negative rate (Damage not predicted, but actually damaging)")
    p = p + ggtitle("Bias of damaging model against {0}".format(suffix1))
    p = p + theme(legend_position='right')
    p = p + theme(legend_title=element_blank())
    p.save("bias_plots/Damaging_fnr_{0}.png".format(suffix2),width=18, height=8,units='in')

    fp_rates_goodfaith = build_plot_dataset(rates, 'fp_gf')
    remember(fp_rates_goodfaith, 'fp_rates_goodfaith_{0}_{1}'.format(suffix1, suffix2))

    fp_rates_goodfaith.variable.cat.reorder_categories(['fp_gf_very_likely', 'fp_gf_likely','fp_gf_unlikely','fp_gf_very_unlikely'],ordered=True,inplace=True)

    fp_rates_goodfaith.variable.cat.rename_categories(['Very likely good faith', 'May be bad faith','Likely bad faith','Very likely bad faith'],inplace=True)

    p = ggplot(fp_rates_goodfaith, aes(x='wiki', y='mean',ymin= 'lower', ymax = 'upper', group='group',color='group')) + geom_pointrange(position=position_dodge(width=0.5)) + facet_wrap(facets='variable', ncol=2, nrow=2, scales='free_y') 

    p = p + ylab("False positive rate (Goodfaith predicted, but actually badfaith)")
    p = p + ggtitle("Bias of goodfaith model against {0}".format(suffix1))
    p = p + theme(legend_position='right')
    p = p + theme(legend_title=element_blank())
    p.save("bias_plots/Goodfaith_fpr_{0}.png".format(suffix2),width=18, height=8,units='in')

    fn_rates_goodfaith = build_plot_dataset(rates, 'fn_gf')
    remember(fn_rates_goodfaith,'fn_rates_goodfaith_{0}_{1}'.format(suffix1, suffix2))

    fn_rates_goodfaith.variable.cat.reorder_categories(['fn_gf_very_likely', 'fn_gf_likely','fn_gf_unlikely','fn_gf_very_unlikely'],ordered=True,inplace=True)

    fn_rates_goodfaith.variable.cat.rename_categories(['Very likely good faith', 'May be bad faith','Likely bad faith','Very likely bad faith'],inplace=True)

    p = ggplot(fn_rates_goodfaith, aes(x='wiki', y='mean',ymin= 'lower', ymax = 'upper', group='group',color='group')) + geom_pointrange(position=position_dodge(width=0.5)) + facet_wrap(facets='variable', ncol=2, nrow=2, scales='free_y') 

    p = p + ylab("False negative rate (Badfaith predicted, but actually goodfaith)")
    p = p + ggtitle("Bias of goodfaith model against {0}".format(suffix1))
    p = p + theme(legend_position='right')
    p = p + theme(legend_title=element_blank())
    p.save("bias_plots/Goodfaith_fnr_{0}.png".format(suffix2),width=18, height=8,units='in')

    p = ggplot(rates, aes(x='wiki', y='dmg_miscalibration_mean',ymax='dmg_miscalibration_upper',ymin='dmg_miscalibration_lower',
                          group='group', color='group', fill='group'))
    p = p + geom_pointrange(position = position_dodge(width=0.5))
    p = p + ylab("P_model(damaging) - P(damaging)")
    p = p + ggtitle("Calibration of ORES damaging model for {0}".format(suffix1))
    p = p + theme(legend_title = element_blank())
    p.save("bias_plots/damaging_miscalibration_{0}.png".format(suffix2), width=12, height=8, unit='cm')

    p = ggplot(rates, aes(x='wiki', y='gf_miscalibration_mean',ymax='gf_miscalibration_upper',ymin='gf_miscalibration_lower',
                          group='group', color='group', fill='group'))
    p = p + geom_pointrange(position = position_dodge(width=0.5))
    p = p + ylab("P_model(goodfaith) - P(goodfaith)")
    p = p + ggtitle("Calibration of ORES goodfaith model on {0}".format(suffix1))
    p = p + theme(legend_title = element_blank())
    p.save("bias_plots/goodfaith_miscalibration_{0}.png".format(suffix2), width=12, height=8, unit='cm')


rates = build_rates(df, dmg_levels, gf_levels)
remember(rates, 'newanon_bias_rates')

make_plots(rates, "newcomers and anons", "newanon")

## now do it for male / female articles
df = df_labels.loc[df_labels.ishuman == True,
                   ["wiki",
                    "revid",
                    "sexorgender",
                    "pred_damaging",
                    "pred_goodfaith",
                    "prob_damaging",
                    "prob_goodfaith",
                    "true_damaging",
                    "true_goodfaith"]]

missing_scores = df.loc[df.prob_damaging.isna(), :]
df = df.loc[~df.prob_damaging.isna(), :]

df['group'] = 'non-binary or unknown'
df.loc[df.sexorgender == 'male', 'group'] = 'men'
df.loc[df.sexorgender == 'female', 'group'] = 'women'

rates = build_rates(df, dmg_levels, gf_levels)
remember(rates,'gender_bias_rates')

make_plots(rates, "revisions to articles on women", "gender")

## now do it for global north / south articles
df = df_labels.loc[~df_labels.economic_region.isnull(),
                   ["wiki",
                    "revid",
                    "economic_region",
                    "pred_damaging",
                    "pred_goodfaith",
                    "prob_damaging",
                    "prob_goodfaith",
                    "true_damaging",
                    "true_goodfaith"]]


missing_scores = df.loc[df.prob_damaging.isna(), :]
df = df.loc[~df.prob_damaging.isna(), :]

df['group'] = 'non-binary or unknown'
df.loc[df.economic_region == 'Global North', 'group'] = 'Global North'
df.loc[df.economic_region == 'Global South', 'group'] = 'Global South'

rates = build_rates(df, dmg_levels, gf_levels)
remember(rates,'global_north_bias_rates')
make_plots(rates, "articles on the Global South", "geo") 
