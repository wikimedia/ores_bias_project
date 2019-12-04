#!/usr/bin/env python3

import pandas as pd
import reverse_geocode
import feather

pddf2 = pd.read_pickle("page_wikidata_properties.pickle")

pivoted = pddf2.pivot_table(index=['entityid','wiki','pageid','title','user','userid','title_namespace_localized','revid'],
                          columns='property',
                          values=['valuelabel','latlongvalue'],
                          aggfunc=lambda s: s.iloc[0])


pivoted = pivoted.drop(('latlongvalue','P21'),axis=1)
pivoted = pivoted.drop(('latlongvalue','P31'),axis=1)

pivoted.columns = pivoted.columns.get_level_values(1)

pivoted = pivoted.rename(columns={"P21":"sexorgender","P31":"ishuman","P625":"latlonginfo"})

pivoted.loc[ ~ pivoted.latlonginfo.isnull(), "is_earth"] = pivoted.loc[ ~ pivoted.latlonginfo.isnull(), :].latlonginfo.apply(lambda s: s[4] == "http://www.wikidata.org/entity/Q2")

pivoted.loc[pivoted.is_earth != True, 'latlonginfo'] = None

pivoted.loc[ ~ pivoted.latlonginfo.isnull(), "latitude"] = pivoted.loc[ ~ pivoted.latlonginfo.isnull(), :].latlonginfo.apply(lambda s: s[0])

pivoted.loc[ ~ pivoted.latlonginfo.isnull(), "longitude"] = pivoted.loc[ ~ pivoted.latlonginfo.isnull(), :].latlonginfo.apply(lambda s: s[1])

pivoted.loc[:,"sexorgender"] = pivoted.sexorgender.str.strip("\"")

pivoted.loc[:,"ishuman"] = pivoted.ishuman == "\"human\""

# correct for vandalism
pivoted.loc[pivoted.sexorgender == "Lesbo",'sexorgender'] = 'female'

# treat transgender male and female as male and female
pivoted.loc[pivoted.sexorgender == 'transgender female','sexorgender'] = 'female'
pivoted.loc[pivoted.sexorgender == 'transgender male','sexorgender'] = 'male'
pivoted = pivoted.reset_index()
pivoted.loc[pivoted.ishuman==True,['wiki','sexorgender','revid']].groupby(['wiki','sexorgender']).count()

with_latlong = ~pivoted.latitude.isnull()
pivoted.loc[with_latlong,"reverse_geocode"] = pivoted.loc[with_latlong,['latitude','longitude']].apply(lambda r: reverse_geocode.search( [(r.latitude,r.longitude)]),axis=1)

pivoted.loc[with_latlong,"country_code"] = pivoted.loc[with_latlong,:].apply(lambda r: r.reverse_geocode[0]['country_code'], axis=1)

canonical_countries = pd.read_feather("canonical_data.countries.feather")

df = pd.merge(pivoted, canonical_countries, left_on="country_code", right_on='iso_code', how='left')

df = df.drop(['iso_code','reverse_geocode','latlonginfo'], axis=1)

df.to_pickle("label_edits_gender_geo.pickle")
