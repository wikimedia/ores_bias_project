#!/usr/bin/env python3
import numpy as np
import pandas as pd
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
conf = conf.set("spark.sql.crossJoin.enabled",'true')
sc = SparkContext(conf= conf)
spark = SparkSession(sc)

# conf = spark.sparkContext._conf.set("spark.sql.crossJoin.enabled",True)
# spark.sparkContext.stop()
# spark = SparkSession.builder.config(conf=conf).getOrCreate()

reader = spark.read
ores_labels = "/user/nathante/ores_bias_data/ores_label_editors"
wikidata_parquet = "/user/joal/wmf/data/wmf/mediawiki/wikidata_parquet/20191202"
mapping_parquet = "/user/joal/wmf/data/wmf/wikidata/item_page_link/20191202"

labels = reader.parquet(ores_labels)

mapping = reader.parquet(mapping_parquet)

# lookup wikidata ids
page_items = labels.join(mapping, on=[labels.pageid == mapping.page_id, labels.ns == mapping.page_namespace, labels.wiki == mapping.wiki_db])

for col in ['page_namespace','page_title','wiki_db','page_id']:
    page_items = page_items.drop(col)

#items = page_items.select(["item_id"]).distinct()

# now lookup wikidata fields

wikidata = reader.parquet(wikidata_parquet)

wikidata_entities = wikidata.join(page_items, on=[page_items.item_id == wikidata.id])

wikidata_entities = wikidata_entities.cache()

for col in ['typ','labels','descriptions','aliases','references','siteLinks','item_id']:
    wikidata_entities = wikidata_entities.drop(col)

claims = wikidata_entities.withColumn("claim",f.explode("claims"))
claims = claims.drop("claims")

snaks = claims.withColumn("mainSnak",f.col("claim").mainSnak)
snaks = snaks.drop("claim")

entitytype_schema = StructType([StructField("entity-type",StringType()), StructField("numeric-id",IntegerType()), StructField("id", StringType())])

globe_coordinate_schema = StructType([StructField("latitude",DoubleType()), StructField("longitude", DoubleType()), StructField("altitude",DoubleType()), StructField("precision",DoubleType()),StructField("globe",StringType())])

values = snaks.withColumn("entitytypeValue",f.from_json(f.col("mainSnak").dataValue.value,schema=entitytype_schema))
values = values.withColumn("latlongvalue",f.from_json(f.col("mainSnak").dataValue.value,schema=globe_coordinate_schema))
values = values.withColumn("property",f.col("mainSnak").property)

values = values.filter(f.col("property").isin({"P31","P21","P625"}))

values = values.withColumnRenamed("id","entityid")

values = values.join(wikidata,on=[values.entitytypeValue.id == wikidata.id], how='left_outer')

values = values.withColumn("valueid", f.col("id"))
values = values.withColumn("valuelabel", f.col("labels")['en'].alias("valuelabel"))

for col in ['typ','labels','descriptions','aliases','references','siteLinks','item_id',"claims","mainSnak",'dataValue']:
    values = values.drop(col)

values = values.filter( (f.col("valueid")=="Q5") | (f.col("property")=="P21") | (f.col("property") == "P625"))

pddf = values.toPandas()

pddf.to_pickle("data/page_wikidata_properties.pickle")
