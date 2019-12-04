# run through spark-submit
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
sc = SparkContext(conf=SparkConf())
spark = SparkSession(sc)
import pyspark.sql.functions as f

reader = spark.read

# ok we can get the table
label_editors = reader.parquet("/user/nathante/ores_bias/nathante.ores_label_editors")

# get the edit history
mw_hist = reader.table("wmf.mediawiki_history")
mw_hist = mw_hist.filter(f.col("snapshot") == "2019-03")
mw_hist = mw_hist.filter(f.col("event_entity") == "revision")
mw_hist = mw_hist.select(
    ["revision_id", "event_timestamp", "event_user_id", "wiki_db"])
mw_hist = mw_hist.withColumn("timestamp",
                             f.from_utc_timestamp(
                                 f.col("event_timestamp"), tz="utc"))

# we want to identify newcomers and anons
# anons are just the folks without a userid
label_editors = label_editors.withColumn("is_anon", (f.col('userid') == 0))

# newcomers are not anons
non_anons = label_editors.filter(f.col("is_anon") == False)

# find the edits by the editors
edit_histories = non_anons.join(mw_hist,
                                on=[non_anons.userid == mw_hist.event_user_id,
                                    non_anons.wiki == mw_hist.wiki_db,
                                    non_anons.revid >= mw_hist.revision_id])

# group by wiki and user
gb = edit_histories.groupBy(['wiki', 'user'])
gb = gb.agg(f.min("timestamp").alias("time_first_edit"),
            f.max("timestamp").alias("time_last_edit"),
            f.count("revision_id").alias("N_edits"))

gb = gb.cache()

#sqlContext.registerFunction("time_delta", lambda y,x:(datetime.strptime(y, '%Y-%m-%d %H:%M:%S.%f')-datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')).total_seconds())

gb = gb.withColumn("time_since_first_edit_s", f.unix_timestamp(
    f.col("time_last_edit")) - f.unix_timestamp(f.col("time_first_edit")))

gb = gb.withColumn("days_since_first_edit",
                   f.col("time_since_first_edit_s") / 60 / 60 / 24)

# count the number of prior edits before
gb = gb.withColumn("is_newcomer", (f.col("N_edits") <= 5)
                   | (f.col("days_since_first_edit") < 30))

gb = gb.withColumnRenamed("wiki", "lwiki")

label_editors = label_editors.join(gb,
                                   on=[label_editors.user == gb.user,
                                       label_editors.wiki == gb.lwiki],
                                   how='left_outer')

label_editors = label_editors.drop("lwiki")

label_editors = label_editors.withColumn("is_newcomer_2",
                                         f.when(f.isnull(
                                             f.col("is_newcomer")),
                                                False).otherwise(
                                                    f.col("is_newcomer")))

label_editors = label_editors.drop("is_newcomer")
label_editors = label_editors.withColumnRenamed("is_newcomer_2", "is_newcomer")

pddf = label_editors.toPandas()
pddf.to_pickle("labeled_newcomers_anons.pickle")
pddf.to_table("labeled_newcomers_anons.tsv")
