from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import Window
import pyspark.sql.functions as f
import mwcomments
from functools import reduce

def broadcast_match_comment(sc):
        wtm = mwcomments.WikiToolMap.load_WikiToolMap()
        broad_wtm = sc.broadcast(wtm)

        def my_match_comment(comment, wiki, timestamp):
                if comment is None:
                        return []

                try:
                    result = list(broad_wtm.value.match(comment,wiki,timestamp))
                    return result
                except Exception as e:
                    return [str(e),wiki,str(broad_wtm.value.wikiToolMap[wiki]._toolMap['undo'])]

        global match_comment
        match_comment = udf(my_match_comment,returnType=ArrayType(StringType()))
        return match_comment


def add_revert_types(wmhist, comment_column='event_comment'):
    wmhist = wmhist.withColumn("revert_tools_match",match_comment(f.col(comment_column),f.col("wiki_db"),f.col("event_timestamp")))
    wmhist = wmhist.withColumn("is_undo", f.array_contains(col='revert_tools_match',value='undo'))
    wmhist = wmhist.withColumn("is_rollback", f.array_contains(col='revert_tools_match',value='rollback'))

    tool_priority = ['huggle','twinkle','fastbuttons','LiveRC','rollback','undo']

    for tool in tool_priority:
            wmhist = wmhist.withColumn("revert_tool_{0}".format(tool), f.when(f.array_contains(f.col("revert_tools_match"),tool),tool).otherwise(None))

    wmhist = wmhist.withColumn("revert_tool",f.coalesce(tool_priority))

    return wmhist

def add_is_newcomer(wmhist):

    wmhist = wmhist.withColumn("event_user_is_newcomer", (wmhist.event_user_is_anonymous == False) &
                               ((f.datediff(wmhist.event_timestamp, wmhist.event_user_creation_timestamp) < 4) &
                                (wmhist.event_user_revision_count < 10)) |
                               ((f.datediff(wmhist.event_timestamp, wmhist.event_user_creation_timestamp) < 90) &
                                (wmhist.event_user_revision_count < 100) &
                                (f.array_contains(col=f.col("event_user_groups"),value='ipblock-exempt'))
                               ))
    
    wmhist = wmhist.withColumn('anon_new_established',
                               f.when(f.col("event_user_is_newcomer"),'newcomer'). \
                               otherwise(f.when(f.col("event_user_is_anonymous"),'anonymous'). \
                                         otherwise("established")))

    return wmhist

def add_user_roles(wmhist):

    def role_filter(rg, role_set):
        if rg is None:
            return False
        else:
            return any(role in role_set for role in rg)

    py_is_admin = lambda rg: role_filter(rg, {"bureaucrat","sysop","steward","arbcom"})

    py_is_bot = lambda rg: role_filter(rg, {"copyviobot","bot"})

    py_is_patroller = lambda rg: role_filter(rg, {"patroller"})

    udf_is_admin = f.udf(py_is_admin,BooleanType())
    udf_is_bot = f.udf(py_is_bot,BooleanType())
    udf_is_patroller = f.udf(py_is_patroller, BooleanType())

    wmhist = wmhist.withColumn("event_user_isadmin", udf_is_admin(wmhist.event_user_groups))
    wmhist = wmhist.withColumn("event_user_isbot1", udf_is_bot(wmhist.event_user_groups))
    wmhist = wmhist.withColumn("event_user_ispatroller", udf_is_patroller(wmhist.event_user_groups))
    wmhist = wmhist.withColumn("event_user_isbot2", f.size(wmhist.event_user_is_bot_by) > 0)

    wmhist = wmhist.withColumn("role_type", f.when(wmhist.event_user_isadmin, "admin").otherwise(
        f.when( (wmhist.event_user_isbot1) | (wmhist.event_user_isbot2),"bot").otherwise(
            f.when(wmhist.event_user_ispatroller, "patroller").otherwise("other")
        )))
    
    return wmhist

def build_wmhist_step1(wmhist):
    wmhist = add_is_newcomer(wmhist)
    wmhist = add_user_roles(wmhist)
    return wmhist

def process_reverts(wmhist):
    # next lets look at time to revert
    reverteds = wmhist.filter((wmhist.page_namespace==0) & (wmhist.revision_is_identity_reverted == True))
    reverteds = reverteds.select(['wiki_db',
                                  'event_user_text',
                                  'revision_id',
                                  'event_timestamp',
                                  'revision_first_identity_reverting_revision_id',
                                  'anon_new_established'])

    reverteds = reverteds.withColumnRenamed("event_timestamp","reverted_timestamp")
    reverteds = reverteds.withColumnRenamed("revision_id","reverted_revision_id")
    reverteds = reverteds.withColumnRenamed("event_user_text","reverted_user_text")

    reverts = wmhist.filter((wmhist.page_namespace==0)&(wmhist.revision_is_identity_revert == True))

    reverts = reverts.select(['wiki_db',
                              'event_user_id',
                              'event_user_text',
                              'event_timestamp',
                              'role_type',
                              'revision_id',
                              'revision_is_identity_reverted',
                              'revision_first_identity_reverting_revision_id',
                              f.col('event_comment').alias('revert_comment')
                              ])


    reverts = reverts.withColumnRenamed("event_user_id","revert_user_id")
    reverts = reverts.withColumnRenamed("event_user_text","revert_user_text")
    reverts = reverts.withColumnRenamed("event_timestamp","revert_timestamp")
    reverts = reverts.withColumnRenamed("revision_id","revert_revision_id")
    reverts = reverts.withColumnRenamed("wiki_db","wiki_db_l")
    reverts = reverts.withColumnRenamed("revision_is_identity_reverted","revert_is_identity_reverted")
    reverts = reverts.withColumnRenamed("revision_first_identity_reverting_revision_id","revert_first_identity_reverting_revision_id")

    ## DataFrame doesn't support window functions by date range in spark 2.3 
    ## https://stackoverflow.com/questions/33207164/spark-window-functions-rangebetween-dates
    reverts = spark.sql(
            """SELECT *, count(revert_revision_id) OVER (
            PARTITION BY revert_user_id
            ORDER BY CAST(revert_timestamp AS timestamp)
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW)
            AS revert_user_Nreverts_past_month from reverts""")

    # use a window function to count the number of reverts in a given timespan

    # exclude self-reverts
    reverts = reverts.join(reverteds,
                           on=[reverts.wiki_db_l == reverteds.wiki_db,
                               reverts.revert_revision_id == reverteds.revision_first_identity_reverting_revision_id,
                               reverts.revert_user_text != reverteds.reverted_user_text],
                           how='inner')

    reverted_reverts = reverts.filter(f.col('revert_is_identity_reverted')==True)
    reverted_reverts = reverted_reverts.withColumnRenamed("revert_timestamp","rr_timestamp")
    reverted_reverts = reverted_reverts.withColumnRenamed("revert_revision_id","rr_revision_id")
    reverted_reverts = reverted_reverts.withColumnRenamed("revert_first_identity_reverting_revision_id","rr_reverting_revision_id")
    reverted_reverts = reverted_reverts.withColumnRenamed("wiki_db",'rr_wiki_db')
    reverted_reverts = reverted_reverts.select(['rr_wiki_db','rr_timestamp','rr_revision_id','rr_reverting_revision_id'])
    reverts = reverts.join(reverted_reverts,
                           on=[reverts.reverted_revision_id == reverted_reverts.rr_revision_id,
                               reverts.wiki_db == reverted_reverts.rr_wiki_db],
                           how='left_outer')

    reverts = reverts.withColumn("rr_ttr",
                                 (f.unix_timestamp(reverts.revert_timestamp) - f.unix_timestamp(reverts.rr_timestamp)) / 1000)

    reverts = reverts.withColumn("is_damage",f.when(f.isnull(f.col("rr_ttr")), True).otherwise(f.col("rr_ttr") >= 48*60*60))

    # we only want to look at the damaging reverts for measuring our vandalism fighting outcomes
    reverts = reverts.filter(f.col("is_damage") == True)
    # convert time to revert into seconds
    reverts = reverts.withColumn("time_to_revert",(f.unix_timestamp(f.col("revert_timestamp")) - f.unix_timestamp(f.col("reverted_timestamp"))) / 1000)
    # let's use median ttr as the metric
    return reverts
