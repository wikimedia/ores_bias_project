from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql.functions as f

def my_match_comment(comment, wiki, timestamp):
        try:
            result = list(broad_wtm.value.match(comment,wiki,timestamp))
            return result
        except Exception as e:
            return [str(e),wiki,str(broad_wtm.value.wikiToolMap[wiki]._toolMap['undo'].match())]

match_comment = udf(my_match_comment,returnType=ArrayType(StringType()))                

def add_revert_types(wmhist):
    wmhist = wmhist.withColumn("comment_match",match_comment(f.col("event_comment"),f.col("wiki_db"),f.col("event_timestamp")))
    wmhist = wmhist.withColumn("is_undo", f.array_contains(col='comment_match',value='undo'))
    wmhist = wmhist.withColumn("is_rollback", f.array_contains(col='comment_match',value='rollback'))

    wmhist = wmhist.withColumn(
        'revert_tool',
        f.when(f.array_contains(f.col("revert_tools_match"),
                                "undo"),
               'undo').otherwise(f.when(f.array_contains(f.col("revert_tools_match"),
                                                         "rollback"),
                                        'rollback').otherwise(f.when(f.array_contains(f.col("revert_tools_match"),
                                                                                      'huggle'),
                                                                     'huggle').otherwise(f.when(f.array_contains(f.col("revert_tools_match"),
                                                                                                                 "twinkle"),
                                                                                                "twinkle").otherwise("otherTool")))))
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

    wmhist = wmhist.withColumn("role_type", f.when(wmhist.event_user_isadmin == True, "admin").otherwise(
        f.when( (wmhist.event_user_isbot1 == True) | (wmhist.event_user_isbot2 == True),"bot").otherwise(
            f.when(wmhist.event_user_ispatroller == True, "patroller").otherwise("other")
        )))
    
    return wmhist

def build_wmhist_step1(wmhist):
    wmhist = add_revert_types(wmhist)
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
                              'week',
                              'event_user_text',
                              'event_timestamp',
                              'role_type',
                              'revision_id',
                              'revision_is_identity_reverted',
                              'revision_first_identity_reverting_revision_id',
                              'revert_tool',
                              'is_undo',
                              'is_rollback'])

    reverts = reverts.withColumnRenamed("event_user_text","revert_user_text")
    reverts = reverts.withColumnRenamed("event_timestamp","revert_timestamp")
    reverts = reverts.withColumnRenamed("revision_id","revert_revision_id")
    reverts = reverts.withColumnRenamed("wiki_db","wiki_db_l")
    reverts = reverts.withColumnRenamed("revision_is_identity_reverted","revert_is_identity_reverted")
    reverts = reverts.withColumnRenamed("revision_first_identity_reverting_revision_id","revert_first_identity_reverting_revision_id")

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
    reverted_reverts = reverted_reverts.withColumnRenamed("week",'rr_week')
    reverted_reverts = reverted_reverts.select(['rr_wiki_db','rr_timestamp','rr_revision_id','rr_reverting_revision_id','rr_week'])
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

    # exclude reverts with ttr > 30 days = 60 seconds * 60 minutes / second * 24hours / day * 30 days
    reverts = reverts.filter(f.col("time_to_revert") <= 30*24*60*60)

    reverts = reverts.withColumn("med_ttr", f.expr('percentile_approx(time_to_revert, 0.5,1)').over(Window.partitionBy(['wiki_db','week'])))

    return reverts
