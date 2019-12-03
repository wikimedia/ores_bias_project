from concurrent.futures import ThreadPoolExecutor
import time
import itertools
import mwapi
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from get_labels import load_labels
import json 

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def get_editor_traits(labels, context, out_schema):
    rev_ids = [json.loads(label)['rev_id'] for label in labels]
    # special case for wikidata, esbooks

    if context == "wikidatawiki":
        host= "https://wikidata.org".format(context.replace("wiki",""))
    elif context == "eswikibooks":
        host= "https://es.wikibooks.org".format(context.replace("wiki",""))
    elif context == "eswikiquote":
        host= "https://es.wikiquote.org".format(context.replace("wiki",""))
    else:
        host= "https://{0}.wikipedia.org".format(context.replace("wiki",""))

    user_agent="Ores bias analysis project by Nate TeBlunthuis <groceryheist@uw.edu>"
    session = mwapi.Session(host,user_agent)

    batches = grouper(rev_ids, 50)

    def table_results(batch, context):
        resultset = batch['query']['pages']
        for _, page_id in enumerate(resultset):
            row = {}
            result = resultset[page_id]
            row['wiki'] = context
            row['ns'] = result['ns']
            row['title'] = result['title']
            row['pageid'] = int(result['pageid'])
            revisions = result['revisions']
            for rev in revisions:
                row['revid'] = int(rev['revid'])
                row['parentid'] = int(rev['parentid'])
                # there are some deleted revisions where we don't get to know the user, let's just exclude them.
                if 'user' in rev:
                    row['user'] = rev['user']
                    row['userid'] = int(rev['userid'])
                    yield row

    def keep_trying(call, *args, **kwargs):
        try:
            result = call(*args, **kwargs)
            return result
        except Exception as e:
            print(e)
            time.sleep(1)
            return keep_trying(call, *args, **kwargs)

    with ThreadPoolExecutor(100) as executor:
        # get revision metadata
        revision_batches = executor.map(lambda batch:keep_trying(call=session.get, action="query", prop='revisions', rvprop=['ids','user','userid'], revids=batch), batches)

        badrevids = []
        rows = []
        for batch in revision_batches:
            if 'badrevids' in batch['query']:
                badrevids.append(batch['query']['badrevids'])
            for row in table_results(batch, context):
                yield row

def move_labels_to_datalake(label_files, wikis):

    fs = pa.hdfs.connect(host='an-coord1001.eqiad.wmnet', port=10000)
    fs = fs.connect()
    parquet_path = "/user/nathante/ores_bias_data/nathante.ores_label_editors"
    if fs.exists(parquet_path):
        fs.rm(parquet_path, recursive=True)

    out_schema = ['wiki', 'ns','pageid','title','revid','parentid','user','userid']
    print("collecting userids")

    for label_file, context in zip(label_files,wikis):
        if label_file is not None:

            labels = load_labels(label_file)

            rows = get_editor_traits(labels,context, out_schema)
            pddf = pd.DataFrame(rows)

            pddf.to_pickle("ores_label_editors.pickle")
            out_table = pa.Table.from_pandas(pddf)

            pq.write_to_dataset(out_table, root_path=parquet_path, partition_cols=['wiki'], filesystem=fs, flavor='spark')

            print ("pushed labels for {0}".format(context))


##    conn.close()

     # query = """INSERT INTO nathante.ores_label_editors PARTITION (wiki='{0}') """.format(context)
     #        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
     #        cursor.executemany(query, list(tuple(r) for r in rows))
    # conn = connect(host='an-coord1001.eqiad.wmnet', port=10000, auth_mechanism='PLAIN')
    # cursor = conn.cursor()
    # cursor.execute("DROP TABLE nathante.ores_label_editors")

    # cursor.execute("CREATE EXTERNAL TABLE nathante.ores_label_editors(ns string, pageid bigint, title string, revid bigint, parentid bigint, user string, userid bigint) PARTITIONED BY (wiki string) STORED AS PARQUET LOCATION '/user/nathante/ores_bias/nathante.ores_label_editors' ")

    # cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
