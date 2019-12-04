import impala
from impala.dbapi import connect
conn = connect(host='an-coord1001.eqiad.wmnet', port=10000,auth_mechanism="PLAIN")
cursor = conn.cursor()

query = """"CREATE TABLE IF NOT EXISTS nathante.ores_label_editors(wiki string, ns string, pageid bigint, title string, revid bigint, parentid bigint, user string, userid bigint ROW FORMAT SERDE 'parquet.have.serde.ParqueteHiveSerDe'
STORED AS
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION "/user/nathante/ores_bias/ores_label_editors";
"""

cursor.execute(query)
