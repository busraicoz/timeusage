# timeusage
How to run?
- Firstly, Cassandra was run with port 9042 on docker. Than, "timeusage" keyspace and
"time_usage" table were created with using CQL as follows;

CREATE KEYSPACE IF NOT EXISTS timeusage WITH REPLICATION = { 'class' :
'SimpleStrategy', 'replication_factor' : '1' };

CREATE TABLE IF NOT EXISTS timeusage.TIME_USAGE(working text,sex text, age
text, primaryneeds double, work double,other double,PRIMARY
KEY(working,age,sex,primaryneeds));
