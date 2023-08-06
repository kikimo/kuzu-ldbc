# Kuzu LDBC SNB Test Suite

## Getting Started

```bash
# 1. install python requirements
$ pip install requirements.txt

# 2. prepare ldbc data, for LDBC datagen setup, see [ldbc_snb_datagen_spark](https://github.com/ldbc/ldbc_snb_datagen_spark) for details.
$ ./tools/run.py --parallelism 1 -- --format csv --scale-factor 0.003 --explode-edges --explode-attrs

# 3. setup kuzudb

See [kuzu](https://github.com/kuzudb/kuzu) for details.

# 4. import data to kuzu db
% ./run.sh
executing: create node table Place (id int64, name string, url string, type string, PRIMARY KEY (id));
executing: create node table Organisation (id int64, type string, name string, url string, PRIMARY KEY (id));
executing: create node table TagClass (id int64, name string, url string, PRIMARY KEY (id));
executing: create node table Tag (id int64, name string, url string, PRIMARY KEY (id));
executing: create node table Person (creationDate TIMESTAMP, deletionDate TIMESTAMP, explicitlyDeleted BOOLEAN, id int64, firstName string, lastName string, gender string, birthday DATE, locationIP string, browserUsed string, PRIMARY KEY (id));
```
