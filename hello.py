from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg import schema, types
from IPython import get_ipython
from IPython.display import display, HTML
from pyiceberg.table.metadata import TableMetadataV2

from config import Config
import yaml
from dotenv import load_dotenv
from duckdb import sql
import os
import pyarrow as pa

ipython = get_ipython()
ipython.run_line_magic("load_ext", "autoreload")
ipython.run_line_magic("autoreload", "2")

load_dotenv(override=True)

with open(os.environ["CONFIG_PATH"], "r") as f:
    config = Config.model_validate(yaml.safe_load(f))

print(yaml.dump(config.model_dump(), sort_keys=False))

os.makedirs(config.warehouse, exist_ok=True)
catalog = SqlCatalog(
    config.catalog_name,
    **{
        "uri": f"sqlite:///{config.warehouse}/{config.catalog_file}.db",
        "warehouse": f"file://{config.warehouse}",
    },
)

# the catalog doesnt have that much info
# guess its meant for handling concurrent access?
# copilot what do you think?
# copilot, answer here please:
#
# I think the catalog is a way to manage the metadata of the tables and namespaces in the iceberg format.

sql(f"""--sql
INSTALL sqlite;
load sqlite;
attach if not exists database '{config.warehouse}/{config.catalog_file}.db' as catalog;
use catalog;
show tables;
""").show()

sql("""--sql
select * from iceberg_namespace_properties;
""").show()

sql("""--sql
select * from iceberg_tables;
""").show(max_width=1000000)

try:
    catalog.drop_table(f"{config.namespace}.my_table")
except Exception as e:
    print(e)
try:
    catalog.drop_namespace(config.namespace)
except Exception as e:
    print(e)

catalog.create_namespace(
    config.namespace,
    {
        "custom_attribute 1": "value 1",
        "custom_attribute 2": "value 2",
    },
)

print(catalog.list_namespaces())

catalog.create_table_if_not_exists(
    f"{config.namespace}.my_table",
    schema.Schema(
        types.NestedField(1, "id", types.IntegerType(), True),
        types.NestedField(2, "name", types.StringType(), True),
    ),
)


# /tmp/warehouse
# ├── my_namespace.db
# │   └── my_table
# │       └── metadata
# │           └── 00000-01676cdd-5ba7-461f-a29b-139824b9ed42.metadata.json
# └── pyiceberg_catalog.db


print(catalog.list_tables(config.namespace))

table = catalog.load_table(
    f"{config.namespace}.my_table",
)


table.update_schema(
    allow_incompatible_changes=False,
).make_column_optional("name").commit()

# /tmp/warehouse
# ├── my_namespace.db
# │   └── my_table
# │       └── metadata
# │           ├── 00000-01676cdd-5ba7-461f-a29b-139824b9ed42.metadata.json
# │           └── 00001-146d246a-cb0f-4599-a1e0-39f625584c7b.metadata.json
# └── pyiceberg_catalog.db


# metadata contains:
# current and previous schemas
# snapshots: how much data was added / deleted, n rows, size, manifest lists
display(HTML(f'<a href="{table.metadata_location.replace("file://","")}">metadata</a>'))

df = pa.Table.from_arrays(
    [
        pa.array([1, 2, 3]),
        pa.array(["a", None, "c"]),
    ],
    schema=pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    ),
)

# a new metadata file is created on append
table.append(df)

# /tmp/warehouse
# ├── my_namespace.db
# │   └── my_table
# │       ├── data
# │       │   └── 00000-0-00c2aa03-e6ff-442e-aa4f-3aeb6e8da2b1.parquet
# │       └── metadata
# │           ├── 00000-01676cdd-5ba7-461f-a29b-139824b9ed42.metadata.json
# │           ├── 00001-146d246a-cb0f-4599-a1e0-39f625584c7b.metadata.json
# │           ├── 00002-ef36493d-5b4a-451e-bd3d-90cc9f34b68d.metadata.json
# │           ├── 00c2aa03-e6ff-442e-aa4f-3aeb6e8da2b1-m0.avro
# │           └── snap-6129598552008616631-0-00c2aa03-e6ff-442e-aa4f-3aeb6e8da2b1.avro
# └── pyiceberg_catalog.db

# table.refresh() # TODO not needed, maybe if an external update happens?
metadata: TableMetadataV2 = table.metadata
current_snapshot = metadata.current_snapshot()
assert current_snapshot is not None
first_snapshot_id = current_snapshot.snapshot_id
table.append(df)
assert first_snapshot_id != table.metadata.current_snapshot().snapshot_id

manifest_list = current_snapshot.manifest_list.replace("file://", "")

# seems to contain same as snapshot, + partitions + key_metadata + manifest_path
sql(f"""--sql
install avro from community;
load avro;
select * from read_avro('{manifest_list}');
""").show(max_width=1000000)

manifest_path = (
    sql(f"""--sql
select manifest_path from read_avro('{manifest_list}');
""")
    .to_arrow_table()
    .to_pydict()["manifest_path"][0]
).replace("file://", "")

# contains:
# data path, partition, metadata: sizes, counts, nulls, min, max
sql(f"""--sql
select * from read_avro('{manifest_path}');
""").show(max_width=1000000)

data_file_path = (
    sql(f"""--sql
select data_file.file_path from read_avro('{manifest_path}');
""")
    .to_arrow_table()
    .to_pydict()["file_path"][0]
    .replace("file://", "")
)

sql(f"""--sql
install parquet;
load parquet;
select * from read_parquet('{data_file_path}');
""").show(max_width=1000000)

display(table.scan().to_pandas())

# /tmp/warehouse
# ├── my_namespace.db
# │   └── my_table
# │       ├── data
# │       │   ├── 00000-0-c2a21de5-e1d8-4273-9672-e44d3be7383b.parquet
# │       │   └── 00000-0-d6649c0c-88af-417a-a3e0-2b7eced8b065.parquet
# │       └── metadata
# │           ├── 00000-69b3cb18-9741-44d9-abb2-882d00853983.metadata.json
# │           ├── 00001-a5e0f62e-d854-43fb-9332-de948ec77436.metadata.json
# │           ├── 00002-d9b5eae8-79f2-4de1-a0e4-8b0d0ad1d648.metadata.json
# │           ├── 00003-7939994f-4a89-462d-bc75-8933964ec610.metadata.json
# │           ├── c2a21de5-e1d8-4273-9672-e44d3be7383b-m0.avro
# │           ├── d6649c0c-88af-417a-a3e0-2b7eced8b065-m0.avro
# │           ├── snap-6682569733342759937-0-d6649c0c-88af-417a-a3e0-2b7eced8b065.avro
# │           └── snap-8010078232737628816-0-c2a21de5-e1d8-4273-9672-e44d3be7383b.avro
# └── pyiceberg_catalog.db


sql(f"""--sql
install iceberg;
load iceberg;
show tables
""").show()

# connecting to catalog not supported:
# https://github.com/duckdb/duckdb_iceberg/issues/57


metadata_location = (
    sql(f"""--sql
select
    metadata_location
from
    iceberg_tables
where
    table_name = 'my_table'
    and table_namespace = '{config.namespace}'
    and catalog_name = '{config.catalog_name}'
""")
    .to_arrow_table()
    .to_pydict()["metadata_location"][0]
).replace("file://", "")

metadata_location

# issue: manifest file a starts with file:///
# https://github.com/duckdb/duckdb/issues/13669
# Cannot open file "file:///...""
# could maybe solve by using s3:// (minio) for warehouse
sql(f"""--sql
-- SET unsafe_enable_version_guessing=true; bug
select * from iceberg_scan('{metadata_location}');
--select * from iceberg_scan('{config.warehouse}/{config.namespace}.db/my_table/metadata', allow_moved_paths = true, version = '1');
""").show(max_width=1000000)


# seems to only support rest, hive, hdfs catalog, not sqlite:
# https://iceberg.apache.org/docs/1.6.1/spark-configuration/
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("example")
    .config("spark.jars", "jars/iceberg-spark-runtime-3.5_2.13-1.7.1.jar")
    .getOrCreate()
)


spark.conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
spark.conf.set("spark.sql.catalog.hadoop_prod.warehouse", config.warehouse)

spark.sql(f"""--sql
use hadoop_prod.{config.namespace}
""")


# Verdict, its probably best to use to_duckdb
# rest catalog is probably most general, but its not supported by duckdb
# https://github.com/duckdb/duckdb_iceberg/issues/16
# use sqlite for now, if we dont need pyspark.

con = catalog.load_table(f"{config.namespace}.my_table").scan().to_duckdb("my_table")

# duckdb will not see this update
table.append(df)

con.sql(f"""--sql
select * from my_table
""").show()
