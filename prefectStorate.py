
from datetime import timedelta, datetime
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
import requests
from prefect.executors import LocalDaskExecutor, LocalExecutor, DaskExecutor
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import substring, when, col
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, lit
from prefect import resource_manager
import pyspark
import prefect
import pandas as pd
from prefect.storage import GitLab, GitHub
from prefect.run_configs import UniversalRun

old_headnumber = ["162","163","164","165","166","167","168","169","123","124","125","127","129","120","121","122","126","128","186","188","199","0162","0163","0164","0165","0166","0167","0168","0169","0123","0124","0125","0127","0129","0120","0121","0122","0126","0128","0186","0188","0199"]
new_headnumber =["32","33","34","35","36","37","38","39","83","84","85","81","82","70","79","77","76","78","56","58","59", "032","033","034","035","036","037","038","039","083","084","085","081","082","070","079","077","076","078","056","058","059"]
path_prefix = "hdfs://192.168.45.92:9000"
batch_size = 500
count = 0
number_pos = 1

@resource_manager
class SparkContext:
    def __init__(self, app_name: str):
        self._app_name = app_name
    
    def setup(self):
        # return (
        #     pyspark.sql.SparkSession
        #     .builder
        #     .appName(self._app_name)
        #     .getOrCreate()
        # )

        conf = SparkConf()\
            .set("spark.executor.cores", "2") \
            .set("spark.cores.max", "4")\
            .set("spark.executor.memory", "10g")\
            .set("spark.driver.maxResultSize", "10g")\
            .set("spark.driver.bindAddress", "0.0.0.0")\
            .set("spark.hadoop.dfs.replication", "1")\
            .setMaster("spark://192.168.45.92:7077")\
            .set("spark.network.timeout", 1200)\
            .setAppName(self._app_name)
        return SparkSession.builder.config(conf=conf).getOrCreate()

    def cleanup(self, spark: pyspark.sql.session.SparkSession):
        spark.stop()

@F.udf(returnType=T.ArrayType(elementType=T.StringType()))
def encode(list_encrypt):
    url = 'http://192.168.45.45:8779/ens'

    payload= {'lisd': list_encrypt}
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request('POST', url, headers=headers, json=payload)

    return response.json()

def standardized_phonenumbers(phone_number):
    for index, headnumber in enumerate(old_headnumber):
        try:
            if phone_number.index(headnumber) == 0:
                phone_number = phone_number.replace(headnumber, new_headnumber[index], 1)   
                return phone_number
        except Exception as e:
            continue
    return phone_number

def encode_by_batch(
    df: DataFrame, 
    decode_col: str,
    encode_col: str,
    batch_size: int, spark):
    df = (
        df
        .select(decode_col)
        .distinct()
    )
    temp = (
        df.orderBy(F.rand())
        .withColumn('index', F.monotonically_increasing_id())
        .withColumn('group', F.round(F.col('index') / batch_size))
        .groupBy('group')
        .agg(F.collect_list(decode_col).alias('values'))
        .withColumn('encode_values', encode(F.col('values')))
    ).cache()

    encoded = (
        temp
        .where(F.size('encode_values') == F.size('values'))
        .select(F.arrays_zip('values', 'encode_values').alias('lst_zip'))
        .select(F.explode('lst_zip').alias('zip'))
        .select(
            F.col('zip.values').alias('values'),
            F.col('zip.encode_values').alias(encode_col),
        )
    )
    path = f'{path_prefix}/encode_progress/{decode_col}/encoded/'    

    (
        encoded
        .withColumn('no', F.lit(count))
        .write.format('parquet')
        .save(
            path=path, 
            mode='overwrite', 
            partitionBy='no',
            partitionOverwriteMode='dynamic',
        )
    )
    encoded_data = spark.read.format('parquet').load(
        path=f'{path_prefix}/encode_progress/{decode_col}/encoded',
    ).drop('no')
    return encoded_data

@task(log_stdout=True)
def extract(spark: pyspark.sql.session.SparkSession) -> DataFrame:
    """Get a list of data"""
    logger = prefect.context.get("logger")
    logger.info(f"Hello, Cloud {number_pos}!")
    data_frame_csv = spark.read.format("csv").option("header", "true").load(f"hdfs://192.168.45.92:9000/raw_data_css/export_pos_{number_pos}.csv")
    return data_frame_csv

@task
def transform(data_frame_csv: DataFrame, spark: pyspark.sql.session.SparkSession) -> DataFrame:
    udf_func = udf(lambda x: standardized_phonenumbers(x),returnType=StringType())
    data_frame_csv = data_frame_csv.withColumn('calling_base', data_frame_csv['CALLING'])\
                                .withColumn('called_base', data_frame_csv['CALLED'])\
                                .withColumn('CALLING', udf_func(data_frame_csv.CALLING))\
                                .withColumn('CALLED', udf_func(data_frame_csv.CALLED))

    encoded_data_calling = encode_by_batch(data_frame_csv, "CALLING", "calling_encoded", batch_size, spark)
    encoded_data_called = encode_by_batch(data_frame_csv, "CALLED", "called_encoded", batch_size, spark)
    encoded_base_data_calling = encode_by_batch(data_frame_csv, "calling_base", "calling_base_encoded", batch_size, spark)
    encoded_base_data_called = encode_by_batch(data_frame_csv, "called_base", "called_base_encoded", batch_size, spark)
    encoded_base_data_called.show()
    data_frame_csv.show()
    cond = [data_frame_csv.CALLING == encoded_data_calling.values]
    cond2 = [data_frame_csv.CALLED == encoded_data_called.values]
    cond3 = [data_frame_csv.calling_base == encoded_base_data_calling.values]
    cond4 = [data_frame_csv.called_base == encoded_base_data_called.values]

    encoded_standardized_data = (
        data_frame_csv
        .join(
            encoded_data_calling,
            cond,
            'left'
        )
        .join(
            encoded_data_called,
            cond2,
            'left'
        )
        .join(
            encoded_base_data_calling,
            cond3,
            'left'
        )
        .join(
            encoded_base_data_called,
            cond4,
            'left'
        )
        .select(encoded_data_calling.calling_encoded.alias('calling'), encoded_data_called.called_encoded.alias('called'), data_frame_csv.DATETIME, data_frame_csv.DURATION, 
                 data_frame_csv.PROVINCE_NAME, data_frame_csv.DISTRICT_NAME, data_frame_csv.VILLAGE_NAME, data_frame_csv.CALLING.alias('calling_data'), 
                 encoded_base_data_calling.calling_base_encoded.alias("calling_base"), encoded_base_data_called.called_base_encoded.alias("called_base")))
    new_column_names = [c.lower() for c in encoded_standardized_data.columns]
    encoded_standardized_data = encoded_standardized_data.toDF(*new_column_names)

    encoded_standardized_data = encoded_standardized_data.withColumn("datetime", encoded_standardized_data.datetime.cast("long"))\
        .withColumn('month', substring('datetime', 5, 2).cast('int'))\
        .withColumn('day', substring('datetime', 7, 2).cast('int'))\
        .withColumn('hour', substring('datetime', 9, 2).cast('int')) \
        .withColumn('is_night', when((col('hour') <= 7) | (col('hour') >= 23), True).otherwise(False).cast("boolean")) \
        .withColumn("duration", encoded_standardized_data.duration.cast("int"))\
        .withColumn('type_paid', lit('prepaid')) \
        .withColumn('month_update', substring('datetime', 0, 6))\
        .withColumn('suffix_calling', substring('calling_data', -3, 3))
    encoded_standardized_data.drop('calling_data')

    encoded_standardized_data = encoded_standardized_data.drop('calling_data')
    encoded_standardized_data.show()
    return encoded_standardized_data

@task
def load(encoded_standardized_data):
    """Print the data to indicate it was received"""
    column_list = ["month_update", "suffix_calling"]
    encoded_standardized_data.repartition("month_update","suffix_calling").write.partitionBy("month_update","suffix_calling").format('parquet').save(
    path=f"hdfs://192.168.45.92:9000/data_parquet/export_pos_partition_re_parquet", mode="append",
    partitionOverwriteMode='dynamic')
    # encoded_standardized_data.repartition("month_update","suffix_calling").write.partitionBy("month_update","suffix_calling").format("delta").save(
    # path=f"hdfs://192.168.45.249:8020/data_parquet/export_pos_partition_re_rep1", mode="append",
    # partitionOverwriteMode='dynamic')
    print(f'complete_file {(str(number_pos))}')

# def main():
schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes=30),
)

# conf = SparkConf()\
#     .set("spark.executor.cores", "2") \
#     .set("spark.cores.max", "4")\
#     .set("spark.driver.maxResultSize", "10g")\
#     .set("spark.executor.memory", "10g")\
#     .set("spark.driver.bindAddress", "0.0.0.0")\
#     .set("spark.hadoop.dfs.replication", "1")\
#     .setMaster("spark://192.168.45.92:7077")\
#     .set("spark.network.timeout", 1200)\
#     .setAppName("convert csv")
# spark = SparkSession.builder.config(conf=conf).getOrCreate()
# data_frame_csv = spark.read.format("csv").option("header", "true").load(f"hdfs://192.168.45.92:9000/raw_data_css/export_pos_{number_pos}.csv")
# udf_func = udf(lambda x: standardized_phonenumbers(x),returnType=StringType())
# data_frame_csv = data_frame_csv.withColumn('calling_base', data_frame_csv['CALLING'])\
#                             .withColumn('called_base', data_frame_csv['CALLED'])\
#                             .withColumn('CALLING', udf_func(data_frame_csv.CALLING))\
#                             .withColumn('CALLED', udf_func(data_frame_csv.CALLED))
# a = data_frame_csv.toPandas()
# print(a)
# data_frame_csv.show()
# print(type(data_frame_csv))
# with Flow("etl", executor=LocalDaskExecutor('threads'), schedule=schedule) as flow:
with Flow("etl", schedule=schedule) as flow:
    with SparkContext('css_convert') as spark:
        reference_data = extract(spark)
        coverted_data = transform(reference_data, spark)
        load(coverted_data)

# Register the flow under the "tutorial" project
# flow.run(executor=LocalDaskExecutor(scheduler="threads"))
# flow.register(project_name="css-scoring")

flow.storage = GitHub(
            repo="tran-hong-khanh/prefect-flows-storate",
            path="prefectStorate.py",
            # host="https://gitlab.vmgmedia.vn/",
            # access_token_secret="glpat-su5uaLifXGHDhJXQRnfM"
        )

