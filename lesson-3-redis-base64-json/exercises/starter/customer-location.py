from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType,
    DateType,
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("customer-location").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", StringType()),
                    ]
                )
            ),
        ),
    ]
)

# Create a StructType for the CustomerLocation schema for the following fields:
# {"accountNumber":"814840107","location":"France"}
customerLocationSchema = StructType(
    [StructField("accountNumber", StringType()), StructField("location", StringType())]
)

# Read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
redisRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
redisStreamingDF = redisRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql

redisStreamingDF.withColumn("value", from_json("value", redisMessageSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisData")

# Using spark.sql, select key, zSetEntries[0].element as customerLocation from RedisData
zSetEntriesEncodedStreamingDF = sparkSession.sql(
    "select key, zSetEntries[0].element as customerLocation from RedisData"
)

# From the dataframe use the unbase64 function to select a column called customerLocation with the base64 decoded
# JSON, and cast it to a string
zSetEntriesDecodedStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customerLocation",
    unbase64(zSetEntriesEncodedStreamingDF.customerLocation).cast("string"),
)

# Using the customer location StructType, deserialize the JSON from the streaming dataframe, selecting column
# customerLocation.* as a temporary view called CustomerLocation
zSetEntriesDecodedStreamingDF.withColumn(
    "customerLocation", from_json("customerLocation", customerLocationSchema)
).select("customerLocation.*").createOrReplaceTempView("CustomerLocation")

# Using spark.sql select * from CustomerLocation
customerLocationStreamingDF = sparkSession.sql("select * from CustomerLocation")

# Write the stream to the console, and configure it to run indefinitely, the console output will look something like
# this:
customerLocationStreamingDF.writeStream.format("console").outputMode(
    "append"
).start().awaitTermination()

# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+
