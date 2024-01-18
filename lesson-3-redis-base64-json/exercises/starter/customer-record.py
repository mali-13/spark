from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType
)

sparkSession = SparkSession.builder.appName("customer-record").getOrCreate()
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

redisRawDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

redisDF = redisRawDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

redisDF.withColumn("value", from_json("value", redisMessageSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisData")

zSetEntriesEncodedStreamingDF = sparkSession.sql(
    "select key, zSetEntries[0].element as customer from RedisData"
)

# Create a StructType for the Customer schema for the following fields: {"customerName":"Frank Aristotle",
# "email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955",
# "location":"Jordan"}
customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType()),
    ]
)

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customer",
    unbase64("customer").cast("string"),
)

zSetDecodedEntriesStreamingDF.withColumn(
    "customer",
    from_json("customer", customerSchema),
).select(col("customer.*")).createOrReplaceTempView("Customer")

customerStreamingDF = sparkSession.sql(
    "select accountNumber, location, birthDay from Customer where birthDay is not null"
)

relevantCustomerFieldsStreamingDF = customerStreamingDF.select(
    "accountNumber",
    "location",
    split(customerStreamingDF.birthDay, "-").getItem(0).alias("birthYear"),
)

relevantCustomerFieldsStreamingDF.writeStream.format("console").outputMode(
    "append"
).start().awaitTermination()
