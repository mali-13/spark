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

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

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

# Create a StructType for the Reservation schema for the following fields: {"reservationId":"814840107",
# "customerName":"Jim Harris", "truckNumber":"15867", "reservationDate":"Sep 29, 2020, 10:06:23 AM"}
reservationSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("customerName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("reservationDate", StringType()),
    ]
)

# Create a StructType for the Payment schema for the following fields:
# {"reservationId":"9856743232","customerName":"Frank Aristotle","date":"Sep 29, 2020, 10:06:23 AM","amount":"946.88"}
paymentSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("customerName", StringType()),
        StructField("date", StringType()),
        StructField("amount", StringType()),
    ]
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("reservation-payment").getOrCreate()

# Set the log level to WARN
sparkSession.sparkContext.setLogLevel("WARN")

# Read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
redisRawDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
redisDF = redisRawDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
redisDF.withColumn("value", from_json("value", redisMessageSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisData")

# Using spark.sql, select key, zSetEntries[0].element as redisEvent from RedisData
zSetEntriesEncodedStreamingDF = sparkSession.sql(
    "select key, zSetEntries[0].element as redisEvent from RedisData"
)

# From the dataframe use the unbase64 function to select a column called redisEvent with the base64 decoded JSON,
# and cast it to a string
zSetDecodedEntriesStreamingDF1 = zSetEntriesEncodedStreamingDF.withColumn(
    "redisEvent", unbase64("redisEvent").cast("string")
)

# Repeat this a second time, so now you have two separate dataframes that contain redisEvent data
zSetDecodedEntriesStreamingDF2 = zSetEntriesEncodedStreamingDF.withColumn(
    "redisEvent", unbase64("redisEvent").cast("string")
)

# Filter DF1 for only those that contain the reservationDate field (customer record)
zSetDecodedEntriesStreamingDF1.filter(col("redisEvent").contains("reservationDate"))

# Filter DF2 for only those that do not (~) contain the reservationDate field (all records other than customer) we will
# filter out null rows later
zSetDecodedEntriesStreamingDF2.filter(~(col("redisEvent").contains("reservationDate")))

# Using the reservation StructType, deserialize the JSON from the first redis decoded streaming dataframe,
# selecting column reservation.* as a temporary view called Reservation
zSetDecodedEntriesStreamingDF1.withColumn(
    "reservation", from_json("redisEvent", reservationSchema)
).select(col("reservation.*")).createOrReplaceTempView("Reservation")

# Using the payment StructType, deserialize the JSON from the second redis decoded streaming dataframe,
# selecting column payment.* as a temporary view called Payment
zSetDecodedEntriesStreamingDF2.withColumn(
    "payment", from_json("redisEvent", paymentSchema)
).select(col("payment.*")).createOrReplaceTempView("Payment")

# Using spark.sql select reservationId, reservationDate from Reservation where reservationDate is not null
reservationStreamingDF = sparkSession.sql(
    "select reservationId, reservationDate from Reservation where reservationDate is not null"
)

# Using spark.sql select reservationId as paymentReservationId, date as paymentDate, amount as paymentAmount from
# Payment
paymentStreamingDF = sparkSession.sql(
    "select reservationId as paymentReservationId, amount as paymentAmount, date as paymentDate from Payment"
)

# Join the reservation and payment data using the expression: reservationId=paymentReservationId
joinedDF = reservationStreamingDF.join(
    paymentStreamingDF, expr("reservationId=paymentReservationId")
)

# Write the stream to the console, and configure it to run indefinitely
# can you find the reservations who haven't made a payment on their reservation?
joinedDF.writeStream.format("console").outputMode("append").start().awaitTermination()
