from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    LongType,
    DateType,
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("bank-withdrawals").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

# Create bank withdrawals kafka message schema StructType including the following JSON elements:
#  {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
withdrawalSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dataAndTime", DateType()),
        StructField("transactionId", LongType()),
    ]
)

# Read the bank-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
withdrawalRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "bank-withdrawals")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
withdrawalStreamingDF = withdrawalRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the kafka message StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "BankWithdrawals"
# it can later be queried with spark.sql
withdrawalStreamingDF.withColumn("value", from_json("value", withdrawalSchema)).select(
    col("value.*")
).createOrReplaceTempView("BankWithdrawals")

# Using spark.sql, select * from BankWithdrawals into a dataframe
withdrawalDF = sparkSession.sql("select * from BankWithdrawals")

# Create an atm withdrawals kafka message schema StructType including the following JSON elements:
# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}
atmWithdrawalSchema = StructType(
    [
        StructField("transactionDate", DateType()),
        StructField("transactionId", LongType()),
        StructField("atmLocation", StringType()),
    ]
)

# Read the atm-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
atmWithdrawalRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "atm-withdrawals")
    .option("startingOffsets", "earliest")
    .load()
)
# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
atmWithdrawalStreamingDF = atmWithdrawalRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the kafka message StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "AtmWithdrawals"
# it can later be queried with spark.sql
atmWithdrawalStreamingDF.withColumn(
    "value", from_json("value", atmWithdrawalSchema)
).select(col("value.*")).createOrReplaceTempView("AtmWithdrawals")

# Using spark.sql, select * from AtmWithdrawals into a dataframe
atmWithdrawalDF = sparkSession.sql(
    "select transactionDate, transactionId as atmTransactionId, atmLocation from AtmWithdrawals"
)

# Join the atm withdrawals dataframe with the bank withdrawals dataframe
atmWithdrawalDF = atmWithdrawalDF.join(
    withdrawalDF, expr("transactionId = atmTransactionId")
)

# write the stream to the kafka in a topic called withdrawals-location, and configure it to run indefinitely,
# the console will not output anything. You will want to attach to the topic using the kafka-console-consumer inside
# another terminal for the "checkpointLocation" option in the writeStream, be sure to use a unique file path to avoid
# conflicts with other spark scripts
atmWithdrawalDF.selectExpr(
    "cast(transactionId as string) as key", "to_json(struct(*)) as value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option(
    "topic", "withdrawals-location"
).option(
    "checkpointLocation", "/tmp/kafkacheckpoint1001"
).start().awaitTermination()
