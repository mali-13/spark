from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    StringType,
    BooleanType,
    ArrayType,
    DateType,
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("bank-deposit").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

# Create deposit a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
# Cast the amount as a FloatType
depositSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dateAndTime", DateType()),
    ]
)

# Read the bank-deposit kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
depositRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "bank-deposits")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
depositStreamingDF = depositRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the kafka message StructType, deserialize the JSON from the streaming dataframe
depositStreamingDF.withColumn("value", from_json("value", depositSchema)).select(
    col("value.*")
).createOrReplaceTempView("BankDeposits")

# Using spark.sql, select * from BankDeposits where amount > 200.00 into a dataframe
depositDF = sparkSession.sql("select * from BankDeposits where amount > 200.00")

# Create a customer kafka message schema StructType including the following JSON elements: {"customerName":"Trevor
# Anandh","email":"Trevor.Anandh@test.com","phone":"1015551212","birthDay":"1962-01-01","accountNumber":"45204068",
# "location":"Togo"}
customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", DateType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType()),
    ]
)

# Read the bank-customers kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
customerRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "bank-customers")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
customerStreamingDF = customerRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the kafka message StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "BankCustomers"
# it can later be queried with spark.sql

customerStreamingDF.withColumn("value", from_json("value", customerSchema)).select(
    col("value.*")
).createOrReplaceTempView("BankCustomers")

# Using spark.sql, select customerName, accountNumber as customerNumber from BankCustomers into a dataframe
customerDF = sparkSession.sql(
    "Select customerName, accountNumber as depositAccountNumber from BankCustomers"
)

# Join the customer dataframe with the deposit dataframe
customerWithDepositDF = customerDF.join(
    depositDF, expr("accountNumber = depositAccountNumber")
).select(col("customerName"), col("accountNumber"), col("amount"))

# Write the stream to the console, and configure it to run indefinitely, the console output will look something like
# this:
customerWithDepositDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()

# . +-------------+------+--------------------+------------+--------------+
# . |accountNumber|amount|         dateAndTime|customerName|customerNumber|
# . +-------------+------+--------------------+------------+--------------+
# . |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
# . |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
# . |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
# . +-------------+------+--------------------+------------+--------------+
