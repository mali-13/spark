from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, DoubleType

# Create a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
depositSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("amount", DoubleType()),
        StructField("dateAndTime", StringType()),
    ]
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("bank-deposits").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

# Read the bank-deposits kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
bankDepositsRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "bank-deposits")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
bankDepositsStreamingDF = bankDepositsRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the kafka message StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "BankDeposits"
# it can later be queried with spark.sql
bankDepositsStreamingDF.withColumn("value", from_json("value", depositSchema)).select(
    col("value.*")
).createOrReplaceTempView("BankDeposits")

# Using spark.sql, select * from BankDeposits
bankDepositsSelectDF = sparkSession.sql("select * from BankDeposits")

# Write the stream to the console, and configure it to run indefinitely, the console output will look something like
# this: +-------------+------+--------------------+ |accountNumber|amount|         dateAndTime|
# +-------------+------+--------------------+ |    103397629| 800.8|Oct 6, 2020 1:27:...|
# +-------------+------+--------------------+
bankDepositsSelectDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()
