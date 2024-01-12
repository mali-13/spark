from pyspark.sql import SparkSession

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("atm-visits").getOrCreate()

# Set the log level to WARN
sparkSession.sparkContext.setLogLevel("WARN")

# Read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
atmVisitsRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "atm-visits")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
atmVisitsStreamingDF = atmVisitsRawStreamingDF.selectExpr(
    "cast(key as string) transactionId", "cast(value as string) location"
)

# Create a temporary streaming view called "ATMVisits" based on the streaming dataframe
atmVisitsStreamingDF.createOrReplaceTempView("ATMVisits")

# Query the temporary view with spark.sql, with this query: "select * from ATMVisits"
atmVisitsSelectStarDF = sparkSession.sql("select * from ATMVisits")

# Write the dataFrame from the last select statement to kafka to the atm-visit-updates topic, on the broker
# kafka:19092 for the "checkpointLocation" option in the writeStream, be sure to use a unique file path to avoid
# conflicts with other spark scripts
atmVisitsSelectStarDF.selectExpr(
    "cast(transactionId as string) as key", "cast(location as string) as value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option(
    "topic", "atm-visit-updates"
).option(
    "checkpointLocation", "/tmp/kafkacheckpoint_1"
).start().awaitTermination()
