from pyspark.sql import SparkSession

# Create a Spak Session, and name the app something relevant
sparkSession = SparkSession.builder.appName("fuel-level").getOrCreate()

# Set the log level to WARN
sparkSession.sparkContext.setLogLevel("WARN")

# read a stream from the kafka topic 'balance-updates', with the bootstrap server kafka:19092, reading from the earliest message
kafkaRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "fuel-level")
    .option("startingOffsets", "earliest")
    .load()
)

# Cast the key and value columns as strings and select them using a select expression function
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Write the dataframe to the console, and keep running indefinitely
kafkaStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()
