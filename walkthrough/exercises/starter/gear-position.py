from pyspark.sql import SparkSession

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("gear-positions").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

# Read the gear-position kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
gearPositionRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "gear-position")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
gearPositionStreamingDF = gearPositionRawStreamingDF.selectExpr(
    "cast(key as string) truckId", "cast(value as string) gearPosition"
)

# Create a temporary streaming view called "GearPosition" based on the streaming dataframe
gearPositionStreamingDF.createOrReplaceTempView("GearPosition")

# Query the temporary view "GearPosition" using spark.sql
gearPositionSelectStarDF = sparkSession.sql("select * from GearPosition")

# Write the dataframe from the last query to a kafka broker at kafka:19092, with a topic called gear-position-updates
gearPositionSelectStarDF.selectExpr(
    "cast(truckId as string) as key", "cast(gearPosition as string) as value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option(
    "topic", "gear-position-updates"
).option(
    "checkpointLocation", "/tmp/kafkacheckpoint"
).start().awaitTermination()
