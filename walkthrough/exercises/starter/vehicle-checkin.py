from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
)

# Create a Vehicle Status kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odometerReading":50513}
vehicleStatusSchema = StructType(
    [
        StructField("truckNumber", StringType()),
        StructField("destination", StringType()),
        StructField("milesFromShop", IntegerType()),
        StructField("odometerReading", IntegerType()),
    ]
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("vehicle-checkin").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

# Read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092,
# configuring the stream to read the earliest messages possible
vehicleStatusRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "vehicle-status")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
vehicleStatusStreamingDF = vehicleStatusRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the kafka message StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "VehicleStatus"
# it can later be queried with spark.sql
vehicleStatusStreamingDF.withColumn(
    "value", from_json("value", vehicleStatusSchema)
).select(col("value.*")).createOrReplaceTempView("VehicleStatus")

# Using spark.sql, select truckNumber as statusTruckNumber, destination, milesFromShop, odometerReading from
# VehicleStatus into a dataframe
vehicleStatusSelectDF = sparkSession.sql(
    "select truckNumber as statusTruckNumber, destination, milesFromShop, odometerReading from "
    "VehicleStatus"
)

# Create a Checkin Status kafka message schema StructType including the following JSON elements:
# {"reservationId":"1601485848310","locationName":"New Mexico","truckNumber":"3944","status":"In"}
vehicleCheckinSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("locationName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("status", StringType()),
    ]
)

# Read the check-in kafka topic as a source into a streaming dataframe with the bootstrap server
# localhost:9092, configuring the stream to read the earliest messages possible
vehicleCheckInRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "check-in")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
vehicleCheckinStreamingDF = vehicleCheckInRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the kafka message StructType, deserialize the JSON from the streaming dataframe

# create a temporary streaming view called "VehicleCheckin"
# it can later be queried with spark.sql
vehicleCheckinStreamingDF.withColumn(
    "value", from_json("value", vehicleCheckinSchema)
).select(col("value.*")).createOrReplaceTempView("VehicleCheckin")

# Using spark.sql, select reservationId, locationName, truckNumber as checkinTruckNumber, status from VehicleCheckin
# into a dataframe
vehicleCheckinSelectDF = sparkSession.sql(
    "select reservationId, locationName, truckNumber as checkinTruckNumber, status from VehicleCheckin"
)

# Join the customer dataframe with the deposit dataframe
checkinStatusDF = vehicleStatusSelectDF.join(
    vehicleCheckinSelectDF,
    expr("statusTruckNumber = checkinTruckNumber"),
)

# Write the stream to the console, and configure it to run indefinitely, the console output will look something like
# this:
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+
# |statusTruckNumber| destination|milesFromShop|odometerReading|reservationId|locationName|checkinTruckNumber|status|
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+
# |             1445|Pennsylvania|          447|         297465|1602364379489|    Michigan|              1445|    In|
# |             1445|     Colardo|          439|         298038|1602364379489|    Michigan|              1445|    In|
# |             1445|    Maryland|          439|         298094|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          439|         298185|1602364379489|    Michigan|              1445|    In|
# |             1445|    Maryland|          439|         298234|1602364379489|    Michigan|              1445|    In|
# |             1445|      Nevada|          438|         298288|1602364379489|    Michigan|              1445|    In|
# |             1445|   Louisiana|          438|         298369|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          438|         298420|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          436|         298471|1602364379489|    Michigan|              1445|    In|
# |             1445|  New Mexico|          436|         298473|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          434|         298492|1602364379489|    Michigan|              1445|    In|
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+

checkinStatusDF.selectExpr(
    "cast(statusTruckNumber as string) as key", "to_json(struct(*)) as value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option(
    "topic", "checkin-status"
).option(
    "checkpointLocation", "/tmp/kafkacheckpoint_6"
).start().awaitTermination()
