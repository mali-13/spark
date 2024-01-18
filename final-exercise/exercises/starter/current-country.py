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

# Create a StructType for the CustomerLocation schema for the following fields:
# {"accountNumber":"814840107","location":"France"}
locationSchema = StructType(
    [StructField("accountNumber", StringType()), StructField("location", StringType())]
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("current-country").getOrCreate()

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
zSetEntriesEncodedDF = sparkSession.sql(
    "select key, zSetEntries[0].element as redisEvent from RedisData"
)

# From the dataframe use the unbase64 function to select a column called redisEvent with the base64 decoded JSON,
# and cast it to a string
zSetEntriesDecodedDF1 = zSetEntriesEncodedDF.withColumn(
    "redisEvent", unbase64("redisEvent").cast("string")
)

# Repeat this a second time, so now you have two separate dataframes that contain redisEvent data
zSetEntriesDecodedDF2 = zSetEntriesEncodedDF.withColumn(
    "redisEvent", unbase64("redisEvent").cast("string")
)

# Filter DF1 for only those that contain the birthDay field (customer record)
zSetEntriesDecodedDF1.filter(col("redisEvent").contains("birthDay"))

# Filter DF2 for only those that contain the location field, we will filter
# out null rows later
zSetEntriesDecodedDF2.filter(col("redisEvent").contains("location"))

# Using the customer StructType, deserialize the JSON from the first redis decoded streaming dataframe,
# selecting column customer.* as a temporary view called Customer
zSetEntriesDecodedDF1.withColumn(
    "customer", from_json("redisEvent", customerSchema)
).select("customer.*").createOrReplaceTempView("Customer")

# Using the customer location StructType, deserialize the JSON from the second redis decoded streaming dataframe,
# selecting column customerLocation.* as a temporary view called CustomerLocation
zSetEntriesDecodedDF2.withColumn(
    "customerLocation", from_json("redisEvent", locationSchema)
).select(col("customerLocation.*")).createOrReplaceTempView("CustomerLocation")

# Using spark.sql select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer
# where birthDay is not null
customerCompleteDF = sparkSession.sql(
    "select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer "
    "where birthDay is not null"
)

# Select the customerAccountNumber, homeLocation, and birth year (using split)
customerDF = customerCompleteDF.select(
    "customerAccountNumber",
    "homeLocation",
    split("birthDay", "-").getItem(0).alias("birthYear"),
)

# Using spark.sql select accountNumber as locationAccountNumber, and location
customerLocationDF = sparkSession.sql(
    "select accountNumber as locationAccountNumber, location from CustomerLocation"
)

# Join the customer and customer location data using the expression: customerAccountNumber = locationAccountNumber
currentAndHomeLocationDF = customerLocationDF.join(
    customerDF, expr("customerAccountNumber = locationAccountNumber")
)

# Write the stream to the console, and configure it to run indefinitely
# can you find the customer(s) who are traveling out of their home countries?
# When calling the customer, customer service will use their birth year to help
# establish their identity, to reduce the risk of fraudulent transactions.
# +---------------------+-----------+---------------------+------------+---------+
# |locationAccountNumber|   location|customerAccountNumber|homeLocation|birthYear|
# +---------------------+-----------+---------------------+------------+---------+
# |            982019843|  Australia|            982019843|   Australia|     1943|
# |            581813546|Phillipines|            581813546| Phillipines|     1939|
# |            202338628|Phillipines|            202338628|       China|     1944|
# |             33621529|     Mexico|             33621529|      Mexico|     1941|
# |            266358287|     Canada|            266358287|      Uganda|     1946|
# |            738844826|      Egypt|            738844826|       Egypt|     1947|
# |            128705687|    Ukraine|            128705687|      France|     1964|
# |            527665995|   DR Congo|            527665995|    DR Congo|     1942|
# |            277678857|  Indonesia|            277678857|   Indonesia|     1937|
# |            402412203|   DR Congo|            402412203|    DR Congo|     1945|
# +---------------------+-----------+---------------------+------------+---------+
currentAndHomeLocationDF.writeStream.format("console").outputMode(
    "append"
).start().awaitTermination()
