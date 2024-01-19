from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType,
    DateType,
    FloatType,
)

# Create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0,
# schema inference is not automatic
redisSchema = StructType(
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
# Create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", DateType()),
    ]
)

# Create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before
# Spark 3.0.0, schema inference is not automatic
stediEventSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType()),
    ]
)

# Create a spark application object
sparkSession = SparkSession.builder.appName("stedi-streaming-app").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

# Using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the
# source Be sure to specify the option that reads all the events from the topic including those that were published
# before you started the spark stream
redisRawDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
#
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet

# Cast the value column in the streaming dataframe as a STRING
redisDF = redisRawDF.selectExpr("cast(value as string) value")

redisDF.withColumn("value", from_json("value", redisSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisSortedSet")

# Execute a sql statement against a temporary view, which statement takes the element field from the 0th
# element in the array of structs and create a column called encodedCustomer the reason we do it this way is that the
# syntax available select against a view is different than a dataframe, and it makes it easy to select the nth
# element of an array in a sql column
encodedCustomerDF = sparkSession.sql(
    "select zSetEntries[0].element as encodedCustomer from RedisSortedSet"
)

# Take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
# +--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212",
# "birthDay":"2001-01-03"}
decodedCustomerDF = encodedCustomerDF.withColumn(
    "customer", unbase64("encodedCustomer").cast("string")
)

# Parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decodedCustomerDF.withColumn("customer", from_json("customer", customerSchema)).select(
    col("customer.*")
).createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null
# as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = sparkSession.sql(
    "select * from CustomerRecords where customerName is not null and phone is not null and birthDay is not null"
)

# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF

# The output should look like this:
# +--------------------+-----
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email", split("birthDay", "-").getItem(0).alias("birthYear")
)

# Using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source Be
# sure to specify the option that reads all the events from the topic including those that were published before you
# started the spark stream
stediEventRawDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

# cast the value column in the streaming dataframe as a STRING
stediEventDF = stediEventRawDF.selectExpr("cast(value as string) value")

# Parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
stediEventDF.withColumn("value", from_json("value", stediEventSchema)).select(
    col("value.*")
).createOrReplaceTempView("CustomerRisk")

# Execute a sql statement against a temporary view, selecting the customer and the score from the temporary
# view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = sparkSession.sql("select customer, score from CustomerRisk")

# Join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
customerWithBirthYearAndRiskDF = customerRiskStreamingDF.join(
    emailAndBirthYearStreamingDF, expr("customer=email")
)

# Sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com",
# "birthYear":"1963"}
customerWithBirthYearAndRiskDF.selectExpr(
    "cast(customer as string) as key", "to_json(struct(*)) as value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option(
    "topic", "senior-risks"
).option(
    "checkpointLocation", "/temp/kafkacheckpoint5001"
).start().awaitTermination()
