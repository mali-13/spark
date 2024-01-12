from pyspark.sql import SparkSession

# Create a variable with the absolute path to the text file
text = "/home/workspace/lesson-1-streaming-dataframes/exercises/starter/Test.txt"

# Create a Spark session
sparkSession = SparkSession.builder.appName("HelloSpark").getOrCreate()

# Set the log level to WARN
sparkSession.sparkContext.setLogLevel('WARN')

# Using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file
textData = sparkSession.read.text(text).cache()


# Create a global variable for number of times the letter a is found
numAs = textData.filter(textData.value.contains('a')).count()
# Create a global variable for number of times the letter b is found
numBs = textData.filter(textData.value.contains('b')).count()

print("*******")
print("*******")
print("*****Lines with a: %i, lines with b: %i" % (numAs, numBs))
print("*******")
print("*******")

sparkSession.stop()