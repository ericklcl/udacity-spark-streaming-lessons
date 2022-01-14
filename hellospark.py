from pyspark.sql import SparkSession

# TODO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt
logFile = "/home/workspace/Test.txt"

# TODO: create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TODO: using the Spark session variable, call the appropriate function referencing the text file path to read the
#  text file
logData = spark.read.text(logFile).cache()

# TODO: call the appropriate function to filter the data containing the letter 'a', and then count the rows that were
#  found
numAs = logData.filter(logData.value.contains('a')).count()

# TODO: call the appropriate function to filter the data containing the letter 'b', and then count the rows that were
#  found
numBs = logData.filter(logData.value.contains('b')).count()

# TO-DO: print the count for letter 'd' and letter 's'
print("********")
print(numAs)
print(numBs)
print("********")

# TO-DO: stop the spark application
spark.stop()
