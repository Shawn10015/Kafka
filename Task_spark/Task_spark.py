from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.appName("Telegram_analysis").config("spark.ui.port", "4050").getOrCreate()

data_type = StructType([
    StructField("time", TimestampType(), True),
    StructField("channel", LongType(), True),
    StructField("text", StringType(), True),
    StructField("media", BooleanType(), True)
])

df = spark.readStream.schema(data_type).parquet("Kafka/Task_spark/Tele_data/")

window_time = "1 minute"  
slide_time = "1 minute"   

#group time in 1 mintues
window_count = df.groupBy(window(col("time"), window_time, slide_time),col("channel")).count().withColumnRenamed("count", "messages_count")

#every mintues 1 message should be good
max_message = 1 
error_frequency = col("messages_count") > max_message

# check
check_message_count = window_count.filter(error_frequency)

frequency_query = check_message_count.writeStream.outputMode("update").format("console").start()

negative_word = ["плохо", "стра", "ненависть", "злой", "боль" ]

#check words
condition = col("text").contains(negative_word[0])
for word in negative_word[1:]:
    condition = condition | col("text").contains(word)

check_bad_word = df.filter(condition)

word_query = check_bad_word.writeStream.outputMode("update").format("console").start()

word_query.awaitTermination()
frequency_query.awaitTermination()