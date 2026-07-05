#!/usr/bin/env python
# coding: utf-8

# In[5]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Simple_API_Streaming").getOrCreate()


# In[6]:


from pyspark.sql.functions import avg, max, min, col
import requests
import pandas as pd
import time


# In[7]:


weather_data = []

for i in range(10):

    url = "https://api.open-meteo.com/v1/forecast?latitude=53.3498&longitude=-6.2603&current=temperature_2m,relative_humidity_2m"

    response = requests.get(url)

    data = response.json()

    weather_data.append({
        "City":"Dublin",
        "Temperature":data["current"]["temperature_2m"],
        "Humidity":data["current"]["relative_humidity_2m"],
        "Time":data["current"]["time"]
    })

    print("Record",i+1,"Downloaded")

    time.sleep(5)


# - Weather API downloading records

# In[8]:


weather_df = pd.DataFrame(weather_data)

weather_df


# In[9]:


spark_df = spark.createDataFrame(weather_df)

spark_df.show()


# In[10]:


spark_df.select(avg("Temperature").alias("Average Temperature")).show()


# In[9]:


spark_df.select(max("Temperature").alias("Highest Temperature")).show()


# In[20]:


spark_df.select(min("Temperature").alias("Lowest Temperature")).show()


# In[12]:


spark_df.describe(["Humidity"]).show()


# In[13]:


spark_df.filter(col("Temperature") > 19).show()


# In[21]:


spark_df.write.mode("overwrite").csv("weather_results")


# In[22]:


get_ipython().system('jps')


# In[23]:


spark_df


# In[24]:


spark_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("hdfs://localhost:9000/CA2_sba2331777/Q3_Output/weather_data")


# - Read from Hadoop

# In[25]:


weather_hdfs = spark.read.option("header", True).csv(
    "hdfs://localhost:9000/CA2_sba2331777/Q3_Output/weather_data"
)

weather_hdfs.show()


# In[26]:


from pyspark.sql.functions import avg

weather_hdfs.select(
    avg("Temperature").alias("Average Temperature")
).show()


# In[27]:


from pyspark.sql.functions import max

weather_hdfs.select(
    max("Temperature").alias("Maximum Temperature")
).show()


# In[28]:


from pyspark.sql.functions import min

weather_hdfs.select(
    min("Temperature").alias("Minimum Temperature")
).show()


# After collecting live weather information from the Open-Meteo API and processing it using PySpark, the processed dataset was stored in the Hadoop Distributed File System (HDFS) under the directory /CA2_sba2331777/Q3_Output/weather_data. Spark automatically created distributed output files (part files) together with the _SUCCESS indicator. The processed data was then read back from HDFS and analysed by calculating the average, maximum, and minimum temperatures, demonstrating the integration between Spark and Hadoop Distributed File System.

# In[29]:


locals()


# In[30]:


spark


# In[36]:


weather_hdfs.show()


# In[37]:


weather_hdfs.printSchema()


# In[38]:


weather_hdfs.describe().show()


# In[39]:


from pyspark.sql.functions import avg

weather_hdfs.groupBy("City") \
    .agg(avg("Temperature").alias("Average Temperature")) \
    .show()


# In[ ]:





# In[ ]:





# In[ ]:




