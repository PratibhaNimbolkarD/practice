from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Pratibha").getOrCreate()

data = [('Pratibha' , 25) , ('Basheer' , 28) , ('Krishna' , 34) , ('Kuldeep' , 56)]

rdd = spark.sparkContext.parallelize(data)

#perform count action in rdd

print("The number of elements in data rdd: ",rdd.count())

#FirstAction : retrieve the first element of RDD

print(rdd.first())

#Takeaction : retrieve the elements of RDD by take action

print(rdd.take(2))