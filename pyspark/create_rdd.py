from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Pratibha').getOrCreate()

#create  RDD

number = [1, 3, 3,2,6]
rdd = spark.sparkContext.parallelize(number)
print(rdd.collect())

# RDD in list of tuple

name_age = [('Pratibha' , 25) , ('Shubham' , 26) , ('Nishta', 28)]
rdd1 = spark.sparkContext.parallelize(name_age)
print("All the elements of lis:" , rdd1.collect())

# perform count operation in this RDD

print("The number of elements in number rdd: " ,rdd.count())
print("The number of elements in name_age rdd: ",rdd1.count())

#FirstAction : retrieve the first element of RDD

print(rdd1.first())

#Takeaction : retrieve the elements of RDD by take action

print(rdd1.take(2))

#ForeachAction : print each element of RDD

rdd1.foreach(lambda x : print(x))

#mapTransformation : function in rdd

result_map = rdd1.map(lambda x : (x[0].lower() , x[1]))
print("All name is upper case" , result_map.collect())
result1_map = rdd1.map(lambda x : (x[0].upper() , x[1]))
print("All name in uppercase" , result1_map.collect())

#filereTransformation : filter function

filter_rdd = rdd1.filter(lambda x : (x[1]>25))
print("All the name where age is greater than 25 :", filter_rdd.collect())