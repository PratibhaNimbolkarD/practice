from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName('broadcast').getOrCreate()

Transaction = [(100, 'Cosmetic' , 150),
               (200, 'Apparel', 250),
               (300, 'Shirt', 400),
               (400, 'Trouser', 500),
               (500, 'Socks', 20),
               (100, 'Belt', 70),
               (200, 'Cosmetic', 250),
               (300, 'Shoe',400),
               (400, 'Socks', 25),
               (500, 'Shorts', 100)]

transaction_df = spark.createDataFrame(data=Transaction , schema=['Store_id', 'Item', 'Amount'])
Store = [
    (100, 'Store_London'),
    (200, 'Store_paris'),
    (300, 'Store_frankfurt'),
    (400, 'Store_stockholm'),
    (500, 'Store_Oslo')
]

store_df = spark.createDataFrame(data=Store , schema=['Store_id' , 'Store_name'])
join_df = transaction_df.join(broadcast(store_df) , transaction_df.Store_id==store_df.Store_id)
store_df.show()
transaction_df.show()
join_df.show()
