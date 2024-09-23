import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,concat, lit

spark = SparkSession.builder \
    .appName("Project Assignment") \
    .getOrCreate()

args = getResolvedOptions(sys.argv, ['s3_bucket'])


s3_bucket = args['s3_bucket']

users_path = s3_bucket+'users.json'
products_path = s3_bucket+'products.json'
orders_path = s3_bucket+'orders.json'

final_output_path = s3_bucket+'merged_data/'

users_df= spark.read.option("multiline",True).json(users_path)
products_df= spark.read.option("multiline",True).json(products_path)
orders_df= spark.read.option("multiline",True).json(orders_path)


users_df_exploded =users_df.select(explode("users").alias("users")).\
                            select("users.user_id",\
                                    col("users.name.first_name"),\
                                    col("users.name.last_name"),\
                                    col("users.contact.email").alias("email"),\
                                    col("users.contact.phone").alias("phone"),\
                                    col("users.address.home.city").alias("home_address_city"),\
                                    col("users.address.home.street").alias("home_address_street"),\
                                    col("users.address.home.zipcode").alias("home_address_zipcode"),\
                                    col("users.address.office.city").alias("office_address_city"),\
                                    col("users.address.office.street").alias("office_address_street"),\
                                    col("users.address.office.zipcode").alias("office_address_zipcode")
                                  )


products_df_exploded =products_df.select(explode("products").alias("products")).\
                            select("products.category",\
                                    col("products.price").alias("product_price"),\
                                    col("products.product_id"),
                                    col("products.product_name").alias("product_product_name"),\
                                    col("products.stock_quantity")                       
                                  )
 


orders_df_exploded =orders_df.select(explode("orders").alias("orders")).\
                            select("orders.customer_id",\
                                    col("orders.items").alias("items"),\
                                    col("orders.order_date").alias("order_date"),
                                    col("orders.order_id").alias("order_id"),\
                                    col("orders.total_amount").alias("total_amount")                       
                                  )

orders_df_exploded1=orders_df_exploded.select("customer_id","order_id","order_date","total_amount",explode("items").alias("items"))    
orders_df_exploded2=orders_df_exploded1.select("customer_id",\
                                               "order_id",\
                                               "order_date",\
                                               "total_amount",\
                                               "items.item_id",\
                                               "items.price",\
                                               col("items.product_name").alias("item_product_name"),\
                                               "items.quantity")


final_output = orders_df_exploded2.join(users_df_exploded ,orders_df_exploded2.customer_id == users_df_exploded.user_id ,"inner").\
                                    join(products_df_exploded,orders_df_exploded2.item_product_name == products_df_exploded.product_product_name , "inner").\
                                    withColumn("full_name", concat("first_name", lit(" "), "last_name")).\
                                    select("user_id",\
                                             "full_name",\
                                             "email",\
                                             "order_id",\
                                             "order_date",\
                                             "item_id",\
                                             "item_product_name",\
                                             "quantity",\
                                             col("product_price").alias("price"),\
                                             "total_amount",\
                                             "product_product_name",\
                                             "category",\
                                             "stock_quantity")

final_output.write.mode("overwrite").json(final_output_path)
