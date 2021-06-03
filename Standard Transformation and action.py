pyspark --master yarn \
  --conf spark.ui.port=12890 \
  --num-executors 2 \
  --executor-memory 512M \
  --packages com.databricks:spark-avro_2.10:2.0.1
  --jars <PATH_TO_JAR>

orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")
ordersFiltered = orders. \
filter(lambda oi: oi.split(",")[3] in ["COMPLETE","CLOSED"])
ordersMap = ordersFiltered. \
map(lambda oi : (int(oi.split(",")[0]), oi.split(",")[1]))
orderItemsMap = orderItems.\
map(lambda ou :(int(ou.split(",")[1]), (int(ou.split(",")[2]), float(ou.split(",")[4]))))
ordersJoin = ordersMap.join(orderItemsMap)
ordersJoinMap = ordersJoin.\
map(lambda oa: ((oa[1][0], oa[1][1][0]), oa[1][1][1]))
from operator import add 
dailyRevenuePerroduct = ordersJoinMap.reduceByKey(add)
productsRaw = open("/data/retail_db/products/part-00000").read().splitlines()
products =  sc.parallelize(productsRaw)
productsMap = products.\
map(lambda oe: (int(oe.split(",")[0]), oe.split(",")[2]))
dailyRevenuePerroductMap = dailyRevenuePerroduct.\
map(lambda oa: (oa[0][1],(oa[0][0],oa[1])))
dailyRevenuePerProductJoin = dailyRevenuePerroductMap.join(productsMap)


dailyRevenuePerProduct = dailyRevenuePerProductJoin.\
map(lambda t: 
((t[1][0][0], -t[1][0][1]),\
(t[1][0][0],round(t[1][0][1], 2),t[1][1])
))

dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()
dailyRevenuePerProductName = dailyRevenuePerProductSorted.\
map(lambda ra: ra[1])

dailyRevenuePerProductNameDF = dailyRevenuePerProductName.\
.coalesce(2).\
toDF(schema=["order_date", "revenue_per_product","product_name"])

dailyRevenuePerProductNameDF.save("/user/itv000439/daily_revenue_avro_python", "com.databricks.spark.avro")
 type(dailyRevenuePerProductNameDF)