pyspark --master yarn --conf spark.ui.port=12888


####Row level transformation
#map and flatmap
orders = sc.textFile("/public/retail_db/orders")

orderItems = sc.textFile("/public/retail_db/order_items")

###DD_YYY_MM to DDYYYMM
orders.map(lambda o:int(o.split(",")[1].split(" ").replace("-",""))).first()

orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),float(oi.split(",")[4])))


ordersCompleted = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE","CLOSED"] and o.split(",")[1][:7] == "2014-01")


##Join order and order_items
#based on the data model and the ER relationship  we will join the data set

#Input data sett should be of form (k,v) and (k,w)
#Inner Join
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")
ordersMap = orders.map(lambda ou: (int(ou.split(",")[0]), ou.split(",")[1]))
order_items_map = orderItems.map(lambda oi:(int(oi.split(",")[1]), float(oi.split(",")[4])))
#ordersMap = orders.map(lambda ou: (int(ou.split(",")[0]), ou.split(",")[1].split(" ")[0].replace("-","")))
order_items_join = ordersMap.join(order_items_map)


##Outter Join
#############################Parent table join Child table########
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")
ordersMap = orders.map(lambda ou: (int(ou.split(",")[0]), o.split(",")[1]))
order_items_map = orderItems.map(lambda oi:(int(oi.split(",")[1]), float(oi.split(",")[4])))
order_items_leftouter_join = ordersMap.leftOuterjoin(order_items_map)
order_1 = order_items_leftouter_join.filter(lambda oq: oq [1] == 1)
#############################Child table join parent table########
order_items_rightouter_join = order_items_map.rightOuterjoin(ordersMap)


###################Aggregate#################

orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsFiltered = orderItems. \
filter(lambda oi: int(oi.split(",")[1]) == 2)
orderItemsSubtotals = orderItemsFiltered. \
map(lambda oi: float(oi.split(",")[4]))
from operator import add
# orderItemsSubtotals.reduce(add)
orderItemsSubtotals.reduce(lambda x, y: x + y)


############CountbyKey#######
ordersStatus = orders. \
map(lambda o: (o.split(",")[3], 1))
countByStatus = ordersStatus.countByKey()
for i in countByStatus: print(i)

##GroupbyKey// Group by key will not use compiler and would execute serial processing
##aggregatebykey/reducebykey // Will divide and conquer the results by processing it parllely
orderItems = sc.textFile("/public/retail_db/order_items")

orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

orderItemsGroupByOrderId = orderItemsMap.groupByKey()

revenuePerOrderId = orderItemsGroupByOrderId. \
map(lambda oi: (oi[0], round(sum(oi[1]), 2)))


#####################sort################
orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), oi))
orderItemsGroupByOrderId = orderItemsMap.groupByKey()
orderItemsSortedBySubtotalPerOrder = orderItemsGroupByOrderId. \
flatMap(lambda oi: 
  sorted(oi[1], key=lambda k: float(k.split(",")[4]), reverse=True)

  #Get revenue for each order_id - reduceByKey
orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

from operator import add
revenuePerOrderId = orderItemsMap. \
reduceByKey(add)

#Alternative way of adding for each key using reduceByKey
revenuePerOrderId = orderItemsMap. \
reduceByKey(lambda x, y: x + y)