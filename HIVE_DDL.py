set hive.metastore.warehouse.dir = /user/itv000439/warehouse

// Create DB 
create database <db_name>;

//Create table 
create table orders(
order_id int,
order_date string,
order_cust_id int,
order_status string
) row format delimited fields terminated by ','

describe <table_name>
describe formatted <table_name>

load data local inpath '/data/retail_db/orders' into table orders



create table order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/order_items' into table order_items;

create database itv000439_retail_db_orc;
use itv000439_retail_db_orc;

create table orders (
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
) stored as orc;

insert into table orders select * from dgadiraju_retail_db_txt.orders;

create table order_items1 (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
) stored as orc;

insert into table order_items select * from dgadiraju_retail_db_txt.order_items;


