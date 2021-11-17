Functions:

String manipulation:

create table customers (
customer_id int,
customer_fname varchar(45),
customer_lname varchar(45),
customer_email varchar(45),
customer_password varchar(45),
customer_street varchar(255),
customer_city varchar(45),
customer_state varchar(45),
customer_zipcode varchar(45)
) row format delimited fields terminated by ','
stored as textfile;
load data local inpath ‘/data/retail_db/customers’ into table customers;

####String Manipulation####

select substr('Hello World, How are you',7,7);
select instr('Hello World, How are you','World');
select 'hello world' like '%hello%';
select lcase('Hello World, How are you');
select ucase('Hello World, How are you');
select initcap('Hello World, How are you');
select length('Hello World, How are you');
select trim('   Hello World, How are you   ');

####String manipulation for orders_table######
use itv000439_retail_db_txt1;
select cast(substring(order_date, 6,2 ) as int) from orders limit 10;

##########Date manipulation########

select date_format(current_date,'d');
select date_format(current_date,'M');
select date_format(current_date,'Y');

Date Functions
current_date
current_timestamp
date_add
date_format
date_sub
datediff
day
dayofmonth
to_date
to_unix_timestamp
to_utc_timestamp
from_unixtime
from_utc_timestamp
minute
month
months_between
next_day


###########Aggregate Function####
sum
average
min
max
count

select count(1) from orders;
select sum(order_item_subtotal) from order_items;
select min(order_item_subtotal) from order_items;
select max(order_item_subtotal) from order_items;


##########Case statement##################
select case order_status when 'CLOSED' then 'No Action' when 'COMPLETE' then 'No Action' else 'Pending Action'  end from orders limit 100;


select order_status,
       case  
            when order_status IN ('CLOSED', 'COMPLETE') then 'No Action' 
            when order_status IN ('ON_HOLD', 'PAYMENT_REVIEW', 'PENDING', 'PENDING_PAYMENT', 'PROCESSING') then 'Pending Action'
            else 'Risky'
       end from orders limit 10;



#########Row level Transformation############

select cast(concat(substring(order_date, 1,4),substring(order_date, 6,2)) as int) from orders limit 10;
select cast()

######Joins############
select o.*,c.* from orders 0 and customers c where o.order_customer_id = c.customer_id limit 10;

##########Aggregate############
select order_status, count(1) from orders group by order_status;

select o.order_id, sum(o.order_item_subtotal) form orders o 
join 
order_items oi on o.order_id = oi.order_item_order_id
group by o.order_id;