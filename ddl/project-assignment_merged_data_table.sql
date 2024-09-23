DROP TABLE IF EXISTS `project-assignment.merged_data_table`;
CREATE EXTERNAL TABLE `project-assignment.merged_data_table`(
  user_id bigint, 
  full_name string, 
  email string, 
  order_id bigint, 
  order_date string, 
  item_id string, 
  item_product_name string, 
  quantity bigint, 
  price double, 
  total_amount double, 
  product_product_name string, 
  category string, 
  stock_quantity bigint)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://project-assignment/merged_data/'
TBLPROPERTIES ('has_encrypted_data'='false');