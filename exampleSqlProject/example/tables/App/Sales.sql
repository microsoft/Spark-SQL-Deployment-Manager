CREATE TABLE App.Sales(
id int,
customerId int,
BranchId int,
ProductId int,
OrderId int,
region string
 )
using delta
PARTITIONED BY (region)
location '$LAKE_PATH/App/Sales'