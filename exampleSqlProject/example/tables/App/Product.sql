CREATE TABLE App.Product(
    Id int not null,
    Name string,
    Price double comment 'price in INR'
)
using delta
location '$LAKE_PATH/App/Product'
