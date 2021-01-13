CREATE TABLE App.Order 
(
    Id INT NOT NULL comment 'primary key',
    ProductId INT,
    CustomerId INT
)
USING DELTA
LOCATION '$LAKE_PATH/App/Order'