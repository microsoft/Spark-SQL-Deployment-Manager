CREATE TABLE App.Customer
(
    Id int not null,
    name string,
    phonenumber string,
    haha int,
    newColumn int comment 'this is really great'
)
USING DELTA
LOCATION '$LAKE_PATH/App/Customer'