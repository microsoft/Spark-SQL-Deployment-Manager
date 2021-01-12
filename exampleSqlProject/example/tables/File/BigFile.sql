create table File.BigFile
(
id int,
filename string,
fileSize long
)
using delta
location '$LAKE_PATH/file/bigfile'