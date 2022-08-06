# 一、命名空间
# 1. 创建一个命名空间
create_namespace 'MOMO_CHAT'

# 2. 查看命名空间
list_namespace

# 3. 删除之前的命名空间
drop_namespace 'MOMO_CHAT'

# 4. 查看某个具体的命名空间
describe_namespace 'MOMO_CHAT'
describe_namespace 'default'

# 5. 在命令MOMO_CHAT命名空间下创建名为：MSG的表，该表包含一个名为C1的列蔟。
# 注意：带有命名空间的表，使用冒号将命名空间和表名连接到一起
create "MOMO_CHAT:MSG", "C1"

# 6. 指定修改某个表的列蔟，它的压缩方式
alter "MOMO_CHAT:MSG", {NAME => "C1", COMPRESSION => "GZ"}

# 7. 删除之前创建的表
disable "MOMO_CHAT:MSG"
drop "MOMO_CHAT:MSG"

# 8. 在创建表时需要指定预分区
create 'MOMO_CHAT:MSG', {NAME => "C1", COMPRESSION => "GZ"}, { NUMREGIONS => 6, SPLITALGO => 'HexStringSplit'}