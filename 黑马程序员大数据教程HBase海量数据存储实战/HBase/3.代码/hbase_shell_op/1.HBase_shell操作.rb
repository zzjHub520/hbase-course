# 创建订单表，表名为ORDER_INFO，该表有一个列蔟为C1
create "ORDER_INFO", "C1"

# 删除表
# 1. 禁用表
disable "ORDER_INFO"

# 2. 删除表
drop "ORDER_INFO"

# 往表中添加一条数据
# put '表名','ROWKEY','列蔟名:列名','值'
# ID	STATUS	PAY_MONEY	PAYWAY	USER_ID	OPERATION_DATE	CATEGORY
# 000001	已提交	4070	1	4944191	2020-04-25 12:09:16	手机;
put "ORDER_INFO", "000001", "C1:STATUS", "已提交"
put "ORDER_INFO", "000001", "C1:PAY_MONEY", 4070
put "ORDER_INFO", "000001", "C1:PAYWAY", 1
put "ORDER_INFO", "000001", "C1:USER_ID", "4944191"
put "ORDER_INFO", "000001", "C1:OPERATION_DATE", "2020-04-25 12:09:16"
put "ORDER_INFO", "000001", "C1:CATEGORY", "手机;"


# 要求将rowkey为：000001对应的数据查询出来。
get "ORDER_INFO", "000001"

# 要将数据中的中文正确的显示
get "ORDER_INFO", "000001", {FORMATTER => 'toString'}

# 将订单ID为000001的状态，更改为「已付款」
put "ORDER_INFO", "000001", "C1:STATUS", "已付款"

# 将订单ID为000001的状态列删除。
delete "ORDER_INFO", "000001", "C1:STATUS"

# 将订单ID为000001的信息全部删除（删除所有的列）
deleteall "ORDER_INFO", "000001"

# 查看HBase中的ORDER_INFO表，一共有多少条记录。
count "ORDER_INFO"

# scan操作
# 需求一：查询订单所有数据
scan "ORDER_INFO", {FORMATTER => 'toString'}

# 需求二： 查询订单数据（只显示3条）
scan "ORDER_INFO", {FORMATTER => 'toString', LIMIT => 3}

# 需求三：只查询订单状态以及支付方式，并且只展示3条数据
scan "ORDER_INFO", {FORMATTER => 'toString', LIMIT => 3, COLUMNS => ['C1:STATUS', 'C1:PAYWAY']}

# 需求四：使用scan来根据rowkey查询数据，也是查询指定列的数据
# scan '表名', {ROWPREFIXFILTER => 'rowkey'}
scan "ORDER_INFO", {ROWPREFIXFILTER => '02602f66-adc7-40d4-8485-76b5632b5b53',FORMATTER => 'toString', LIMIT => 3, COLUMNS => ['C1:STATUS', 'C1:PAYWAY']}

# Scan + Filter
# 使用RowFilter查询指定订单ID的数据
# 只查询订单的ID为：02602f66-adc7-40d4-8485-76b5632b5b53、订单状态以及支付方式
scan "ORDER_INFO", {FILTER => "RowFilter(=,'binary:02602f66-adc7-40d4-8485-76b5632b5b53')", COLUMNS => ['C1:STATUS', 'C1:PAYWAY'], FORMATTER => 'toString'}

# 查询状态为「已付款」的订单
scan "ORDER_INFO", {FILTER => "SingleColumnValueFilter('C1', 'STATUS', =, 'binary:已付款')", FORMATTER => 'toString'}

# 查询支付方式为1，且金额大于3000的订单
scan "ORDER_INFO", {FILTER => "SingleColumnValueFilter('C1', 'PAYWAY', =, 'binary:1') AND SingleColumnValueFilter('C1', 'PAY_MONEY', >, 'binary:3000')", FORMATTER => 'toString'}

# HBase的计数器
#	需求一：对0000000020新闻01:00 - 02:00访问计数+1
get_counter 'NEWS_VISIT_CNT','0000000020_01:00-02:00', 'C1:CNT'
incr 'NEWS_VISIT_CNT','0000000020_01:00-02:00','C1:CNT'

# 创建一个USER_INFO表，两个列蔟C1、C2
create 'USER_INFO', 'C1', 'C2'
# 新增列蔟C3
alter 'USER_INFO', 'C3'
# 删除列蔟C3
alter 'USER_INFO', 'delete' => 'C3'
