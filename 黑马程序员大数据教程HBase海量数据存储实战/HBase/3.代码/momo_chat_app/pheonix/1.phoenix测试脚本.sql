-- id
-- status
-- money
-- pay_way
-- user_id
-- operation_time
-- category

-- 一、在Phoenix创建表（订单明细表）
-- 1. 创建表
create table if not exists ORDER_DTL(
    id varchar primary key,
    C1.status varchar,
    C1.money double,
    C1.pay_way integer,
    C1.user_id varchar,
    C1.operation_time varchar,
    C1.category varchar
);

-- 2. 删除表
drop table if exists ORDER_DTL;

-- 创建表
create table if not exists ORDER_DTL(
    "id" varchar primary key,
    "C1"."status" varchar,
    "C1"."money" double,
    "C1"."pay_way" integer,
    "C1"."user_id" varchar,
    "C1"."operation_time" varchar,
    "C1"."category" varchar
);

-- select "id" from ORDER_DTL;

-- 3. 插入一条数据
-- 000001	已提交	4070	1	4944191	2020-04-25 12:09:16	手机;
-- 双引号表示引用一个表或者字段
-- 单引号表示字符串
upsert into "ORDER_DTL" values('000001', '已提交', 4070, 1, '4944191', '2020-04-25 12:09:16', '手机;');

-- 4. 查询一条数据
select * from "ORDER_DTL";

-- 5. 更新一条数据
-- 将ID为'000001'的订单状态修改为已付款。
upsert into "ORDER_DTL"("id", "C1"."status") values('000001', '已付款');

-- 6. 指定ID查询数据
select * from "ORDER_DTL" where "id" = '000001';

-- 7. 删除指定ID的数据
delete from "ORDER_DTL" where "id" = '000001';

-- 8. 查询表中一共有多少条数据
select count(*) from "ORDER_DTL";
-- 第一页
select * from "ORDER_DTL" limit 10 offset 0;
-- 第二页
select * from "ORDER_DTL" limit 10 offset 10;
-- 第三页
select * from "ORDER_DTL" limit 10 offset 20;

-- 二、Phoenix预分区
-- 1. 使用指定rowkey来进行预分区
drop table if exists ORDER_DTL;
create table if not exists ORDER_DTL(
    "id" varchar primary key,
    C1."status" varchar,
    C1."money" float,
    C1."pay_way" integer,
    C1."user_id" varchar,
    C1."operation_time" varchar,
    C1."category" varchar
) 
CONPRESSION='GZ'
SPLIT ON ('3','5','7');

-- 2. 直接指定Region的数量来进行预分区
drop table if exists ORDER_DTL;
create table if not exists ORDER_DTL(
    "id" varchar primary key,
    C1."status" varchar,
    C1."money" float,
    C1."pay_way" integer,
    C1."user_id" varchar,
    C1."operation_time" varchar,
    C1."category" varchar
) 
CONPRESSION='GZ',
SALT_BUCKETS=10;

-- 二、在phoenix中创建二级索引
-- 根据用户ID来查询订单的ID以及对应的支付金额
-- 建立一个覆盖索引，加快查询
create index IDX_USER_ID on ORDER_DTL(C1."user_id") include ("id", C1."money");

-- 删除索引
drop index IDX_USER_ID on ORDER_DTL;

-- 强制使用索引查询
explain select /*+ INDEX(ORDER_DTL IDX_USER_ID) */ * from ORDER_DTL where "user_id" = '8237476';

-- 建立本地索引
-- 因为我们要在很多的列上建立索引，所以不太使用使用覆盖索引
create local index IDX_LOCAL_ORDER_DTL_MULTI_IDX on ORDER_DTL("id", C1."status", C1."money", C1."pay_way", C1."user_id") ;
explain select * from ORDER_DTL WHERE C1."status" = '已提交';
explain select * from ORDER_DTL WHERE C1."pay_way" = 1;
