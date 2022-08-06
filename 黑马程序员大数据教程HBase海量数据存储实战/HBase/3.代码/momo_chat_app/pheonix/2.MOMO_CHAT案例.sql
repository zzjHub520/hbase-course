-- 1. 建立HBase已经有的表和Phoenix视图的映射
-- 创建视图
create view if not exists "MOMO_CHAT"."MSG"(
    id varchar primary key,
    "C1"."msg_time" varchar,
    "C1"."sender_nickyname" varchar,
    "C1"."sender_account" varchar,
    "C1"."sender_sex" varchar,
    "C1"."sender_ip" varchar,
    "C1"."sender_os" varchar,
    "C1"."sender_phone_type" varchar,
    "C1"."sender_network" varchar,
    "C1"."sender_gps" varchar,
    "C1"."receiver_nickyname" varchar,
    "C1"."receiver_ip" varchar,
    "C1"."receiver_account" varchar,
    "C1"."receiver_os" varchar,
    "C1"."receiver_phone_type" varchar,
    "C1"."receiver_network" varchar,
    "C1"."receiver_gps" varchar,
    "C1"."receiver_sex" varchar,
    "C1"."msg_type" varchar,
    "C1"."distance" varchar,
    "C1"."message" varchar
);

-- 查询一条数据
select * from "MOMO_CHAT"."MSG" limit 1;

-- 根据日期、发送人账号、接收人账号查询历史消息
-- 日期查询：2020-09-10 11:28:05
select
    *
from
    "MOMO_CHAT"."MSG"
where
    substr("msg_time", 0, 10) = '2020-09-10'
and "sender_account" = '13514684105'
and "receiver_account" = '13869783495';

-- 10 rows selected (5.648 seconds)

select * from "MOMO_CHAT"."MSG" where substr("msg_time", 0, 10) = '2020-09-10' and "sender_account" = '13514684105' and "receiver_account" = '13869783495';


CREATE LOCAL INDEX LOCAL_IDX_MOMO_MSG ON MOMO_CHAT.MSG(substr("msg_time", 0, 10), "sender_account", "receiver_account");
drop index LOCAL_IDX_MOMO_MSG ON MOMO_CHAT.MSG;

0 rows selected (0.251 seconds)
explain select * from "MOMO_CHAT"."MSG" where substr("msg_time", 0, 10) = '2020-09-10' and "sender_account" = '13514684105' and "receiver_account" = '13869783495';

-- 删除索引
drop index LOCAL_IDX_MOMO_MSG on MOMO_CHAT.MSG;