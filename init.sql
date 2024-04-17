CREATE database IF NOT EXISTS recommendation;
use recommendation;


DROP TABLE IF EXISTS t_recommend;

CREATE TABLE IF NOT EXISTS t_recommend(
 user_id varchar(256) COMMENT '用户id,新用户使用000000代替',
 item varchar(256) COMMENT '物品',
 rating double COMMENT '推荐指数'
) ENGINE = InnoDB,
DEFAULT CHARSET = utf8mb4,
COLLATE = utf8mb4_unicode_ci,
COMMENT = '推荐表';


DROP TABLE IF EXISTS t_recommend_realtime;

CREATE TABLE IF NOT EXISTS t_recommend_realtime(
 user_id varchar(256) COMMENT '用户id,新用户使用000000代替',
 item varchar(256) COMMENT '物品',
 rating double COMMENT '推荐指数'
) ENGINE = InnoDB,
DEFAULT CHARSET = utf8mb4,
COLLATE = utf8mb4_unicode_ci,
COMMENT = '实时推荐表';


DROP TABLE IF EXISTS user_codes;

CREATE TABLE IF NOT EXISTS user_codes(
 user_id varchar(256) COMMENT '用户id',
 us_index_value int COMMENT '用户序号'
) ENGINE = InnoDB,
DEFAULT CHARSET = utf8mb4,
COLLATE = utf8mb4_unicode_ci,
COMMENT = '用户映射表';

DROP TABLE IF EXISTS item_codes;

CREATE TABLE IF NOT EXISTS item_codes(
 item varchar(256) COMMENT '物品名称',
 it_index_value int COMMENT '物品序号'
) ENGINE = InnoDB,
DEFAULT CHARSET = utf8mb4,
COLLATE = utf8mb4_unicode_ci,
COMMENT = '物品映射表';