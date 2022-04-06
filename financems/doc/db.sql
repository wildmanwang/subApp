-- 账户
create table ac_account(
    id              int auto_increment primary key,
    name            varchar(100) not null               comment '账户名称',
    simple_name     varchar(20) not null                comment '账户简称',
    entity_type     tinyint not null                    comment '实体类型 1:平台 2:服务商 3:商家 4:师傅',
    entity_id       int not null                        comment '实体ID',
    ac_type         tinyint not null                    comment '记账类型 1:收付 2:预收 3:预付 4:信用',
    other_en_type   tinyint default 0                   comment '预付实体类型 1:平台 2:服务商 3:商家 4:师傅',
    other_en_id     int default 0                       comment '预付实体ID',
    ac_balance      decimal(12,2) default 0.00          comment '账户余额',
    ac_balance_time datetime                            comment '账户余额更新时间',
    credit_line     decimal(12,2) default 0.00          comment '信用账户授信额度',
    check_flag      tinyint default 0                   comment '对账标志 0:不对账 1:对账',
    check_balance   decimal(12,2) default 0.00          comment '第三方记账余额',
    check_time      datetime                            comment '第三方记账同步时间',
    created_time    timestamp null default current_timestamp,
    updated_time    timestamp null on update current_timestamp,
    index idx_entity (entity_id)
) comment = '账户表';

-- 账单明细
create table ac_bill_flow(
    id              int auto_increment primary key,
    out_ac          int                                 comment '应付账户',
    out_simple      varchar(20)                         comment '应付账户简称',
    in_ac           int                                 comment '应收账户',
    in_simple       varchar(20)                         comment '应收账户简称',
    ac_type         tinyint not null                    comment '记账类型 1:收付 2:预收 3:预付 4:信用',
    busi_type       tinyint not null                    comment '业务类型 1:充值 2:提现 3:还款 4:安装 5:售后',
    busi_bill       varchar(50)                         comment '业务单据号',
    third_bill      varchar(50)                         comment '第三方单据号',
    orig_amt        decimal(12,2) not null              comment '原始交易金额',
    real_amt        decimal(12,2) not null              comment '实际交易金额',
    ac_date         datetime not null                   comment '财务日期',
    busi_summary    varchar(255) not null               comment '业务摘要',
    frush_flag      tinyint default 0                   comment '冲红标志 0:非冲红 1:冲红 2:被冲红',
    frush_bill      int                                 comment '冲红原单',
    frush_remark    varchar(255)                        comment '冲红备注',
    pay_flag        tinyint default 0                   comment '支付标志 0:未支付 1:已支付 2:无需支付',
    check_flag      tinyint default 0                   comment '对账标志 0:未平账 1:已平账',
    close_flag      tinyint default 0                   comment '关账标志 0:未关账 1:已关账',
    bill_remark     varchar(255)                        comment '账单备注',
    created_time    timestamp null default current_timestamp,
    updated_time    timestamp null on update current_timestamp,
    index idx_outac (out_ac),
    index idx_inac (in_ac),
    index idx_busibill (busi_bill)
) comment = '账单明细';

-- 支付流水
create table ac_pay_flow(
    id              int auto_increment primary key,
    out_ac          int                                 comment '付款账户',
    out_simple      varchar(20)                         comment '付款方账户简称',
    out_balance     decimal(12,2)                       comment '付款后余额',
    in_ac           int                                 comment '收款款账户',
    in_simple       varchar(20)                         comment '收款账户简称',
    in_balance      decimal(12,2)                       comment '收款后余额',
    busi_type       tinyint not null                    comment '业务类型 1:充值 2:提现 3:还款 4:安装 5:售后',
    bill_flow       int                                 comment '账单ID',
    pay_type        int not null                        comment '付款方式',
    pay_amt         decimal(12,2) not null              comment '支付金额',
    pay_time        datetime not null                   comment '支付时间',
    ac_date         datetime not null                   comment '财务日期',
    busi_summary    varchar(255) not null               comment '业务摘要',
    frush_flag      tinyint default 0                   comment '冲红标志 0:非冲红 1:冲红 2:被冲红',
    frush_bill      int                                 comment '冲红原单',
    frush_remark    varchar(255)                        comment '冲红备注',
    check_flag      tinyint default 0                   comment '对账标志 0:未平账 1:已平账',
    bill_remark     varchar(255)                        comment '付款备注',
    created_time    timestamp null default current_timestamp,
    updated_time    timestamp null on update current_timestamp,
    index idx_outac (out_ac),
    index idx_inac (in_ac),
    index idx_paytype (pay_type)
) comment = '支付明细';

-- 付款方式
create table ac_pay_type(
    id              int auto_increment primary key,
    name            varchar(50) not null unique         comment '名称',
    pay_mode        tinyint not null                    comment '支付模式 1:现金 2:第三方 3:银行卡 4:票券 9:其他',
    actual_flag     tinyint not null                    comment '实付标志 0:虚收 1:实收',
    fee_rate        decimal(10,8) not null default 0.00 comment '费率',
    status          tinyint not null                    comment '状态 0:未生效 1:正常',
    remark          varchar(50)                         comment '备注',
    created_time    timestamp null default current_timestamp,
    updated_time    timestamp null on update current_timestamp
) comment = '付款方式';
insert into ac_pay_type ( name, pay_mode, actual_flag, status ) value ( '现金', 1, 1, 1 );
insert into ac_pay_type ( name, pay_mode, actual_flag, status ) value ( '微信', 2, 1, 1 );
insert into ac_pay_type ( name, pay_mode, actual_flag, status ) value ( '支付宝', 2, 1, 1 );
insert into ac_pay_type ( name, pay_mode, actual_flag, status ) value ( '银行卡', 3, 1, 1 );

-- 数据字典
create table base_dict(
    id              int auto_increment primary key,
    bd_type         varchar(50) not null                comment '字典类型',
    bd_label        varchar(50) not null                comment '字典标签',
    bd_value        int not null                        comment '字典值',
    bd_remark       varchar(50)                         comment '备注',
    created_time    timestamp null default current_timestamp,
    updated_time    timestamp null on update current_timestamp,
    index idx_bdtype (bd_type)
) comment = '数据字典';
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '实体类型', '平台', 1 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '实体类型', '服务商', 2 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '实体类型', '商家', 3 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '实体类型', '师傅', 4 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '记账类型', '收付', 1 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '记账类型', '预收', 2 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '记账类型', '预付', 3 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '记账类型', '信用', 4 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '业务类型', '充值', 1 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '业务类型', '提现', 2 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '业务类型', '还款', 3 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '业务类型', '安装', 4 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '业务类型', '售后', 5 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '支付模式', '现金', 1 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '支付模式', '第三方', 2 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '支付模式', '银行卡', 3 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '支付模式', '票券', 4 );
insert into base_dict ( bd_type, bd_label, bd_value ) values ( '支付模式', '其他', 9 );
