-- 账户
create table ac_account(
    id              int auto_increment primary key,
    name            varchar(100) not null unique        comment '账户名称',
    simple_name     varchar(20) not null unique         comment '账户简称',
    entity_type     tinyint not null                    comment '实体类型 1:平台 2:服务商 3:商家 4:师傅',
    entity_id       int not null                        comment '实体ID',
    ac_type         tinyint not null                    comment '记账类型 1:收款 2:预收 3:信用',
    ac_balance      decimal(12,2) default 0.00          comment '账户余额',
    ac_balance_time datetime                            comment '账户余额更新时间',
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
    fund_type       tinyint not null                    comment '交易类型 1:常规 2:充值 3:提现',
    ac_type         tinyint not null                    comment '记账类型 1:收款 2:预收 3:信用',
    busi_type       tinyint not null                    comment '业务类型 1:余额操作 2:安装 3:维修',
    busi_bill       varchar(50)                         comment '业务单据号',
    third_bill      varchar(50)                         comment '第三方单据号',
    orig_amt        decimal(12,2) not null              comment '原始交易金额',
    real_amt        decimal(12,2) not null              comment '实际交易金额',
    ac_date         datetime not null                   comment '财务日期',
    busi_summary    varchar(255) not null               comment '业务摘要',
    frush_flag      tinyint default 0                   comment '冲红标志 0:非冲红 1:冲红',
    frush_remark    varchar(255)                        comment '冲红备注',
    check_flag      tinyint default 0                   comment '对账标志 0:未平账 1:已平账',
    pay_flag        tinyint default 0                   comment '支付标志 0:未支付 1:已支付',
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
    bill_flow       int not null                        comment '账单ID',
    fund_type       tinyint not null                    comment '交易类型 1:常规 2:充值 3:提现',
    pay_type        int not null                        comment '付款方式',
    pay_amt         decimal(12,2) not null              comment '支付金额',
    pay_time        datetime not null                   comment '支付时间',
    frush_flag      tinyint default 0                   comment '冲红标志 0:非冲红 1:冲红',
    frush_remark    varchar(255)                        comment '冲红备注',
    check_flag      tinyint default 0                   comment '对账标志 0:未平账 1:已平账',
    created_time    timestamp null default current_timestamp,
    updated_time    timestamp null on update current_timestamp,
    index idx_outac (out_ac),
    index idx_inac (in_ac),
    index idx_paytype (pay_type)
) comment = '支付明细';

-- 付款方式
create table ac_pay_type(
    id              int auto_increment primary key,
    pay_mode        tinyint not null                    comment '支付模式 1:现金 2:微信 3:支付宝 4:银行卡 5:票券 9:其他',
    actual_flag     tinyint not null                    comment '实付标志 0:虚收 1:实收',
    status          tinyint not null                    comment '状态 0:未生效 1:正常',
    remark          varchar(50)                         comment '备注',
    created_time    timestamp null default current_timestamp,
    updated_time    timestamp null on update current_timestamp
) comment = '付款方式';