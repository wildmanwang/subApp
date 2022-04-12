if __name__ == "__main__":
    import os
    from appConfig import Settings
    from appFinance import AppFinance

    myPath = os.path.abspath(os.path.dirname(__file__))
    mySett = Settings(myPath, "config")

    # 创建账户
    # myAcc = AppFinance(mySett, 0)
    # rtn = myAcc.fCreateAc({"name": "CSP客户服务平台", "simple_name": "CSP", "entity_type": 1, "entity_id": 1, "ac_type": 1, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "巧匠上门服务有限公司", "simple_name": "巧匠上门", "entity_type": 2, "entity_id": 1, "ac_type": 1, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "鲁班到家家居售后服务平台", "simple_name": "鲁班到家", "entity_type": 2, "entity_id": 2, "ac_type": 1, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "CSP客户服务平台", "simple_name": "CSP", "entity_type": 1, "entity_id": 1, "ac_type": 2, "other_en_type": 2, "other_en_id": 2, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "万师傅家居服务平台", "simple_name": "万师傅", "entity_type": 2, "entity_id": 3, "ac_type": 1, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "CSP客户服务平台", "simple_name": "CSP", "entity_type": 1, "entity_id": 1, "ac_type": 3, "other_en_type": 2, "other_en_id": 3, "credit_line": 10000.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "石将军家居服务有限公司", "simple_name": "石将军", "entity_type": 3, "entity_id": 1, "ac_type": 1, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "华南万达家居服务有限公司", "simple_name": "华南万达", "entity_type": 3, "entity_id": 2, "ac_type": 3, "other_en_type": 1, "other_en_id": 1, "credit_line": 5000.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "张大前", "simple_name": "张师傅", "entity_type": 4, "entity_id": 1, "ac_type": 1, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "周强", "simple_name": "周师傅", "entity_type": 4, "entity_id": 2, "ac_type": 1, "credit_line": 0.00, "check_flag": 0})
    # print(rtn["info"]) 

    # CSP向鲁班到家充值2000
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # rtn = myAcc.fAcPutin(4, 2000)
    # print(rtn["info"])

    # CSP从鲁班到家充值提现200
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # rtn = myAcc.fAcGetback(4, 200)
    # print(rtn["info"])

    # 万师傅还款300
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 3, "other_en_type": 2, "other_en_id": 3})
    # rtn = myAcc.fAcPayback(2, 300)
    # print(rtn["info"])

    # 华南万达还款1200
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 2, "acType": 3, "other_en_type": 1, "other_en_id": 1})
    # rtn = myAcc.fAcPayback(3, 1200)
    # print(rtn["info"])

    # 鲁班到家充值500
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # rtn = myAcc.fAcPutin(3, 500)
    # iFlow = rtn["entities"]
    # print(rtn["info"])

    # 鲁班到家充值500冲红
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # for line in iFlow:
    #     rtn = myAcc.fFrushPay(line, "充多了退回来")
    #     print(rtn["info"])

    # 石将军实付CSP佣金80
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 1, "acType": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(80, 80, acOther, 4, 'a0001')
    # print(rtn["info"])

    # 石将军调整CSP佣金30
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 1, "acType": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiAdjust(1, 30, 30, "空跑费")
    # print(rtn["info"])

    # CSP实付巧匠上门佣金100
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(100, 100, acOther, 4, 'a0002')
    # print(rtn["info"])

    # 巧匠上门实付张师傅佣金60
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 1, "acType": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 4, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(60, 60, acOther, 4, 'a0002')
    # print(rtn["info"])

    # 石将军实付CSP佣金110
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 1, "acType": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(110, 110, acOther, 4, 'a0003')
    # print(rtn["info"])

    # CSP扣款鲁班到家佣金95
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fBusiNew(95, 95, acOther, 4, 'a0003')
    # print(rtn["info"])

    # 石将军实付CSP佣金100
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 1, "acType": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(100, 100, acOther, 4, 'a0004')
    # print(rtn["info"])

    # CSP记账万师傅佣金80
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 3, "other_en_type": 2, "other_en_id": 3})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 3, "acType": 1})
    # rtn = myAcc.fBusiNew(80, 80, acOther, 4, 'a0004')
    # print(rtn["info"])

    # 华南万达授信扣款CSP佣金100
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 2, "acType": 3, "other_en_type": 1, "other_en_id": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(100, 100, acOther, 4, 'a0005')
    # print(rtn["info"])

    # CSP实付巧匠上门佣金90
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(90, 90, acOther, 4, 'a0005')
    # print(rtn["info"])

    # 华南万达授信扣款CSP佣金120
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 2, "acType": 3, "other_en_type": 1, "other_en_id": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(120, 120, acOther, 4, 'a0006')
    # print(rtn["info"])

    # CSP扣款鲁班到家佣金110
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fBusiNew(110, 110, acOther, 4, 'a0006')
    # print(rtn["info"])

    # 华南万达授信扣款CSP佣金80
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 3, "enID": 2, "acType": 3, "other_en_type": 1, "other_en_id": 1})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 1})
    # rtn = myAcc.fBusiNew(80, 80, acOther, 4, 'a0007')
    # print(rtn["info"])

    # CSP授信扣款万师傅佣金70
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 3, "other_en_type": 2, "other_en_id": 3})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 3, "acType": 1})
    # rtn = myAcc.fBusiNew(70, 70, acOther, 4, 'a0007')
    # print(rtn["info"])

    # 鲁班到家收款提现80
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fAcGetout(1, 80)
    # print(rtn["info"])

    # 张师傅收款提现50
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 4, "enID": 1, "acType": 1})
    # rtn = myAcc.fAcGetout(1, 50)
    # print(rtn["info"])

    # CSP多方式支付鲁班：扣款90+票券20
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fBusiNew(110, 100, acOther, 4, 'a0006', paylist=[{"pay_type": 5, "pay_amt": 30}])
    # print(rtn["info"])

    # CSP多方式支付鲁班：支付宝70
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fBusiNew(70, 70, acOther, 4, 'a0006', paylist=[{"pay_type": 3, "pay_amt": 70}])
    # print(rtn["info"])

    # CSP多方式支付鲁班：支付宝80
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fBusiNew(80, 80, acOther, 4, 'a0006', paylist=[{"pay_type": 3, "pay_amt": 100}])
    # print(rtn["info"])

    # CSP多方式支付鲁班：支付宝30
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fBusiNew(80, 80, acOther, 4, 'a0006', paylist=[{"pay_type": 4, "pay_amt": 50}, {"pay_type": 3, "pay_amt": 60}])
    # iFlow = rtn["entities"]["bill"]
    # print(rtn["info"])
    # 支付账单冲红
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # for line in iFlow:
    #    rtn = myAcc.fFrushBill(line, '测试账单冲红')
    #    print(rtn["info"])

    # CSP信用支付万师傅：票券20
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 3, "other_en_type": 2, "other_en_id": 3})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 3, "acType": 1})
    # rtn = myAcc.fBusiNew(100, 90, acOther, 4, 'a0007', paylist=[{"pay_type": 5, "pay_amt": 20}])
    # iFlow = rtn["entities"]["bill"]
    # print(rtn["info"])
    # 支付账单冲红
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 3, "other_en_type": 2, "other_en_id": 3})
    # for line in iFlow:
    #     rtn = myAcc.fFrushBill(line, '测试账单冲红')
    #     print(rtn["info"])

    # CSP扣款鲁班：余额不足
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 2, "other_en_type": 2, "other_en_id": 2})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 1})
    # rtn = myAcc.fBusiNew(1100, 1100, acOther, 4, 'a0006')
    # print(rtn["info"])

    # CSP信用支付万师傅：信用不足
    # myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 3, "other_en_type": 2, "other_en_id": 3})
    # acOther = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 3, "acType": 1})
    # rtn = myAcc.fBusiNew(10000, 10000, acOther, 4, 'a0007')
    # print(rtn["info"])

    # 巧匠上门实付张师傅佣金60
    myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 4, "enID": 1, "acType": 1})
    acOther = AppFinance(mySett, 1, {"type": 2, "enType": 1, "enID": 1, "acType": 3, "other_en_type": 2, "other_en_id": 3})
    rtn = myAcc.fBusiNew(60, 60, acOther, 4, 'a0002')
    print(rtn["info"])
