# flink_kafka-tdengine



## 项目描述

将特定格式的数采数据从kafka读取到tdengine（多列模式），支持根据kafka的topic名称，每条数据的tag信息，自动生成超级表，对应的子表及相应的字段。同时能够根据数据同步新增的字段。

并行度设置为partition的数量，taskmanager设置3个g左右的java heap，所有容器加起来大概每秒能够写入一万五千条数据，也就是45万个tag点。

kafka json格式：{...多个tag信息字段,"ts":1712725213155,"Tags":[...{{
      "TagName": "tdengine_test_column_example_1_int",
      "ValueType": "INT",
      "TagValue": 268
    }}]}

## 部署

maven package

 
