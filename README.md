# kafka-consumer-lag

Return kafka lag in influxdb format

./kafka-consumer-lag --config config.yml

``` yaml
---
checks:
  - group: test1
    topics:
      - test1

  - group: test2
    topics:
      - test2

brokers:
  - localhost:9092

```

kafka_lag,group=test1,topic=test1 lag=1323 1576214316
kafka_lag,group=test2,topic=test2 lag=214 1576214316
