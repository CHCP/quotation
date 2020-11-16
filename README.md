# HPQ(High Performance Quotation)
## 1、Golang Client
* [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go.git), base on C library:librdkafka, high performance, but a bit hard to install and use.
* [sarama](https://github.com/Shopify/sarama.git), pure Golang Client, easy to install and use.

Finally, I choose sarama to test.

## 2、How to run 
* Clone Code
git clone https://github.com/CHCP/quotation.git

* Build Producer
go build -o kafka_producer kafka_producer.go

* Build Consumer
go build -o kafka_consumer kafka_consumer.go

## 3、Producer Performance Test
* Async Test Result
  80000 msg/Sec,CPU:60%
  ![Preivew](images/async.png)

  ![Preivew](images/async_cpu.png)

* Sync Test Result
  1200 msg/Sec, CPU:10%

  ![Preivew](images/sync.png)

  ![Preivew](images/sync_cpu.png)

## 4、Consumer Performance Test