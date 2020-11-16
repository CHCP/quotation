/*
Function  : kafka_producer.go
Author	  : Gordon Wang
Created At: 2020.11.14
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"
	"zerologix/quotation/config"

	"github.com/Shopify/sarama"
)

func main() {
	flag.StringVar(&config.Brokers, "brokers", "localhost:9092", "Connect to Kafka brokers.")
	flag.StringVar(&config.Topic, "topic", "testtopic", "Kafka topic.")
	flag.StringVar(&config.Group, "group", "", "Kafka group.")
	flag.Parse()
	if 0 == len(config.Brokers) || 0 == len(config.Topic) {
		fmt.Println("Usage: kafka_producer -brokers host:port -topic topic -count count -interval interval")
		os.Exit(1)
	}

	fmt.Printf("HPQ producer startup, brokers = %s, topic = %s.\n", config.Brokers, config.Topic)
	SendAsyncMessage()
}

func SendAsyncMessage() {
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Partitioner = sarama.NewRandomPartitioner
	conf.Producer.Retry.Max = 3

	producer, err := sarama.NewAsyncProducer([]string{config.Brokers}, conf)
	if nil != err {
		panic(err)
	}
	defer producer.AsyncClose()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	//define msg buffer
	inputMsg := config.InputMessage{
		S: "AUDCAD",
		U: time.Now(),
		C: 0.86181,
		V: 1,
	}

	//defien msg
	msg := &sarama.ProducerMessage{
		Topic: config.Topic,
	}

	// run goroutine to test
	var succeed, errors int
	timeBegin := time.Now().UnixNano()
	go func() {
		for {
			//对每个发送的消息赋值
			inputMsg.U = time.Now()
			inputStr, _ := json.Marshal(inputMsg)
			msg.Value = sarama.StringEncoder(inputStr)
			msg.Key = sarama.StringEncoder(strconv.Itoa(int(time.Now().Unix())))

			select {
			case producer.Input() <- msg:
				succeed++
			case err := <-producer.Errors():
				errors++
				fmt.Println("HPQ producer error:", err)
			case <-signals:
				doneCh <- struct{}{}
			}

			//延迟100毫秒
			//time.Sleep(100 * time.Millisecond)
		}
	}()
	<-doneCh

	fmt.Printf("HPQ Producer finished send succssful messages = %d, error messages = %d, duration = %dms.\n", succeed, errors, (time.Now().UnixNano()-timeBegin)/1e6)
}
