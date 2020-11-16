/*
Function  : kafka_producer.go
Author	  : Gordon Wang
Created At: 2020.11.15
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
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
		fmt.Println("Usage: kafka_consumer -brokers host:port -topic topic -count count -interval interval")
		os.Exit(1)
	}

	ReadMessage()
}

func ReadMessage() {
	fmt.Printf("HPQ consumer startup, brokers = %s, topic = %s.\n", config.Brokers, config.Topic)

	conf := sarama.NewConfig()
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.Initial = sarama.OffsetNewest

	//consumer
	consumer, err := sarama.NewConsumer([]string{config.Brokers}, conf)
	if nil != err {
		panic(err)
	}
	defer consumer.Close()

	//partitionConsumer
	partitionConsumer, err := consumer.ConsumePartition(config.Topic, 0, sarama.OffsetNewest)
	if nil != err {
		panic(err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	// run goroutine to test
	var succeed, errors int
	timeBegin := time.Now().UnixNano()
	go func() {
		for {
			select {
			case <-partitionConsumer.Messages():
				succeed++
				/*
					case msg := <-partitionConsumer.Messages():
						succeed++
						fmt.Printf("HPQ consumer message, KEY=%s, VALUE=%s, offset=%d.\n", msg.Key, msg.Value, msg.Offset)

						//Calculate OutputOHLC
						var outputOHLC config.OutputOHLC
						json.Unmarshal([]byte(msg.Value), &outputOHLC)
						outputOHLC.O = 1.31136
						outputOHLC.H = 1.31262
						outputOHLC.L = 1.31118
						outputOHLC.T = "M1"
						outputStr, _ := json.Marshal(outputOHLC)
						fmt.Println("OutputOHLC:", string(outputStr))
				*/
			case err := <-partitionConsumer.Errors():
				errors++
				fmt.Println("HPQ consumer error:", err)
			case <-signals:
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh

	fmt.Printf("HPQ consumer read succssful messages = %d, error messages = %d, duration = %dms.\n", succeed, errors, (time.Now().UnixNano()-timeBegin)/1e6)
}
