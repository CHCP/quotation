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
	//Command line parameters
	flag.StringVar(&config.Brokers, "brokers", "localhost:9092", "Connect to Kafka brokers.")
	flag.StringVar(&config.Topic, "topic", "hpqtopic", "Kafka topic.")
	flag.StringVar(&config.Group, "group", "", "Kafka group.")
	flag.IntVar(&config.Interval, "interval", 100, "Kafka send message interval ms.")
	var async bool
	flag.BoolVar(&async, "async", true, "Run as async.")
	flag.Parse()
	if 0 == len(config.Brokers) || 0 == len(config.Topic) {
		fmt.Println("Usage: kafka_producer -brokers host:port -topic topic -interval interval")
		os.Exit(1)
	}

	//Run as async or sync
	if async {
		SendAsyncMessage()
	} else {
		SendSyncMessage()
	}
}

func SendAsyncMessage() {
	fmt.Printf("HPQ async producer startup, brokers = %s, topic = %s.\n", config.Brokers, config.Topic)

	//NewConfig
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Retry.Max = 3
	//多个Partition不能设置为NewRandomPartitioner
	//conf.Producer.Partitioner = sarama.NewRandomPartitioner
	//设置后影响性能
	//conf.Producer.Return.Successes = true
	//conf.Producer.Return.Errors = true

	//NewProducer
	producer, err := sarama.NewAsyncProducer([]string{config.Brokers}, conf)
	if nil != err {
		panic(err)
	}
	defer producer.AsyncClose()

	//Define exit sigal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	//define OHLC buffer
	inputOHLC := config.InputOHLC{
		S: "AUDCAD",
		U: time.Now(),
		C: 0.86181,
		V: 1,
	}

	//define send msg
	msg := &sarama.ProducerMessage{
		Topic: config.Topic,
	}

	// run goroutine to test
	var succeed, errors int
	timeBegin := time.Now().UnixNano()
	go func() {
		for {
			//对每个发送的消息赋值
			inputOHLC.U = time.Now()
			inputStr, _ := json.Marshal(inputOHLC)
			msg.Value = sarama.ByteEncoder(inputStr)
			msg.Key = sarama.StringEncoder(strconv.Itoa(int(time.Now().Unix())))

			select {
			case producer.Input() <- msg:
				succeed++
			case err := <-producer.Errors():
				errors++
				fmt.Println("HPQ async producer error:", err)
			case <-signals:
				doneCh <- struct{}{}
			}

			//延迟Interval毫秒
			if int(config.Interval) > 0 {
				time.Sleep(time.Duration(config.Interval) * time.Millisecond)
			}
		}
	}()
	<-doneCh

	fmt.Printf("HPQ async producer send succssful messages = %d, error messages = %d, duration = %dms.\n", succeed, errors, (time.Now().UnixNano()-timeBegin)/1e6)
}

func SendSyncMessage() {
	fmt.Printf("HPQ sync producer startup, brokers = %s, topic = %s.\n", config.Brokers, config.Topic)

	//NewConfig
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true

	//NewProducer
	producer, err := sarama.NewSyncProducer([]string{config.Brokers}, conf)
	if nil != err {
		panic(err)
	}
	defer producer.Close()

	//define OHLC buffer
	inputOHLC := config.InputOHLC{
		S: "AUDCAD",
		U: time.Now(),
		C: 0.86181,
		V: 1,
	}

	//define send msg
	msg := &sarama.ProducerMessage{
		Topic: config.Topic,
	}

	// run goroutine to test
	var succeed, errors int
	timeBegin := time.Now().UnixNano()
	for i := 0; i < 80000; i++ {
		//对每个发送的消息赋值
		inputOHLC.U = time.Now()
		inputStr, _ := json.Marshal(inputOHLC)
		msg.Value = sarama.ByteEncoder(inputStr)
		msg.Key = sarama.StringEncoder(strconv.Itoa(int(time.Now().Unix())))

		if _, _, err := producer.SendMessage(msg); nil == err {
			succeed++
		} else {
			errors++
			fmt.Println("HPQ sync producer error:", err)
		}

		//延迟Interval毫秒
		if int(config.Interval) > 0 {
			time.Sleep(time.Duration(config.Interval) * time.Millisecond)
		}
	}

	fmt.Printf("HPQ sync producer send succssful messages = %d, error messages = %d, duration = %dms.\n", succeed, errors, (time.Now().UnixNano()-timeBegin)/1e6)
}
