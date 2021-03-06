/*
Function  : kafka_consumer.go
Author	  : Gordon Wang
Created At: 2020.11.15
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"quotation/config"

	"github.com/Shopify/sarama"
	"github.com/mohae/deepcopy"
)

//定义带互斥锁的stockMap数据结构
var stockMap = struct {
	sync.RWMutex
	m map[string]config.OHLC
}{m: make(map[string]config.OHLC)}

func main() {
	//Command line parameters
	flag.StringVar(&config.Brokers, "brokers", "localhost:9092", "Connect to Kafka brokers.")
	flag.StringVar(&config.Topic, "topic", "hpqtopic", "Kafka topic.")
	flag.StringVar(&config.Group, "group", "", "Kafka group.")
	flag.Parse()
	if 0 == len(config.Brokers) || 0 == len(config.Topic) {
		fmt.Println("Usage: kafka_consumer -brokers host:port -topic topic")
		os.Exit(1)
	}

	ReadOnePartition()
	//ReadMultiPartition()
}

//MultiPartition
func ReadMultiPartition() {
	fmt.Printf("HPQ consumer startup, brokers = %s, topic = %s.\n", config.Brokers, config.Topic)

	//Sync the goroutine process
	var wg sync.WaitGroup

	//Consumer config
	conf := sarama.NewConfig()
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.Initial = sarama.OffsetNewest

	//consumer
	consumer, err := sarama.NewConsumer([]string{config.Brokers}, conf)
	if nil != err {
		panic(err)
	}
	defer consumer.Close()

	//Define successful/errors number
	var succeed, errors int
	timeBegin := time.Now().UnixNano()

	//Get all partition
	partitionList, _ := consumer.Partitions(config.Topic)
	for partition := range partitionList {
		//partitionConsumer
		partitionConsumer, err := consumer.ConsumePartition(config.Topic, int32(partition), sarama.OffsetNewest)
		if nil != err {
			fmt.Println(err.Error())
			continue
		}
		defer partitionConsumer.AsyncClose()

		// run goroutine to test
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range partitionConsumer.Messages() {
				succeed++
				quotation(msg)
			}
		}()
	}

	wg.Wait()
	fmt.Printf("HPQ consumer read succssful messages = %d, error messages = %d, duration = %dms.\n", succeed, errors, (time.Now().UnixNano()-timeBegin)/1e6)
}

//OnePartition
func ReadOnePartition() {
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
			case msg := <-partitionConsumer.Messages():
				succeed++
				quotation(msg)
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

/*TODO:
1、对一个stock的ask值进行10个时段的计算，可以使用10个goroutine分别计算性能更高。
*/
func quotation(msg *sarama.ConsumerMessage) {
	//fmt.Printf("HPQ consumer message, KEY=%s, VALUE=%s, partition=%d, offset=%d.\n", msg.Key, msg.Value, msg.Partition, msg.Offset)

	//获取该symbol的InputOHLC数据。
	var inputOHLC config.InputOHLC
	json.Unmarshal([]byte(msg.Value), &inputOHLC)

	//查询stockMap中是否有该symbol值，找到的话，按照该symbol ask值进行10个时段的计算
	stockMap.RLock()
	stock, ok := stockMap.m[inputOHLC.S]
	stockMap.RUnlock()
	if ok {
		//计算M1数据
		calculateM1(&inputOHLC, &stock.M1)

		//计算M5数据
		calculateM5(&inputOHLC, &stock.M5)

		//计算M15数据
		calculateM15(&inputOHLC, &stock.M15)

		//计算M30数据
		calculateM30(&inputOHLC, &stock.M30)

		//计算H1数据
		calculateH1(&inputOHLC, &stock.H1)

		//计算H2数据
		calculateH2(&inputOHLC, &stock.H2)

		//计算H4数据
		calculateH4(&inputOHLC, &stock.H4)

		//计算D1数据
		calculateD1(&inputOHLC, &stock.D1)

		//计算W1数据
		calculateW1(&inputOHLC, &stock.W1)

		//计算MN数据
		calculateMN(&inputOHLC, &stock.MN)

		//更新map
		stockMap.Lock()
		stockMap.m[inputOHLC.S] = stock
		stockMap.Unlock()
		//fmt.Println(stockMap.m)
	} else {
		//没有找到则初始化包含10个时间段的symbol，加入到stockMap中。
		//构造10个时段的结构
		newM1 := config.OutputOHLC{
			S: inputOHLC.S,
			U: inputOHLC.U, //使用初始记录的时间
			C: inputOHLC.C, //所有价格赋予初始记录的ask价格
			V: inputOHLC.V, //数量赋予初始记录的数量
			O: inputOHLC.C, //所有价格赋予初始记录的ask价格
			H: inputOHLC.C, //所有价格赋予初始记录的ask价格
			L: inputOHLC.C, //所有价格赋予初始记录的ask价格
			T: "M1",
		}

		//使用深拷贝迅速构造其他九个结构，并使用type assertion进行类型转换
		newM5 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newM5.T = "M5"
		newM15 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newM15.T = "M15"
		newM30 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newM30.T = "M30"
		newH1 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newH1.T = "H1"
		newH2 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newH2.T = "H2"
		newH4 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newH4.T = "H4"
		newD1 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newD1.T = "D1"
		newW1 := deepcopy.Copy(newM1).(config.OutputOHLC)
		newW1.T = "W1"
		newMN := deepcopy.Copy(newM1).(config.OutputOHLC)
		newMN.T = "MN"

		//构造新的OHLC结构
		newOHLC := config.OHLC{
			M1:  newM1,
			M5:  newM5,
			M15: newM15,
			M30: newM30,
			H1:  newH1,
			H2:  newH2,
			H4:  newH4,
			D1:  newD1,
			W1:  newW1,
			MN:  newMN,
		}

		//插入到map中
		stockMap.Lock()
		stockMap.m[inputOHLC.S] = newOHLC
		stockMap.Unlock()
		//fmt.Println(stockMap.m)
	}
}

//计算M1时段的OHLC
func calculateM1(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && (now.Hour() == inputOHLC.U.Hour() && (now.Minute() == inputOHLC.U.Minute())) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算M5时段的OHLC
func calculateM5(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	i := int(now.Minute() / 5)
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && (now.Hour() == inputOHLC.U.Hour() && (inputOHLC.U.Minute() > i*5) && (inputOHLC.U.Minute() <= (i+1)*5)) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算M15时段的OHLC
func calculateM15(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	i := int(now.Minute() / 15)
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && (now.Hour() == inputOHLC.U.Hour() && (inputOHLC.U.Minute() > i*15) && (inputOHLC.U.Minute() <= (i+1)*15)) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算M30时段的OHLC
func calculateM30(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	if now.Minute() < 30 {
		if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && (now.Hour() == inputOHLC.U.Hour() && (inputOHLC.U.Minute() > 0) && (inputOHLC.U.Minute() <= 29)) {
			calculateOHLC(inputOHLC, stock)
		}
	} else {
		if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && (now.Hour() == inputOHLC.U.Hour() && (inputOHLC.U.Minute() >= 30) && (inputOHLC.U.Minute() < 59)) {
			calculateOHLC(inputOHLC, stock)
		}
	}
}

//计算H1时段的OHLC
func calculateH1(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && (now.Hour() == inputOHLC.U.Hour()) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算H2时段的OHLC,往前推一个小时，未考虑临界时间点。
func calculateH2(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && ((now.Hour() == inputOHLC.U.Hour()) || (now.Hour()-1 == inputOHLC.U.Hour())) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算H4时段的OHLC,往前推一个小时，往后退2个小时，未考虑临界时间点。
func calculateH4(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	hbegin := now.Hour() - 1
	hend := now.Hour() + 2
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) && (inputOHLC.U.Hour() >= hbegin) && (inputOHLC.U.Hour() <= hend) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算D1时段的OHLC
func calculateD1(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) && (now.Day() == inputOHLC.U.Day()) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算W1时段的OHLC
func calculateW1(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//TODO: 判断输入symbol时间是否符合当前时段
	calculateOHLC(inputOHLC, stock)
}

//计算MN时段的OHLC
func calculateMN(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//判断输入symbol时间是否符合当前时段
	now := time.Now()
	if (now.Year() == inputOHLC.U.Year()) && (now.Month() == inputOHLC.U.Month()) {
		calculateOHLC(inputOHLC, stock)
	}
}

//计算各个时段的OHLC
func calculateOHLC(inputOHLC *config.InputOHLC, stock *config.OutputOHLC) {
	//字段S/O/T不赋值
	//原有数量加上新ask请求的数量
	stock.V = stock.V + inputOHLC.V

	//比较时间，如果新记录的时间晚于原有时间，则对ClosePrice赋值
	if inputOHLC.U.Unix()-stock.U.Unix() > 0 {
		stock.C = inputOHLC.C
	}

	//HighPrice
	if inputOHLC.C > stock.H {
		stock.H = inputOHLC.C
	}

	//LowPrice
	if inputOHLC.C < stock.L {
		stock.L = inputOHLC.C
	}

	//U timestamp
	stock.U = inputOHLC.U
}
