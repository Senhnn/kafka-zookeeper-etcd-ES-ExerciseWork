/********************************
* DATE:2021/5/20/23:06
* CREATEOR:SENHNN
* DESCRIPTION:专门向kafka中写日志
********************************/
package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)
type logData struct {
	topic string
	data string
}
var (
	client sarama.SyncProducer // 声明一个全局的连接kafka的生产者client
	g_logDataChan chan *logData = make(chan *logData, 100000)
)

/*初始化client*/
func Init(addrs []string, maxSize int) (err error) {
	config := sarama.NewConfig() // 新建配置
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follwer都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true // 陈宫交付的消息将在success channel返回。
	// 初始化logDataChan管道
	//g_logDataChan = make(chan *logData, maxSize)

	// 连接到kafka
	client ,err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}

	go sendToKafka()

	return
}
 // 从chan中取出日志，向kafka发送日志信息
func sendToKafka() {
	for {
		select {
		case data := <- g_logDataChan:
			// 构造一个消息
			msg := &sarama.ProducerMessage{}
			msg.Topic = data.topic
			msg.Value = sarama.StringEncoder(data.data)

			// 发送信息到kafka
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("send msg failed, err:", err)
				return
			}
			fmt.Printf("pid:%v offset:%v\n", pid, offset)
		default:
			time.Sleep(time.Millisecond * 50)
		}

	}
}

// 把日志数据发送到内部的chan中
func SendToChan(topic, data string) {
	ld := &logData {
		topic: topic,
		data:  data,
	}
	g_logDataChan <- ld
}