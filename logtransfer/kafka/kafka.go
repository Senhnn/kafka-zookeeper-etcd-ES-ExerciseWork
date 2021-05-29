package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"logtransfer/es"
)


/*初始化kafka,并建立消费者读取数据*/
func Init(addrs []string, topic string) (err error) {
	consumer, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有分区
	if err != nil {
		fmt.Printf("fail to get list of partition err:%v\n", err)
		return
	}
	fmt.Println("分区列表:", partitionList)
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("fail to start consumer for partition %d,err:%v\n", partition, err)
			return err
		}
		//defer pc.AsyncClose()

		// 异步从每个分区消费信息
		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d, Offset:%d, Key:%v, Value:%v\n", msg.Partition, msg.Offset, msg.Key,
					string(msg.Value))
				// 发送至ES
				//var logdata = LogData{
				//	Data : string(msg.Value),
				//}
				//err := json.Unmarshal(msg.Value, logdata)
				if err != nil {
					fmt.Printf("Json.unmarshal failed! err:%v\n", err)
					continue
				}
				logdata := es.LogData {Topic: topic, Data: string(msg.Value)}
				es.SendToESChan(&logdata)
			}
		}(pc)
	}
	return
}
