package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"logtransfer/conf"
	"logtransfer/es"
	"logtransfer/kafka"
	"time"
)

func main() {
	// 0.加载配置文件。
	var cfg = new(conf.LogTransferCfg)
	err := ini.MapTo(cfg, "./conf/config.ini")
	if err != nil {
		fmt.Printf("init config failed, err:%v\n", err)
		return
	}
	fmt.Printf("cfg:%v\n", cfg)
	// 1.初始化ES。
	err = es.Init(cfg.ESCfg.Address, cfg.ESCfg.ChanSize, cfg.ESCfg.GoroutineNums)
	if err != nil {
		fmt.Printf("init ES failed! err:%v\n", err)
		return
	}
	fmt.Println("init ES success.")

	// 2.初始化kafka,并建立分区的kafka消费者。
	// 每个分区的消费者分别取出数据，通过SendToES将数据发送至ES。
	err = kafka.Init([]string{cfg.KafkaCfg.Address}, cfg.KafkaCfg.Topic)
	if err != nil {
		fmt.Printf("init kafka consumer fail, err:%v\n", err)
		return
	}
	time.Sleep(time.Hour)
	// 2.从kafka取日志数据。
	// 3.发往ES。
}
