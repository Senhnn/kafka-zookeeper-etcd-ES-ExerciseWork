/********************************
* DATE:2021/5/20/23:06
* CREATEOR:SENHNN
* DESCRIPTION:logAgent入口
********************************/
package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"logagent/conf"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/taillog"
	"logagent/utils"
	"sync"
	"time"
)

var cfg = new(conf.AppConfig)

/*
* 读取日志并发送到kafka
*/
//func run() {
//	// 1.读取日志
//	for {
//		select {
//		case line := <- taillog.ReadChan():
//			// 2.发送到kafka
//			kafka.SendToKafka(cfg.KafkaConfig.Topic, line.Text)
//		default:
//			time.Sleep(time.Second)
//		}
//	}
//}
func main() {
	// 0.加载配置文件
	err := ini.MapTo(cfg, "src/logagent/conf/config.ini")
	if err != nil {
		fmt.Printf("config.ini load fail, err = %v\n",err)
		return
	}

	// 1.初始化kafka连接。
	err = kafka.Init([]string{cfg.KafkaConfig.Address}, cfg.KafkaConfig.ChanMaxSize)
	if err != nil {
		fmt.Printf("Kafka连接失败! err:%v\n", err)
		return
	}
	fmt.Println("kafka连接成功！")

	// 2.初始化etcd
	etcd.Init(cfg.EtcdConfig.Address, time.Duration(cfg.EtcdConfig.Timeout) * time.Second)
	// 根据ip拉取每个logAgent独有的配置。
	ipStr, err := utils.GetOutBoundIP()
	if err != nil {
		panic(err)
	}
	fmt.Println(ipStr)
	etcdConfKey := fmt.Sprintf(cfg.EtcdConfig.Key, ipStr)
	fmt.Println(etcdConfKey)
	// 2.1从etcd中获取日志收集的配置信息
	logEntryCfg, err := etcd.GetConfig(etcdConfKey)
	if err != nil {
		fmt.Printf("init etcd.GetConfig failed, err:%v\n", err)
		return
	}
	fmt.Println("get cfg from etcd success,", logEntryCfg)
	for i, v := range logEntryCfg {
		fmt.Println(i , "=", v)
	}

	// 3.收集日志发往kafka
	taillog.Init(logEntryCfg)
	// 2.2派遣一个watch去监视日志收集项的变化（有变化计时通知logAgent实现热加载配置）
	newConfChan := taillog.NewCfgChan()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go etcd.WatchCfg(etcdConfKey, newConfChan)
	wg.Wait()
}

