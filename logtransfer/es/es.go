package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
)

var (
	g_client *elastic.Client // ES客户端
	g_ch chan *LogData
)

type LogData struct {
	Topic string `json:"topic"`
	Data string `json:"data"`
}

// 初始化ES，准备接受kafka消费者发来的数据，并保存。
func Init(address string, chSize int, goroNums int) (err error) {
	if strings.HasPrefix(address, "http://") {
		address = "http://" + address
	}
	g_client, err = elastic.NewClient(elastic.SetURL("http://127.0.0.1:9200"))
	if err != nil {
		panic(err)
	}
	fmt.Println("connect ES success!")
	// 初始化管道
	g_ch = make(chan *LogData, chSize)
	// 开启从管道中发送信息给ES。
	for i := 0; i < goroNums; i++ {
		go SendToES()
	}
	return
}

func SendToESChan(msg *LogData) {
	g_ch <- msg
}

func SendToES() error {
	for {
		select {
		case msg := <- g_ch:
			put, err := g_client.Index().Index(msg.Topic).BodyJson(msg).Do(context.Background())
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println(put.Id, put.Index, put.Type)
		default:
			time.Sleep(time.Second)
		}
	}
}