package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            []string{"0.0.0.0:2379"},
		DialTimeout:          5 * time.Second,
	})
	if err != nil {
		fmt.Printf("connnect to etcd failed, err:%v\n", err)
		return
	}
	fmt.Println("connect to etcd success! ")
	defer cli.Close()
	ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
	_, err = cli.Put(ctx, "/logagent/192.168.0.10/collect_config", `[{"path":"D:/GoWeb/src/logagent/taillog/my_log","topic":"web_log"}]`)
	if err != nil {
		fmt.Println("Put failed!")
		fmt.Println(err)
		return
	}
}