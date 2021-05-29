package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)
type LogEntry struct {
	Path string `json:path` // 日志存放的路径
	Topic string `json:topic` // 日志要发往kafka中的哪里个Topic
}
var (
	cli *clientv3.Client
)
// 初始化ETCD函数
func Init(addr string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:[]string{addr},
		DialTimeout:timeout,
	})
	if err != nil {
		fmt.Println("put failed")
		return
	}
	return
}

// 从ETCD中获取配置项目
func GetConfig(key string) (logEntryCfg []*LogEntry, err error) {
	logEntryCfg = make([]*LogEntry, 16)
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	if err != nil {
		fmt.Printf("get %s failed.\n", key)
		return
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		fmt.Println(string(ev.Value))
		//cur := &LogEntry{}
		err = json.Unmarshal(ev.Value, &logEntryCfg)
		if err != nil {
			fmt.Printf("unmarshal etcd value failed, err:%v\n", err)
			return
		}
		//logEntryCfg = append(logEntryCfg, cur)
	}
	return
}

func WatchCfg(key string, newCfgCh chan <- []*LogEntry) {
	ch := cli.Watch(context.Background(), key)
	for wresp := range ch {
		for _, evt := range wresp.Events {
			fmt.Printf("Type:%v, key:%v, value:%v\n", evt.Type, string(evt.Kv.Key), string(evt.Kv.Value))
			// 发生变化后立即通知taillog.takMgr
			var newConf []*LogEntry
			// 删除操作
			if evt.Type != clientv3.EventTypeDelete {
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Printf("unmarshal failed, err:%v\n", err)
					continue
				}
				fmt.Println("get new config")
			}
			newCfgCh <- newConf
		}
	}
}