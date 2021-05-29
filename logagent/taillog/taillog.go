package taillog

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"logagent/kafka"
)
var (
	tailObj *tail.Tail
	LogChan chan string
)

type TailTask struct {
	path string
	topic string
	instance *tail.Tail
	// 用context实现子goroutine的退出
	ctx context.Context
	cancelfunc context.CancelFunc
}

func NewTailTask(path, topic string) (newtailobj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	newtailobj = &TailTask{
		path:       path,
		topic:      topic,
		ctx:	    ctx,
		cancelfunc: cancel,
	}
	newtailobj.init() // 根据路径打开对应的日志
	return
}

func (t *TailTask)init() {
	config := tail.Config{
		Location:    &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件那里开始读取
		ReOpen:      true, // 重新打开
		MustExist:   false, // 文件不存在会报错
		Poll:        true,
		Follow:      true, // 是否跟踪，日志文件过大时会切割文件，跟踪则可以实现保证文件切割后依然有序读取。
	}
	var err error = nil
	fmt.Println(t.path)
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
	}
	go t.run() // 采集日志
}

func (t *TailTask) run() {
	for {
		select {
		case <- t.ctx.Done():
			fmt.Println("Task exit,", t.path + "#" + t.topic)
			return
		case l := <- t.instance.Lines:
			//kafka.SendToKafka(t.topic, line.Text) // 此时函数调用函数，读日志和写日志同步。
			//新策略：
			// 1.先把日志发送到一个通道中
			//fmt.Println(l)
			if l == nil {
				continue
			}
			fmt.Println(l.Text)
			kafka.SendToChan(t.topic, l.Text)
		default:
		}
	}
}

