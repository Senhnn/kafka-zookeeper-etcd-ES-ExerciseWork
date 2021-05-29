package taillog

import (
	"fmt"
	"logagent/etcd"
	"time"
)
var takMgr *tailLogManager

// tailTask管理者
type tailLogManager struct {
	logEntry []*etcd.LogEntry
	tskMap map[string]*TailTask
	newCfgChan chan []*etcd.LogEntry
}

func Init(logEntryCfg []*etcd.LogEntry) {
	takMgr = &tailLogManager{
		logEntry:logEntryCfg,
		tskMap:make(map[string]*TailTask, 32),
		newCfgChan:make(chan []*etcd.LogEntry),
	}
	for _, logEntry := range logEntryCfg {
		tailTask := NewTailTask(logEntry.Path, logEntry.Topic)
		mapPath := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		takMgr.tskMap[mapPath] = tailTask
	}

	go takMgr.run()
}

// 监听自己的newCfgChan，有了新的配置过来之后就做对应处理
// 1.配置新增
// 2.配置删除
// 3.配置变更
func (t *tailLogManager) run() {
	for {
		select {
		case newCfg := <- t.newCfgChan:
			for _, newConfig := range newCfg {
				mapPath := fmt.Sprintf("%s_%s", newConfig.Path, newConfig.Topic)
				_, inMap := t.tskMap[mapPath]
				if inMap {
					// 已经存在。
					continue
				} else {
					// 新增的
					newTailObj := NewTailTask(newConfig.Path, newConfig.Topic)
					fmt.Println(newConfig.Path, newConfig.Topic)
					t.tskMap[mapPath] = newTailObj
				}
				for _, c1 := range t.logEntry {
					isDelete := true
					for _, c2 := range newCfg {
						if c2.Path == c1.Path || c1.Topic == c2.Topic {
							isDelete = false
							continue
						}
					}
					if isDelete {
						// 把t.logEntry中的tailObj给停掉
						mapPath := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
						t.tskMap[mapPath].cancelfunc()
					}
				}
			}
			fmt.Println("新配置来了！", newCfg)

		default:
			time.Sleep(time.Second)
		}
	}
}

func NewCfgChan() chan <- []*etcd.LogEntry {
	return takMgr.newCfgChan
}