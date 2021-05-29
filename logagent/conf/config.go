package conf

type AppConfig struct {
	KafkaConfig `ini:"kafka"`
	EtcdConfig `ini:"etcd"`
}

type KafkaConfig struct {
	Address string `ini:"address"`
	ChanMaxSize int `ini:"chan_max_size"`
}

type TaillogConfig struct {
	FileName string `ini:"filename"`
}

type EtcdConfig struct {
	Address string `ini:"address"`
	Timeout int `ini:"timeout"`
	Key string `ini:"collect_log_key"`
}
