package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
	"strconv"
	"time"
)

// RabbitMQ 的 work 模式指的是一个消息只能被一个消费者获取
func main() {
	rabbitmq := RabbitMQ.NewRabbitMQSimple("" + "imoocSimple")

	for i := 0; i <= 100; i++ {
		rabbitmq.PublishSimple("Hello imooc!" + strconv.Itoa(i))
		time.Sleep(1 * time.Second)
		fmt.Println(i)
	}

}
