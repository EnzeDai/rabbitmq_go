package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
)

func main() {
	rabbitmq := RabbitMQ.NewRabbitMQSimple("imoocSimple")
	rabbitmq.PublishSimple("Hello imooc!")
	fmt.Println("send success!")
}
