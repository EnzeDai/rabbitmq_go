package RabbitMQ

/*
 *	Date:  2022-12-22	Author: Orlando Dai
 *	Content: rabbitmq的五种工作模式的定义
 */
import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// url 格式 amqp://账号:密码@rabbitmq服务器地址:端口号/vhost
/* === 5672 是默认数据端口；15672 是默认管理端口 === */
const MQURL = "amqp://imoocuser:imoocuser@127.0.0.1:5672/imooc"

type RabbitMQ struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	QueueName string // 队列名称
	Exchange  string // 交换机
	Key       string // key
	Mqurl     string // 连接信息
}

// 创建 RabbitMQ 结构体实例
func NewRabbitMQ(queueName string, exchange string, key string) *RabbitMQ {
	return &RabbitMQ{QueueName: queueName, Exchange: exchange, Key: key, Mqurl: MQURL}
}

// 断开 channel 和 connection，r 是接收者变量
func (r *RabbitMQ) Destory() {
	r.channel.Close()
	r.conn.Close()
}

// 打印错误信息
func (r *RabbitMQ) failOnErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}

/* ============= simple 模式下的生产消费 ============= */
/*     即一个生产者一个消费者，消息通过消息队列传递         */

// 创建 simple 模式的 rabbitmq 实例，传入参数仅为 queueName
func NewRabbitMQSimple(queueName string) *RabbitMQ {
	rabbitmq := NewRabbitMQ(queueName, "", "")
	var err error
	// 创建 rabbitmq 连接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "创建连接时发生错误！")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取 channel 失败！")
	return rabbitmq
}

// simple 模式下的生产者代码
func (r *RabbitMQ) PublishSimple(message string) {
	// 1. 申请队列，若队列不存在，创建；若存在，跳过创建（保证队列存在）
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		false, // 数据是否持久化
		false, // 最后一个生产者断开连接时是否自动删除
		false, // 是否排他性
		false, // 是否阻塞
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	// 2. 发送消息到队列中
	r.channel.Publish(
		r.Exchange,
		r.QueueName,
		false, // 若为true，根据exchange类型和routekey规则，若无法找到符合条件的队列，则把发送的消息返回给发送者
		false, // 若为true，当exchange发送消息到队列后发现队列上没有绑定消费者，则把消息返回给发送者
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

// simple 模式下的消费者代码
func (r *RabbitMQ) ConsumeSimple() {
	// 1. 申请队列，若队列不存在，创建；若存在，跳过创建（保证队列存在）
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		false, // 数据是否持久化
		false, // 最后一个生产者断开连接时是否自动删除
		false, // 是否排他性
		false, // 是否阻塞
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	// 2.接收（消费）消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		"",    // 不区分多个消费者
		true,  // 是否自动应答
		false, // 是否有排他性
		false, // 若设置为true，表示不能将同一个connection中发送的消息传递给该connection中的消费者
		false, // 队列消费是否阻塞
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	forever := make(chan bool)
	// 启动协程处理消息
	go func() {
		for d := range msgs {
			// 实现我们要处理的逻辑函数
			log.Printf("[*] Receive a message: %s", d.Body)
		}
	}()

	log.Printf("[*] Waiting for messages, To exit press CTRL+C")
	<-forever
}

/* ============= PubSub 模式下的生产消费 ============== */
/*   消息可以被多个消费者获取，即通过订阅（subscribe）方式   */

// PubSub 模式下的 rabbitmq 实例，传入参数仅为 exchangeName
func NewRabbitMQPubSub(exchangeName string) *RabbitMQ {
	rabbitmq := NewRabbitMQ("", exchangeName, "")
	var err error
	// 创建 rabbitmq 连接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "创建连接时发生错误！")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取 channel 失败！")
	return rabbitmq
}

// 订阅模式下的生产者代码
func (r *RabbitMQ) PublishPub(message string) {
	// 1. 申请交换机，若交换机不存在，创建；若存在，不创建
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"fanout", // 广播类型
		true,     // 数据持久化
		false,
		false, // true 表示该exchange不可以被client用于推送消息，仅用于exchange间的绑定
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare an exchange")

	// 2. 发送消息到交换机中
	r.channel.Publish(
		r.Exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

// 订阅模式下的消费者代码
func (r *RabbitMQ) ReceiveSub() {
	// 1. 申请交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"fanout", // 广播类型
		true,     // 数据持久化
		false,
		false, // true 表示该exchange不可以被client用于推送消息，仅用于exchange间的绑定
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare an exchange")

	// 2. 尝试创建队列，且不能写队列名字
	q, err := r.channel.QueueDeclare(
		"", // 随机生成队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare a queue")

	// 3. 绑定队列到 exchange 中
	err = r.channel.QueueBind(
		q.Name,
		"",
		r.Exchange,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to bind a queue")

	// 4. 消费消息
	msgs, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to consume messages")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("[*] Received a message: %s", d.Body)
		}
	}()
	fmt.Println("退出请按 CTRL + C")
	<-forever
}

/* ============ Routing 模式下的生产消费 ============ */
// 一个消息可以被多个消费者获取，且消息的目标队列可被生产者指定

// Routing 模式下的 rabbitmq 实例，传参为交换机名称和routingKey
func NewRabbitMQRouting(exchangeName string, routingKey string) *RabbitMQ {
	rabbitmq := NewRabbitMQ("", exchangeName, routingKey)
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "创建连接时发生错误！")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取 channel 失败！")
	return rabbitmq
}

// routing 模式发送消息
func (r *RabbitMQ) PublishRouting(message string) {
	// 1. 申请交换机，设置为 direct 类型
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare an exchange")

	// 2. 发送消息
	err = r.channel.Publish(
		r.Exchange,
		r.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	r.failOnErr(err, "Failed to publish messages")
}

// routing 模式接受消息
func (r *RabbitMQ) ReceiveRouting() {
	// 1. 申请交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare an exchange")

	// 2. 申请创建队列，不要写队列名
	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare a queue")

	// 3. 绑定队列到交换机
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.Exchange,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to bind a queue")

	// 4. 消费消息
	msgs, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to consume messages")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("[*] Received a message: %s", d.Body)
		}
	}()
	fmt.Println("退出请按 CTRL + C")
	<-forever
}

/* ============ Topic 模式下的生产消费 ============ */

// Topic 模式下的 rabbitmq 实例，和 routing 模式一致
func NewRabbitMQTopic(exchangeName string, routingKey string) *RabbitMQ {
	rabbitmq := NewRabbitMQ("", exchangeName, routingKey)
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "创建连接时发生错误！")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取 channel 失败！")
	return rabbitmq
}

// Topic 模式发送消息
func (r *RabbitMQ) PublishTopic(message string) {
	// 1. 申请交换机，设置为 topic 类型
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare an exchange")

	// 2. 发送消息
	err = r.channel.Publish(
		r.Exchange,
		r.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	r.failOnErr(err, "Failed to publish messages")
}

// Topic 模式消费消息，匹配规则：“*” 匹配一个单词，“#” 匹配多个单词（可以是零个）
// 如 imooc.* 匹配 imooc.hello，但不能匹配 imooc.hello.user
func (r *RabbitMQ) ReceiveTopic() {
	// 1. 申请交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare an exchange")

	// 2. 申请创建队列，不要写队列名
	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare a queue")

	// 3. 绑定队列到交换机
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.Exchange,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to bind a queue")

	// 4. 消费消息
	msgs, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnErr(err, "Failed to consume messages")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("[*] Received a message: %s", d.Body)
		}
	}()
	fmt.Println("退出请按 CTRL + C")
	<-forever
}
