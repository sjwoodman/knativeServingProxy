package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/matzew/kafka-receiver/pkg/config"
)

func main() {

	kafkaConfig := config.GetConfig()

	kafkaConfig.BootStrapServers = os.Getenv("INPUT_DATA_BOOTSTRAP_SERVERS")

	log.Printf("BOOTSTRAP_SERVERS: %s", os.Getenv("INPUT_DATA_BOOTSTRAP_SERVERS"))


	consumer, err := sarama.NewConsumer([]string{kafkaConfig.BootStrapServers}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// OffsetNewest
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic
	partitionConsumer, err := consumer.ConsumePartition(os.Getenv("INPUT_DATA"), 0, initialOffset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0

	log.Printf("Connected to %s", os.Getenv("INPUT_DATA"))

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():

			req, err := http.NewRequest("POST", "http://192.168.64.32:32380", bytes.NewBuffer(msg.Value))
			req.Host = "helloworld-go.myproject.example.com"

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			defer resp.Body.Close()

			fmt.Println("response Status:", resp.Status)
			fmt.Println("response Headers:", resp.Header)
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println("response Body:", string(body))

			log.Printf("Consumed message offset %s", msg.Value)
			consumed++

			sendResponse(body)
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}

func sendResponse(payload []byte) {

	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true

	flowConfig := config.GetConfig()
	flowConfig.BootStrapServers = os.Getenv("OUTPUT_DATA_BOOTSTRAP_SERVERS")


	producer, err := sarama.NewSyncProducer([]string{flowConfig.BootStrapServers}, cfg)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	msg := &sarama.ProducerMessage{
		Topic: os.Getenv("OUTPUT_DATA"),
		Value: sarama.StringEncoder(payload),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", flowConfig.KafkaTopic, partition, offset)
}
