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

	//Setup the input
	inputConfig := config.GetConfig()
	inputConfig.BootStrapServers = os.Getenv("INPUT_DATA_BOOTSTRAP_SERVERS")
	inputConfig.KafkaTopic = os.Getenv("INPUT_DATA")

	log.Printf("BOOTSTRAP_SERVERS: %s", os.Getenv("INPUT_DATA_BOOTSTRAP_SERVERS"))

	consumer, err := sarama.NewConsumer([]string{inputConfig.BootStrapServers}, nil)
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
	partitionConsumer, err := consumer.ConsumePartition(inputConfig.KafkaTopic, 0, initialOffset)
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

	//Setup Output
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true

	outputConfig := config.GetConfig()
	outputConfig.BootStrapServers = os.Getenv("OUTPUT_DATA_BOOTSTRAP_SERVERS")
	outputConfig.KafkaTopic = os.Getenv("OUTPUT_DATA")

	producer, err := sarama.NewSyncProducer([]string{outputConfig.BootStrapServers}, cfg)
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

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			//"http://172.30.143.13"
			//helloworld-go.myproject.example.com

			//Call the Serving function
			req, err := http.NewRequest("POST", os.Getenv("URL"), bytes.NewBuffer(msg.Value))
			req.Host = os.Getenv("HOST")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error sending to Serving function: %s", err)
			}

			defer resp.Body.Close()

			fmt.Println("response Status:", resp.Status)
			fmt.Println("response Headers:", resp.Header)

			body, _ := ioutil.ReadAll(resp.Body)

			fmt.Println("response Body:", string(body))
			consumed++

			//Send the response downstream
			outputMsg := &sarama.ProducerMessage{
				Topic: outputConfig.KafkaTopic,
				Value: sarama.StringEncoder(body),
			}

			partition, offset, err := producer.SendMessage(outputMsg)
			if err != nil {
				log.Printf("Error sending to Serving Kafka output: %s", err)
			}

			fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", outputConfig.KafkaTopic, partition, offset)

		case <-signals:
			break ConsumerLoop
		}
	}
}
