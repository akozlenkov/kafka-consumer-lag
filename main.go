package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gookit/validate"
	"github.com/jawher/mow.cli"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"time"
)

var (
	results []Result
	kafkaClient sarama.Client
)

type Result struct {
	Group string
	Topic string
	Timestamp time.Time
	Lag int64
}

type Config struct {
	Check []struct{
		Group 	string		`yaml:"group",validate:"required"`
		Topic	[]string	`yaml:"topics",validate:"required|minLen:1"`
	} `yaml:"checks",validate:"required|minLen:1"`

	Broker		[]string	`yaml:"brokers",validate:"required|minLen:1"`
}

func main() {
	app := cli.App("kafka-consumer-lag", "Show kafka lag")
	app.Spec = "--config"

	var (
		config = app.StringOpt("config", "kafka-consumer-lag.yml", "Path to kafka-consumer-lag config")
	)

	app.Action = func() {
		if !fileExists(*config) {
			exitWitError(fmt.Errorf("Config %s not found", *config))
		}

		var conf Config

		file, err := ioutil.ReadFile(*config)
		if err != nil {
			exitWitError(err)
		}

		if err = yaml.Unmarshal(file, &conf); err != nil {
			exitWitError(err)
		}

		v := validate.Struct(conf)

		if !v.Validate() {
			exitWitError(v.Errors)
		}

		kafkaClient, err = sarama.NewClient(conf.Broker, nil)
		if err != nil {
			exitWitError(err)
		}

		results = make([]Result, 0)

		for _, c := range conf.Check {
			for _, t := range c.Topic {
				processGroup(c.Group, t)
			}
		}

		for _, r := range results {
			fmt.Printf("kafka_lag,group=%s,topic=%s lag=%d %d\n", r.Group, r.Topic, r.Lag, r.Timestamp.Unix())
		}
	}

	app.Run(os.Args)
}

func processGroup(groupId string, topic string) {
	partitions, err := kafkaClient.Partitions(topic)
	if err != nil {
		exitWitError(err)
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, kafkaClient)
	if err != nil {
		exitWitError(err)
	}

	var lag int64
	var controlChannel = make(chan []int64)
	for _, partition := range partitions {
		go processPartition(topic, partition, controlChannel, offsetManager)
	}
	for range partitions {
		response := <-controlChannel
		lag += response[0]
	}
	results = append(results, Result{
		Group:    groupId,
		Topic:     topic,
		Timestamp: time.Now(),
		Lag:       lag,
	})
}

func processPartition(topic string, partition int32, controlChannel chan []int64, offsetManager sarama.OffsetManager) {
	pom, err := offsetManager.ManagePartition(topic, int32(partition))
	if err != nil {
		exitWitError(err)
	}

	consumerOffset, _ := pom.NextOffset()
	offset, err := kafkaClient.GetOffset(topic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		exitWitError(err)
	}
	var response = make([]int64, 0)
	lag := offset - consumerOffset + 1
	response = append(response, lag)
	controlChannel <- response
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func exitWitError(err error) {
	fmt.Println(err)
	cli.Exit(1)
}
