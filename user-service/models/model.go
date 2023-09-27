package models

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type ProducerConfig struct {
	BootstrapServers        string `yaml:"bootstrap.servers"`
	SecurityProtocol        string `yaml:"security.protocol"`
	SSLKeyLocation          string `yaml:"ssl.key.location"`
	SSLKeyPassword          string `yaml:"ssl.key.password"`
	SSLKeyStoreLocation     string `yaml:"ssl.keystore.location"`
	SSLKeyStorePassword     string `yaml:"ssl.keystore.password"`
	SSLCALocation           string `yaml:"ssl.ca.location"`
	KeySerializer           string `yaml:"key.serializer"`
	ValueSerializer         string `yaml:"value.serializer"`
	Acks                    string `yaml:"acks"`
	Retries                 int    `yaml:"retries"`
	BatchSize               int    `yaml:"batch.size"`
	LingerMs                int    `yaml:"linger.ms"`
	BufferMemory            int    `yaml:"buffer.memory"`
	IdentificationAlgorithm string `yaml:"ssl.endpoint.identification.algorithm"`
}

type ConsumerConfig struct {
	BootstrapServers   string `yaml:"bootstrap.servers"`
	GroupID            string `yaml:"group.id"`
	AutoOffsetReset    string `yaml:"auto.offset.reset"`
	EnableAutoCommit   string `yaml:"enable.auto.commit"`
	AutoCommitInterval string `yaml:"auto.commit.interval.ms"`
	SessionTimeout     string `yaml:"session.timeout.ms"`
	SecurityProtocol   string `yaml:"security.protocol"`
	SSLCALocation      string `yaml:"ssl.ca.location"`
}

func ReadConfig(filename string, out interface{}) error {
	file, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("Could not read file: %s", err)
		return err
	}

	err = yaml.Unmarshal(file, out)
	if err != nil {
		log.Printf("Could not unmarshal YAML: %s", err)
		return err
	}
	return nil
}
