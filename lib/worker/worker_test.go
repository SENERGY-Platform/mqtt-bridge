/*
 * Copyright 2020 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package worker

import (
	"context"
	"github.com/SENERGY-Platform/mqtt-bridge/lib/config"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	uuid "github.com/satori/go.uuid"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func Test(t *testing.T) {
	config := config.Config{
		Debug: true,
		TopicMapping: map[string]string{
			"source.a":   "target.a",
			"source.a.1": "target.a.1",
			"source.a.2": "target.a.2",
			"foo":        "bar",
		},
	}
	sourceMessages := map[string][]string{
		"source.a": {
			"1", "2", "3",
		},
		"source.a.1": {
			"a", "b", "c",
		},
		"foo": {
			"bar", "b", "a", "r",
		},
	}

	expectedTargetMessages := map[string][]string{
		"target.a": {
			"1", "2", "3",
		},
		"target.a.1": {
			"a", "b", "c",
		},
		"bar": {
			"bar", "b", "a", "r",
		},
	}
	test(t, config, sourceMessages, expectedTargetMessages)
}

func TestWithEnv(t *testing.T) {
	os.Setenv("TOPIC_MAPPING", `source.a:target.a, 
											source.a.1:target.a.1, 
											source.a.2:target.a.2, 
											foo:bar`)
	config, err := config.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	sourceMessages := map[string][]string{
		"source.a": {
			"1", "2", "3",
		},
		"source.a.1": {
			"a", "b", "c",
		},
		"foo": {
			"bar", "b", "a", "r",
		},
	}

	expectedTargetMessages := map[string][]string{
		"target.a": {
			"1", "2", "3",
		},
		"target.a.1": {
			"a", "b", "c",
		},
		"bar": {
			"bar", "b", "a", "r",
		},
	}
	test(t, config, sourceMessages, expectedTargetMessages)
}

func test(t *testing.T, conf config.Config, sourceMessages map[string][]string, expectedTargetMessages map[string][]string) {
	ctx, cancel := context.WithCancel(context.Background())
	done := sync.WaitGroup{}
	defer time.Sleep(2 * time.Second) //wait for container stop
	defer done.Wait()
	defer cancel()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Error(err)
		return
	}

	if conf.SourceBroker == "" {
		sourceBroker, err := Mqtt(pool, ctx, "SOURCE")
		if err != nil {
			t.Error(err)
			return
		}
		conf.SourceBroker = sourceBroker
	}

	if conf.TargetBroker == "" {
		targetBroker, err := Mqtt(pool, ctx, "TARGET")
		if err != nil {
			t.Error(err)
			return
		}
		conf.TargetBroker = targetBroker
	}

	conf.TargetQos = 2
	conf.SourceQos = 2

	err = Run(conf, ctx, done)
	if err != nil {
		t.Error(err)
		return
	}

	targetConsumer := paho.NewClient(paho.NewClientOptions().
		SetAutoReconnect(true).
		SetCleanSession(false).
		SetClientID(uuid.NewV4().String()).
		AddBroker(conf.TargetBroker))
	if token := targetConsumer.Connect(); token.Wait() && token.Error() != nil {
		t.Error(token.Error())
		return
	}

	sourceProducer := paho.NewClient(paho.NewClientOptions().
		SetAutoReconnect(true).
		SetCleanSession(false).
		SetClientID(uuid.NewV4().String()).
		AddBroker(conf.SourceBroker))
	if token := sourceProducer.Connect(); token.Wait() && token.Error() != nil {
		t.Error(token.Error())
		return
	}

	resultTargetMessages := map[string][]string{}
	resultMux := sync.Mutex{}
	for topic, _ := range expectedTargetMessages {
		targetConsumer.Subscribe(topic, 2, func(client paho.Client, message paho.Message) {
			resultMux.Lock()
			defer resultMux.Unlock()
			resultTargetMessages[message.Topic()] = append(resultTargetMessages[message.Topic()], string(message.Payload()))
		})
	}

	producerWait := sync.WaitGroup{}
	for topic, messages := range sourceMessages {
		go func(topic string, messages []string) {
			producerWait.Add(1)
			defer producerWait.Done()
			for _, msg := range messages {
				token := sourceProducer.Publish(topic, 2, false, msg)
				if token.Wait() && token.Error() != nil {
					t.Error(token.Error())
					return
				}
			}
		}(topic, messages)
	}

	producerWait.Wait()
	time.Sleep(10 * time.Second)

	resultMux.Lock()
	defer resultMux.Unlock()
	if !reflect.DeepEqual(expectedTargetMessages, resultTargetMessages) {
		t.Error(resultTargetMessages)
		return
	}
	t.Log(resultTargetMessages)
}

func Mqtt(pool *dockertest.Pool, ctx context.Context, logname string) (brokerUrl string, err error) {
	log.Println("start mqtt")
	container, err := pool.Run("erlio/docker-vernemq", "latest", []string{
		"DOCKER_VERNEMQ_ALLOW_ANONYMOUS=on",
		"DOCKER_VERNEMQ_LOG__CONSOLE__LEVEL=debug",
	})
	if err != nil {
		return "", err
	}
	go Dockerlog(pool, ctx, container, logname)
	go func() {
		<-ctx.Done()
		log.Println("DEBUG: remove container " + container.Container.Name)
		container.Close()
	}()
	err = pool.Retry(func() error {
		log.Println("DEBUG: try to connection to broker")
		options := paho.NewClientOptions().
			SetAutoReconnect(true).
			SetCleanSession(false).
			SetClientID(uuid.NewV4().String()).
			AddBroker("tcp://" + container.Container.NetworkSettings.IPAddress + ":1883")

		client := paho.NewClient(options)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Println("Error on Mqtt.Connect(): ", token.Error())
			return token.Error()
		}
		defer client.Disconnect(0)
		return nil
	})
	return "tcp://" + container.Container.NetworkSettings.IPAddress + ":1883", err
}

func Dockerlog(pool *dockertest.Pool, ctx context.Context, repo *dockertest.Resource, name string) {
	out := &LogWriter{logger: log.New(os.Stdout, "["+name+"]", 0)}
	err := pool.Client.Logs(docker.LogsOptions{
		Stdout:       true,
		Stderr:       true,
		Context:      ctx,
		Container:    repo.Container.ID,
		Follow:       true,
		OutputStream: out,
		ErrorStream:  out,
	})
	if err != nil && err != context.Canceled {
		log.Println("DEBUG-ERROR: unable to start docker log", name, err)
	}
}

type LogWriter struct {
	logger *log.Logger
}

func (this *LogWriter) Write(p []byte) (n int, err error) {
	this.logger.Print(string(p))
	return len(p), nil
}
