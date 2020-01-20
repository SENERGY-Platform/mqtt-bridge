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
	uuid "github.com/satori/go.uuid"
	"log"
	"sync"
	"time"
)

func Source(config config.Config, ctx context.Context, done sync.WaitGroup) (sink chan paho.Message, err error) {
	sink = make(chan paho.Message, 20)
	clientId := uuid.NewV4().String()
	if config.Debug {
		log.Println("DEBUG: source client_id =", clientId)
	}
	options := paho.NewClientOptions().
		SetPassword(config.SourcePw).
		SetUsername(config.SourceUser).
		SetAutoReconnect(true).
		SetCleanSession(false).
		SetClientID(clientId).
		AddBroker(config.SourceBroker)

	client := paho.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Source.Connect(): ", token.Error())
		return sink, token.Error()
	}
	filter := map[string]byte{}
	for topic, _ := range config.TopicMapping {
		filter[topic] = byte(config.SourceQos)
	}

	var handler paho.MessageHandler
	handler = func(client paho.Client, message paho.Message) {
		sink <- message
	}

	if token := client.SubscribeMultiple(filter, handler); token.Wait() && token.Error() != nil {
		log.Println("Error on Source.SubscribeMultiple(): ", token.Error())
		return sink, token.Error()
	}

	done.Add(1)
	go func() {
		<-ctx.Done()
		client.Disconnect(200)
		time.Sleep(200 * time.Millisecond)
		close(sink)
		done.Done()
	}()
	return sink, nil
}
