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
	"github.com/SENERGY-Platform/mqtt-bridge/lib/config"
	paho "github.com/eclipse/paho.mqtt.golang"
	uuid "github.com/satori/go.uuid"
	"log"
	"sync"
	"time"
)

func Target(config config.Config, done sync.WaitGroup, source chan paho.Message) error {
	clientId := uuid.NewV4().String()
	if config.Debug {
		log.Println("DEBUG: target client_id =", clientId)
	}
	options := paho.NewClientOptions().
		SetPassword(config.TargetPw).
		SetUsername(config.TargetUser).
		SetAutoReconnect(true).
		SetCleanSession(false).
		SetClientID(clientId).
		AddBroker(config.TargetBroker)

	client := paho.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Target.Connect(): ", token.Error())
		return token.Error()
	}

	done.Add(1)
	go func() {
		for msg := range source {
			targetTopic, ok := config.TopicMapping[msg.Topic()]
			if ok {
				token := client.Publish(targetTopic, byte(config.TargetQos), false, msg.Payload())
				if token.Wait() && token.Error() != nil {
					log.Println("ERROR: unable to publish on target ", targetTopic, token.Error())
				}
			} else if config.Debug {
				log.Println("DEBUG: missing mapping for topic", msg.Topic())
			}
		}
		client.Disconnect(200)
		time.Sleep(200 * time.Millisecond)
		done.Done()
	}()
	return nil
}
