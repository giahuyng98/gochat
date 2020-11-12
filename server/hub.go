package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	//"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
)

var ctx = context.Background()

type SMessage struct {
	sender *Client
	m      *InMessage
}

type Hub struct {
	clients                 map[string][]*Client
	mu                      sync.RWMutex
	register                chan *Client
	unregister              chan *Client
	message                 chan SMessage
	redisClient             *redis.Client
	redisChan               <-chan *redis.Message
	redisMessageChannelName string
	kafkaWriter             *kafka.Writer
	kafkaReader             *kafka.Reader
}

func NewHub(redisHost, redisChan, kafkaHost, kafkaTopic, kafkaGroup string) *Hub {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisHost,
	})
	log.Println("redis connected:", redisHost)
	sub := redisClient.Subscribe(ctx, redisChan)
	iface, err := sub.Receive(ctx)
	if err != nil {
		panic(err)
	}
	switch iface.(type) {
	case *redis.Subscription:
		// subscribe succeeded
	case *redis.Message:
		// received first message
	case *redis.Pong:
		// pong received
	default:
		panic("redis error subscribe")
		// handle error
	}
	log.Println("redis subscribe:", redisChan)
	h := &Hub{
		register:                make(chan *Client),
		unregister:              make(chan *Client),
		clients:                 make(map[string][]*Client),
		message:                 make(chan SMessage),
		redisClient:             redisClient,
		redisChan:               sub.Channel(),
		redisMessageChannelName: redisChan,
		kafkaWriter: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      []string{kafkaHost},
			Topic:        kafkaTopic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
		}),
		kafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{kafkaHost},
			GroupID:   kafkaGroup,
			Topic:     kafkaTopic,
			MinBytes:  1,    // 10KB
			MaxBytes:  10e6, // 10MB,
			Partition: 0,    // TODO: change it
			MaxWait:   10 * time.Millisecond,
		}),
	}

	log.Println("kafka config:", h.kafkaReader.Config())
	go h.Run()
	go h.RunConsumeLoop()
	return h
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.uid] = append(h.clients[client.uid], client)
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			// TODO: optimize
			for idx := range h.clients[client.uid] {
				if h.clients[client.uid][idx] == client {
					head := h.clients[client.uid][:idx]
					tail := h.clients[client.uid][idx+1:]
					h.clients[client.uid] = append(head, tail...)
					fmt.Println(client, idx)
					break
				}
			}
			h.mu.Unlock()

			// receive message from websocket client
		case msg := <-h.message:
			outMsg := OutMessage{
				From: msg.sender.uid,
				To:   msg.m.To,
				Msg:  msg.m.Msg,
			}
			bytes, err := json.Marshal(outMsg)

			if err != nil {
				log.Fatal("json failed to marshal messages:", err)
			}
			log.Println("websocket incoming", outMsg)
			err = h.kafkaWriter.WriteMessages(ctx, kafka.Message{Value: bytes})
			log.Println("kafka produce done", outMsg)

			if err != nil {
				log.Fatal("kafka failed to write messages:", err)
			}

			// receive message from redis channel
		case redisMsg := <-h.redisChan:
			var msg OutMessage
			err := json.Unmarshal([]byte(redisMsg.Payload), &msg)
			if err != nil {
				log.Println("parse redis message fail:", err)
			} else {
				if msg.To == "broadcast" {
					h.SendBroadcast(msg)
				} else {
					h.SendToUser(msg.To, msg)
				}
			}
		}
	}
}

func (h *Hub) RunConsumeLoop() {
	for {
		m, err := h.kafkaReader.FetchMessage(ctx)
		log.Println("kafka fetched message")
		if err != nil {
			log.Println("kafka reader:", err)
			break
		}
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		// publish redis channel
		h.redisClient.Publish(ctx, h.redisMessageChannelName, m.Value)
		log.Println("redis publis done")
		h.kafkaReader.CommitMessages(ctx, m)
	}

	if err := h.kafkaReader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func (h *Hub) SendToUser(uid string, msg interface{}) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, client := range h.clients[uid] {
		client.ch <- msg
	}
	return nil
}

func (h *Hub) SendBroadcast(msg interface{}) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, users := range h.clients {
		for _, session := range users {
			session.ch <- msg
		}
	}
	return nil

}

