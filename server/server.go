package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var hub *Hub

func wsHandler(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}

	vars := mux.Vars(r)
	client := &Client{
		uid:  vars["uid"],
		conn: c,
		sid:  uuid.New().String(),
		ch:   make(chan interface{}),
	}
	log.Println("register:", client)
	hub.register <- client

	go client.ReadLoop(hub)
	go client.WriteLoop(hub)
}

var host = flag.String("h", "localhost:8080", "http service address")
var redisHost = flag.String("rh", "localhost:6379", "redis address")
var redisChan = flag.String("rc", "message", "redis channel")
var kafkaHost = flag.String("kh", "localhost:9092", "kafka host")
var kafkaTopic = flag.String("kt", "message", "kafka topic")
var kafkaGroup = flag.String("kg", "message-group-1", "kafka consumer group")

//var redis = flag.String("redis", "localhost:8080", "http service address")

func main() {
	flag.Parse()

	r := mux.NewRouter()
	hub = NewHub(*redisHost, *redisChan, *kafkaHost, *kafkaTopic, *kafkaGroup)
	r.HandleFunc("/{uid}", wsHandler)

	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(*host, nil))
}
