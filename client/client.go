package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
)

var h = flag.String("h", "localhost:8080", "http service address")
var user = flag.String("u", "broadcast", "username")

const (
	C_GREEN = "\033[0;32m"
	C_RESET = "\033[0m"
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: *h, Path: "/" + *user}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})
	message := make(chan interface{})

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf(C_GREEN+"\nrecv: %s"+C_RESET, message)
		}
	}()

	go func() {
		//reader := bufio.NewReader(os.Stdin)
		scanner := bufio.NewScanner(os.Stdin)
		defer close(message)
		for {
			fmt.Println("Send message to...")
			fmt.Print("Enter username: ")
			scanner.Scan()
			user := scanner.Text()
      if user == "" {
        user = "broadcast"
      }
			fmt.Print("Enter message: ")
			scanner.Scan()
			msg := scanner.Text()
			if err != nil {
				break
			}
			fmt.Printf("Send to user [%v], message [%v]\n", user, msg)
			message <- map[string]interface{}{"to": user, "msg": msg}
		}
		if err := scanner.Err(); err != nil {
			log.Println(err)
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case msg, ok := <-message:
			if !ok {
				return
			}
			err := c.WriteJSON(msg)
			if err != nil {
				log.Println("write:", err)
				return
			}
		case <-interrupt:
			log.Println("interrupt")

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}

