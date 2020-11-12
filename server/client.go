package main

import (
	"log"

	"github.com/gorilla/websocket"
)

type InMessage struct {
  To  string `json:"to"`
  Msg string `json:"msg"`
}

type OutMessage struct {
  From string `json:"from"`
  To   string `json:"to"`
  Msg  string `json:"msg"`
}

type Client struct {
	uid  string
	sid  string
	conn *websocket.Conn
	ch   chan interface{}
}

func (c *Client) ReadLoop(h *Hub) {
	defer func() {
		c.conn.Close()
		h.unregister <- c
	}()
	for {
		var msg InMessage
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			log.Println("read & parse:", err)
			break
		}
		h.message <- SMessage{sender: c, m: &msg}
	}
}

func (c *Client) WriteLoop(h *Hub) {
	defer func() {
		c.conn.Close()
		h.unregister <- c
	}()
	for {
		msg := <-c.ch
		err := c.conn.WriteJSON(msg)
		if err != nil {
			log.Println("parse & write:", err)
			break
		}
	}
}
