// 发布者
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/nats-io/go-nats"
)

func usage() {
	log.Fatalf("Usage: nats-pub [-s server (%s)] <主題> <訊息> \n", "127.0.0.1:6016")
}

func main() {
	// 定義連接到server的URL
	var urls = flag.String("s", "127.0.0.1:6016", "The nats server URLs (separated by comma)")

	// 下面判斷參數
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Publisher")}

	// 連接到gnatsd
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer nc.Close()

	// 下面定義subject和msg
	subj, msg := args[0], []byte(args[1])
	// 發布消息
	nc.Publish(subj, msg)
	// 刷新緩衝
	nc.Flush()
	fmt.Printf("Published [%s] : '%s'\n", subj, msg)
}
