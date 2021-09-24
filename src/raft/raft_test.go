package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestRaft_timeout(t *testing.T) {
	ch := make(chan bool, 5)
	//每个1s生成一个信号
	go func() {
		for i := 0; i < 5; i++ {
			ch <- true
			fmt.Printf("send msg...%d\n", i)
			time.Sleep(1 * time.Second)
		}
	}()
	for i := 0; i < 5; i++ {
		select {
		case <-ch:
			fmt.Printf("get msg...\n")
		case <-time.After(10 * time.Millisecond):
			return
		}
	}
	/*for i := 0; i < 5; i++ {
		<-ch
		fmt.Printf("get msg...%d\n", i)
	}*/
}
