package main

import (
	"fmt"
	"github.com/douban/goyaml"
	"io/ioutil"
	"testing"
    "os"
)

func Test_GetEyeConfig(t *testing.T) {
	eye := &Eye{
		Servers:   []string{"localhost:7900 0 1 2", "127.0.0.1:7900 A -1 B"},
		Port:      7905,
		WebPort:   7908,
		Threads:   8,
		N:         3,
		W:         2,
		R:         1,
        Buckets:   16,
        Listen:    "0.0.0.0",
        Slow:      200,
		Proxies:   []string{"localhost:7905"},
		AccessLog: "/log/beansproxy/beansproxy.log",
		ErrorLog:  "/log/beansproxy/beansproxy_error.log",
		Basepath:  "/var/lib/beanseye",
        Readonly:  false,
	}

    content1, _ := goyaml.Marshal(eye)
    ioutil.WriteFile("./example.yaml", content1, os.ModePerm)

	var new_eye Eye
	content, _ := ioutil.ReadFile("./example.yaml")

	goyaml.Unmarshal(content, &new_eye)
	fmt.Println("eye")
	fmt.Println(*eye)
	fmt.Println("new_eye")
	fmt.Println(new_eye)

}
