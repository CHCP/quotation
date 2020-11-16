/*
Function  : kafka_consumer.go
Author	  : Gordon Wang
Created At: 2020.11.14
*/

package main

import (
	"os"
	"strings"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/labstack/gommon/log"
)

func main1() {
	//加载echo
	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	//设置日志级别
	e.Use(middleware.Logger())
	e.Logger.SetPrefix("HPQ")
	e.Logger.SetHeader("[Echo] [${level}] ${time_rfc3339_nano} [${prefix}] [${short_file}(line:${line})]:")
	if debug := os.Getenv("TM_LOG_LEVEL"); strings.ToLower(debug) == "info" {
		e.Logger.SetLevel(log.INFO)
	} else if strings.ToLower(debug) == "warn" {
		e.Logger.SetLevel(log.WARN)
	} else if strings.ToLower(debug) == "error" {
		e.Logger.SetLevel(log.ERROR)
	} else {
		e.Logger.SetLevel(log.DEBUG)
	}

	// Connect Kafka

	e.Logger.Infof("HPQ consumer is startup.")
	e.Logger.Fatal(e.Start(":6789"))
}
