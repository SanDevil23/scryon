package main

import (
	"fmt"
	"github.com/sandevil23/scryon/pkg/config"
	"github.com/sandevil23/scryon/pkg/logger"
)

func main() {
	cfg, _ := config.LoadBase("smoke-test")
	log := logger.New(cfg.LogLevel)
	log.Info("packages compile correctly", "service", cfg.ServiceName)
	fmt.Println("✅ smoke test passed")
}