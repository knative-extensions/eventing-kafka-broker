package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
	_ "go.uber.org/automaxprocs"

	"github.com/pierdipi/sacura"
)

const (
	filePathFlag = "config"
)

func main() {

	path := flag.String(filePathFlag, "", "Path to the configuration file")
	flag.Parse()

	if path == nil || *path == "" {
		log.Printf("invalid flag %s", filePathFlag)
		usage()
		return
	}

	if err := run(*path); err != nil {
		log.Fatal(err)
	}
}

func usage() {
	log.Printf(`
sacura --%s <absolute_path_to_config_file>
`, filePathFlag)
}

func run(path string) error {

	log.Println("Reading configuration ...")

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", path, err)
	}

	config, err := sacura.FileConfig(f)
	if err != nil {
		return fmt.Errorf("failef to read config from file %s: %w", path, err)
	}

	c, _ := json.Marshal(&config)
	log.Println("config", string(c))

	ctx, cancel := context.WithCancel(NewContext())

	log.Println("Creating channels")
	buffer := int(math.Min(float64(int(config.ParsedDuration)*config.Sender.FrequencyPerSecond), math.MaxInt8))

	sent := make(chan string, buffer)
	received := make(chan string, buffer)

	go func() {
		defer cancel()
		defer close(sent)

		log.Println("Starting attacker ...")

		time.Sleep(time.Second * 10) // Waiting for receiver to start

		metrics := sacura.StartSender(config, sent)
		logMetrics(metrics)
	}()

	log.Println("Creating state manager ...")
	sm := sacura.NewStateManager()
	receivedSignal := sm.ReadReceived(received)
	sentSignal := sm.ReadSent(sent)

	log.Println("Starting receiver ...")
	if err := sacura.StartReceiver(ctx, config.Receiver, received); err != nil {
		return fmt.Errorf("failed to start receiver: %w", err)
	}

	log.Println("Waiting for attacker to finish ...")
	<-ctx.Done()

	log.Println("Attacker finished sending events - waiting for events")
	<-time.After(config.ParsedTimeout)

	log.Println("Waiting for received channel signal")
	<-receivedSignal

	log.Println("Waiting for sent channel signal")
	<-sentSignal

	if diff := sm.Diff(); diff != "" {
		return fmt.Errorf("set state is not correct: %s", diff)
	}

	return nil
}

func logMetrics(metrics vegeta.Metrics) {
	jsonMetrics, err := json.MarshalIndent(metrics, " ", " ")
	if err != nil {
		log.Println("failed to marshal metrics", err)
		return
	}

	log.Println("metrics", string(jsonMetrics))
}

var onlyOneSignalHandler = make(chan struct{})

// SetupSignalHandler registered for SIGTERM and SIGINT. A stop channel is returned
// which is closed on one of these signals. If a second signal is caught, the program
// is terminated with exit code 1.
func SetupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

// NewContext creates a new context with SetupSignalHandler()
// as our Done() channel.
func NewContext() context.Context {
	return &signalContext{stopCh: SetupSignalHandler()}
}

type signalContext struct {
	stopCh <-chan struct{}
}

func (scc *signalContext) Deadline() (deadline time.Time, ok bool) {
	return
}

func (scc *signalContext) Done() <-chan struct{} {
	return scc.stopCh
}

func (scc *signalContext) Err() error {
	select {
	case _, ok := <-scc.Done():
		if !ok {
			return errors.New("received a termination signal")
		}
	default:
	}
	return nil
}

func (scc *signalContext) Value(_ interface{}) interface{} {
	return nil
}
