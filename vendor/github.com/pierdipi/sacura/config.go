package sacura

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/go-yaml/yaml"
	vegeta "github.com/tsenart/vegeta/v12/lib"
)

type Config struct {
	Sender SenderConfig `json:"sender" yaml:"sender"`

	Receiver ReceiverConfig `json:"receiver" yaml:"receiver"`

	Duration string `json:"duration" yaml:"duration"`
	Timeout  string `json:"timeout" yaml:"timeout"`

	ParsedDuration time.Duration
	ParsedTimeout  time.Duration
}

type SenderConfig struct {
	Target             string `json:"target" yaml:"target"`
	FrequencyPerSecond int    `json:"frequency" yaml:"frequency"`
	Workers            uint64 `json:"workers" yaml:"workers"`
	KeepAlive          bool   `json:"keepAlive" yaml:"keepAlive"`
}

type ReceiverConfig struct {
	Port    int    `json:"port" yaml:"port"`
	Timeout string `json:"timeout"`

	ParsedTimeout time.Duration
}

func FileConfig(r io.Reader) (Config, error) {

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read: %w", err)
	}

	config := &Config{}
	if err := yaml.Unmarshal(b, config); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal file content %s: %w", string(b), err)
	}

	return *config, config.validate()
}

func (c *Config) validate() error {
	var err error

	c.ParsedDuration, err = time.ParseDuration(c.Duration)
	if err != nil {
		return invalidErr("duration", err)
	}

	c.ParsedTimeout, err = time.ParseDuration(c.Timeout)
	if err != nil {
		return invalidErr("timeout", err)
	}

	if c.Sender.FrequencyPerSecond <= 0 {
		return invalidErr("sender.frequency", errors.New("frequency cannot be less or equal to 0"))
	}

	if c.Sender.Target == "" {
		return invalidErr("sender.target", errors.New("target cannot be empty"))
	}

	if u, err := url.Parse(c.Sender.Target); err != nil {
		return invalidErr("sender.target", err)
	} else if !u.IsAbs() {
		return invalidErr("sender.target", errors.New("target must be an absolute URL"))
	}

	if c.Sender.Workers == 0 {
		c.Sender.Workers = vegeta.DefaultWorkers
	}

	c.Receiver.ParsedTimeout, err = time.ParseDuration(c.Receiver.Timeout)
	if err != nil {
		return invalidErr("receiver.timeout", err)
	}

	return err
}

func invalidErr(field string, err error) error {
	return fmt.Errorf("invalid %s: %w", field, err)
}
