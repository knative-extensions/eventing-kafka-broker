package sacura

import (
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func StartSender(config Config, sentOut chan string) vegeta.Metrics {

	rate := vegeta.Rate{
		Freq: config.Sender.FrequencyPerSecond,
		Per:  time.Second,
	}

	targeter := NewTargeterGenerator(config.Sender.Target, sentOut)

	attacker := vegeta.NewAttacker(
		vegeta.Workers(config.Sender.Workers),
		vegeta.KeepAlive(config.Sender.KeepAlive),
		vegeta.MaxWorkers(config.Sender.Workers),
	)

	var metrics vegeta.Metrics
	for res := range attacker.Attack(targeter, rate, config.ParsedDuration, "Sacura") {
		metrics.Add(res)
	}
	metrics.Close()

	return metrics
}
