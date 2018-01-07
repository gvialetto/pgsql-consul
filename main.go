package main

import (
	"context"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	defaultConnstring     = "user=postgres dbname=postgres"
	defaultConsulEndpoint = "127.0.0.1:8500"
)

type commandFlags struct {
	pgConnString    string
	pgCheckInterval time.Duration
	pgClusterName   string
	consulEndpoint  string
	consulResync    time.Duration
	debug           bool
}

func getCommandLineFlags() *commandFlags {
	cmdFlags := commandFlags{}
	flag.StringVar(
		&cmdFlags.pgConnString,
		"db",
		defaultConnstring,
		"PostgreSQL database connection string",
	)
	flag.DurationVar(
		&cmdFlags.pgCheckInterval,
		"check-interval",
		10*time.Second,
		"Interval between checks to PostgreSQL",
	)
	flag.StringVar(
		&cmdFlags.pgClusterName,
		"cluster",
		"main",
		"Name of the PostgreSQL cluster",
	)
	flag.StringVar(
		&cmdFlags.consulEndpoint,
		"consul",
		defaultConsulEndpoint,
		"Consul API endpoint",
	)
	flag.DurationVar(
		&cmdFlags.consulResync,
		"consul-resync",
		30*time.Second,
		"Frequency at which consul is resynchronized if no updates are detected",
	)
	flag.BoolVar(
		&cmdFlags.debug,
		"debug",
		false,
		"Enable verbose logging",
	)
	flag.Parse()
	return &cmdFlags
}

func main() {
	cmdFlags := getCommandLineFlags()
	if cmdFlags.debug {
		log.SetLevel(log.DebugLevel)
	}
	pgConnConfig, err := pgxConnConfig(cmdFlags.pgConnString)
	if err != nil {
		log.Error("Invalid connection parameters for PostgreSQL connection")
		os.Exit(1)
	}
	ctx := context.Background()
	statusChan := make(chan dbState)
	var wg sync.WaitGroup
	wg.Add(1)
	started := pgMonitor(ctx, &pgMonitorParams{
		connConfig:    *pgConnConfig,
		interval:      cmdFlags.pgCheckInterval,
		wg:            &wg,
		statusChannel: statusChan,
	})
	if !started {
		os.Exit(1)
	}
	wg.Add(1)
	consulRegistrator(ctx, &consulRegistratorParams{
		endpoint:      cmdFlags.consulEndpoint,
		resyncTime:    cmdFlags.consulResync,
		wg:            &wg,
		statusChannel: statusChan,
		clusterName:   cmdFlags.pgClusterName,
	})
	wg.Wait()
}
