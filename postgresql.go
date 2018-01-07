package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

type dbState int

const (
	dbStateMASTER dbState = iota
	dbStateSLAVE
	dbStateUNKNOWN
)

func (d dbState) String() string {
	switch d {
	case dbStateMASTER:
		return "MASTER"
	case dbStateSLAVE:
		return "SLAVE"
	default:
		return "UNKNOWN"
	}
}

type pgMonitorParams struct {
	connConfig    pgx.ConnConfig
	interval      time.Duration
	wg            *sync.WaitGroup
	statusChannel chan<- dbState
}

type pgConn struct {
	conn   *pgx.Conn
	config pgx.ConnConfig
}

func (pc *pgConn) Close() {
	pc.conn.Close()
}

func (pc *pgConn) TryRecover() bool {
	tmpConn, err := pgx.Connect(pc.config)
	if err != nil {
		return false
	}
	defer pc.conn.Close()
	pc.conn = tmpConn
	return true
}

func (pc *pgConn) IsMasterDb() (bool, error) {
	inRecovery := true
	row := pc.conn.QueryRow("SELECT pg_is_in_recovery()")
	if row == nil {
		// Better to assume we're not master anymore if we lose the connection
		// and can't complete the query
		return true, errors.New("Database connection lost")
	}
	row.Scan(&inRecovery)
	return !inRecovery, nil
}

func pgxConnConfig(connstring string) (*pgx.ConnConfig, error) {
	envConnConfig, errEnvConfig := pgx.ParseEnvLibpq()
	paramConnConfig, errParamConfig := pgx.ParseConnectionString(connstring)
	var finalConnConfig pgx.ConnConfig
	// If we have an error in getting the parameters, we will ignore the
	// failing connection config, otherwise we'll merge them.
	// If getting the config from both will fail, we'll error out.
	switch {
	case errEnvConfig != nil, errParamConfig == nil:
		finalConnConfig = paramConnConfig
	case errEnvConfig == nil, errParamConfig != nil:
		finalConnConfig = envConnConfig
	case errEnvConfig == nil, errParamConfig == nil:
		finalConnConfig = envConnConfig.Merge(paramConnConfig)
	case errEnvConfig != nil, errParamConfig != nil:
		return nil, errors.New("Invalid connection parameters")
	}
	return &finalConnConfig, nil
}

func pgMonitor(ctx context.Context, params *pgMonitorParams) bool {
	conn, err := pgx.Connect(params.connConfig)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("Could not connect to database")
		return false
	}
	recoverableConn := &pgConn{conn: conn, config: params.connConfig}
	go func() {
		defer recoverableConn.Close()
		ticker := time.NewTicker(params.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				params.wg.Done()
				return
			case <-ticker.C:
				isMaster, err := recoverableConn.IsMasterDb()
				if err != nil {
					log.Error("Database is dead")
					recoverableConn.TryRecover()
					params.statusChannel <- dbStateUNKNOWN
					continue
				}
				log.WithFields(log.Fields{
					"master": isMaster,
				}).Debug("Database is alive")
				if isMaster {
					params.statusChannel <- dbStateMASTER
				} else {
					params.statusChannel <- dbStateSLAVE
				}
			}
		}
	}()
	return true
}
