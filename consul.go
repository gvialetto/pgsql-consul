package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
)

type consulRegistratorParams struct {
	endpoint      string
	wg            *sync.WaitGroup
	statusChannel <-chan dbState
	resyncTime    time.Duration
	clusterName   string
}

func makeConsulRegistrationData(
	client *consul.Client,
	cluster string,
) (*consul.AgentServiceRegistration, error) {
	// first try to get the name from the agent
	host, err := client.Agent().NodeName()
	if err != nil {
		// if that fails, fall back to the host
		host, _ = os.Hostname()
	}
	if host == "" {
		return nil, errors.New("Could not determine hostname")
	}

	id := fmt.Sprintf("%s-%s-%s", host, "postgres", cluster)
	return &consul.AgentServiceRegistration{
		ID:   id,
		Name: fmt.Sprintf("%s-%s", "postgres", cluster),
		Port: 5432,
		Check: &consul.AgentServiceCheck{
			TCP:                            "127.0.0.1:5432",
			Interval:                       "30s",
			DeregisterCriticalServiceAfter: "90m",
		},
	}, nil
}

func doRegister(
	client *consul.Client,
	state dbState,
	data *consul.AgentServiceRegistration,
) error {
	switch state {
	case dbStateMASTER:
		data.Tags = []string{"master"}
	case dbStateSLAVE:
		data.Tags = []string{"slave"}
	case dbStateUNKNOWN:
		data.Tags = []string{}
	}
	err := client.Agent().ServiceRegister(data)
	if err != nil {
		return err
	}
	switch state {
	case dbStateUNKNOWN:
		client.Agent().EnableServiceMaintenance(data.ID, "Lost database connection")
	default:
		client.Agent().DisableServiceMaintenance(data.ID)
	}
	return nil
}

func consulRegistrator(
	ctx context.Context,
	params *consulRegistratorParams,
) bool {
	consulCfg := consul.DefaultConfig()
	consulCfg.Address = params.endpoint
	client, err := consul.NewClient(consulCfg)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("Failed to initialize consul client")
		return false
	}
	registrationData, err := makeConsulRegistrationData(
		client, params.clusterName,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("Failed to initialize consul client")
	}
	go func() {
		// start with the database being a slave/secondary node
		currentState := dbStateUNKNOWN
		timerRenewal := time.NewTimer(params.resyncTime)
		for {
			select {
			case <-ctx.Done():
				params.wg.Done()
				return
			case <-timerRenewal.C:
				log.Debugf("No status changes. Renewing service")
				err := doRegister(client, currentState, registrationData)
				if err != nil {
					log.Error(err)
				}
				timerRenewal.Reset(params.resyncTime)
			case state := <-params.statusChannel:
				if state == currentState {
					continue
				}
				// Stop the timer, since we are renewing already
				if !timerRenewal.Stop() {
					<-timerRenewal.C
				}
				log.WithFields(log.Fields{
					"new_state": state,
					"old_state": currentState,
				}).Info("Database status change detected.")
				err := doRegister(client, state, registrationData)
				if err != nil {
					log.Error(err)
				}
				currentState = state
				timerRenewal.Reset(params.resyncTime)
			}
		}
	}()
	return true
}
