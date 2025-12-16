// Copyright 2020-2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package app implements boilerplate code shared by the producer and consumer.
//
// It implements Main, which both the producer and consumer use within their main functions.
// It also binds all relevant flags.
package app

import (
	"context"
	"github.com/bufbuild/buf-examples/bufstream/iceberg-quickstart/pkg/csr"
	"github.com/bufbuild/buf-examples/bufstream/iceberg-quickstart/pkg/kafka"
	"log/slog"
	"os"
	"os/signal"

	"github.com/spf13/pflag"
)

// Config contains all application configuration needed by the producer and consumer.
type Config struct {
	Kafka kafka.Config
	CSR   csr.Config
}

// Main is used by the producer and consumer within their main functions.
//
// It sets up logging, interrupt handling, and binds and parses all flags. Afterwards, it calls
// do to invoke the application logic.
func Main(do func(context.Context, Config) error) {
	// Set up slog. We use the global logger throughout this demo.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	// Cancel the context on interrupt, i.e. ctrl+c for our purposes.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := run(ctx, do); err != nil {
		slog.Error("program error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, do func(context.Context, Config) error) error {
	config, err := parseConfig()
	if err != nil {
		return err
	}
	return do(ctx, config)
}

func parseConfig() (Config, error) {
	flagSet := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	config := Config{
		Kafka: kafka.Config{
			BootstrapServers: []string{"0.0.0.0:9092"},
			RootCAPath:       "",
			Topic:            "email-updated",
			Group:            "email-verifier",
			ClientID:         "bufstream-demo",
		},
		CSR: csr.Config{
			URL:      "",
			Username: "",
			Password: "",
		},
	}

	flagSet.StringArrayVarP(
		&config.Kafka.BootstrapServers,
		"bootstrap",
		"b",
		config.Kafka.BootstrapServers,
		"The Bufstream bootstrap server addresses.",
	)
	flagSet.StringVarP(
		&config.Kafka.Topic,
		"topic",
		"t",
		config.Kafka.Topic,
		"The Kafka topic name to use.",
	)
	flagSet.StringVarP(
		&config.Kafka.Group,
		"group",
		"g",
		config.Kafka.Group,
		"The Kafka consumer group ID.",
	)
	flagSet.StringVarP(
		&config.CSR.URL,
		"csr-url",
		"c",
		config.CSR.URL,
		"The Confluent Schema Registry URL.",
	)
	flagSet.StringVarP(
		&config.CSR.Username,
		"csr-user",
		"u",
		config.CSR.Username,
		"The Confluent Schema Registry username, if authentication is needed.",
	)
	flagSet.StringVarP(
		&config.CSR.Password,
		"csr-pass",
		"p",
		config.CSR.Password,
		"The Confluent Schema Registry password/token, if authentication is needed.",
	)
	flagSet.StringVarP(
		&config.Kafka.RootCAPath,
		"tls-root-ca-path",
		"",
		config.Kafka.RootCAPath,
		"A path to root CA certificate for kafka TLS.",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return Config{}, err
	}
	return config, nil
}
