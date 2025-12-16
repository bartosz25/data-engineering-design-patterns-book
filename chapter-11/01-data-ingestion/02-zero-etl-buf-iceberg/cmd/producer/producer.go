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

package main

import (
	"context"
	"errors"
	"github.com/brianvoe/gofakeit/v7"
	demov1 "github.com/bufbuild/buf-examples/bufstream/iceberg-quickstart/gen/bufstream/demo/v1"

	"github.com/bufbuild/buf-examples/bufstream/iceberg-quickstart/pkg/app"
	"github.com/bufbuild/buf-examples/bufstream/iceberg-quickstart/pkg/csr"
	"github.com/bufbuild/buf-examples/bufstream/iceberg-quickstart/pkg/kafka"
	"github.com/bufbuild/buf-examples/bufstream/iceberg-quickstart/pkg/produce"
	"github.com/google/uuid"
	"log/slog"
)

func main() {
	// See the app package for the boilerplate we use to set up the producer and
	// consumer, including bound flags.
	app.Main(run)
}

func run(ctx context.Context, config app.Config) error {
	client, err := kafka.NewKafkaClient(config.Kafka, false)
	if err != nil {
		return err
	}
	defer client.Close()

	// NewSerializer creates a CSR-based Serializer if there is a CSR URL,
	// otherwise it creates a single-type Serializer for demov1.EmailUpdated.
	serializer, err := csr.NewSerializer[*demov1.EmailUpdated](config.CSR)
	if err != nil {
		return err
	}
	defer func() { _ = serializer.Close() }()

	producer := produce.NewProducer[*demov1.EmailUpdated](
		client,
		serializer,
		config.Kafka.Topic,
	)

	const msgs = 100
	slog.Info("Creating messages", "max", msgs)
	for i := range msgs {
		id := newID()
		msg := newSemanticallyValidEmailUpdated(id)
		// Produces semantically-valid EmailUpdated message, where both email
		// fields are valid email addresses.
		if err := producer.ProduceProtobufMessage(ctx, id, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			slog.Error("error on produce or semantically valid protobuf message", "error", err)
		} else {
			slog.Info("Published message", "number", i+1, "of", msgs, "new email", msg.GetNewEmailAddress())
		}
	}

	return nil
}

// newID returns a new UUID.
//
// This is also used as the record key.
func newID() string {
	return uuid.New().String()
}

func newSemanticallyValidEmailUpdated(id string) *demov1.EmailUpdated {
	return &demov1.EmailUpdated{
		Id:              id,
		OldEmailAddress: gofakeit.Email(),
		NewEmailAddress: gofakeit.Email(),
	}
}
