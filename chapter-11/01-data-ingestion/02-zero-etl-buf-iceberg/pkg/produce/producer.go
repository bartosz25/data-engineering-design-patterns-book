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

// Package produce implements a toy producer.
package produce

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer is an example producer to a given topic using given Protobuf message type.
//
// A Producer takes a Kafka client and a topic, and sends one of two types of data:
//
//   - A Protobuf message of the given type.
//   - Invalid data that could not be parsed as any Protobuf message.
//
// This is a toy example, but shows the basics you need to send Protobuf messages
// to Kafka using franz-go.
type Producer[M proto.Message] struct {
	client     *kgo.Client
	serializer serde.Serializer
	topic      string
}

// NewProducer returns a new Producer.
//
// Always use this constructor to construct Producers.
func NewProducer[M proto.Message](
	client *kgo.Client,
	serializer serde.Serializer,
	topic string,
) *Producer[M] {
	return &Producer[M]{
		client:     client,
		topic:      topic,
		serializer: serializer,
	}
}

// ProduceProtobufMessage serializes the given Protobuf messages, and synchronously
// sends it to the Producer's topic with the given key.
func (p *Producer[M]) ProduceProtobufMessage(ctx context.Context, key string, message M) error {
	payload, err := p.serializer.Serialize(p.topic, message)
	if err != nil {
		return err
	}
	return p.produce(ctx, key, payload)
}

// ProduceInvalid synchronously sends data to the Producer's topic that could
// never be intererpreted as a Protobuf message.
func (p *Producer[M]) ProduceInvalid(ctx context.Context, key string) error {
	return p.produce(ctx, key, []byte("\x00foobar"))
}

func (p *Producer[M]) produce(ctx context.Context, key string, payload []byte) error {
	produceResults := p.client.ProduceSync(
		ctx,
		&kgo.Record{
			Key:   []byte(key),
			Value: payload,
			Topic: p.topic,
		},
	)
	if err := produceResults.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce: %w", err)
	}
	return nil
}
