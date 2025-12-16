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

// Package csr implements helper functionality around the Confluent Schema Registry.
package csr

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Config is all the configuration needed to connect to a CSR Instance.
//
// Note that the schemaregistry package has its own NewConfig.* functions, which we call.
// However, we're bringing this down to exactly what we need for this demo.
type Config struct {
	// The URL of the CSR instance.
	//
	// The absence of this field says to not connect to the CSR.
	URL string
	// The username to use for authentication, if any.
	Username string
	// The password to use for authentication, if any.
	Password string
}

// NewSerializer creates a new Serializer for the given Config.
//
// This creates a CSR-based Serializer if there is a CSR URL,
// otherwise it creates a single-type Serializer for type M.
func NewSerializer[M proto.Message](config Config) (serde.Serializer, error) {
	if config.URL != "" {
		csrClient, err := newCSRClient(config)
		if err != nil {
			return nil, err
		}
		return newCSRProtobufSerializer(csrClient)
	}
	return newSingleTypeProtobufSerializer[M](), nil
}

// NewDeserializer creates a new Deserializer for the given Config.
//
// This creates a CSR-based Deserializer if there is a CSR URL,
// otherwise it creates a single-type Deserializer for type M.
func NewDeserializer[M proto.Message](config Config) (serde.Deserializer, error) {
	if config.URL != "" {
		csrClient, err := newCSRClient(config)
		if err != nil {
			return nil, err
		}
		return newCSRProtobufDeserializer(csrClient)
	}
	return newSingleTypeProtobufDeserializer[M](), nil
}

func newCSRClient(config Config) (schemaregistry.Client, error) {
	return schemaregistry.NewClient(newCSRConfig(config))
}

func newSingleTypeProtobufSerializer[M proto.Message]() serde.Serializer {
	return singleTypeProtobufSerializer[M]{}
}

func newSingleTypeProtobufDeserializer[M proto.Message]() serde.Deserializer {
	return singleTypeProtobufDeserializer[M]{}
}

func newCSRProtobufSerializer(csrClient schemaregistry.Client) (serde.Serializer, error) {
	return protobuf.NewSerializer(csrClient, serde.ValueSerde, protobuf.NewSerializerConfig())
}

func newCSRProtobufDeserializer(csrClient schemaregistry.Client) (serde.Deserializer, error) {
	deserializer, err := protobuf.NewDeserializer(csrClient, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	deserializer.ProtoRegistry = protoregistry.GlobalTypes
	return deserializer, nil
}

func newCSRConfig(config Config) *schemaregistry.Config {
	if config.Username != "" && config.Password != "" {
		return schemaregistry.NewConfigWithBasicAuthentication(
			config.URL,
			config.Username,
			config.Password,
		)
	}
	return schemaregistry.NewConfig(config.URL)
}

type singleTypeProtobufSerializer[M proto.Message] struct{}

func (singleTypeProtobufSerializer[M]) ConfigureSerializer(
	schemaregistry.Client,
	serde.Type,
	*serde.SerializerConfig,
) error {
	return nil
}

func (singleTypeProtobufSerializer[M]) Serialize(_ string, value interface{}) ([]byte, error) {
	message, ok := value.(M)
	if !ok {
		return nil, fmt.Errorf("unknown message type: %T", value)
	}
	return proto.Marshal(message)
}

func (singleTypeProtobufSerializer[M]) Close() error {
	return nil
}

type singleTypeProtobufDeserializer[M proto.Message] struct{}

func (singleTypeProtobufDeserializer[M]) ConfigureDeserializer(
	schemaregistry.Client,
	serde.Type,
	*serde.DeserializerConfig,
) error {
	return nil
}

func (singleTypeProtobufDeserializer[M]) Deserialize(_ string, payload []byte) (interface{}, error) {
	var message M
	var ok bool
	message, ok = message.ProtoReflect().Type().New().Interface().(M)
	if !ok {
		return nil, fmt.Errorf("did not get message type %T from ProtoReflect", message)
	}
	if err := proto.Unmarshal(payload, message); err != nil {
		return nil, err
	}
	return message, nil
}

func (singleTypeProtobufDeserializer[M]) DeserializeInto(_ string, payload []byte, value interface{}) error {
	message, ok := value.(M)
	if !ok {
		return fmt.Errorf("unknown message type: %T", value)
	}
	return proto.Unmarshal(payload, message)
}

func (singleTypeProtobufDeserializer[M]) Close() error {
	return nil
}
