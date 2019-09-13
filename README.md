# kafka-avro-confluent [![CircleCI](https://circleci.com/gh/ovotech/kafka-avro-confluent.svg?style=svg)](https://circleci.com/gh/ovotech/kafka-avro-confluent)

Kafka De/Serializer using avro and Confluent's Schema Registry

## Migrating from 1.1.1-4 -> 1.1.1-5+

1.1.1-5 adds support for logical types. This support is automatic and will be used both in serialisation and deserialisation.
If you need to deserialise to the underlying primitive types you will need to disable logical type conversion
at the point of creating your deserialiser i.e.

```clojure
(des/->avro-deserializer schema-registry :convert-logical-types? false)
```

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/ovotech/kafka-avro-confluent.svg)](https://clojars.org/ovotech/kafka-avro-confluent)
```
[ovotech/kafka-avro-confluent "2.1.0-1"]
```


```clojure
(ns kafka-avro-confluent.readme-test
  (:require [kafka-avro-confluent.v2.deserializer :as des]
            [kafka-avro-confluent.v2.serializer :as ser]))

;; initialise the Confluent Schema Registry client:
(def config
  {;; NOTE auth optional!
   ;; :schema-registry/username "mr.anderson"
   ;; :schema-registry/password "42"
   :schema-registry/base-url "http://localhost:8081"})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Deserializer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; implements org.apache.kafka.common.serialization.Deserializer

(des/->avro-deserializer config)

;; Without using logical types
(binding [abracad.avro.conversion/*use-logical-types* false]
  (des/->avro-deserializer schema-registry))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Serializer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; implements org.apache.kafka.common.serialization.Serializer

;; meant to be used as a value-serializer in a KafkaProducer
(ser/->avro-serializer config schema)

;; If you want to use it as a key-serializer:
(ser/->avro-serializer config :key? true schema)

;; Using with a KafkaProducer:
;; e.g. (org.apache.kafka.clients.producer.KafkaProducer. key-serializer
;;                                                        value-serializer)

;; If the serializer will be used with multiple topics (each with its own schema):
(ser/->avro-serializer config :key? true {:schemas {topic1 schema1
                                                    topic2 schema2}})

```


## Versions

The versions use this format:

```bash
${kafka_version}-${build_number}
```

For example:

```ruby
0.10.0-4 # Kafka v = 0.10.0, kafka-avro-confluent build = 4
1.0.1-1  # Kafka v = 1.0.1 , kafka-avro-confluent build = 1
```

## License

Copyright © 2017 OVO Energy Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
