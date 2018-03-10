(ns kafka-avro-confluent.readme-test
  (:require [clojure.walk :as walk]
            [kafka-avro-confluent.deserializers :as des]
            [kafka-avro-confluent.schema-registry-client :as reg]
            [kafka-avro-confluent.serializers :as ser])
  (:import kafka_avro_confluent.Serde))

;; initialise the Confluent Schema Registry client:
(def schema-registry
  (reg/->schema-registry-client
   {;; NOTE auth optional!
    ;; :username "mr.anderson"
    ;; :password "42"
    :base-url "http://localhost:8081"}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Deserializer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; implements org.apache.kafka.common.serialization.Deserializer

(des/->avro-deserializer schema-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Serializer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; implements org.apache.kafka.common.serialization.Serializer
(def some-value-schema {:type "record" :name "Foo"}) 

;; meant to be used as a value-serializer in a KafkaProducer
(ser/->avro-serializer schema-registry some-value-schema)

;; If you want to use it as a key-serializer:
(def some-key-schema {:type "string"})
(ser/->avro-serializer schema-registry :key some-key-schema)

;; This is also valid:
(ser/->avro-serializer schema-registry :value some-value-schema)

;; Using with a KafkaProducer:
;; e.g. (org.apache.kafka.clients.producer.KafkaProducer. key-serializer
;;                                                        value-serializer)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Serde
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; implementation of org.apache.kafka.common.serialization.Serde

(def serde
  (let [isKey  false
        config (walk/stringify-keys
                {:schema-registry-client {:base-url "http://localhost:8081"}
                 :serializer             {:avro-schema {:type "string"}}})]
    (doto (Serde.) (.configure config isKey))))
