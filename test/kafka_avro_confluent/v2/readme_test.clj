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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Serializer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; implements org.apache.kafka.common.serialization.Serializer

;; meant to be used as a value-serializer in a KafkaProducer
(ser/->avro-serializer config)

;; If you want to use it as a key-serializer:
(ser/->avro-serializer config :key? true)

;; Using with a KafkaProducer:
;; e.g. (org.apache.kafka.clients.producer.KafkaProducer. key-serializer
;;                                                        value-serializer)
