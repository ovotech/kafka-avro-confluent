(ns kafka-avro-confluent.v2.serializer
  "Avro serializer for Apache Kafka using Confluent's Schema Registry.
  Use for serializing Kafka keys and values.

  See https://avro.apache.org/
  See http://docs.confluent.io/current/schema-registry/docs"
  (:require [abracad.avro :as avro]
            [clojure.spec.alpha :as s]
            [kafka-avro-confluent.v2.common :as kc]
            [kafka-avro-confluent.v2.specs :as ks]
            [thdr.kfk.avro-bridge.core :as ab])
  (:import io.confluent.kafka.serializers.KafkaAvroSerializer
           org.apache.kafka.common.serialization.Serializer))

(require 'kafka-avro-confluent.v2.specs)

(gen-class
 :name kafka_avro_confluent.v2.AvroSerializer
 :implements [org.apache.kafka.common.serialization.Serializer]
 :state confluent_serializer
 :init init
 :main false
 :methods [])

(defn -init "Default, no arg constructor." []
  [[] (KafkaAvroSerializer.)])

(s/fdef -configure
  :args (s/cat :this some?
               :config :kafka.serde/config
               :key? boolean?))
(defn -configure [this config key?]
  (.configure (.confluent-serializer this)
              (kc/clj-config->confluent-config config)
              (boolean key?)))

(s/fdef -serialize
  :args (s/cat :this some?
               :topic string?
               :avro-record ::ks/avro-record))
(defn -serialize [this topic avro-record]
  (let [avro-record (ab/->java (avro/parse-schema (:schema avro-record))
                               (:value avro-record)
                               {:java-field-fn name})]
    (.serialize (.confluent-serializer this)
                topic
                avro-record)))

(defn -close [this]
  (-> this
      .confluent-serializer
      .close))

(defn ->avro-serializer
  "Avro serializer for Apache Kafka using Confluent's Schema Registry."
  ^kafka_avro_confluent.v2.AvroSerializer
  [config & {:keys [key?]}]
  (doto (kafka_avro_confluent.v2.AvroSerializer.)
    (.configure config (boolean key?))))
