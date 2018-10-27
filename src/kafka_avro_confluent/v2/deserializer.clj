(ns kafka-avro-confluent.v2.deserializer
  "Avro deserializer for Apache Kafka using Confluent's Schema Registry.
  Use for deserializing Kafka keys and values.
   See https://avro.apache.org/
   See http://docs.confluent.io/current/schema-registry/docs"
  (:require [clojure.spec.alpha :as s]
            [kafka-avro-confluent.v2.common :as kc]
            [thdr.kfk.avro-bridge.core :as ab])
  (:import io.confluent.kafka.serializers.KafkaAvroDeserializer
           org.apache.kafka.common.serialization.Deserializer))

(require 'kafka-avro-confluent.v2.specs)

(gen-class
 :name kafka_avro_confluent.v2.AvroDeserializer
 :implements [org.apache.kafka.common.serialization.Deserializer]
 :state confluent_deserializer
 :init init
 :main false
 :methods [])

(defn -init "Default, no arg constructor." []
  [[] (KafkaAvroDeserializer.)])

(s/fdef -configure
  :args (s/cat :this some?
               :config :kafka.serde/config
               :key? boolean?))
(defn -configure [this config key?]
  (.configure (.confluent-deserializer this)
              (kc/clj-config->confluent-config config)
              key?))

(def ^:private clj-field-fn (comp keyword str))
(s/fdef -deserialize
  :args (s/cat :this some?
               :topic string?
               :data bytes?))
(defn -deserialize [this topic data]
  (let [avro-record (.deserialize (.confluent-deserializer this)
                                  topic
                                  data)]
    (ab/->clj avro-record {:clj-field-fn clj-field-fn})))

(defn -close [this]
  (-> this
      .confluent-deserializer
      .close))

(defn ->avro-deserializer
  "Avro deserializer for Apache Kafka using Confluent's Schema Registry."
  ^kafka_avro_confluent.v2.AvroDeserializer
  [config]
  (doto (kafka_avro_confluent.v2.AvroDeserializer.)
    (.configure config false)))
