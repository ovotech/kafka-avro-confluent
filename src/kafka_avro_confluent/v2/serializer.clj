(ns kafka-avro-confluent.v2.serializer
  "Avro serializer for Apache Kafka using Confluent's Schema Registry.
  Use for serializing Kafka keys and values.

  See https://avro.apache.org/
  See http://docs.confluent.io/current/schema-registry/docs
  See https://github.com/damballa/abracad"
  (:require [abracad.avro :as avro]
            [clojure.spec.alpha :as s]
            [kafka-avro-confluent.magic :as magic]
            [kafka-avro-confluent.v2.schema-registry-client :as sr]
            [kafka-avro-confluent.v2.specs :as ks])
  (:import java.io.ByteArrayOutputStream
           java.nio.ByteBuffer
           org.apache.kafka.common.serialization.Serializer))
(require 'kafka-avro-confluent.v2.specs)

(defn- #^"[B" schema-id->bytes [schema-id]
  (-> (ByteBuffer/allocate 4)
      (.putInt schema-id)
      .array))

(defn- ->serialized-bytes [schema-id avro-schema data]
  (with-open [out (ByteArrayOutputStream.)]
    (.write out magic/magic)
    (.write out (schema-id->bytes schema-id))
    (.write out #^"[B" (avro/binary-encoded avro-schema data))
    (.toByteArray out)))

(defn- -serialize*
  [schema-registry key? topic schema data]
  (when data
    (let [avro-schema      (avro/parse-schema schema)
          subject          (format "%s-%s" topic (if key? "key" "value"))
          schema-id        (sr/post-schema schema-registry subject schema)
          serialized-bytes (->serialized-bytes schema-id avro-schema data)]
      serialized-bytes)))

;;,------------
;;| Boilerplate
;;`------------

(gen-class
 :name kafka_avro_confluent.v2.AvroSerializer
 :implements [org.apache.kafka.common.serialization.Serializer]
 :state state
 :init init
 :main false
 :methods [])

(defn- get-field [this key] (@(.state this) key))

(defn -init "Default, no arg constructor." [] [[] (atom nil)])

(s/fdef -configure
        :args (s/cat :this some?
                     :config :kafka.serde/config
                     :key? boolean?))
(defn -configure [this config key?]
  (reset! (.state this)
          {:schema-registry-client (->> config
                                        (s/conform :kafka.serde/config)
                                        sr/->schema-registry-client)
           :key?                  key?}))

(s/fdef -serialize
        :args (s/cat :this some?
                     :topic string?
                     :avro-record ::ks/avro-record))
(defn -serialize [this topic avro-record]
  (-serialize* (get-field this :schema-registry-client)
               (get-field this :key?)
               topic
               (:schema avro-record)
               (:value avro-record)))

;; TODO cleanup memo caches?
(defn -close [this])

(defn ->avro-serializer
  "Avro serializer for Apache Kafka using Confluent's Schema Registry."
  ^kafka_avro_confluent.v2.AvroSerializer
  [config & {:keys [key?]}]
  (doto (kafka_avro_confluent.v2.AvroSerializer.)
    (.configure config (boolean key?))))
