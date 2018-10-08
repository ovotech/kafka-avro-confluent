(ns kafka-avro-confluent.v2.deserializer
  "Avro deserializer for Apache Kafka using Confluent's Schema Registry.
  Use for deserializing Kafka keys and values.
   See https://avro.apache.org/
   See http://docs.confluent.io/current/schema-registry/docs
   See https://github.com/damballa/abracad"
  (:require [abracad.avro :as avro]
            [clojure.spec.alpha :as s]
            [kafka-avro-confluent.magic :as magic]
            [kafka-avro-confluent.schema-registry-client :as sr]
            [kafka-avro-confluent.v2.specs :as ks])
  (:import java.nio.ByteBuffer
           org.apache.kafka.common.serialization.Deserializer))

(defn #^"[B" byte-buffer->bytes
  [^ByteBuffer buffer]
  (let [array (byte-array (.remaining buffer))]
    (.get buffer array)
    array))

(defn- -deserialize*
  [schema-registry data]
  (when data
    (let [buffer    (ByteBuffer/wrap data)
          magic     (.get buffer)
          _         (assert (= magic/magic magic) (str "Found different magic byte: " magic))
          schema-id (.getInt buffer)
          schema    (sr/get-schema-by-id schema-registry schema-id)]
      (avro/decode schema (byte-buffer->bytes buffer)))))

;;,------------
;;| Boilerplate
;;`------------

(gen-class
 :name kafka_avro_confluent.v2.AvroDeserializer
 :implements [org.apache.kafka.common.serialization.Deserializer]
 :state state
 :init init
 :main false
 :methods [])

(defn- get-field [this key] (@(.state this) key))

(defn -init "Default, no arg constructor." [] [[] (atom nil)])

(s/fdef -configure
        :args (s/cat :this some?
                     :config ::ks/config
                     :isKey boolean?))
(defn -configure [this config isKey]
  (let [{:keys [schema-registry]
         :as   config} (s/conform ::ks/config config)]
    (reset! (.state this)
            {:schema-registry-client (sr/->schema-registry-client schema-registry)})))

(s/fdef -deserialize
        :args (s/cat :this some?
                     :topic string?
                     :data bytes?))
(defn -deserialize [this _topic data]
  (-deserialize* (get-field this :schema-registry-client) data))

;; TODO cleanup memo caches?
(defn -close [this])

(defn ->avro-deserializer
  "Avro deserializer for Apache Kafka using Confluent's Schema Registry."
  ^kafka_avro_confluent.v2.AvroDeserializer
  [config]
  (doto (kafka_avro_confluent.v2.AvroDeserializer.)
    ;; NOTE .. the isKey is actually ignored in the deserializer;
    (.configure config false)))
