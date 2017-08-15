(ns kafka-avro-confluent.deserializers
  (:require [abracad.avro :as avro]
            [abracad.avro.edn :as aedn]
            [kafka-avro-confluent.schema-registry-client :as registry])
  (:import java.nio.ByteBuffer
           org.apache.kafka.common.serialization.Deserializer))

(defn- byte-buffer->bytes
  [buffer]
  (let [array (byte-array (.remaining buffer))]
    (.get buffer array)
    array))

(defn- -deserialize
  [schema-registry data]
  (when data
    (let [buffer    (ByteBuffer/wrap data)
          _magic    (.get buffer)
          schema-id (.getInt buffer)
          ;; FIXME this will hammer registry if used in prd settings
          schema    (registry/get-schema-by-id schema-registry schema-id)]

      (avro/decode schema (byte-array (byte-buffer->bytes buffer))))))

(deftype AvroDeserializer [schema-registry]
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data] (-deserialize schema-registry data))
  (close [_]))

(defn ->avro-deserializer
  "Avro deserializer for Apache Kafka using Confluent's Schema Registry.
  Use for deserializing Kafka keys and values.
   See https://avro.apache.org/
   See http://docs.confluent.io/current/schema-registry/docs
   See https://github.com/damballa/abracad"
  (^AvroDeserializer [schema-registry] (AvroDeserializer. schema-registry)))
