(ns kafka-avro-confluent.serializers
  (:require [abracad.avro :as avro]
            [kafka-avro-confluent.magic :as magic]
            [kafka-avro-confluent.schema-registry-client :as registry])
  (:import java.io.ByteArrayOutputStream
           java.nio.ByteBuffer
           org.apache.kafka.common.serialization.Serializer))

(def ^:private post-schema-memo (memoize registry/post-schema))

(defn- byte-buffer->bytes
  [buffer]
  (let [array (byte-array (.remaining buffer))]
    (.get buffer array)
    array))

(defn- schema-id->bytes [schema-id]
  (-> (ByteBuffer/allocate 4)
      (.putInt schema-id)
      .array))

(defn- ->serialized-bytes [schema-id avro-schema data]
  (with-open [out (ByteArrayOutputStream.)]
    (.write out magic/magic)
    (.write out (schema-id->bytes schema-id))
    (.write out (avro/binary-encoded avro-schema data))
    (.toByteArray out)))

(defn- -serialize
  [schema-registry topic schema data]
  (when data
    (let [avro-schema      (avro/parse-schema schema)
          schema-id        (post-schema-memo schema-registry topic schema)
          serialized-bytes (->serialized-bytes schema-id avro-schema data)]
      serialized-bytes)))

(deftype AvroSerializer [schema-registry schema]
  Serializer
  (configure [_ _ _])
  (serialize [_ topic data] (-serialize schema-registry topic schema data))
  (close [_]))

(defn ->avro-serializer
  "Avro serializer for Apache Kafka using Confluent's Schema Registry.
  Use for serializing Kafka keys values.
  Values will be serialized according to the provided schema.
   See https://avro.apache.org/
   See http://docs.confluent.io/current/schema-registry/docs
   See https://github.com/damballa/abracad"
  ;; FIXME https://github.com/miner/eastwood#wrong-tag---an-incorrect-type-tag
  ^AvroSerializer
  [schema-registry schema]
  (AvroSerializer. schema-registry schema))

