(ns kafka-avro-confluent.serializers
  (:require [abracad.avro :as avro]
            [cheshire.core :as json]
            [kafka-avro-confluent.magic :as magic]
            [kafka-avro-confluent.schema-registry-client :as registry])
  (:import java.io.ByteArrayOutputStream
           java.nio.ByteBuffer
           kafka_avro_confluent.serializers.AvroSerializer
           org.apache.kafka.common.serialization.Serializer))

;; TODO make client memoized ??
(def ^:private post-schema-memo (memoize registry/post-schema))
(def ^:private get-subject-memo (memoize registry/get-subject-by-topic))

;; NOTE @bill can you explain to me what's going on here?
(defn- get-avro-schema
  [schema-registry topic edn-avro-schema]
  (if edn-avro-schema
    (avro/parse-schema edn-avro-schema)
    (-> (get-subject-memo schema-registry topic)
        json/parse-string
        (get "schema")
        json/parse-string
        avro/parse-schema)))
(defn- resolve-schema-id
  [schema-registry topic schema]
  (if schema
    (post-schema-memo schema-registry topic schema)
    (-> (get-subject-memo topic)
        json/parse-string
        (get "id")
        int)))


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
    (let [avro-schema      (get-avro-schema schema-registry topic schema)
          schema-id        (resolve-schema-id schema-registry topic schema)
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
  (^kafka_avro_confluent.serializers.AvroSerializer [schema-registry schema]
   (AvroSerializer. schema-registry schema)))

