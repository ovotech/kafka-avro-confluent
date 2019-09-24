(ns kafka-avro-confluent.deserializers
  "Avro deserializers that fetch schemas from the Confluent Schema Registry.

  They all implement org.apache.kafka.common.serialization.Deserializer"
  (:require [abracad.avro :as avro]
            [kafka-avro-confluent.magic :as magic]
            [kafka-avro-confluent.schema-registry-client :as registry]
            [clojure.tools.logging :as log])
  (:import java.nio.ByteBuffer
           org.apache.kafka.common.serialization.Deserializer))

(defn #^"[B" byte-buffer->bytes
  [^ByteBuffer buffer]
  (let [array (byte-array (.remaining buffer))]
    (.get buffer array)
    array))

(defn- -deserialize
  [schema-registry data convert-logical-types?]
  (when data
    (let [buffer    (ByteBuffer/wrap data)
          magic     (.get buffer)
          _         (assert (= magic/magic magic) (str "Found different magic byte: " magic))
          schema-id (.getInt buffer)]

      (if-let [schema (registry/get-schema-by-id schema-registry schema-id)]
        (binding [abracad.avro.conversion/*use-logical-types* convert-logical-types?]
          (avro/decode schema (byte-buffer->bytes buffer)))
        (throw (Exception. (format "Schema %s not found in registry." schema-id)))))))

(deftype AvroDeserializer [schema-registry convert-logical-types?]
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data] (-deserialize schema-registry data convert-logical-types?))
  (deserialize [_ _ _headers data] (-deserialize schema-registry data convert-logical-types?))
  (close [_]))

(defn ->avro-deserializer
  "Avro deserializer for Apache Kafka using Confluent's Schema Registry.
  Use for deserializing Kafka keys and values.
   See https://avro.apache.org/
   See http://docs.confluent.io/current/schema-registry/docs
   See https://github.com/damballa/abracad"
  ;; FIXME https://github.com/miner/eastwood#wrong-tag---an-incorrect-type-tag
  ^kafka_avro_confluent.deserializers.AvroDeserializer
  [schema-registry-client-or-config
   & {:keys [convert-logical-types?]
      :or   {convert-logical-types? true}}]
  (AvroDeserializer. (registry/->schema-registry-client schema-registry-client-or-config)
                     convert-logical-types?))
