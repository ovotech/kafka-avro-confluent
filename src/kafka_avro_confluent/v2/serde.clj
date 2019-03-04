(ns kafka-avro-confluent.v2.serde
  (:require [kafka-avro-confluent.v2.deserializer :as kac.des]
            [kafka-avro-confluent.v2.serializer :as kac.ser])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn ->avro-serde
  [config & {:keys [key?]
             :or   {key? false}}]
  (Serdes/serdeFrom (kac.ser/->avro-serializer config :key? key?)
                    (kac.des/->avro-deserializer config)))
