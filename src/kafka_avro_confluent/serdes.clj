(ns kafka-avro-confluent.serdes
  (:require [kafka-avro-confluent.deserializers :as kac.des]
            [kafka-avro-confluent.serializers :as kac.ser])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn ->avro-serde
  ([schema-registry-client-or-config schema]
   (->avro-serde schema-registry-client-or-config :value schema))
  ([schema-registry-client-or-config serializer-type schema]
   (Serdes/serdeFrom (kac.ser/->avro-serializer schema-registry-client-or-config serializer-type schema)
                     (kac.des/->avro-deserializer schema-registry-client-or-config))))
