(ns kafka-avro-confluent.serdes
  (:require [kafka-avro-confluent.deserializers :as kac.des]
            [kafka-avro-confluent.serializers :as kac.ser])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn ->avro-serde
  ([schema-registry schema]
   (->avro-serde schema-registry :value schema))
  ([schema-registry serializer-type schema]
   (Serdes/serdeFrom (kac.ser/->avro-serializer schema-registry serializer-type schema)
                     (kac.des/->avro-deserializer schema-registry))))
