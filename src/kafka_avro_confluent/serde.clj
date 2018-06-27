(ns kafka-avro-confluent.serde
  "Avro Serde that:
  - posts schemas to the Schema Registry when serializing;
  - fetches schemas from the Schema Registry when deserializing.

  Implements org.apache.kafka.common.serialization.Serde"
  (:require [abracad.avro :as avro]
            [kafka-avro-confluent.schema-registry-client :as sr]
            [kafka-avro-confluent.deserializers :as des]
            [kafka-avro-confluent.serializers :as ser]
            [clojure.spec.alpha :as s]
            [clojure.walk :refer [keywordize-keys]]))

(gen-class
 :name kafka_avro_confluent.Serde
 :implements [org.apache.kafka.common.serialization.Serde]
 :state state
 :init init
 :main false
 :methods [[schemaRegistryClient []
            kafka_avro_confluent.schema_registry_client.SchemaRegistry]
           [getLocation [] String]])

(defn -init "Default, no arg constructor." [] [[] (atom nil)])

(s/def ::schema-registry-client ::sr/config)
(s/def ::avro-schema some?)
(s/def ::serializer (s/keys :req-un [::avro-schema]))

(s/def ::config
  (s/keys :req-un [::schema-registry-client ::serializer]))

(defn -configure [this m isKey]
  (let [{:keys [serializer schema-registry-client] :as config} (keywordize-keys (into {} m))
        _               (s/assert ::config config)
        serializer-type (if isKey :key :value)
        avro-schema     (:avro-schema serializer)
        sr              (sr/->schema-registry-client schema-registry-client)
        s               (ser/->avro-serializer sr serializer-type avro-schema)
        d               (des/->avro-deserializer sr)]
    (reset! (.state this)
            {:schema-registry-client sr
             :serializer             s
             :deserializer           d})))

(defn- get-field [this key] (@(.state this) key))
(defn -serializer [this] (get-field this :serializer))
(defn -deserializer [this] (get-field this :deserializer))
(defn -schemaRegistryClient [this] (get-field this :schema-registry-client))

;; TODO cleanup memo caches?
(defn -close [this])
