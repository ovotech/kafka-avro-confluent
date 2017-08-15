(ns kafka-avro-confluent.utils.schema-registry
  (:require [integrant.core :as ig])
  (:import [io.confluent.kafka.schemaregistry.rest SchemaRegistryConfig SchemaRegistryRestApplication]))

(defn ->schema-registry [properties-file]
  (let [config (SchemaRegistryConfig. properties-file)
        app    (SchemaRegistryRestApplication. config)
        server (.createServer app)]
    (.start server)
    server))

(defmethod ig/init-key ::schema-registry [_ {:keys [properties-file]}]
  (->schema-registry properties-file))

(defmethod ig/halt-key! ::schema-registry [_ server]
  (.stop server))
