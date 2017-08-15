(ns kafka-avro-confluent.utils.core
  (:require [integrant.core :as ig]
            [integrant.repl :as igr]))

(def ig-config
  {:kafka-avro-confluent.utils.schema-registry/schema-registry {:properties-file "test/resources/schema-registry.properties"
                                                                :_kafka          (ig/ref :kafka-avro-confluent.utils.kafka/kafka)
                                                                :_zookeeper      (ig/ref :kafka-avro-confluent.utils.zookeeper/zookeeper)}
   :kafka-avro-confluent.utils.kafka/kafka                     {:_zookeeper (ig/ref :kafka-avro-confluent.utils.zookeeper/zookeeper)}
   :kafka-avro-confluent.utils.zookeeper/zookeeper             {}})

(igr/set-prep! (constantly ig-config))

(defn with-embedded-zookareg [f]
  (try
    (ig/load-namespaces ig-config)
    (igr/go)
    (f)
    (finally
      (igr/halt))))

(comment
  ;;;;
  (ig/load-namespaces ig-config)

  (igr/go)

  integrant.repl.state/config

  (igr/halt)
  ;;;;
)
