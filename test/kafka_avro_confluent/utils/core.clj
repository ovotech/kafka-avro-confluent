(ns kafka-avro-confluent.utils.core
  (:require [integrant.core :as ig]))

(def ig-config
  {:kafka-avro-confluent.utils.schema-registry/schema-registry {:properties-file "test/resources/schema-registry.properties"
                                                                :_kafka          (ig/ref :kafka-avro-confluent.utils.kafka/kafka)
                                                                :_zookeeper      (ig/ref :kafka-avro-confluent.utils.zookeeper/zookeeper)}
   :kafka-avro-confluent.utils.kafka/kafka                     {:_zookeeper (ig/ref :kafka-avro-confluent.utils.zookeeper/zookeeper)}
   :kafka-avro-confluent.utils.zookeeper/zookeeper             {}})


(defn with-embedded-zookareg [f]
  (let [system (atom nil)]
    (try
      (ig/load-namespaces ig-config)
      (reset! system (ig/init ig-config))
      (f)
      (finally
        (swap! system ig/halt!)))))

(comment
  ;;;;
  (require '[integrant.repl :as igr])

  (igr/set-prep! (constantly ig-config))

  (ig/load-namespaces ig-config)

  (igr/go)

  integrant.repl.state/config

  (igr/halt)
  ;;;;
)
