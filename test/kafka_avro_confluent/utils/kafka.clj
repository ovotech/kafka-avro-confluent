(ns kafka-avro-confluent.utils.kafka
  (:require [franzy.embedded.broker :as broker]
            [franzy.embedded.protocols :as protos]
            [integrant.core :as ig]))

(defn ->broker []
  (let [b (broker/make-startable-broker)]
    (protos/startup b)
    b))

(defmethod ig/init-key ::kafka [_ _]
  (->broker))

(defmethod ig/halt-key! ::kafka [_ b]
  (protos/attempt-shutdown b))
