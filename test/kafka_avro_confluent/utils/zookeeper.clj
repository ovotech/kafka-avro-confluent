(ns kafka-avro-confluent.utils.zookeeper
  (:require [integrant.core :as ig])
  (:import org.apache.curator.test.TestingServer))

(defmethod ig/init-key ::zookeeper [_ _] (TestingServer. 2181))

(defmethod ig/halt-key! ::zookeeper [_ zk] (.stop zk))
