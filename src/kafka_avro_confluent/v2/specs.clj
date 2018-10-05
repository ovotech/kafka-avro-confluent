(ns kafka-avro-confluent.v2.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.walk :as w]
            [kafka-avro-confluent.schema-registry-client :as sr]))

(s/def ::schema-registry ::sr/config)
(s/def ::config
  (s/and (s/conformer #(try
                         (->> %
                              (into {})
                              w/keywordize-keys)
                         (catch Exception ex
                           :clojure.spec.alpha/invalid)))
         (s/keys :req-un [::schema-registry])))

(s/def ::schema any?)
(s/def ::value any?)
(s/def ::avro-record
  (s/keys :req-un [::schema ::value]))
