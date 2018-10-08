(ns kafka-avro-confluent.v2.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.walk :as w]))

(s/def ::non-blank-string (s/and string? (complement string/blank?)))

(s/def :schema-registry/base-url ::non-blank-string)
(s/def :schema-registry/username ::non-blank-string)
(s/def :schema-registry/password ::non-blank-string)

(s/def :serde/config
  (s/and (s/conformer #(try
                         (->> %
                              (into {})
                              w/keywordize-keys)
                         (catch Exception ex
                           :clojure.spec.alpha/invalid)))
         (s/keys :req [:schema-registry/base-url]
                 :opt [:schema-registry/username
                       :schema-registry/password])))

(s/def :avro-record/schema any?)
(s/def :avro-record/value any?)
(s/def ::avro-record
  (s/keys :req-un [:avro-record/schema :avro-record/value]))
