(ns kafka-avro-confluent.v2.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.walk :as w]))

(s/def ::non-blank-string (s/and string? (complement string/blank?)))

(s/def ::base-url ::non-blank-string)
(s/def ::username ::non-blank-string)
(s/def ::password ::non-blank-string)

(s/def :schema-registry/config
  (s/keys :req-un [::base-url]
          :opt-un [::username ::password]))

(s/def :serde/config
  (s/and (s/conformer #(try
                         (->> %
                              (into {})
                              w/keywordize-keys)
                         (catch Exception ex
                           :clojure.spec.alpha/invalid)))
         (s/keys :req [:schema-registry/config])))

(s/def :avro-record/schema any?)
(s/def :avro-record/value any?)
(s/def ::avro-record
  (s/keys :req-un [::schema ::value]))
