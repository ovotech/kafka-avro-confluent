(ns kafka-avro-confluent.v2.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.walk :as w]))

(s/def ::non-blank-string (s/and string? (complement string/blank?)))

(s/def :schema-registry/base-url ::non-blank-string)
(s/def :schema-registry/username ::non-blank-string)
(s/def :schema-registry/password ::non-blank-string)

(s/def :confluent/schema.registry.url ::non-blank-string)
(s/def :confluent/basic.auth.credentials.source ::non-blank-string)
(s/def :confluent/basic.auth.user.info ::non-blank-string)

(s/def :kafka.serde/config
  (s/and (s/conformer #(try
                         (w/keywordize-keys %)
                         (catch Exception ex
                           :clojure.spec.alpha/invalid)))
         (s/or :clj (s/keys :req [:schema-registry/base-url]
                            :opt [:schema-registry/username
                                  :schema-registry/password])
               :confluent
               (s/keys :req-un [:confluent/schema.registry.url]
                       :opt-un [:confluent/basic.auth.credentials.source
                                :confluent/basic.auth.user.info]))))

(s/def :avro-record/schema any?)
(s/def :avro-record/value any?)
(s/def ::avro-record
  (s/keys :req-un [:avro-record/schema :avro-record/value]))
