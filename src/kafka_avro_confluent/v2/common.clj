(ns kafka-avro-confluent.v2.common
  (:require [clojure.walk :as walk]))

(defn clj-config->confluent-config
  [config]
  (let [;; NOTE it supports the keys defined in kafka-avro-confluent.v2.specs
        {:keys [schema-registry/base-url
                schema-registry/username
                schema-registry/password]
         :as   config} (walk/keywordize-keys config)
        converted-keys (cond-> {:schema.registry.url base-url}
                         (and username password)
                         (assoc :basic.auth.credentials.source "USER_INFO"
                                :basic.auth.user.info (str username ":" password)))
        ;; ... but the official confluent config will take precedence if it is present
        ;; https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html
        confluent-keys (select-keys config
                                    [:schema.registry.url
                                     :basic.auth.credentials.source
                                     :basic.auth.user.info])]
    (walk/stringify-keys (merge converted-keys
                                confluent-keys))))


