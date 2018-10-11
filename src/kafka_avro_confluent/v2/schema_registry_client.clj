(ns kafka-avro-confluent.v2.schema-registry-client
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.core.memoize :refer [memo]]
            [clojure.pprint :as pprint]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log])
  (:import clojure.lang.ExceptionInfo))

(require 'kafka-avro-confluent.v2.specs)

(defn- schema->json
  [schema]
  (let [schema-str (json/generate-string schema)]
    (json/generate-string {"schema" schema-str})))

(defn- merge-ex-data
  [ex data]
  (condp instance? ex
    ExceptionInfo
    (ExceptionInfo. (.getMessage ex)
                    (merge (ex-data ex)
                           data)
                    ex)
    Exception
    (ExceptionInfo. (.getMessage ex)
                    data
                    ex)))
(defn- pretty
  [x]
  (with-out-str (pprint/pprint x)))

(defn- -get-config
  [{:schema-registry/keys [base-url username password]
    :as                   _config}]
  (-> (http/get
       (str base-url "/config")
       {:basic-auth   [username password]
        :as           :json
        :conn-timeout 1000})
      :body))

(defn -healthy?
  [config]
  (try
    (contains? (-get-config config)
               :compatibilityLevel)
    (catch Exception e
      false)))

(defn -list-subjects
  [{:schema-registry/keys [base-url username password]
    :as                   _config}]
  (-> (http/get
       (str base-url "/subjects")
       {:basic-auth   [username password]
        :as           :json
        :conn-timeout 1000})
      :body))

(defn- -post-schema
  [{:schema-registry/keys [base-url username password]
    :as                   _config}
   subject
   schema]
  ;; TODO better url creation
  (let [url  (str base-url
                  "/subjects/"
                  subject
                  "/versions")
        body (schema->json schema)]
    (try
      (-> (http/post url
                     {:basic-auth   [username password]
                      :content-type "application/vnd.schemaregistry.v1+json"
                      :as           :json
                      :body         body})
          (get-in [:body :id]))
      (catch Exception ex
        (let [exi (merge-ex-data ex {:post-url  url
                                     :post-body body
                                     :schema    schema})]
          (log/error ex
                     "Post to schema registry failed!"
                     (pretty (ex-data exi)))
          (throw exi))))))

(defn- -get-schema-by-id
  [{:schema-registry/keys [base-url username password]
    :as                   _config}
   id]
  (let [url  (str base-url "/schemas/ids/" id)
        resp (http/get url
                       {:as         :json
                        :basic-auth [username password]})]
    (-> resp
        :body
        :schema
        (json/parse-string true))))

(defn- -get-latest-schema-by-subject
  [{:schema-registry/keys [base-url username password]
    :as                   _config}
   subject]
  (let [url  (str base-url
                  "/subjects/"
                  subject
                  "/versions/latest")
        resp (http/get url
                       {:as         :json
                        :basic-auth [username password]})]
    (-> resp
        :body
        :schema
        (json/parse-string true))))

(defprotocol SchemaRegistry
  "A Confluent Schema Registry client."
  (get-config  [this] "Returns the Schema Registry configuration map.")
  (healthy? [this] "Can the Schema Registry be contacted, and do its responses look right?")
  (list-subjects [this] "List all subjects.")
  (post-schema  [this subject schema] "Posts an Avro Schema. Return the Schema Id.")
  (get-schema-by-id [this id] "Fetches a Schema by Schema Id.")
  (get-latest-schema-by-subject [this subject] "Gets the latest Schema for a given subject."))

(defrecord SchemaRegistryImpl [memoized-fns config]
  SchemaRegistry
  (healthy? [this] (-healthy? config))
  (get-config [_] (-get-config config))
  (list-subjects [_] (-list-subjects config))

  (post-schema
    [_ subject schema]
    ((:post-schema memoized-fns) config subject schema))

  (get-latest-schema-by-subject
    [_ subject]
    ((:get-latest-schema-by-subject memoized-fns) config subject))

  (get-schema-by-id
    [_ id]
    ((:get-schema-by-id memoized-fns) config id)))

(s/fdef ->schema-registry-client
        :args (s/cat :config :kafka.serde/config))
(defn ->schema-registry-client
  "Returns an instance of the schema-registry-client"
  (^kafka_avro_confluent.schema_registry_client.SchemaRegistry
   [config]
   (s/assert :kafka.serde/config config)
   (let [memoized-fns
         {:post-schema                  (memo -post-schema)
          :get-schema-by-id             (memo -get-schema-by-id)
          :get-latest-schema-by-subject (memo -get-latest-schema-by-subject)}]
     (->SchemaRegistryImpl memoized-fns config))))
