(ns kafka-avro-confluent.schema-registry-client
  "A client for the Confluent Schema Registry."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.core.memoize :refer [memo]]
            [clojure.pprint :as pprint]
            [clojure.tools.logging :as log])
  (:import clojure.lang.ExceptionInfo))

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
  [config]
  (-> (http/get
       (str (:base-url config) "/config")
       {:basic-auth   [(:username config) (:password config)]
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

(defn -list-subjects [config]
  (-> (http/get
       (str (:base-url config) "/subjects")
       {:basic-auth   [(:username config) (:password config)]
        :as           :json
        :conn-timeout 1000})
      :body))

(defn- -post-schema
  [config subject schema]
  ;; TODO better url creation
  (let [url  (str (:base-url config)
                  "/subjects/"
                  subject
                  "/versions")
        body (schema->json schema)]
    (try
      (-> (http/post url
                     {:basic-auth [(:username config)
                                   (:password config)]
                      :content-type
                      "application/vnd.schemaregistry.v1+json"
                      :as         :json
                      :body       body})
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
  [config id]
  (let [url  (str (:base-url config) "/schemas/ids/" id)
        resp (http/get url
                       {:as         :json
                        :basic-auth [(:username config)
                                     (:password config)]})]
    (-> resp
        :body
        :schema
        (json/parse-string true))))

(defn- -get-latest-schema-by-subject
  [config subject]
  (let [url  (str (:base-url config)
                  "/subjects/"
                  subject
                  "/versions/latest")
        resp (http/get url
                       {:as         :json
                        :basic-auth [(:username config)
                                     (:password config)]})]
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

(s/def ::non-blank-string (s/and string? (complement string/blank?)))
(s/def ::base-url ::non-blank-string)
(s/def ::username ::non-blank-string)
(s/def ::password ::non-blank-string)

(s/def ::config
  (s/keys :req-un [::base-url]
          :opt-un [::username ::password]))

(s/fdef ->schema-registry-client
        :args (s/cat :config ::config))
(defn ->schema-registry-client
  "Returns an instance of the schema-registry-client"
  (^kafka_avro_confluent.schema_registry_client.SchemaRegistry
   [config]
   (s/assert ::config config)
   (let [memoized-fns
         {:post-schema                  (memo -post-schema)
          :get-schema-by-id             (memo -get-schema-by-id)
          :get-latest-schema-by-subject (memo -get-latest-schema-by-subject)}]
     (->SchemaRegistryImpl memoized-fns config))))
