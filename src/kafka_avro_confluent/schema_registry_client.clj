(ns kafka-avro-confluent.schema-registry-client
  (:require [abracad.avro :as avro]
            [cheshire.core :as json]
            [clj-http.client :as http]))

(defn- schema->json
  [schema]
  (let [schema-str (json/generate-string schema)]
    (json/generate-string {"schema" schema-str})))

(defn- -post-schema
  [config subject schema]
  (let [url  (str (:base-url config) "/subjects/" subject "/versions")
        resp (http/post url
                        {:basic-auth   [(:username config) (:password config)]
                         :content-type "application/vnd.schemaregistry.v1+json"
                         :as           :json
                         :body         (schema->json schema)})]
    (get-in resp [:body :id])))

(defn- -get-schema-registry-config
  [config]
  (-> (http/get (str (:base-url config) "/config")
                {:basic-auth   [(:username config) (:password config)]
                 :conn-timeout 1000})
      :body
      (json/parse-string true)))

(defn- -get-schema-by-id
  [config id]
  (let [url  (str (:base-url config) "/schemas/ids/" id)
        resp (http/get url
                       {:as :json
                        :basic-auth [(:username config)
                                     (:password config)]})]
    (:body resp)))

(defn- -get-latest-schema-by-subject
  [config subject]
  (let [url  (str (:base-url config) "/subjects/" subject "/versions/latest")
        resp (http/get url
                       {:as :json
                        :basic-auth [(:username config)
                                     (:password config)]})]
    (:body resp)))

(defn- -get-avro-schema-by-id
  [config id]
  (-> (-get-schema-by-id config id)
      :schema
      avro/parse-schema))

(defprotocol SchemaRegistry
  (healthy? [this])
  (get-config [this])
  (post-schema [this subject schema])
  (get-latest-schema-by-subject [this subject])
  (get-avro-schema-by-id [this id]))

(defrecord SchemaRegistryImpl [config]
  SchemaRegistry
  (healthy? [this]
    (try
      (contains? (get-config this)
                 :compatibilityLevel)
      (catch Exception e
        false)))

  (get-config [_] (-get-schema-registry-config config))

  (post-schema
    [_ subject schema]
    (-post-schema config subject schema))

  (get-latest-schema-by-subject
    [_ subject]
    (-get-latest-schema-by-subject config subject))

  (get-avro-schema-by-id [_ id] (-get-avro-schema-by-id config id)))

(def ->schema-registry-client ->SchemaRegistryImpl)

(comment

  (def sr (->schema-registry-client {:base-url "http://localhost:8081"}))

  (healthy? sr)

  (def test-schema {:type   "record",
                    :name   "Foo",
                    :fields [{:name "fooId", :type "string"}]})

  (let [id (post-schema sr "foo" test-schema)]
    (get-schema-by-id sr id)))
