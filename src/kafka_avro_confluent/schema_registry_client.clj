(ns kafka-avro-confluent.schema-registry-client
  (:require [abracad.avro :as avro]
            [cheshire.core :as json]
            [clj-http.client :as http]))

(defn- schema->json
  [schema]
  (let [schema-str (json/generate-string schema)]
    (json/generate-string {"schema" schema-str})))

(defn- -get-schema-by-id
  [config id]
  (let [url  (str (:base-url config) "/schemas/ids/" id)
        resp (http/get url
                       {:as :json
                        :basic-auth [(:username config) (:password config)]})]
    (-> resp
        :body
        :schema
        avro/parse-schema)))

(defprotocol SchemaRegistry
  (healthy? [this])
  (get-config [this])
  (get-subject-by-topic [this topic])
  (post-schema [this subject schema])
  (get-schema-by-id [this id]))

(defrecord SchemaRegistryImpl [config]
  SchemaRegistry
  (get-config [_]
    (-> (http/get (str (:base-url config) "/config")
                  {:basic-auth   [(:username config) (:password config)]
                   :conn-timeout 1000})
        :body
        (json/parse-string true)))

  (healthy? [this]
    (try
      (contains? (get-config this)
                 :compatibilityLevel)
      (catch Exception e
        false)))

  ;; TODO rename? document? what does it do?
  (get-subject-by-topic
    [_ topic]
    (let [{:keys [username password base-url]} config
          url  (format "%s/subjects/%s/versions/1"
                       base-url
                       (str topic "-value"))
          resp (http/get url
                         {:basic-auth [username password]}
                         :as :json)]
      (:body resp)))

  (post-schema
    [_ subject schema]
    (let [url  (str (:base-url config) "/subjects/" subject "-value/versions")
          resp (http/post url
                          {:basic-auth   [(:username config) (:password config)]
                           :content-type "application/vnd.schemaregistry.v1+json"
                           :as           :json
                           :body         (schema->json schema)})]
      (get-in resp [:body :id])))

  (get-schema-by-id [_ id] (-get-schema-by-id config id)))

(def ->schema-registry-client ->SchemaRegistryImpl)

(comment

  (def sr (->schema-registry-client {:base-url "http://localhost:8081"}))

  (healthy? sr)

  (def test-schema {:type   "record",
                    :name   "Foo",
                    :fields [{:name "fooId", :type "string"}]})

  (let [id (post-schema sr "foo" test-schema)]
    (get-schema-by-id sr id)))
