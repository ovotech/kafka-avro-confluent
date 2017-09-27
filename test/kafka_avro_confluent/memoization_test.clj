(ns kafka-avro-confluent.memoization-test
  (:require [clojure.test :refer :all]
            [kafka-avro-confluent.deserializers :as sut-des]
            [kafka-avro-confluent.schema-registry-client :as sut-reg]
            [kafka-avro-confluent.serializers :as sut-ser]
            [zookareg.core :as zkr])
  (:import java.util.UUID))


(defn ->dummy-schema [field-name]
  {:type   "record"
   :name   "Record"
   :fields [{:name field-name
             :type "string"}]})

(defn ->dummy-data [field-name] {(keyword field-name) "42"})

(defn dummy-topic [] (str (UUID/randomUUID)))

(deftest avro-serde

  (zkr/with-zookareg zkr/default-config
    (let [schema-registry (sut-reg/->schema-registry-client {:base-url "http://localhost:8081"})
          serializer      (sut-ser/->avro-serializer schema-registry (->dummy-schema :FOO))
          deserializer    (sut-des/->avro-deserializer schema-registry)
          topic           (dummy-topic)
          dummy-data      (->dummy-data :FOO)]
      (is (= dummy-data
             (->> dummy-data
                  (.serialize serializer topic)
                  (.deserialize deserializer topic))))))

  (zkr/with-zookareg zkr/default-config
    (let [schema-registry (sut-reg/->schema-registry-client {:base-url "http://localhost:8081"})
          serializer      (sut-ser/->avro-serializer schema-registry (->dummy-schema :BAR))
          deserializer    (sut-des/->avro-deserializer schema-registry)
          topic           (dummy-topic)
          dummy-data      (->dummy-data :BAR)]
      (is (= dummy-data
             (->> dummy-data
                  (.serialize serializer topic)
                  (.deserialize deserializer topic)))))))
