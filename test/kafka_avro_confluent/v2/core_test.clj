(ns kafka-avro-confluent.v2.core-test
  (:require [clojure.test :refer :all]
            [kafka-avro-confluent.v2.deserializer :as sut-des]
            [kafka-avro-confluent.schema-registry-client :as sut-reg]
            [kafka-avro-confluent.v2.serializer :as sut-ser]
            [zookareg.core :as zkr]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest])
  (:import java.util.UUID))

(stest/instrument)

(use-fixtures :once zkr/with-zookareg-fn)

(def dummy-schema {:type   "record"
                   :name   "Foo"
                   :fields [{:name "fooId"
                             :type "string"}]})

(def dummy-data {:fooId "42"})

(defn dummy-topic []
  (str (UUID/randomUUID)))

(def config
  {:schema-registry {:base-url "http://localhost:8081"}})

(def schema-registry-client
  (sut-reg/->schema-registry-client (:schema-registry config)))

(deftest avro-serde
  (testing "Can round-trip"
    (let [serializer   (sut-ser/->avro-serializer config false)
          deserializer (sut-des/->avro-deserializer config)
          topic (dummy-topic)]
      (is (= dummy-data
             (->> {:value dummy-data :schema dummy-schema}
                  (.serialize serializer topic)
                  (.deserialize deserializer topic))))

      (testing "uses :value as default `serializer-type`"
        (is (sut-reg/get-latest-schema-by-subject schema-registry-client
                                                  (str topic "-value")))))))

(deftest avro-serde-with-isKey=true-test
  (let [serializer   (sut-ser/->avro-serializer config true)
        deserializer (sut-des/->avro-deserializer config)
        topic        (dummy-topic)]
    (is (= dummy-data
           (->> {:value dummy-data :schema dummy-schema}
                (.serialize serializer topic)
                (.deserialize deserializer topic))))

    (is (sut-reg/get-latest-schema-by-subject schema-registry-client
                                              (str topic "-key")))))
