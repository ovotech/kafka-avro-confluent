(ns kafka-avro-confluent.core-test
  (:require [clojure.test :refer :all]
            [kafka-avro-confluent.deserializers :as sut-des]
            [kafka-avro-confluent.schema-registry-client :as sut-reg]
            [kafka-avro-confluent.serializers :as sut-ser]
            [zookareg.core :as zkr])
  (:import java.util.UUID))

(use-fixtures :once zkr/with-zookareg-fn)

(def dummy-schema {:type   "record"
                   :name   "Foo"
                   :fields [{:name "fooId"
                             :type "string"}]})

(def dummy-data {:fooId "42"})

(defn dummy-topic []
  (str (UUID/randomUUID)))

(def schema-registry (sut-reg/->schema-registry-client {:base-url "http://localhost:8081"}))

(deftest avro-serde
      (testing "Can round-trip"
        (let [serializer   (sut-ser/->avro-serializer schema-registry dummy-schema)
              deserializer (sut-des/->avro-deserializer schema-registry)
              topic (dummy-topic)]

          (is (= dummy-data
                 (->> dummy-data
                      (.serialize serializer topic)
                      (.deserialize deserializer topic))))

          (testing "uses :value as default `serializer-type`"
            (is (sut-reg/get-latest-schema-by-subject schema-registry
                                                      (str topic "-value")))))))

(deftest avro-serde-with-explicit-serializer-type

      (testing ":value"
        (let [serializer   (sut-ser/->avro-serializer schema-registry :value dummy-schema)
              deserializer (sut-des/->avro-deserializer schema-registry)
              topic        (dummy-topic)]

          (is (= dummy-data
                 (->> dummy-data
                      (.serialize serializer topic)
                      (.deserialize deserializer topic))))

          (is (sut-reg/get-latest-schema-by-subject schema-registry
                                                    (str topic "-value")))))

      (testing ":key"
        (let [serializer   (sut-ser/->avro-serializer schema-registry :key dummy-schema)
              deserializer (sut-des/->avro-deserializer schema-registry)
              topic        (dummy-topic)]

          (is (= dummy-data
                 (->> dummy-data
                      (.serialize serializer topic)
                      (.deserialize deserializer topic))))

          (is (sut-reg/get-latest-schema-by-subject schema-registry
                                                    (str topic "-key")))))

      (testing "throws when invalid `serializer-type`"
        (is (thrown? AssertionError
                     (sut-ser/->avro-serializer schema-registry
                                                :nefarious-serializer-type
                                                dummy-schema)))))
