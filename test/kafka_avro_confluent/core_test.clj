(ns kafka-avro-confluent.core-test
  (:require [clojure.test :refer :all]
            [kafka-avro-confluent.deserializers :as sut-des]
            [kafka-avro-confluent.schema-registry-client :as sut-reg]
            [kafka-avro-confluent.serdes :as sut-serde]
            [kafka-avro-confluent.serializers :as sut-ser]
            [zookareg.core :as zkr])
  (:import java.util.UUID
           (java.time LocalDate)))

(use-fixtures :once zkr/with-zookareg-fn)

(def dummy-schema1 {:type   "record"
                    :name   "Foo"
                    :fields [{:name "fooId"
                              :type "string"}
                             {:name "fooDate"
                              :type {:type        :int
                                     :logicalType :date}}]})

(def dummy-schema2 {:type   "record"
                    :name   "Bar"
                    :fields [{:name "barId"
                              :type "string"}
                             {:name "barString"
                              :type {:type :string}}]})

(def dummy-data1 {:fooId "42" :fooDate (LocalDate/of 2018 11 23)})
(def dummy-data2 {:barId "666" :barString "whatever"})

(defn dummy-topic []
  (str (UUID/randomUUID)))

(def schema-registry-config
  {:base-url "http://localhost:8081"})
(def schema-registry
  (sut-reg/->schema-registry-client schema-registry-config))

(deftest avro-serde
  (testing "Can round-trip"
    (let [serializer   (sut-ser/->avro-serializer schema-registry dummy-schema1)
          deserializer (sut-des/->avro-deserializer schema-registry)
          topic        (dummy-topic)]

      (is (= dummy-data1
             (->> dummy-data1
                  (.serialize serializer topic)
                  (.deserialize deserializer topic))))

      (testing "works with nil headers"
        (is (= dummy-data1
               (->> dummy-data1
                    (.serialize serializer topic nil)
                    (.deserialize deserializer topic nil)))))

      (testing "uses :value as default `serializer-type`"
        (is (sut-reg/get-latest-schema-by-subject schema-registry
                                                  (str topic "-value")))))))

(deftest avro-serde-with-config-constructor
  (testing "Can round-trip"
    (let [serializer   (sut-ser/->avro-serializer schema-registry-config dummy-schema1)
          deserializer (sut-des/->avro-deserializer schema-registry-config)
          topic        (dummy-topic)]
      (is (= dummy-data1
             (->> dummy-data1
                  (.serialize serializer topic)
                  (.deserialize deserializer topic)))))))

(deftest avro-serde-with-explicit-serializer-type

  (testing ":value"
    (let [serializer   (sut-ser/->avro-serializer schema-registry
                                                  :value
                                                  dummy-schema1)
          deserializer (sut-des/->avro-deserializer schema-registry)
          topic        (dummy-topic)]

      (is (= dummy-data1
             (->> dummy-data1
                  (.serialize serializer topic)
                  (.deserialize deserializer topic))))

      (is (sut-reg/get-latest-schema-by-subject schema-registry
                                                (str topic "-value")))))

  (testing ":key"
    (let [serializer   (sut-ser/->avro-serializer schema-registry
                                                  :key
                                                  dummy-schema1)
          deserializer (sut-des/->avro-deserializer schema-registry)
          topic        (dummy-topic)]

      (is (= dummy-data1
             (->> dummy-data1
                  (.serialize serializer topic)
                  (.deserialize deserializer topic))))

      (is (sut-reg/get-latest-schema-by-subject schema-registry
                                                (str topic "-key")))))

  (testing "throws when invalid `serializer-type`"
    (is (thrown? AssertionError
                 (sut-ser/->avro-serializer schema-registry
                                            :nefarious-serializer-type
                                            dummy-schema1)))))

(deftest can-stop-auto-conversion-of-logical-types
  (testing "Can round-trip"
    (let [serializer   (sut-ser/->avro-serializer schema-registry dummy-schema1)
          deserializer (sut-des/->avro-deserializer schema-registry :convert-logical-types? false)
          topic        (dummy-topic)
          {:keys [fooDate]} (->> dummy-data1
                                 (.serialize serializer topic)
                                 (.deserialize deserializer topic))]

      (is (= 17858 fooDate)))))

(deftest avro-Serde
  (testing "Can round-trip"
    (let [serde        (sut-serde/->avro-serde schema-registry dummy-schema1)
          serializer   (.serializer serde)
          deserializer (.deserializer serde)
          topic        (dummy-topic)]
      (is (= dummy-data1
             (->> dummy-data1
                  (.serialize serializer topic)
                  (.deserialize deserializer topic))))
      (testing "uses :value as default `serializer-type`"
        (is (sut-reg/get-latest-schema-by-subject schema-registry
                                                  (str topic "-value")))))))

(deftest serde-can-support-multiple-topics
  (let [[topic1 topic2] [(dummy-topic)
                         (dummy-topic)]
        serde        (->> (sut-ser/->schemas-definition {topic1 dummy-schema1
                                                         topic2 dummy-schema2})
                          (sut-serde/->avro-serde schema-registry))
        serializer   (.serializer serde)
        deserializer (.deserializer serde)]
    (is (= dummy-data1
           (->> dummy-data1
                (.serialize serializer topic1)
                (.deserialize deserializer topic1)))
        "Can serialize/deserialize data from topic1")
    (is (= dummy-data2
           (->> dummy-data2
                (.serialize serializer topic2)
                (.deserialize deserializer topic2)))
        "Can serialize/deserialize data from topic2")))