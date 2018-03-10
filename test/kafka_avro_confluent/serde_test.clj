(ns kafka-avro-confluent.serde-test
  (:require  [clojure.test :refer :all]
             [clojure.walk :as walk]
             [zookareg.core :as zkr]
             [clojure.spec.alpha :as s]
             [kafka-avro-confluent.schema-registry-client :as sut-reg])
  (:import kafka_avro_confluent.Serde
           java.util.UUID))

(use-fixtures :once zkr/with-zookareg-fn)

(defn roundtrip-ok? [isKey]
  (testing "Can round-trip"
    (let [schema       {:type   "record"
                        :name   "Foo"
                        :fields [{:name "fooId"
                                  :type "string"}]}
          config       (walk/stringify-keys
                        {:schema-registry-client {:base-url "http://localhost:8081"}
                         :serializer             {:avro-schema schema}})
          serde        (doto (Serde.)
                         (.configure config isKey))
          serializer   (.serializer serde)
          deserializer (.deserializer serde)
          topic        (str (UUID/randomUUID))
          dummy-data   {:fooId "42"}]
      (is (= dummy-data
             (->> dummy-data
                  (.serialize serializer topic)
                  (.deserialize deserializer topic))))
      (testing "posts to a subject with right suffix"
        (let [subject-suffix (str "-" (if isKey "key" "value"))
              exp-subject    (str topic subject-suffix)
              sr             (.schemaRegistryClient serde)]
          (is (= schema
                 (sut-reg/get-latest-schema-by-subject sr exp-subject))
              (str "couldn't find schema on exp subject;"
                   {:exp-subject    exp-subject
                    :schema         schema
                    :found-subjects (sut-reg/list-subjects sr)})))))))

(deftest serde-test
  (roundtrip-ok? false)
  (roundtrip-ok? true))
