(ns kafka-avro-confluent.core-test
  (:require [clojure.test :refer :all]
            [kafka-avro-confluent.core :refer :all]
            [kafka-avro-confluent.deserializers :as sut-des]
            [kafka-avro-confluent.schema-registry-client :as sut-reg]
            [kafka-avro-confluent.serializers :as sut-ser]
            [kafka-avro-confluent.utils.core :as tu]))

(use-fixtures :once tu/with-embedded-zookareg)

(deftest avro-serde
  (testing "Can roundtrip"
    (let [schema {:type   "record",
                  :name   "Foo",
                  :fields [{:name "fooId", :type "string"}]}
          data   {:fooId "42"}

          registry     (sut-reg/->schema-registry-client {:base-url "http://localhost:8081"})
          serializer   (sut-ser/->avro-serializer registry schema)
          deserializer (sut-des/->avro-deserializer registry)]
      (is (= data
             (->> data
                  (.serialize serializer "topic-foo")
                  (.deserialize deserializer "topic-foo")))))))
