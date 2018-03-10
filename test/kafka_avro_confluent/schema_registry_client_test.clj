(ns kafka-avro-confluent.schema-registry-client-test
  (:require [kafka-avro-confluent.schema-registry-client :as sut]
            [clojure.test :refer :all]
            [zookareg.core :as zkr]
            [cheshire.core :as json]
            [abracad.avro :as avro]))

(defn ->dummy-schema
  [name]
  {:type   "record"
   :name   name
   :fields [{:name "fooId"
             :type "string"}]})

(defn parse-schema
  [schema]
  (-> schema :schema (json/parse-string true)))

(deftest posting-and-retrieving-schemas
  (zkr/with-zookareg zkr/default-config
    (let [c      (sut/->schema-registry-client {:base-url "http://localhost:8081"})
          schema (->dummy-schema "Foo")]
      (sut/post-schema c "subject" schema)
      (is (= schema
             (parse-schema (sut/get-latest-schema-by-subject c "subject")))))))

(defn roundtrip-first-schema-post [schema-name]
  (zkr/with-zookareg zkr/default-config
    (let [c                  (sut/->schema-registry-client {:base-url "http://localhost:8081"})
          schema             (->dummy-schema schema-name)
          ;; NOTE I'm pretty sure we can rely on Schema Reg. assigning 1 as first ID
          expected-schema-id 1
          actual-schema-id   (sut/post-schema c "subject" schema)
          ;; NOTE this is the memoized function that should break if called with same arg
          retrieved-schema   (->> expected-schema-id
                                  (sut/get-avro-schema-by-id c)
                                  avro/unparse-schema)]
      (is (= expected-schema-id actual-schema-id))
      (is (= schema retrieved-schema)))))

(deftest core-calls-memoization
  (testing "should use different memo caches across object instances"
    (roundtrip-first-schema-post "First")
    (roundtrip-first-schema-post "Different")))

(deftest healthcheck
  (testing "is healthy when all the deps are up"
    (zkr/with-zookareg zkr/default-config
      (let [c (sut/->schema-registry-client
               {:base-url "http://localhost:8081"})]
        (is (sut/healthy? c)))))
  (testing "is unhealthy when deps are down"
    ;; NOTE no zookareg
    (let [c (sut/->schema-registry-client
             {:base-url "http://localhost:8081"})]
      (is (not (sut/healthy? c))))))
