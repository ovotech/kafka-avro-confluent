(ns kafka-avro-confluent.schema-registry-client-test
  (:require [abracad.avro :as avro]
            [cheshire.core :as json]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [kafka-avro-confluent.schema-registry-client :as sut]
            [zookareg.core :as zkr]))

(defn ->dummy-schema
  [name]
  {:type   "record"
   :name   name
   :fields [{:name "fooId"
             :type "string"}]})

(defn parse-schema
  [schema]
  (-> schema :schema (json/parse-string true)))

(deftest ->schema-registry-client-test
  (testing "blows up when invalid config is passed"
    (s/check-asserts true)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Spec assertion failed"
                          (sut/->schema-registry-client {})))))
(deftest healthy?-test
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

(deftest get-config-test
  (testing "can get config"
    (zkr/with-zookareg zkr/default-config

      (let [c (sut/->schema-registry-client
               {:base-url "http://localhost:8081"})
            config (sut/get-config c)]
        (is (= "BACKWARD" (:compatibilityLevel config)))))))

(deftest posting-and-getting-schemas
  (zkr/with-zookareg zkr/default-config
    (let [c      (sut/->schema-registry-client
                  {:base-url "http://localhost:8081"})
          schema (->dummy-schema "Foo")
          post-resp-schema-id
          (sut/post-schema c "subject" schema)]
      (is (integer? post-resp-schema-id))
      (is (= schema
             (sut/get-latest-schema-by-subject c "subject")))
      (is (= schema
             (sut/get-schema-by-id c post-resp-schema-id))))))

(defn roundtrip-first-schema-post [schema-name]
  (zkr/with-zookareg zkr/default-config
    (let [c                  (sut/->schema-registry-client
                              {:base-url "http://localhost:8081"})
          schema             (->dummy-schema schema-name)
          actual-schema-id   (sut/post-schema c "subject" schema)
          expected-schema-id 1
          ;; NOTE I'm pretty sure we can rely on Schema Reg. assigning 1 as first ID
          _                  (is (= expected-schema-id actual-schema-id))
          ;; NOTE this is the memoized function that should break if called with same arg
          retrieved-schema   (->> expected-schema-id
                                  (sut/get-schema-by-id c))]
      (is (= schema retrieved-schema)))))

(deftest core-calls-memoization
  (testing "should use different memo caches across schema-registry object instances"
    (roundtrip-first-schema-post "First")
    (roundtrip-first-schema-post "Different")))
