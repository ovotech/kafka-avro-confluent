(ns user
  (:require [zookareg.core :as zkr]
            [kafka-avro-confluent.schema-registry-client :as sut-reg]))

(comment

  (zkr/init-zookareg)

  (def sr (sut-reg/->schema-registry-client {:base-url "http://localhost:8081"}))

  (sut-reg/get-schema-by-id sr 1)



  )
