(defproject parkside-securities/kafka-avro-confluent "0.9.1-SNAPSHOT"

  :description "An Avro Kafka De/Serializer lib that works with Confluent's Schema Registry"

  :url "http://github.com/parkside-securities/kafka-avro-confluent"

  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.memoize "0.7.1"]
                 [parkside-securities/abracad "0.4.15-SNAPSHOT"]
                 [cheshire "5.8.0"]

                 [clj-http "3.8.0"]

                 [org.apache.kafka/kafka-clients "1.0.1"
                  :exclusions [org.scala-lang/scala-library]]
                 [org.clojure/tools.logging "0.4.0"]]

  :aot [kafka-avro-confluent.serializers
        kafka-avro-confluent.serde
        kafka-avro-confluent.deserializers]

  :repositories {"confluent" "https://packages.confluent.io/maven"}

  :profiles {:dev {:dependencies   [[vise890/zookareg "1.0.1-1"]]}
             :ci {:deploy-repositories [["clojars" {:url           "https://clojars.org/repo"
                                                    :username      :env ;; LEIN_USERNAME
                                                    :password      :env ;; LEIN_PASSWORD
                                                    :sign-releases false}]]}})
