(defproject ovotech/kafka-avro-confluent "0.9.0"

  :description "An Avro Kafka De/Serializer lib that works with Confluent's Schema Registry"

  :url "http://github.com/ovotech/kafka-avro-confluent"

  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.9.0-beta2"]
                 [org.clojure/core.memoize "0.5.9"]
                 [com.damballa/abracad "0.4.13"]
                 [cheshire "5.8.0"]

                 [clj-http "3.7.0"]

                 [org.apache.kafka/kafka-clients "0.10.2.1"
                  :exclusions [org.scala-lang/scala-library]]
                 [fipp "0.6.12"]
                 [org.clojure/tools.logging "0.4.0"]]

  :aot [kafka-avro-confluent.serializers
        kafka-avro-confluent.deserializers]

  :repositories {"confluent" "https://packages.confluent.io/maven"}

  :profiles {:dev {:dependencies   [[vise890/zookareg "0.5.6"]]}
             :ci {:deploy-repositories [["clojars" {:url           "https://clojars.org/repo"
                                                    :username      :env ;; LEIN_USERNAME
                                                    :password      :env ;; LEIN_PASSWORD
                                                    :sign-releases false}]]}})
