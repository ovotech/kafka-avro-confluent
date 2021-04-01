(defproject ovotech/kafka-avro-confluent "2.7.0-1"

  :description "An Avro Kafka De/Serializer lib that works with Confluent's Schema Registry"

  :url "http://github.com/ovotech/kafka-avro-confluent"

  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.memoize "0.7.1"]
                 [ovotech/abracad "0.4.16"]
                 [cheshire "5.10.0"]

                 [clj-http "3.12.0"]

                 [org.apache.kafka/kafka-clients "2.7.0"
                  :exclusions [org.scala-lang/scala-library]]
                 [org.clojure/tools.logging "0.4.1"]]

  :aot [kafka-avro-confluent.serializers
        kafka-avro-confluent.deserializers
        kafka-avro-confluent.v2.serializer
        kafka-avro-confluent.v2.deserializer]

  :repositories {"confluent" "https://packages.confluent.io/maven"}

  :profiles {:dev {:dependencies   [[vise890/zookareg "2.7.0-1"]
                                    [ch.qos.logback/logback-classic "1.2.3"]
                                    [ch.qos.logback/logback-core "1.2.3"]]
                   :plugins  [[lein-ancient "0.7.0"]]
                   :resource-paths ["dev/resources" "test/resources"]}
             :ci  {:deploy-repositories       [["clojars" {:url           "https://clojars.org/repo"
                                                           :username      :env ;; LEIN_USERNAME
                                                           :password      :env ;; LEIN_PASSWORD
                                                           :sign-releases false}]]

                   :clean-non-project-classes true}})
