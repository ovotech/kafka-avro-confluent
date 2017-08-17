(defproject ovotech/kafka-avro-confluent "0.4.0"
  :description "An Avro Kafka De/Serializer lib that works with Confluent's Schema Registry"

  :url "http://github.com/ovotech/kafka-avro-confluent"

  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.damballa/abracad "0.4.13"]
                 [cheshire "5.7.1"]

                 [clj-http "3.6.1"]

                 [org.apache.kafka/kafka-clients "0.10.2.1"
                  :exclusions [org.scala-lang/scala-library]] ]

  :repositories {"confluent" "http://packages.confluent.io/maven"}

  :aot :all

  :profiles {:uberjar {:aot :all}
             :dev {:resource-paths ["test/resources"]
                   :dependencies   [[integrant "0.5.0"]
                                    [integrant/repl "0.2.0"]
                                    [io.confluent/kafka-schema-registry "3.3.0"
                                     :exclusions [org.apache.kafka/kafka-clients
                                                  org.apache.kafka/kafka_2.11]]

                                    [org.apache.kafka/kafka_2.11 "0.10.2.1"
                                     :exclusions [org.apache.zookeeper/zookeeper]]
                                    [bigsy/franzy-embedded "0.0.3"]

                                    [org.apache.curator/curator-test "4.0.0"]]}})
