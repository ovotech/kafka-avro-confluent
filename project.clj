(defproject kafka-avro-confluent "0.1.0-SNAPSHOT"
  :description "An Avro Kafka De/Serializer lib that works with Confluent's Schema Registry"

  :url "http://github.com/ovotech/kafka-avro-confluent"

  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-clients "0.11.0.0-cp1"]
                 [com.damballa/abracad "0.4.13"]
                 [cheshire "5.7.1"]
                 [clj-http "3.6.1"]]

  :repositories {"confluent" "http://packages.confluent.io/maven"}

  :profiles {:uberjar {:aot :all}
             :dev {:resource-paths ["test/resources"]
                   :dependencies   [[integrant "0.5.0"]
                                    [integrant/repl "0.2.0"]

                                    [io.confluent/kafka-schema-registry "3.3.0"]
                                    [bigsy/franzy-embedded "0.0.3"]
                                    [org.apache.curator/curator-test "4.0.0"]]}})
