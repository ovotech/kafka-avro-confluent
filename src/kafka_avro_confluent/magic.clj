(ns kafka-avro-confluent.magic
  "The Confluent Schema Registry Magic Byte.

  https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format")

(def ^Integer/TYPE magic 0x0)
