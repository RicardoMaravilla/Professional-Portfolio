file://<WORKSPACE>/src/main/scala/com/ibm/dswdiaods/pricing_onprem/OnPrem.scala
### java.lang.IndexOutOfBoundsException: -1 is out of bounds (min 0, max 2)

occurred in the presentation compiler.

presentation compiler configuration:
Scala version: 2.13.12
Classpath:
<WORKSPACE>/src/main/resources [exists ], <WORKSPACE>/.bloop/root/bloop-bsp-clients-classes/classes-Metals-B2ljqp8FTYuaBx2r0rmQrg== [exists ], <HOME>/Library/Caches/bloop/semanticdb/com.sourcegraph.semanticdb-javac.0.9.9/semanticdb-javac-0.9.9.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-library/2.13.12/scala-library-2.13.12.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0-dswdia-SNAPSHOT/spark-sql-kafka-0-10_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-core_2.13/4.0.0-dswdia-SNAPSHOT/spark-core_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-sql_2.13/4.0.0-dswdia-SNAPSHOT/spark-sql_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/3.5.0/spark-avro_2.13-3.5.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka_2.13/3.6.1/kafka_2.13-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.3.1/httpclient5-5.3.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/za/co/absa/abris_2.13/6.3.0/abris_2.13-6.3.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/redis/clients/jedis/5.1.0/jedis-5.1.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scalactic/scalactic_2.13/3.2.17/scalactic_2.13-3.2.17.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cloud/cloudant/0.8.1/cloudant-0.8.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/bettercloud/vault-java-driver/5.1.0/vault-java-driver-5.1.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-sbt-local/com/ibm/dswdia/core_2.13/3.0.3/core_2.13-3.0.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/module/jackson-module-scala_2.13/2.16.1/jackson-module-scala_2.13-2.16.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-io/commons-io/2.15.1/commons-io-2.15.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/alibaba/fastjson2/fastjson2/2.0.45/fastjson2-2.0.45.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/joda-time/joda-time/2.12.6/joda-time-2.12.6.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/lihaoyi/requests_2.13/0.8.0/requests_2.13-0.8.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/lihaoyi/ujson_2.13/3.1.4/ujson_2.13-3.1.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/opencsv/opencsv/5.9/opencsv-5.9.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/play/play-json_2.13/2.10.4/play-json_2.13-2.10.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-http_2.13/10.5.3/akka-http_2.13-10.5.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0-dswdia-SNAPSHOT/spark-token-provider-kafka-0-10_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/modules/scala-parallel-collections_2.13/1.0.4/scala-parallel-collections_2.13-1.0.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/packages.confluent.io/maven/org/apache/kafka/kafka-clients/6.2.1-ccs/kafka-clients-6.2.1-ccs.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-tags_2.13/4.0.0-dswdia-SNAPSHOT/spark-tags_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/avro/avro-mapred/1.11.3/avro-mapred-1.11.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/twitter/chill_2.13/0.10.0/chill_2.13-0.10.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/twitter/chill-java/0.10.0/chill-java-0.10.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-launcher_2.13/4.0.0-dswdia-SNAPSHOT/spark-launcher_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-kvstore_2.13/4.0.0-dswdia-SNAPSHOT/spark-kvstore_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-network-common_2.13/4.0.0-dswdia-SNAPSHOT/spark-network-common_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-network-shuffle_2.13/4.0.0-dswdia-SNAPSHOT/spark-network-shuffle_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-unsafe_2.13/4.0.0-dswdia-SNAPSHOT/spark-unsafe_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-common-utils_2.13/4.0.0-dswdia-SNAPSHOT/spark-common-utils_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/javax/activation/activation/1.1.1/activation-1.1.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/curator/curator-recipes/5.2.0/curator-recipes-5.2.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/zookeeper/zookeeper/3.9.1/zookeeper-3.9.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/jakarta/servlet/jakarta.servlet-api/4.0.3/jakarta.servlet-api-4.0.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-codec/commons-codec/1.16.0/commons-codec-1.16.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/commons/commons-compress/1.25.0/commons-compress-1.25.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.14.0/commons-lang3-3.14.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/commons/commons-text/1.11.0/commons-text-1.11.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/commons/commons-collections4/4.4/commons-collections4-4.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ning/compress-lzf/1.1.2/compress-lzf-1.1.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/roaringbitmap/RoaringBitmap/1.0.1/RoaringBitmap-1.0.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/modules/scala-xml_2.13/2.2.0/scala-xml_2.13-2.2.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-reflect/2.13.12/scala-reflect-2.13.12.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/json4s/json4s-jackson_2.13/3.7.0-M11/json4s-jackson_2.13-3.7.0-M11.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/jersey/core/jersey-client/2.41/jersey-client-2.41.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/jersey/core/jersey-common/2.41/jersey-common-2.41.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/jersey/core/jersey-server/2.41/jersey-server-2.41.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/jersey/containers/jersey-container-servlet/2.41/jersey-container-servlet-2.41.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/jersey/containers/jersey-container-servlet-core/2.41/jersey-container-servlet-core-2.41.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/jersey/inject/jersey-hk2/2.41/jersey-hk2-2.41.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-all/4.1.106.Final/netty-all-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-native-epoll/4.1.106.Final/netty-transport-native-epoll-4.1.106.Final-linux-x86_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-native-epoll/4.1.106.Final/netty-transport-native-epoll-4.1.106.Final-linux-aarch_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-native-kqueue/4.1.106.Final/netty-transport-native-kqueue-4.1.106.Final-osx-aarch_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-native-kqueue/4.1.106.Final/netty-transport-native-kqueue-4.1.106.Final-osx-x86_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/clearspring/analytics/stream/2.9.6/stream-2.9.6.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/dropwizard/metrics/metrics-core/4.2.21/metrics-core-4.2.21.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/dropwizard/metrics/metrics-jvm/4.2.21/metrics-jvm-4.2.21.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/dropwizard/metrics/metrics-json/4.2.21/metrics-json-4.2.21.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/dropwizard/metrics/metrics-graphite/4.2.21/metrics-graphite-4.2.21.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/dropwizard/metrics/metrics-jmx/4.2.21/metrics-jmx-4.2.21.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.16.1/jackson-databind-2.16.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/net/razorvine/pickle/1.3/pickle-1.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/net/sf/py4j/py4j/0.10.9.7/py4j-0.10.9.7.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/commons/commons-crypto/1.1.0/commons-crypto-1.1.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/rocksdb/rocksdbjni/8.3.2/rocksdbjni-8.3.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/univocity/univocity-parsers/2.9.1/univocity-parsers-2.9.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-sketch_2.13/4.0.0-dswdia-SNAPSHOT/spark-sketch_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-catalyst_2.13/4.0.0-dswdia-SNAPSHOT/spark-catalyst_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/orc/orc-core/1.9.2/orc-core-1.9.2-shaded-protobuf.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/orc/orc-mapreduce/1.9.2/orc-mapreduce-1.9.2-shaded-protobuf.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/hive/hive-storage-api/2.8.1/hive-storage-api-2.8.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/parquet/parquet-column/1.13.1/parquet-column-1.13.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.13.1/parquet-hadoop-1.13.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/ws/xmlschema/xmlschema-core/2.3.0/xmlschema-core-2.3.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/xbean/xbean-asm9-shaded/4.24/xbean-asm9-shaded-4.24.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/tukaani/xz/1.9/xz-1.9.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka-server-common/3.6.1/kafka-server-common-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka-group-coordinator/3.6.1/kafka-group-coordinator-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka-metadata/3.6.1/kafka-metadata-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka-storage-api/3.6.1/kafka-storage-api-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka-tools-api/3.6.1/kafka-tools-api-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka-raft/3.6.1/kafka-raft-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/kafka/kafka-storage/3.6.1/kafka-storage-3.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/net/sourceforge/argparse4j/argparse4j/0.7.0/argparse4j-0.7.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-validator/commons-validator/1.7/commons-validator-1.7.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.13.5/jackson-dataformat-csv-2.13.5.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.14.3/jackson-datatype-jdk8-2.14.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/net/sf/jopt-simple/jopt-simple/5.0.4/jopt-simple-5.0.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/bitbucket/b_c/jose4j/0.9.3/jose4j-0.9.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/modules/scala-collection-compat_2.13/2.10.0/scala-collection-compat_2.13-2.10.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/modules/scala-java8-compat_2.13/1.0.2/scala-java8-compat_2.13-1.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/scala-logging/scala-logging_2.13/3.9.4/scala-logging_2.13-3.9.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.10/slf4j-api-2.0.10.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-cli/commons-cli/1.4/commons-cli-1.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.2.4/httpcore5-5.2.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.2.4/httpcore5-h2-5.2.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/packages.confluent.io/maven/io/confluent/kafka-avro-serializer/6.2.1/kafka-avro-serializer-6.2.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/6.2.1/kafka-schema-registry-client-6.2.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/za/co/absa/commons/commons_2.13/1.0.0/commons_2.13-1.0.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.16.1/jackson-core-2.16.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/json/json/20231013/json-20231013.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/code/gson/gson/2.10.1/gson-2.10.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cloud/cloudant-common/0.8.1/cloudant-common-0.8.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cloud/sdk-core/9.19.0/sdk-core-9.19.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/config/1.4.3/config-1.4.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cos/ibm-cos-java-sdk/2.13.2/ibm-cos-java-sdk-2.13.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cloud/secrets-manager/2.0.5/secrets-manager-2.0.5.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/zaxxer/HikariCP/2.5.1/HikariCP-2.5.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/sun/mail/jakarta.mail/2.0.1/jakarta.mail-2.0.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.22.0/log4j-core-2.22.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.16.1/jackson-annotations-2.16.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/thoughtworks/paranamer/paranamer/2.8/paranamer-2.8.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/lihaoyi/geny_2.13/1.0.0/geny_2.13-1.0.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/lihaoyi/upickle-core_2.13/3.1.4/upickle-core_2.13-3.1.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-beanutils/commons-beanutils/1.9.4/commons-beanutils-1.9.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/play/play-functional_2.13/2.10.4/play-functional_2.13-2.10.4.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jsr310/2.15.1/jackson-datatype-jsr310-2.15.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-http-core_2.13/10.5.3/akka-http-core_2.13-10.5.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/avro/avro-ipc/1.11.3/avro-ipc-1.11.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/esotericsoftware/kryo-shaded/4.0.2/kryo-shaded-4.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/fusesource/leveldbjni/leveldbjni-all/1.8/leveldbjni-all-1.8.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.61.Final/netty-tcnative-boringssl-static-2.0.61.Final-linux-x86_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.61.Final/netty-tcnative-boringssl-static-2.0.61.Final-linux-aarch_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.61.Final/netty-tcnative-boringssl-static-2.0.61.Final-osx-aarch_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.61.Final/netty-tcnative-boringssl-static-2.0.61.Final-osx-x86_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/crypto/tink/tink/1.9.0/tink-1.9.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/ivy/ivy/2.5.1/ivy-2.5.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/oro/oro/2.0.8/oro-2.0.8.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/slf4j/jul-to-slf4j/2.0.10/jul-to-slf4j-2.0.10.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/slf4j/jcl-over-slf4j/2.0.10/jcl-over-slf4j-2.0.10.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j2-impl/2.22.0/log4j-slf4j2-impl-2.22.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.22.0/log4j-api-2.22.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/logging/log4j/log4j-1.2-api/2.22.0/log4j-1.2-api-2.22.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/curator/curator-framework/5.2.0/curator-framework-5.2.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/zookeeper/zookeeper-jute/3.9.1/zookeeper-jute-3.9.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/yetus/audience-annotations/0.13.0/audience-annotations-0.13.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.61.Final/netty-tcnative-boringssl-static-2.0.61.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/json4s/json4s-core_2.13/3.7.0-M11/json4s-core_2.13-3.7.0-M11.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/jakarta/ws/rs/jakarta.ws.rs-api/2.1.6/jakarta.ws.rs-api-2.1.6.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/hk2/external/jakarta.inject/2.6.1/jakarta.inject-2.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/jakarta/annotation/jakarta.annotation-api/1.3.5/jakarta.annotation-api-1.3.5.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/hk2/osgi-resource-locator/1.0.3/osgi-resource-locator-1.0.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/jakarta/validation/jakarta.validation-api/2.0.2/jakarta.validation-api-2.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/hk2/hk2-locator/2.6.1/hk2-locator-2.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/javassist/javassist/3.29.2-GA/javassist-3.29.2-GA.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-buffer/4.1.106.Final/netty-buffer-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-codec/4.1.106.Final/netty-codec-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-codec-http/4.1.106.Final/netty-codec-http-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-codec-http2/4.1.106.Final/netty-codec-http2-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-codec-socks/4.1.106.Final/netty-codec-socks-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-common/4.1.106.Final/netty-common-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-handler/4.1.106.Final/netty-handler-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-native-unix-common/4.1.106.Final/netty-transport-native-unix-common-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-handler-proxy/4.1.106.Final/netty-handler-proxy-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-resolver/4.1.106.Final/netty-resolver-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport/4.1.106.Final/netty-transport-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-classes-epoll/4.1.106.Final/netty-transport-classes-epoll-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-classes-kqueue/4.1.106.Final/netty-transport-classes-kqueue-4.1.106.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-transport-native-epoll/4.1.106.Final/netty-transport-native-epoll-4.1.106.Final-linux-riscv64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/org/apache/spark/spark-sql-api_2.13/4.0.0-dswdia-SNAPSHOT/spark-sql-api_2.13-4.0.0-dswdia-SNAPSHOT.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/codehaus/janino/janino/3.1.9/janino-3.1.9.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/codehaus/janino/commons-compiler/3.1.9/commons-compiler-3.1.9.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/jaxb/txw2/3.0.2/txw2-3.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/datasketches/datasketches-java/5.0.1/datasketches-java-5.0.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/orc/orc-shims/1.9.2/orc-shims-1.9.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/airlift/aircompressor/0.25/aircompressor-0.25.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/jetbrains/annotations/17.0.0/annotations-17.0.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/threeten/threeten-extra/1.7.1/threeten-extra-1.7.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/parquet/parquet-common/1.13.1/parquet-common-1.13.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/parquet/parquet-encoding/1.13.1/parquet-encoding-1.13.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/parquet/parquet-format-structures/1.13.1/parquet-format-structures-1.13.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/parquet/parquet-jackson/1.13.1/parquet-jackson-1.13.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/pcollections/pcollections/4.0.1/pcollections-4.0.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/github/ben-manes/caffeine/caffeine/2.9.3/caffeine-2.9.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/commons-digester/commons-digester/2.1/commons-digester-2.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/packages.confluent.io/maven/io/confluent/kafka-schema-serializer/6.2.1/kafka-schema-serializer-6.2.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/packages.confluent.io/maven/io/confluent/common-utils/6.2.1/common-utils-6.2.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/swagger/swagger-annotations/1.6.2/swagger-annotations-1.6.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/swagger/swagger-core/1.6.2/swagger-core-1.6.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/4.12.0/okhttp-4.12.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/squareup/okhttp3/logging-interceptor/4.12.0/logging-interceptor-4.12.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/squareup/okhttp3/okhttp-urlconnection/4.12.0/okhttp-urlconnection-4.12.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/reactivex/rxjava2/rxjava/2.2.21/rxjava-2.2.21.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib-jdk8/1.9.10/kotlin-stdlib-jdk8-1.9.10.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib/1.9.22/kotlin-stdlib-1.9.22.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cos/ibm-cos-java-sdk-s3/2.13.2/ibm-cos-java-sdk-s3-2.13.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cos/ibm-cos-java-sdk-kms/2.13.2/ibm-cos-java-sdk-kms-2.13.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cos/ibm-cos-java-sdk-core/2.13.2/ibm-cos-java-sdk-core-2.13.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/ibm/cloud/secrets-manager-sdk-common/2.0.5/secrets-manager-sdk-common-2.0.5.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/sun/activation/jakarta.activation/2.0.1/jakarta.activation-2.0.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-parsing_2.13/10.5.3/akka-parsing_2.13-10.5.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/esotericsoftware/minlog/1.3.0/minlog-1.3.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/objenesis/objenesis/2.5.1/objenesis-2.5.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-tcnative-classes/2.0.61.Final/netty-tcnative-classes-2.0.61.Final.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.61.Final/netty-tcnative-boringssl-static-2.0.61.Final-windows-x86_64.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.19.6/protobuf-java-3.19.6.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/curator/curator-client/5.2.0/curator-client-5.2.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/json4s/json4s-ast_2.13/3.7.0-M11/json4s-ast_2.13-3.7.0-M11.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/json4s/json4s-scalap_2.13/3.7.0-M11/json4s-scalap_2.13-3.7.0-M11.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/hk2/external/aopalliance-repackaged/2.6.1/aopalliance-repackaged-2.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/hk2/hk2-api/2.6.1/hk2-api-2.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/hk2/hk2-utils/2.6.1/hk2-utils-2.6.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/modules/scala-parser-combinators_2.13/2.3.0/scala-parser-combinators_2.13-2.3.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.13.1/antlr4-runtime-4.13.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/arrow/arrow-vector/14.0.2/arrow-vector-14.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/arrow/arrow-memory-netty/14.0.2/arrow-memory-netty-14.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/datasketches/datasketches-memory/2.2.0/datasketches-memory-2.2.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/checkerframework/checker-qual/3.19.0/checker-qual-3.19.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/errorprone/error_prone_annotations/2.10.0/error_prone_annotations-2.10.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/2.11.1/jackson-dataformat-yaml-2.11.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/swagger/swagger-models/1.6.2/swagger-models-1.6.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/squareup/okio/okio/3.6.0/okio-3.6.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/reactivestreams/reactive-streams/1.0.3/reactive-streams-1.0.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib-jdk7/1.9.10/kotlin-stdlib-jdk7-1.9.10.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/javax/annotation/javax.annotation-api/1.3.2/javax.annotation-api-1.3.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/sun/xml/bind/jaxb-core/2.3.0.1/jaxb-core-2.3.0.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/sun/xml/bind/jaxb-impl/2.3.8/jaxb-impl-2.3.8.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.14/httpclient-4.5.14.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/software/amazon/ion/ion-java/1.5.1/ion-java-1.5.1.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.15.2/jackson-dataformat-cbor-2.15.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/arrow/arrow-format/14.0.2/arrow-format-14.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/arrow/arrow-memory-core/14.0.2/arrow-memory-core-14.0.2.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/flatbuffers/flatbuffers-java/1.12.0/flatbuffers-java-1.12.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/squareup/okio/okio-jvm/3.6.0/okio-jvm-3.6.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/javax/activation/javax.activation-api/1.2.0/javax.activation-api-1.2.0.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/jakarta/xml/bind/jakarta.xml.bind-api/2.3.3/jakarta.xml.bind-api-2.3.3.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ch/qos/logback/logback-core/1.2.10/logback-core-1.2.10.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ch/qos/logback/logback-classic/1.2.10/logback-classic-1.2.10.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib-common/1.9.10/kotlin-stdlib-common-1.9.10.jar [exists ]
Options:
-Yrangepos -Xplugin-require:semanticdb


action parameters:
offset: 29812
uri: file://<WORKSPACE>/src/main/scala/com/ibm/dswdiaods/pricing_onprem/OnPrem.scala
text:
```scala
/*
Name: OnPrem.scala
Description: Logic to handle OnPrem data part information
Created by: Alvaro Gonzalez <alvaro.glez@ibm.com>
Created Date: 2021/06/14
Notes:
Modification:
    date        owner       description
	20221210 	Ricardo		Onprem modification to finish the logic
	20230123	Ricardo		GPH logic added
*/
package com.ibm.dswdiaods.pricing_onprem

import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties.{db2_ods_properties_streaming}
import OnPrem.insert_record
import Redis.onprem_primary_keys_check
import ApiConnector._

import com.opencsv.CSVWriter
import play.api.libs.json.{JsNull, JsObject, JsValue}
import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import java.sql.Types
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat
import scala.util.{Failure, Success}
import org.apache.logging.log4j.{LogManager,Logger}
import org.apache.spark.sql.execution.columnar.INT
import com.ibm.db2.cmx.runtime.Data
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.ContentTypes
import java.io._


object OnPrem {
	/**
	  * Validates data incoming from Kafka, when data is valid
	  * this is loaded to base tables in ODS: wwpp1.cntry_price
	  * and wwpp1.cntry_price_tierd.
	  * When data is not valid this is loaded to error tables:
	  * wwpp1.cntry_price_err and wwpp1.cntry_price_err
	  * as well as utol.error_log
	  *
	  * If the data have an start date > than the actual date
	  * is going to be inserted in the wwpp1.futr_cntry_price
	  * except if it is an GPH record.
	  *
	  * @param <code>raw_record</code> an org.apache.spark.sql.Row containing
	  * pricing data from Kafka
	  *
	  * @param  <code>table_name</code> a String containing the table name
	  *
	  * @param pool Core Database object
	  *
	  * @return Boolean -> True for normal exec, False if an error happen
	  */

	val logger: Logger = LogManager.getLogger(this.getClass())

	def cntry_price_upsert(raw_record: org.apache.spark.sql.Row, table_name: String, pool: Database): Boolean = {
		var topic = raw_record.getAs[String]("topic").replace(".", " ").split(" ")

		val base_table = table_name
		var error_table = s"${base_table}_err"

		val futr_table = s"futr_${base_table}"

		val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("CNTRY_CODE"),
			raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			raw_record.getAs[String]("PRICE_START_DATE").replace(" ", "")
		)

		var sap_extrct_date = raw_record.getAs[String]("SAP_EXTRCT_DATE")
		sap_extrct_date = sap_extrct_date.replace(" ", "-") // replace space to dash
												// from '2021-05-04 05:04:07.241965000000'
												// to   '2021-05-04-05:04:07.241965000000'
		sap_extrct_date = sap_extrct_date.replace(":", ".") // replace colon to dash
												// from '2021-05-04 05:04:07.241965000000'
												// to   '2021-05-04-05.04.07.241965000000'
		sap_extrct_date = sap_extrct_date.dropRight(6) // delete extra zeros added as miliseconds
												// from '2021-05-04-05.04.07.241965000000'
												// to   '2021-05-04-05.04.07.241965'

		val record: Map[String, Any] = Map(
			"part_num" 					-> raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			"cntry_code" 				-> raw_record.getAs[String]("CNTRY_CODE"),
			"iso_currncy_code" 			-> raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			"sap_distribtn_chnl_code" 	-> raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			"price_start_date" 			-> raw_record.getAs[String]("PRICE_START_DATE"),
			"price_end_date" 			-> raw_record.getAs[String]("PRICE_END_DATE"),
			"srp_price" 				-> raw_record.getAs[Double]("SRP_PRICE"),
			"svp_level_a" 				-> raw_record.getAs[Double]("SVP_LEVEL_A"),
			"svp_level_b" 				-> raw_record.getAs[Double]("SVP_LEVEL_B"),
			"svp_level_c" 				-> raw_record.getAs[Double]("SVP_LEVEL_C"),
			"svp_level_d" 				-> raw_record.getAs[Double]("SVP_LEVEL_D"),
			"svp_level_e" 				-> raw_record.getAs[Double]("SVP_LEVEL_E"),
			"svp_level_f" 				-> raw_record.getAs[Double]("SVP_LEVEL_F"),
			"svp_level_g" 				-> raw_record.getAs[Double]("SVP_LEVEL_G"),
			"svp_level_h" 				-> raw_record.getAs[Double]("SVP_LEVEL_H"),
			"svp_level_i" 				-> raw_record.getAs[Double]("SVP_LEVEL_I"),
			"svp_level_j" 				-> raw_record.getAs[Double]("SVP_LEVEL_J"),
			"svp_level_ed" 				-> raw_record.getAs[Double]("SVP_LEVEL_ED"),
			"svp_level_gv" 				-> raw_record.getAs[Double]("SVP_LEVEL_GV"),
			"gph_status" 				-> raw_record.getAs[Int]("GPH_STATUS"),
			"sap_extrct_date" 			-> sap_extrct_date
		)

		// Step 1: Validate existing part number in error table
		val err_seq_num: String = get_err_seq_num(error_table, primary_keys, pool)

		if (err_seq_num != ""){
			delete_from_error_log(err_seq_num, pool)
			delete_from_table(error_table, primary_keys, pool)
		}

		// Step 2: Validate primary keys
		if (!validate_primary_keys(primary_keys, base_table, record, pool)){
			return false
		}

		// Step 3: Validate if current price does exist, if so, update its end date to
		// be equal to incoming start date - 1 day

		if (is_update(base_table, primary_keys, pool)){
			update_record(base_table, record, pool)
		} else {
			val current_timestamp = DateTimeFormatter.ofPattern("YYYY-MM-dd").format(LocalDateTime.now())
			val dateformat = new SimpleDateFormat("yyyy-MM-dd")
			val today_date = dateformat.parse(current_timestamp)
			// If the start_date is greater than the local date the record is going to be inserted into the futr table
			// If it is an GPH record is going to be inserted in the bu table
			if(dateformat.parse(raw_record.getAs[String]("PRICE_START_DATE")).compareTo(today_date) >  0 && raw_record.getAs[String]("GPH_STATUS") == '0'){
				if (is_update(futr_table, primary_keys, pool)){
					update_record(futr_table, record, pool)
				}else {
					insert_record(futr_table, record, pool)
				}
			}else{
				insert_record(base_table, record, pool)
			}
		}

		return true
	}


	/**
	  * Validates data incoming from Kafka, when data is valid
	  * this is loaded to base tables in ODS: wwpp1.cntry_price
	  * and wwpp1.cntry_price_tierd.
	  * When data is not valid this is loaded to error tables:
	  * wwpp1.cntry_price_err and wwpp1.cntry_price_err
	  * as well as utol.error_log
	  *
	  * @param <code>record</code> an org.apache.spark.sql.Row containing
	  * pricing data from Kafka
	  *
	  * @param <code>table_name</code> an string containing
	  * the table name
	  *
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def cntry_price_delete(raw_record: org.apache.spark.sql.Row, table_name: String, pool: Database): Boolean = {
		var topic = raw_record.getAs[String]("topic").replace(".", " ").split(" ")
		val base_table = table_name

		val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("CNTRY_CODE"),
			raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			raw_record.getAs[String]("PRICE_START_DATE")
		)

		val affected_rows = delete_from_table(base_table , primary_keys, pool)

		if (affected_rows < 0){
			logger.info(s"There was a problem with the delete.")
			return false
		}

		logger.info(s"${primary_keys(0)} was deleted properly.")

		return true
	}

	/**
	  * Get error sequence number from error table
	  *
	  * @param table String with the table name
	  * @param parameters Array[String] with the PK value
	  * @param pool Core Database object
	  *
	  * @return String
	  */
	def get_err_seq_num(table: String, parameters: Array[String], pool: Database): String = {

		val query = s"""SELECT err_seq_num
						FROM   wwpp1.${table}
						WHERE  part_num                = '${parameters(0)}'
						AND    cntry_code              = '${parameters(1)}'
						AND    iso_currncy_code        = '${parameters(2)}'
						AND    sap_distribtn_chnl_code = '${parameters(3)}'
						AND    price_start_date        = '${parameters(4)}'"""


		val result = pool.run(query)

		if(result.isEmpty){return ""}

		var err_seq_num: String = result.apply(0).get("ERR_SEQ_NUM").toString()

		return err_seq_num
	}

	/**
	  * Delete record in the error log table
	  *
	  * @param err_seq_num String with the num of error
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def delete_from_error_log(err_seq_num: String, pool: Database): Unit = {
		val delete = s"DELETE FROM utol.error_log WHERE err_seq_num = $err_seq_num"
		val result = pool.run(delete)
		val affected_rows: Int = result.apply(0).get("Affected rows").toInt
	}

	/**
	  * Delete record from given table
	  *
	  * @param table String with the table name
	  * @param parameters Array[String] with the PK value
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def delete_from_table(table: String, parameters: Array[String], pool: Database): Int = {

		val delete: String = s"""DELETE FROM wwpp1.${table}
								WHERE  part_num                = '${parameters(0)}'
								AND    cntry_code              = '${parameters(1)}'
								AND    iso_currncy_code        = '${parameters(2)}'
								AND    sap_distribtn_chnl_code = '${parameters(3)}'
								AND    price_start_date        = '${parameters(4)}'"""

		val result = pool.run(delete)
		val affected_rows: Int = result.apply(0).get("Affected rows").toInt

		return affected_rows
	}

	/**
	 * Validate primary keys
	 * First validate if the record is the redis key
	 * And if there is not, then validate if the record is in the table
	 * @param primary_keys Array[String] with the PK value
	 * @param base_table String with the table name (base)
	 * @param record Map[String, any] with the values to be validate
	 * @param pool Core Database object
	 *
	 * @return Boolean
	 */
	def validate_primary_keys(primary_keys: Array[String], base_table: String, record: Map[String, Any], pool: Database): Boolean = {
		// REDIS validation will be used for MVP2
		val redis_response = onprem_primary_keys_check(primary_keys)

		// Validate incoming part number against WWPP2.WWIDE_FNSHD_PART
		if (redis_response(0) == false){
			if (!validate_part_number(primary_keys(0), pool)){
				val col_name = "part_num"
				val error_description = "part_num is missing in wwpp2.wwide_fnshd_part"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		// Validate incoming country code against SHAR2.CODE_DSCR
		if (redis_response(1) == false){
			if (!validate_cntry_code(primary_keys(1), pool)){
				val col_name = "cntry_code"
				val error_description = "cntry_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		// Validate incoming iso currncy code against SHAR2.CODE_DSCR
		if (redis_response(2) == false){
			if (!validate_iso_currency_code(primary_keys(2), pool)){
				val col_name = "iso_currncy_code"
				val error_description = "iso_currncy_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		// Validate incoming sap distribution channel code against SHAR2.CODE_DSCR
		if (redis_response(3) == false){
			if (!validate_sap_distribtn_chnl_code(primary_keys(3), pool)){
				val col_name = "sap_distribtn_chnl_code"
				val error_description = "sap_distribtn_chnl_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		return true
	}

	/**
	  * Get current timestamp
	  *
	  * @return String
	  */
	def get_current_timestamp(): String = {
		
		val current_timestamp = DateTimeFormatter
						.ofPattern("YYYY-MM-dd-HH.mm.ss.SSSSSS")
						.format(LocalDateTime.now)
		return current_timestamp
	}

	/**
	  * Check if is valid the record
	  *
	  * @param query String with the select statement
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def is_valid(query: String, pool: Database): Boolean = {
		val result = pool.run(query)

		if(result.isEmpty){ return false }

		return true
	}

	/**
	  * Validate part number
	  *
	  * @param part_num String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_part_number(part_num: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM WWPP2.WWIDE_FNSHD_PART
								WHERE part_num = '${part_num}'"""

		return is_valid(query, pool)
	}

	/**
	  * Validate country code
	  *
	  * @param cntry_code String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_cntry_code(cntry_code: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM  shar2.code_dscr
								WHERE col_name = 'cntry_code'
								AND   code = '${cntry_code}'"""

		return is_valid(query, pool)
	}

	/**
	  * Validate iso currency code
	  *
	  * @param iso_currncy_code String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_iso_currency_code(iso_currncy_code: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM shar2.code_dscr
								WHERE col_name = 'iso_currncy_code'
								AND   code = '${iso_currncy_code}'"""

		return is_valid(query, pool)
	}

	/**
	  * Validate SAP distribution channel code
	  *
	  * @param sap_distribtn_chnl_code String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_sap_distribtn_chnl_code(sap_distribtn_chnl_code: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM  shar2.code_dscr
								WHERE col_name = 'sap_distribtn_chnl_code'
								AND   code = '${sap_distribtn_chnl_code}'"""

		return is_valid(query, pool)
	}

	/**
	  * Insert record in error log table with the sp
	  *
	  * @param parameters Array[Any] with the PK values
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def insert_error_log(parameters: Array[Any], pool: Database): String = {
		var statement = "CALL utol.dp_insrt_error_log(p_error_date=>?, p_table_name=>?, p_col_name=>?, p_error_dscr=>?, p_err_seq_num=>?, o_status=>?)"
		val conn = pool.ds.getConnection()

		val call_statement = conn.prepareCall(statement)

		call_statement.setString("p_error_date", parameters(0).toString())
		call_statement.setString("p_table_name", parameters(1).toString().split("\\.")(1)) //takes just the name of table 
		call_statement.setString("p_col_name", parameters(2).toString())
		call_statement.setString("p_error_dscr", parameters(3).toString())
		call_statement.setInt("p_err_seq_num", parameters(4).asInstanceOf[Int])

		call_statement.registerOutParameter("o_status", Types.VARCHAR)
		call_statement.executeUpdate()
		return call_statement.getString("p_err_seq_num")
	}

	/**
		* Insert record in the error table in case that one of the PK are missing.
		*
		* @param table String with the name of the table
		* @param record Map[String, Any] with the values to be inserted.
		* @param err_seq_num String with the err num generated
		* @param pool Core Database object
		*
		* @return Int
		*/
	def insert_record_err(table: String, record: Map[String, Any], err_seq_num: BigInt, pool: Database): Int = {
		var affected_rows = -1
		val current_timestamp = get_current_timestamp()

		val insert = s"""INSERT INTO wwpp1.${table}_err (
							part_num,
							cntry_code,
							iso_currncy_code,
							sap_distribtn_chnl_code,
							price_start_date,
							price_end_date,
							srp_price,
							svp_level_a,
							svp_level_b,
							svp_level_c,
							svp_level_d,
							svp_level_e,
							svp_level_f,
							svp_level_g,
							svp_level_h,
							svp_level_i,
							svp_level_j,
							svp_level_ed,
							svp_level_gv,
							sap_extrct_date,
							sap_ods_add_date,
							sap_ods_mod_date,
							err_seq_num
						) VALUES (
							'${record("part_num")}',
							'${record("cntry_code")}',
							'${record("iso_currncy_code")}',
							'${record("sap_distribtn_chnl_code")}',
							'${record("price_start_date")}',
							'${record("price_end_date")}',
							${record("srp_price")},
							${record("svp_level_a")},
							${record("svp_level_b")},
							${record("svp_level_c")},
							${record("svp_level_d")},
							${record("svp_level_e")},
							${record("svp_level_f")},
							${record("svp_level_g")},
							${record("svp_level_h")},
							${record("svp_level_i")},
							${record("svp_level_j")},
							${record("svp_level_ed")},
							${record("svp_level_gv")},
							'${record("sap_extrct_date")}',
							'${current_timestamp}',
							'${current_timestamp}',
							${err_seq_num}
						)"""

		val result = pool.run(insert)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		if (affected_rows < 0){
			val message = s"""Problem with wwpp1.${table} insert for:
			part_num                  	= '${record("part_num")}'
			and cntry_code              = '${record("cntry_code")}'
			and iso_currncy_code        = '${record("iso_currncy_code")}'
			and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
			and price_start_date        = '${record("price_start_date")}'
			"""

			logger.error(message)

			return -1
		}

		logger.info(s"Part: ${record("part_num")} succesfully inserted in the Err table")
		return affected_rows
	}

	/**
	  * Prepare error record for error log and generate the err_seq_num
	  *
	  * @param col_name String with the name of the column missed
	  * @param base_table String with the table of the process
	  * @param error_description String with the description of the error
	  * @param record Map[String, Any] with the values
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def set_invalid_part(col_name: String, base_table: String, error_description: String, record: Map[String, Any], pool: Database): Unit = {
		val error_id = 0
		val current_timestamp = get_current_timestamp().replace(" ", "-").replace(":", ".")
		val sp_parameters = Array[Any](
			current_timestamp,
			base_table,
			col_name,
			error_description,
			error_id
		)

		insert_error_log(sp_parameters, pool)

		val query = s"""SELECT err_seq_num
						FROM   utol.error_log
						WHERE  error_date	= '${current_timestamp}'
						AND    table_name	= '${base_table}'
						AND    error_dscr	= '${error_description}'"""


		val result = pool.run(query)

		var err_seq_num: BigInt = BigInt(result.apply(0).get("ERR_SEQ_NUM"))

		insert_record_err(base_table, record, err_seq_num, pool)
	}

	/**
	  * Validate if record exists on given table
	  *
	  * @param table String with the table name
	  * @param parameters Array[String] with the PK values
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def is_update(table: String, parameters: Array[String], pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM  wwpp1.${table}
								WHERE part_num                = '${parameters(0)}'
								AND   cntry_code              = '${parameters(1)}'
								AND   iso_currncy_code        = '${parameters(2)}'
								AND   sap_distribtn_chnl_code = '${parameters(3)}'
								AND   price_start_date        = '${parameters(4)}'"""

		return is_valid(query, pool)
	}

	/**
	  * Insert record in given table, main method of the process
	  *
	  * @param table String with the table name
	  * @param record Map[String, Any] wit the values to be inserted
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def insert_record(table: String, record: Map[String, Any], pool: Database): Int = {
		var affected_rows = -1
		val current_timestamp = get_current_timestamp()

		val insert = s"""INSERT INTO wwpp1.${table} (
							part_num,
							cntry_code,
							iso_currncy_code,
							sap_distribtn_chnl_code,
							price_start_date,
							price_end_date,
							srp_price,
							svp_level_a,
							svp_level_b,
							svp_level_c,
							svp_level_d,
							svp_level_e,
							svp_level_f,
							svp_level_g,
							svp_level_h,
							svp_level_i,
							svp_level_j,
							svp_level_ed,
							svp_level_gv,
							sap_extrct_date,
							sap_ods_add_date,
							sap_ods_mod_date,
							gph_ind
						) VALUES (
							'${record("part_num")}',
							'${record("cntry_code")}',
							'${record("iso_currncy_code")}',
							'${record("sap_distribtn_chnl_code")}',
							'${record("price_start_date")}',
							'${record("price_end_date")}',
							${record("srp_price")},
							${record("svp_level_a")},
							${record("svp_level_b")},
							${record("svp_level_c")},
							${record("svp_level_d")},
							${record("svp_level_e")},
							${record("svp_level_f")},
							${record("svp_level_g")},
							${record("svp_level_h")},
							${record("svp_level_i")},
							${record("svp_level_j")},
							${record("svp_level_ed")},
							${record("svp_level_gv")},
							'${record("sap_extrct_date")}',
							'${current_timestamp}',
							'${current_timestamp}',
							${record("gph_status")}
						)"""

		val result = pool.run(insert)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		if (affected_rows < 0){
			val message = s"""Problem with wwpp1.${table} insert for:
			part_num                  	= '${record("part_num")}'
			and cntry_code              = '${record("cntry_code")}'
			and iso_currncy_code        = '${record("iso_currncy_code")}'
			and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
			and price_start_date        = '${record("price_start_date")}'
			"""

			logger.error(message)

			val col_name = "ROW"
			val error_description = "Error: Insert failed"

			set_invalid_part(col_name, table, error_description, record, pool)

			return -1
		}

		// Invalidate records with price_start_date greater than the currently loaded, we want to maintain only the lastest price cable feed as valid,
		// if any price cable was previously loaded its being dimissed, since biz can make price changes because of marketplace they want to keep only latest feed sent.

		val delete = s"""DELETE FROM wwpp1.${table}
							WHERE part_num              = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		> '${record("price_start_date")}'
							"""

		var affected_rows_delete = -1
		val result_delete = pool.run(delete)
		if(!result_delete.isEmpty){affected_rows_delete = result_delete(0).get("Affected rows").toInt}

		if (affected_rows_delete > 0){
			val message = s"""Record deleted wwpp1.${table} for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date        > '${record("price_start_date")}'"""

			//logger.info(message)
		}

		// if the price_start_date is older than current date, we want to keep 
		// ONLY the record being inserted and expire/delete the records with price start date older

		val delete_older = s"""DELETE FROM wwpp1.${table}
							WHERE part_num              = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		< '${record("price_start_date")}'
							and price_start_date 		< CURRENT DATE
							"""

		var affected_rows_delete_older = -1
		val result_delete_older = pool.run(delete_older)
		if(!result_delete_older.isEmpty){affected_rows_delete_older = result_delete_older(0).get("Affected rows").toInt}

		if (affected_rows_delete_older > 0){
			val message_older = s"""Record deleted wwpp1.${table} for:
								part_num              = '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date 		< '${record("price_start_date")}'
								and price_start_date 		< CURRENT DATE'"""

			//logger.info(message_older)
		}

		logger.info(s"Part: ${record("part_num")} succesfully inserted into wwpp1.${table}")
		return affected_rows
	}

	/**
	  * Update record in given table, secondary method of the process
	  *
	  * @param table String with the table name
	  * @param record Map[String, Any] with the values to be inserted
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def update_record(table: String, record: Map[String, Any], pool: Database): Int = {
		var affected_rows = -1
		val update = s"""UPDATE wwpp1.${table}
						SET price_end_date          	= '${record("price_end_date")}',
							srp_price               	= ${record("srp_price")},
							svp_level_a             	= ${record("svp_level_a")},
							svp_level_b             	= ${record("svp_level_b")},
							svp_level_c             	= ${record("svp_level_c")},
							svp_level_d             	= ${record("svp_level_d")},
							svp_level_e             	= ${record("svp_level_e")},
							svp_level_f             	= ${record("svp_level_f")},
							svp_level_g             	= ${record("svp_level_g")},
							svp_level_h             	= ${record("svp_level_h")},
							svp_level_i             	= ${record("svp_level_i")},
							svp_level_j             	= ${record("svp_level_j")},
							svp_level_ed            	= ${record("svp_level_ed")},
							svp_level_gv            	= ${record("svp_level_gv")},
							sap_extrct_date         	= '${record("sap_extrct_date")}',
							mod_by_user_name        	= 'ODSOPER',
							sap_ods_mod_date        	= '${get_current_timestamp()}',
							gph_ind            			= ${record("gph_status")}
						WHERE part_num                  = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		= '${record("price_start_date")}'
							and sap_extrct_date 		< '${record("sap_extrct_date")}'
							"""

		val result = pool.run(update)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		//logger.info("---- Aff ROWS update -----:" + affected_rows)

		if (affected_rows < 0){
			val message = s"""Problem with wwpp1.${table} update for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date        = '${record("price_start_date")}'"""

			logger.error(message)

			val col_name = "ROW"
			val error_description = "Error: Update failed"

			set_invalid_part(col_name, table, error_description, record, pool)

			return -1
		}
		// Invalidate records with price_start_date greater than the currently loaded, we want to maintain only the lastest price cable feed as valid,
		// if any price cable was previously loaded its being dimissed, since biz can make price changes because of marketplace they want to keep only latest feed sent.

		val delete = s"""DELETE FROM wwpp1.${table}
							WHERE part_num              = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		> '${record("price_start_date")}'
							"""

		var affected_rows_delete = -1
		val result_delete = pool.run(delete)
		if(!result_delete.isEmpty){affected_rows_delete = result_delete(0).get("Affected rows").toInt}

		if (affected_rows_delete > 0){
			val message = s"""Record deleted wwpp1.${table} for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date        > '${record("price_start_date")}'"""

			//logger.info(message)
		}

		logger.info(s"Part: ${record("part_num")} succesfully updated in wwpp1.${table}")
		return affected_rows
	}

	//----------------------------------------------------------------//
	def futr_pricing_csv_send(pool: Database, price_start_date: String): Boolean = {
		val query = "SELECT * FROM WWPP1.FUTR_CNTRY_PRICE WHERE PRICE_START_DATE >= " + price_start_date
		val result = pool.run(query)

		val csv_result = saveToCSV(result.toSeq)

		if(!csv_result){
			logger.error("Failed to save/write the csv result.")
			return false
		}

		return true
	}

	def saveToCSV(str: Seq[Array, A@@]): Boolean = {
		val outputFile = new BufferedWriter(new FileWriter("Result.csv"))
		val csvWriter = new CSVWriter(outputFile)
		val data = str.head
		data match {
			case JsObject(fields) => {
			var listOfRecords = new ListBuffer[Array[String]]()
			//val csvFields = Array("Open","High","Low","Close","Volume")
			//listOfRecords += csvFields
			fields.values.foreach(value => {
				val jsObject = value.as[JsObject]
				val nameList = List(jsObject("1. open").toString, jsObject("2. high").toString, jsObject("3. low").toString, jsObject("4. close").toString, jsObject("5. volume").toString)
				listOfRecords += Array(nameList.toString)
				csvWriter.writeAll(listOfRecords.toList.asJava)
				println("Written!")
				outputFile.close()
			})
			}
			case JsNull => println("Null")
							return false
		}

		return true
	}

	def writeToFile(content: String, campaignId: String): File = {
		val tempFolder = System.getProperty("tmp/csv/futr_pricing/")
		val file = new File(s"$tempFolder/retailers_$campaignId.csv")
		val bw = new BufferedWriter(new FileWriter(file))
		bw.write(content)
		bw.close()
		file
	}

}

```



#### Error stacktrace:

```
scala.collection.mutable.ArrayBuffer.apply(ArrayBuffer.scala:106)
	scala.reflect.internal.Types$Type.findMemberInternal$1(Types.scala:1030)
	scala.reflect.internal.Types$Type.findMember(Types.scala:1035)
	scala.reflect.internal.Types$Type.memberBasedOnName(Types.scala:661)
	scala.reflect.internal.Types$Type.member(Types.scala:625)
	scala.tools.nsc.typechecker.Contexts$SymbolLookup.nextDefinition$1(Contexts.scala:1435)
	scala.tools.nsc.typechecker.Contexts$SymbolLookup.apply(Contexts.scala:1636)
	scala.tools.nsc.typechecker.Contexts$Context.lookupSymbol(Contexts.scala:1286)
	scala.tools.nsc.typechecker.Typers$Typer.typedIdent$2(Typers.scala:5572)
	scala.tools.nsc.typechecker.Typers$Typer.typedIdentOrWildcard$1(Typers.scala:5631)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6095)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6153)
	scala.tools.nsc.typechecker.Typers$Typer.typedType(Typers.scala:6337)
	scala.tools.nsc.typechecker.Typers$Typer.typedType(Typers.scala:6340)
	scala.tools.nsc.typechecker.Namers$Namer.valDefSig(Namers.scala:1790)
	scala.tools.nsc.typechecker.Namers$Namer.memberSig(Namers.scala:1976)
	scala.tools.nsc.typechecker.Namers$Namer.typeSig(Namers.scala:1926)
	scala.tools.nsc.typechecker.Namers$Namer$MonoTypeCompleter.completeImpl(Namers.scala:874)
	scala.tools.nsc.typechecker.Namers$LockingTypeCompleter.complete(Namers.scala:2123)
	scala.tools.nsc.typechecker.Namers$LockingTypeCompleter.complete$(Namers.scala:2121)
	scala.tools.nsc.typechecker.Namers$TypeCompleterBase.complete(Namers.scala:2116)
	scala.reflect.internal.Symbols$Symbol.completeInfo(Symbols.scala:1565)
	scala.reflect.internal.Symbols$Symbol.info(Symbols.scala:1537)
	scala.tools.nsc.typechecker.Namers$DependentTypeChecker.$anonfun$check$2(Namers.scala:2201)
	scala.tools.nsc.typechecker.Namers$DependentTypeChecker.$anonfun$check$1(Namers.scala:2200)
	scala.tools.nsc.typechecker.Namers$DependentTypeChecker.check(Namers.scala:2199)
	scala.tools.nsc.typechecker.Namers$Namer.methodSig(Namers.scala:1482)
	scala.tools.nsc.typechecker.Namers$Namer.memberSig(Namers.scala:1975)
	scala.tools.nsc.typechecker.Namers$Namer.typeSig(Namers.scala:1926)
	scala.tools.nsc.typechecker.Namers$Namer$MonoTypeCompleter.completeImpl(Namers.scala:874)
	scala.tools.nsc.typechecker.Namers$LockingTypeCompleter.complete(Namers.scala:2123)
	scala.tools.nsc.typechecker.Namers$LockingTypeCompleter.complete$(Namers.scala:2121)
	scala.tools.nsc.typechecker.Namers$TypeCompleterBase.complete(Namers.scala:2116)
	scala.reflect.internal.Symbols$Symbol.completeInfo(Symbols.scala:1565)
	scala.reflect.internal.Symbols$Symbol.info(Symbols.scala:1537)
	scala.reflect.internal.Symbols$Symbol.tpeHK(Symbols.scala:1491)
	scala.reflect.internal.Types$Type.computeMemberType(Types.scala:724)
	scala.reflect.internal.Symbols$MethodSymbol.typeAsMemberOf(Symbols.scala:3085)
	scala.reflect.internal.Types$Type.memberType(Types.scala:719)
	scala.tools.nsc.interactive.Global$Members.$anonfun$add$1(Global.scala:1015)
	scala.tools.nsc.interactive.Global$OnTypeError.onTypeError(Global.scala:1388)
	scala.tools.nsc.interactive.Global$Members.add(Global.scala:1015)
	scala.tools.nsc.interactive.Global.addScopeMember$1(Global.scala:1044)
	scala.tools.nsc.interactive.Global.$anonfun$scopeMembers$3(Global.scala:1072)
	scala.tools.nsc.interactive.Global.$anonfun$scopeMembers$3$adapted(Global.scala:1071)
	scala.reflect.internal.Scopes$Scope.foreach(Scopes.scala:455)
	scala.tools.nsc.interactive.Global.scopeMembers(Global.scala:1071)
	scala.tools.nsc.interactive.Global.completionsAt(Global.scala:1285)
	scala.meta.internal.pc.SignatureHelpProvider.$anonfun$treeSymbol$1(SignatureHelpProvider.scala:390)
	scala.Option.map(Option.scala:242)
	scala.meta.internal.pc.SignatureHelpProvider.treeSymbol(SignatureHelpProvider.scala:388)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCall$.unapply(SignatureHelpProvider.scala:185)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCallTraverser.visit(SignatureHelpProvider.scala:316)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCallTraverser.traverse(SignatureHelpProvider.scala:310)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCallTraverser.fromTree(SignatureHelpProvider.scala:279)
	scala.meta.internal.pc.SignatureHelpProvider.signatureHelp(SignatureHelpProvider.scala:27)
	scala.meta.internal.pc.ScalaPresentationCompiler.$anonfun$signatureHelp$1(ScalaPresentationCompiler.scala:310)
```
#### Short summary: 

java.lang.IndexOutOfBoundsException: -1 is out of bounds (min 0, max 2)