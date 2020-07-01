(defproject com.github.csm/crux-ddb-s3 "0.1.0-SNAPSHOT"
  :description "Crux TxLog on DynamoDB and S3"
  :url "https://github.com/csm/crux-ddb-s3"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core "20.06-1.9.1-beta"]
                 [software.amazon.awssdk/dynamodb "2.13.41"]
                 [software.amazon.awssdk/s3 "2.13.41"]]
  :java-source-paths ["src"]
  :javac-options ["-target" "8" "-source" "8"]
  :repl-options {:init-ns crux-ddb-s3.core})
