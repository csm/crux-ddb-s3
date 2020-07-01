(ns crux.ddb-s3.impl
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient)
           (software.amazon.awssdk.services.dynamodb.model AttributeValue GetItemResponse GetItemRequest ConditionalCheckFailedException PutItemRequest UpdateItemRequest AttributeDefinition ScalarAttributeType KeySchemaElement KeyType CreateTableRequest ProvisionedThroughput DescribeTableRequest ResourceNotFoundException DescribeTableResponse TableStatus)
           (java.util.function Function)
           (java.util.concurrent CompletableFuture TimeUnit Executors ScheduledExecutorService)
           (crux.ddb_s3 DDBS3Configurator)
           (java.util Collection Date)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model HeadBucketRequest NoSuchBucketException CreateBucketRequest CreateBucketConfiguration BucketLocationConstraint ListObjectsV2Request ListObjectsV2Response S3Object GetObjectRequest PutObjectRequest)
           (software.amazon.awssdk.regions.providers DefaultAwsRegionProviderChain)
           (software.amazon.awssdk.core.async AsyncResponseTransformer AsyncRequestBody)
           (software.amazon.awssdk.core ResponseBytes)))

(def +db-key+ "crux.ddb-s3.root")

(definline S [v] `(-> (AttributeValue/builder) (.s ~v) (.build)))
(definline N [v] `(-> (AttributeValue/builder) (.n (str ~v)) (.build)))

(def ^:private -catch (reify Function (apply [_ e] e)))

(def ^:private retry-executor (delay (Executors/newSingleThreadScheduledExecutor)))

(defn- ^CompletableFuture delayed-future
  [delay ^TimeUnit delay-units]
  (let [future (CompletableFuture.)]
    (.schedule ^ScheduledExecutorService  @retry-executor
               (reify Callable
                 (call [_] (.complete future true)))
               (long delay) delay-units)
    future))

(def ^:private attribute-definitions
  [(-> (AttributeDefinition/builder)
       (.attributeName "key")
       (.attributeType ScalarAttributeType/S)
       (.build))])

(def ^:private key-schema
  [(-> (KeySchemaElement/builder)
       (.attributeName "key")
       (.keyType KeyType/HASH)
       (.build))])

(defn create-table
  [^DDBS3Configurator configurator ^DynamoDbAsyncClient client table-name]
  (let [request (-> (CreateTableRequest/builder)
                    (.tableName table-name)
                    (.attributeDefinitions ^Collection attribute-definitions)
                    (.keySchema ^Collection key-schema)
                    (.provisionedThroughput ^ProvisionedThroughput (-> (ProvisionedThroughput/builder)
                                                                       (.readCapacityUnits 1)
                                                                       (.writeCapacityUnits 1)
                                                                       (.build)))
                    (->> (.createTable configurator))
                    (.build))]
    (.createTable client ^CreateTableRequest request)))

(defn ensure-table-exists
  [^DDBS3Configurator configurator ^DynamoDbAsyncClient client table-name]
  (-> (.describeTable client ^DescribeTableRequest (-> (DescribeTableRequest/builder)
                                                       (.tableName table-name)
                                                       (.build)))
      (.exceptionally
        (reify Function
          (apply [_ e]
            (if (or (instance? ResourceNotFoundException e)
                    (instance? ResourceNotFoundException (.getCause e)))
              nil
              (throw e)))))
      (.thenCompose
        (reify Function
          (apply [_ response]
            (if response
              (if (or (not= (set attribute-definitions)
                            (-> ^DescribeTableResponse response (.table) (.attributeDefinitions) (set)))
                      (not= (set key-schema)
                            (-> ^DescribeTableResponse response (.table) (.keySchema) (set))))
                (throw (ex-info "table exists but has an invalid schema"
                                {:attribute-definitions (-> ^DescribeTableResponse response (.table) (.attributeDefinitions))
                                 :key-schema (-> ^DescribeTableResponse response (.table) (.keySchema))
                                 :required-attribute-definitions attribute-definitions
                                 :required-key-schema key-schema}))
                (CompletableFuture/completedFuture :ok))
              (create-table configurator client table-name)))))))

(defn ensure-table-ready
  [^DynamoDbAsyncClient client table-name]
  (-> (.describeTable client ^DescribeTableRequest (-> (DescribeTableRequest/builder)
                                                       (.tableName table-name)
                                                       (.build)))
      (.thenCompose
        (reify Function
          (apply [_ result]
            (if (some-> ^DescribeTableResponse result (.table) (.tableStatus) (= TableStatus/ACTIVE))
              (CompletableFuture/completedFuture true)
              (.thenCompose (delayed-future 1 TimeUnit/SECONDS)
                            (reify Function
                              (apply [_ _]
                                (ensure-table-ready client table-name))))))))))

(defn create-bucket
  [^DDBS3Configurator configurator ^S3AsyncClient client bucket-name]
  (.createBucket client
                 ^CreateBucketRequest
                 (-> (CreateBucketRequest/builder)
                     (.bucket bucket-name)
                     (.createBucketConfiguration
                       ^CreateBucketConfiguration
                       (-> (CreateBucketConfiguration/builder)
                           (.locationConstraint (BucketLocationConstraint/fromValue
                                                  (str (.getRegion (DefaultAwsRegionProviderChain.)))))
                           (.build)))
                     (->> (.createBucket configurator))
                     (.build))))

(defn ensure-bucket-exists
  [^DDBS3Configurator configurator ^S3AsyncClient client bucket-name]
  (-> (.headBucket client ^HeadBucketRequest (-> (HeadBucketRequest/builder)
                                                 (.bucket bucket-name)
                                                 (.build)))
      (.exceptionally
        (reify Function
          (apply [_ e]
            (if (or (instance? NoSuchBucketException e)
                    (instance? NoSuchBucketException (.getCause e)))
              nil
              (throw e)))))
      (.thenCompose
        (reify Function
          (apply [_ result]
            (if (instance? Throwable result)
              (create-bucket configurator client bucket-name)
              (CompletableFuture/completedFuture true)))))))


(defn- init-tx-info
  [^DynamoDbAsyncClient client table-name]
  (log/info (pr-str {:task ::init-tx-info :phase :begin}))
  (let [start (System/currentTimeMillis)
        tx 0
        request (-> (PutItemRequest/builder)
                    (.tableName table-name)
                    (.item {"key"             (S +db-key+)
                            "currentTx"       (N tx)})
                    (.conditionExpression "attribute_not_exists(#key)")
                    (.expressionAttributeNames {"#key" "key"})
                    (.build))]
    (-> (.putItem client ^PutItemRequest request)
        (.exceptionally -catch)
        (.thenApply (reify Function
                      (apply [_ _]
                        (log/info (pr-str {:task ::init-tx-info :phase :end :ms (- (System/currentTimeMillis) start)}))
                        tx))))))

(defn- increment-tx
  [^DynamoDbAsyncClient client table-name tx]
  (log/info (pr-str {:task ::increment-tx :phase :begin :tx tx}))
  (let [start (System/currentTimeMillis)
        next-tx (unchecked-inc tx)
        request (-> (UpdateItemRequest/builder)
                    (.tableName table-name)
                    (.updateExpression "SET #tx = :newTx")
                    (.conditionExpression "#tx = :oldTx")
                    (.key {"key" (S +db-key+)})
                    (.expressionAttributeNames {"#tx"   "currentTx"})
                    (.expressionAttributeValues {":newTx"   (N next-tx)
                                                 ":oldTx"   (N tx)})
                    (.build))]
    (-> (.updateItem client ^UpdateItemRequest request)
        (.thenApply (reify Function
                      (apply [_ _]
                        (log/info (pr-str {:task ::increment-tx :phase :end :next-tx next-tx :ms (- (System/currentTimeMillis) start)}))
                        next-tx))))))

(defn select-next-txid
  [^DynamoDbAsyncClient client table-name & {:keys [delay] :or {delay 10}}]
  (let [start (System/currentTimeMillis)]
    (log/info (pr-str {:task ::select-next-txid :phase :begin}))
    (let [get-request (-> (GetItemRequest/builder)
                          (.tableName table-name)
                          (.consistentRead true)
                          (.attributesToGet ["currentTx"])
                          (.key {"key" (S +db-key+)})
                          (.build))]
      (-> (.getItem client ^GetItemRequest get-request)
          (.thenCompose (reify Function
                          (apply [_ response]
                            (log/debug (pr-str {:task ::select-next-txid :phase :read-db-info :info response :ms (- (System/currentTimeMillis) start)}))
                            (if (.hasItem ^GetItemResponse response)
                              (increment-tx client table-name
                                            (-> ^GetItemResponse response (.item) ^AttributeValue (get "currentTx") (.n) (Long/parseLong)))
                              (init-tx-info client table-name)))))
          (.exceptionally -catch)
          (.thenCompose
            (reify Function
              (apply [_ result]
                (if (instance? Throwable result)
                  (if (or (instance? ConditionalCheckFailedException result)
                          (instance? ConditionalCheckFailedException (.getCause ^Throwable result)))
                    (-> (delayed-future delay TimeUnit/MILLISECONDS)
                        (.thenCompose (reify Function
                                        (apply [_ _]
                                          (select-next-txid client table-name :delay (max 60000 (long (* delay 1.5))))))))
                    (throw result))
                  (do
                    (log/info (pr-str {:task ::select-next-txid :phase :end :result result :ms (- (System/currentTimeMillis) start)}))
                    (CompletableFuture/completedFuture result))))))))))

(defn submit-tx
  [^DDBS3Configurator configurator ^DynamoDbAsyncClient ddb-client ^S3AsyncClient s3-client table-name bucket-name prefix tx-events]
  (-> (select-next-txid ddb-client table-name)
      (.thenCompose
        (reify Function
          (apply [_ tx]
            (let [tx-date (Date.)
                  key (format "%s%016x.%016x" prefix tx (.getTime tx-date))
                  blob (.freeze configurator tx-events)]
              (-> (.putObject s3-client (-> (PutObjectRequest/builder)
                                            (.bucket bucket-name)
                                            (.key key)
                                            (->> (.putObject configurator))
                                            ^PutObjectRequest (.build))
                              (AsyncRequestBody/fromBytes blob))
                  (.thenApply
                    (reify Function
                      (apply [_ _]
                        #:crux.tx{:tx-id   tx
                                  :tx-time tx-date}))))))))))

(defn object->tx
  [^DDBS3Configurator configurator ^S3AsyncClient client ^S3Object object bucket-name prefix]
  (try
    (let [[tx date] (string/split (subs (.key object) (.length prefix)) #"\.")
          tx (Long/parseLong tx 16)
          date (Date. (Long/parseLong date 16))
          object @(.getObject client ^GetObjectRequest (-> (GetObjectRequest/builder)
                                                           (.bucket bucket-name)
                                                           (.key (.key object))
                                                           (.build))
                              (AsyncResponseTransformer/toBytes))]
      #:crux.tx{:tx-id     tx
                :tx-time   date
                :tx-events (.thaw configurator (.asByteArray ^ResponseBytes object))})
    (catch Exception x
      (log/warn x (str "skipping invalid object from S3 with key: " (.key object)))
      nil)))

(defn object-iterator
  [^S3AsyncClient client bucket-name prefix from-tx continuation-token]
  (let [page ^ListObjectsV2Response @(.listObjectsV2 client
                                                     ^ListObjectsV2Request
                                                     (cond-> (ListObjectsV2Request/builder)
                                                             :then (.bucket bucket-name)
                                                             (some? from-tx) (.startAfter (format "%016x" from-tx))
                                                             (some? continuation-token) (.continuationToken continuation-token)
                                                             :then (.prefix prefix)
                                                             :then (.build)))
        next-page (when (.isTruncated page)
                    (lazy-seq (object-iterator client bucket-name prefix nil (.continuationToken page))))]
    (concat (.contents page) next-page)))

(defn tx-iterator
  [^DDBS3Configurator configurator ^S3AsyncClient client bucket-name prefix from-tx]
  (let [object-iter (object-iterator client bucket-name prefix from-tx nil)]
    (sequence (comp (map #(object->tx configurator client % bucket-name prefix))
                    (remove nil?))
              object-iter)))

(defn last-tx
  [^DynamoDbAsyncClient client table-name]
  (-> (.getItem client ^GetItemRequest (-> (GetItemRequest/builder)
                                           (.tableName table-name)
                                           (.key {"key" (S +db-key+)})
                                           (.build)))
      (.thenApply
        (reify Function
          (apply [_ item]
            (when item
              (when-let [item (.item ^GetItemResponse item)]
                (when-let [tx (some-> item ^AttributeValue (get "currentTx") (.n) (Long/parseLong))]
                  {:crux.tx/tx-id tx}))))))))