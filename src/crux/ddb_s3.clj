(ns crux.ddb-s3
  (:require [crux.db :as db]
            [crux.ddb-s3.impl :as impl]
            [crux.io :as cio])
  (:import (crux.ddb_s3 DDBS3Configurator)))

(defrecord DDB+S3TxLog [configurator ddb-client s3-client table-name bucket-name prefix]
  db/TxLog
  (submit-tx [_ tx-events]
    (impl/submit-tx configurator ddb-client s3-client table-name bucket-name prefix tx-events))
  (open-tx-log ^crux.api.ICursor [_ after-tx-id]
    (cio/->cursor #() (impl/tx-iterator configurator s3-client bucket-name prefix after-tx-id)))
  (latest-submitted-tx [_]
    @(impl/last-tx ddb-client table-name)))

(def ddb-s3-tx-log
  {::configurator {:start-fn (fn [_ _] (reify DDBS3Configurator))}
   :crux.node/tx-log {:start-fn (fn [{::keys [configurator]} {::keys [table bucket prefix]}]
                                  (let [ddb-client (.makeDynamoDBClient configurator)
                                        s3-client (.makeS3Client configurator)]
                                    @(impl/ensure-table-exists configurator ddb-client table)
                                    @(impl/ensure-table-ready ddb-client table)
                                    @(impl/ensure-bucket-exists configurator s3-client bucket)
                                    (->DDB+S3TxLog configurator ddb-client s3-client table bucket prefix)))
                      :args {::table {:required? true
                                      :crux.config/type :crux.config/string
                                      :doc "DynamoDB table"}
                             ::bucket {:required? true
                                       :crux.config/type :crux.config/string
                                       :doc "S3 bucket"}
                             ::prefix {:crux.config/type :crux.config/string
                                       :default "tx"
                                       :doc "S3 prefix"}}
                      :deps #{::configurator}}})