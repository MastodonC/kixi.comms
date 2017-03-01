(ns kixi.comms.components.kinesis-test
  (:require [kixi.comms.components.kinesis :refer :all]
            [clojure
             [spec :as s]
             [test :refer :all]]
            [com.stuartsierra.component :as component]
            [kixi.comms.schema]
            [kixi.comms :as comms]
            [kixi.comms.components.test-base :refer :all]))
