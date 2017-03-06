(ns kixi.comms.components.kinesis-test
  (:require [kixi.comms.components.kinesis :refer :all]
            [clojure
             [spec :as s]
             [test :refer :all]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre :refer [error info]]
            [kixi.comms.schema]
            [kixi.comms :as comms]
            [kixi.comms.components.test-base :refer :all]
            [kixi.comms.components.all-component-tests :as all-tests]))

(def test-kinesis "http://localhost:4567")
(def test-dynamodb "http://localhost:8000")
(def test-region "eu-central-1")
(def test-stream-names {:command "test-command" :event "test-event"})

(def system (atom nil))

(defn kinesis-system
  [system]
  (when-not @system
    (reset! system
            (component/start-system
             (component/system-map
              :kinesis
              (map->Kinesis {:kinesis-endpoint test-kinesis
                             :dynamodb-endpoint test-dynamodb
                             :create-delay 1000
                             :region test-region
                             :stream-names test-stream-names}))))))

(use-fixtures :once (cycle-system-fixture kinesis-system system))

(deftest kinesis-command-roundtrip-test
  (all-tests/command-roundtrip-test (:kinesis @system)))
