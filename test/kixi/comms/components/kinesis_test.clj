(ns kixi.comms.components.kinesis-test
  (:require [kixi.comms.components.kinesis :refer :all]
            [clojure
             [spec :as s]
             [test :refer :all]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre :refer [error info]]
            [environ.core :refer [env]]
            [kixi.comms.schema]
            [kixi.comms :as comms]
            [kixi.comms.components.test-base :refer :all]
            [kixi.comms.components.all-component-tests :as all-tests]))

(def test-kinesis (or (env :kinesis-endpoint) "http://localhost:4567"))
(def test-dynamodb (or (env :dynamodb-endpoint) "http://localhost:8000"))
(def test-region "eu-central-1")
(def test-stream-names {:command "kixi-comms-test-command"
                        :event   "kixi-comms-test-event"})

(def system (atom nil))

(defn kinesis-system
  [system]
  (when-not @system
    (comms/set-verbose-logging! true)
    (reset! system
            (component/start-system
             (component/system-map
              :kinesis
              (map->Kinesis {:profile "test"
                             :kinesis-endpoint test-kinesis
                             :dynamodb-endpoint test-dynamodb
                             :create-delay 1000
                             :region test-region
                             :stream-names test-stream-names}))))))

(defn reset-streams!
  [stream-names]
  (fn [all-tests]
    (all-tests)
    (info "Cleaning up streams")
    (delete-streams! test-kinesis stream-names)))

(use-fixtures :once (cycle-system-fixture kinesis-system system))
#_(use-fixtures :each (reset-streams! (vals test-stream-names)))

(deftest kinesis-command-roundtrip-test
  (binding [*wait-per-try* 500]
    (all-tests/command-roundtrip-test (:kinesis @system)
                                      {:initial-position-in-stream :TRIM_HORIZON})))

(deftest kinesis-event-roundtrip-test
  (binding [*wait-per-try* 500]
    (all-tests/event-roundtrip-test (:kinesis @system)
                                    {:initial-position-in-stream :TRIM_HORIZON})))

(deftest kinesis-only-correct-handler-gets-message
  (binding [*wait-per-try* 500]
    (all-tests/only-correct-handler-gets-message (:kinesis @system)
                                                 {:initial-position-in-stream :TRIM_HORIZON})))

(deftest kinesis-multiple-handlers-get-same-message
  (binding [*wait-per-try* 500]
    (all-tests/multiple-handlers-get-same-message (:kinesis @system)
                                                  {:initial-position-in-stream :TRIM_HORIZON})))

(deftest kinesis-roundtrip-command->event
  (binding [*wait-per-try* 500]
    (all-tests/roundtrip-command->event (:kinesis @system)
                                        {:initial-position-in-stream :TRIM_HORIZON})))

(deftest kinesis-roundtrip-command->event-with-key
  (binding [*wait-per-try* 500]
    (all-tests/roundtrip-command->event-with-key (:kinesis @system)
                                                 {:initial-position-in-stream :TRIM_HORIZON})))

(deftest kinesis-processing-time-gt-session-timeout
  (binding [*wait-per-try* 500]
    (all-tests/processing-time-gt-session-timeout (:kinesis @system)
                                                  {:initial-position-in-stream :TRIM_HORIZON})))

(deftest kinesis-detaching-a-handler
  (binding [*wait-per-try* 500]
    (all-tests/detaching-a-handler (:kinesis @system)
                                   {:initial-position-in-stream :TRIM_HORIZON})))
