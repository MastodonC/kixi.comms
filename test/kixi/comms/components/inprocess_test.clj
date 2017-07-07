(ns kixi.comms.components.inprocess-test
  (:require [kixi.comms.components.inprocess :refer :all]
            [clojure.test :as t]
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

(def profile (env :profile "local"))
(def app-name "kixi-comms-test")

(def system (atom nil))


(defn inprocess-system
  [system]
  (when-not @system
    (comms/set-verbose-logging! true)
    (reset! system
            (component/start-system
             (component/system-map
              :inprocess
              (map->InProcess {:profile profile
                              :app app-name                               
                              :metric-level :NONE}))))))

(defn cycle-system-fixture*
  [system-func system-atom]
  (fn [all-tests]
    [all-tests]
    (timbre/with-merged-config
      {:level :debug}
      (system-func system-atom)
      (all-tests)

      (component/stop-system @system-atom)
      (reset! system-atom nil)

      (info "Finished"))))

(use-fixtures :once (cycle-system-fixture* inprocess-system system))


(def opts {})

(def long-wait 50)

(deftest inprocess-command-roundtrip-test
  (binding [*wait-per-try* long-wait]
    (all-tests/command-roundtrip-test (:inprocess @system) opts)))

(deftest inprocess-event-roundtrip-test
  (binding [*wait-per-try* long-wait]
    (all-tests/event-roundtrip-test (:inprocess @system) opts)))

(deftest inprocess-only-correct-handler-gets-message
  (binding [*wait-per-try* long-wait]
    (all-tests/only-correct-handler-gets-message (:inprocess @system) opts)))

(deftest inprocess-multiple-handlers-get-same-message
  (binding [*wait-per-try* long-wait]
    (all-tests/multiple-handlers-get-same-message (:inprocess @system) opts)))

(deftest inprocess-roundtrip-command->event
  (binding [*wait-per-try* long-wait]
    (all-tests/roundtrip-command->event (:inprocess @system) opts)))

(deftest inprocess-roundtrip-command->event-with-key
  (binding [*wait-per-try* long-wait]
    (all-tests/roundtrip-command->event-with-key (:inprocess @system) opts)))

(deftest inprocess-roundtrip-command->multi-event
  (binding [*wait-per-try* long-wait]
    (all-tests/roundtrip-command->multi-event (:inprocess @system) opts)))

(deftest inprocess-detaching-a-handler
  (binding [*wait-per-try* long-wait]
    (all-tests/detaching-a-handler (:inprocess @system) opts)))

(deftest inprocess-infinite-loop-defended
  (binding [*wait-per-try* long-wait]
    (all-tests/infinite-loop-defended (:inprocess @system) opts)))

