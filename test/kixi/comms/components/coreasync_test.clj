(ns kixi.comms.components.coreasync-test
  (:require [kixi.comms.components.coreasync :refer :all]
            [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [clojure
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


(defn coreasync-system
  [system]
  (when-not @system
    (comms/set-verbose-logging! true)
    (reset! system
            (component/start-system
             (component/system-map
              :coreasync
              (map->CoreAsync {:profile profile
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

(use-fixtures :once (cycle-system-fixture* coreasync-system system))


(def opts {})

(def long-wait 50)

(deftest coreasync-command-roundtrip-test
  (binding [*wait-per-try* long-wait]
    (all-tests/command-roundtrip-test (:coreasync @system) opts)))

(deftest coreasync-event-roundtrip-test
  (binding [*wait-per-try* long-wait]
    (all-tests/event-roundtrip-test (:coreasync @system) opts)))

(deftest coreasync-only-correct-handler-gets-message
  (binding [*wait-per-try* long-wait]
    (all-tests/only-correct-handler-gets-message (:coreasync @system) opts)))

(deftest coreasync-multiple-handlers-get-same-message
  (binding [*wait-per-try* long-wait]
    (all-tests/multiple-handlers-get-same-message (:coreasync @system) opts)))

(deftest coreasync-roundtrip-command->event
  (binding [*wait-per-try* long-wait]
    (all-tests/roundtrip-command->event (:coreasync @system) opts)))

(deftest coreasync-roundtrip-command->event-with-key
  (binding [*wait-per-try* long-wait]
    (all-tests/roundtrip-command->event-with-key (:coreasync @system) opts)))

(deftest coreasync-roundtrip-command->multi-event
  (binding [*wait-per-try* long-wait]
    (all-tests/roundtrip-command->multi-event (:coreasync @system) opts)))

(comment "Test not applicable as core async has no session"
         (deftest coreasync-processing-time-gt-session-timeout
           (binding [*wait-per-try* long-wait]
             (all-tests/processing-time-gt-session-timeout (:coreasync @system) opts))))

(deftest coreasync-detaching-a-handler
  (binding [*wait-per-try* long-wait]
    (all-tests/detaching-a-handler (:coreasync @system) opts)))

(deftest coreasync-infinite-loop-defended
  (binding [*wait-per-try* long-wait]
    (all-tests/infinite-loop-defended (:coreasync @system) opts)))
