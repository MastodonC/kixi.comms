(ns kixi.comms.components.kafka-test
  {:kafka true}
  (:require [clojure
             [test :refer :all]]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as component]
            [kixi.comms.schema]
            [kixi.comms :as comms]
            [kixi.comms.components.kafka :refer :all]
            [kixi.comms.messages :refer :all :exclude [uuid]]
            [kixi.comms.components.test-base :refer :all]
            [kixi.comms.components.all-component-tests :as all-tests]))

(def zookeeper-ip "127.0.0.1")
(def zookeeper-port 2181)
(def group-id "test-group")
(def system (atom nil))

(defn kafka-system
  [system]
  (when-not @system
    (reset! system
            (component/start-system
             (component/system-map
              :kafka
              (map->Kafka {:host zookeeper-ip
                           :port zookeeper-port
                           :consumer-config {;to set this lower, need to configure the brokers group.min.session.timeout.ms
                                             :session.timeout.ms 6000
                                             :auto.commit.interval.ms 1500}}))))))

(use-fixtures :once (cycle-system-fixture kafka-system system))

(deftest brokers-list-test
  (let [bl (first (brokers "/" zookeeper-ip zookeeper-port))]
    (is (re-find #"\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{4,5}" bl))))

(deftest handle-result-tests
  (is (successful? (handle-result (:kafka @system)
                                  :command
                                  {}
                                  {:kixi.comms.event/key :received-result
                                   :kixi.comms.event/version "1.0.0"
                                   :kixi.comms.event/payload {}})))
  (is (successful? (handle-result (:kafka @system)
                                  :event
                                  {}
                                  {:kixi.comms.event/key :received-result
                                   :kixi.comms.event/version "1.0.0"
                                   :kixi.comms.event/payload {}})))
  (is (successful? (handle-result (:kafka @system)
                                  :event
                                  {}
                                  [{:kixi.comms.event/key :received-result
                                    :kixi.comms.event/version "1.0.0"
                                    :kixi.comms.event/payload {}}
                                   {:kixi.comms.event/key :received-result2
                                    :kixi.comms.event/version "1.0.0"
                                    :kixi.comms.event/payload {}}])))
  (is (successful? (handle-result (:kafka @system)
                                  :event {} nil)))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :command {} nil)))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :command {} 123)))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :command {} {:foo 123})))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :event {} {:foo 123})))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :event {} [{:foo 123}
                                                  {:kixi.comms.event/key :received-result
                                                   :kixi.comms.event/version "1.0.0"
                                                   :kixi.comms.event/payload {}}]))))

(deftest kafka-command-roundtrip-test
  (all-tests/command-roundtrip-test (:kafka @system)))

(deftest kafka-event-roundtrip-test
  (all-tests/event-roundtrip-test (:kafka @system)))

(deftest kafka-only-correct-handler-gets-message
  (all-tests/only-correct-handler-gets-message (:kafka @system)))

(deftest kafka-multiple-handlers-get-same-message
  (all-tests/multiple-handlers-get-same-message (:kafka @system)))

(deftest kafka-roundtrip-command->event
  (all-tests/roundtrip-command->event (:kafka @system)))

(deftest kafka-roundtrip-command->event-with-key
  (all-tests/roundtrip-command->event-with-key (:kafka @system)))

(deftest kafka-processing-time-gt-session-timeout
  (all-tests/processing-time-gt-session-timeout (:kafka @system)))

(deftest kafka-detaching-a-handler
  (all-tests/detaching-a-handler (:kafka @system)))

(deftest kafka-events-are-partitioned
  (all-tests/events-are-partitioned (:kafka @system)))

(deftest kafka-commands-are-partitioned
  (all-tests/commands-are-partitioned (:kafka @system)))

(deftest kafka-command-produced-events-are-partitioned
  (all-tests/command-produced-events-are-partitioned(:kafka @system)))
