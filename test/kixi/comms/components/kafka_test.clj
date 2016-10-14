(ns kixi.comms.components.kafka-test
  (:require [clojure
             [spec :as s]
             [test :refer :all]]
            [com.stuartsierra.component :as component]
            [kixi.comms :as comms]
            [kixi.comms.components.kafka :refer :all]))

(def zookeeper-ip "127.0.0.1")
(def zookeeper-port 2181)
(def group-id "test-group")

(def system (atom nil))

(defn start-kafka-system
  []
  (reset! system
          (component/start-system
           (component/system-map
            :kafka (map->Kafka {:host zookeeper-ip
                                :port zookeeper-port
                                :group-id group-id})))))
(defn cycle-system-fixture
  [all-tests]
  (start-kafka-system)
  (all-tests)
  (component/stop-system @system)
  (reset! system nil))

(use-fixtures :once cycle-system-fixture)

(defn wait-for-atom
  ([a]
   (wait-for-atom a 10))
  ([a tries]
   (wait-for-atom a tries 100))
  ([a tries ms]
   (loop [try tries]
     (when (pos? try)
       (if @a
         @a
         (do
           (Thread/sleep ms)
           (recur (dec try))))))))

(deftest formatting-tests
  (is (not
       (s/explain-data :kixi.comms.message/command
                       (format-message :command :test/foo "1.0.0" {:foo :bar} nil))))
  (is (not
       (s/explain-data :kixi.comms.message/event
                       (format-message :event :test/foo "1.0.0" {:foo :bar} {:origin "local"})))))

(deftest brokers-list-test
  (let [bl (first (brokers zookeeper-ip zookeeper-port))]
    (is (re-find #"\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{4,5}" bl))))

(deftest command-roundtrip-test
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-command-handler! (:kafka @system) :test/foo "1.0.0" (partial reset! result))
    (comms/send-command! (:kafka @system) :test/foo "1.0.0" {:foo "123" :id id})
    (wait-for-atom result 20 500)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.command/payload :id])))))

(deftest event-roundtrip-test
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! (:kafka @system) :test/foo "1.0.0" (partial reset! result))
    (comms/send-event! (:kafka @system) :test/foo "1.0.0" {:foo "123" :id id})
    (wait-for-atom result 20 500)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.event/payload :id])))))
