(ns kixi.comms.components.kafka-test
  (:require [clojure.test :refer :all]
            [kixi.comms.components.kafka :refer :all]
            [clojure.core.async :as async]
            [com.stuartsierra.component :as component]))

(def zookeeper-ip "127.0.0.1")
(def zookeeper-port 2181)
(def group-id "test-group")

(defn start-kafka-system
  []
  (component/start-system
   (component/system-map
    :kafka (map->Kafka {:host zookeeper-ip
                        :port zookeeper-port
                        :group-id group-id}))))

(deftest create-test
  (let [k (map->Kafka {:host zookeeper-ip
                       :port zookeeper-port
                       :group-id group-id})]""
       (is (= (:host k) zookeeper-ip))
       (is (= (:port k) zookeeper-port))))

(deftest brokers-list-test
  (is (re-find #"\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{4,5}"
               (first (brokers zookeeper-ip zookeeper-port)))))

#_(deftest create-producer-test
    (let [kc (async/chan)
          ic (async/chan)
          bl (brokers zookeeper-ip zookeeper-port)]
      (create-producer kc ic bl)
      (async/put! ic "hello")
      (async/put! kc true)))

(deftest as-system-test
  (let [check-keys [:broker-list
                    :kill-ch
                    :producer-ch
                    :consumer-ch]
        s (start-kafka-system)]
    (doseq [k check-keys]
      (is (get-in s [:kafka k])))
    (let [s' (component/stop-system s)]
      (doseq [k check-keys]
        (is (not (get-in s' [:kafka k])))))))

(deftest roundtrip-test
  (let [{:keys [kafka]} (start-kafka-system)]
    (component/stop-system s)))

#_(deftest send-command
    (let [s (component/start-system
             (component/system-map
              :kafka (map->Kafka {:host zookeeper-ip :port zookeeper-port})))]


      (component/stop-system s)))
