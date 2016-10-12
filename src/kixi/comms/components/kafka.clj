(ns kixi.comms.components.kafka
  (:require [clojure.core.async :as async]
            [com.stuartsierra.component :as component]
            [franzy.serialization.deserializers :as deserializers]
            [franzy.serialization.serializers :as serializers]
            [franzy.clients.producer.defaults :as pd]
            [franzy.clients.producer.client :as producer]
            [franzy.clients.consumer.defaults :as cd]
            [franzy.clients.consumer.callbacks :as callbacks]
            [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :as cp]
            [kixi.comms :as comms]
            [zookeeper :as zk]
            [cheshire.core :refer [parse-string]]))

;; https://github.com/pingles/clj-kafka/blob/321f2d6a90a2860f4440431aa835de86a72e126d/src/clj_kafka/zk.clj#L8
(defn brokers
  "Get brokers from zookeeper"
  [host port]
  (let [z (atom nil)]
    (try
      (reset! z (zk/connect (str host ":" port)))
      (if-let [broker-ids (zk/children @z "/brokers/ids")]
        (let [brokers (doall (map (comp #(parse-string % true)
                                        #(String. ^bytes %)
                                        :data
                                        #(zk/data @z (str "/brokers/ids/" %)))
                                  broker-ids))]
          (when (seq brokers)
            (mapv (fn [{:keys [host port]}] (str host ":" port)) brokers))))
      (finally
        (when @z
          (zk/close @z))))))

(defn listen-for-messages!
  [consumer running? {:keys [commands events]}]
  (async/go-loop []
    (let [cr (cp/poll! consumer)]
      (println "GOT SUMMET" cr))
    (when @running?
      (recur))))

(defn create-producer
  [kill-chan in-chan broker-list]
  (async/go
    (let [key-serializer     (serializers/string-serializer)
          value-serializer   (serializers/string-serializer)
          pc                 {:bootstrap.servers broker-list}
          po                 (pd/make-default-producer-options)
          producer           (producer/make-producer
                              pc
                              key-serializer
                              value-serializer
                              po)]
      (loop []
        (let [[msg p] (async/alts! [in-chan kill-chan])]
          (if (= p in-chan)
            (do (prn "Sending msg:" msg)
                (recur))
            (do (prn "Stopping Producer")
                (.close producer)))))))

  (defn create-consumer
    [kill-chan register-chan group-id {:keys [commands events] :as topics} broker-list]
    (async/go
      (let [key-deserializer   (deserializers/string-deserializer)
            value-deserializer (deserializers/string-deserializer)
            cc                 {:bootstrap.servers       broker-list
                                :group.id                group-id
                                :auto.offset.reset       :earliest
                                :enable.auto.commit      true
                                :auto.commit.interval.ms 1000}
            listener            (callbacks/consumer-rebalance-listener
                                 (fn [tps]
                                   (println "topic partitions assigned:" tps))
                                 (fn [tps]
                                   (println "topic partitions revoked:" tps)))
            co                 (cd/make-default-consumer-options
                                {:rebalance-listener-callback listener})
            consumer           (consumer/make-consumer
                                cc
                                key-deserializer
                                value-deserializer)]
        (cp/subscribe-to-partitions! consumer (vals topics))
        (loop []
          (let [[msg p] (async/alts! [in-chan register-chan kill-chan])]
            (condp = p
              register-chan
              (do (prn "Got new handler")
                  (recur))
              in-chan
              (do (prn "Receiving msg:" msg)
                  (recur))
              kill-chan
              (do
                (prn "Stopping Consumer")
                (cp/clear-subscriptions! consumer)
                (.close consumer)))))))))

(defrecord Kafka [host port group-id topics]
  comms/Communications
  (send-event! [this event version payload])
  (send-command! [this command version payload])
  (attach-event-handler! [this event version handler])
  (attach-command-handler! [this event version handler])
  component/Lifecycle
  (start [component]
    (let [topics (or topics {:commands "command" :events "event"})
          broker-list        (brokers host port)
          kill-chan          (async/chan)
          kill-mult          (async/mult kill-chan)
          producer-kill-chan (async/chan 1)
          consumer-kill-chan (async/chan 1)
          producer-chan      (async/chan)
          consumer-chan      (async/chan)]
      (async/tap kill-mult producer-kill-chan)
      (async/tap kill-mult consumer-kill-chan)
      (create-producer producer-kill-chan
                       producer-chan
                       broker-list)
      (create-consumer consumer-kill-chan
                       consumer-chan
                       group-id
                       topics
                       broker-list)
      (assoc component
             :kill-ch kill-chan
             :producer-ch producer-chan
             :consumer-ch consumer-chan
             :broker-list broker-list)))
  (stop [component]
    (let [{:keys [kill-ch]} component]
      (async/put! kill-ch true)
      (dissoc component
              :kill-ch
              :producer-ch
              :consumer-ch
              :broker-list))))
