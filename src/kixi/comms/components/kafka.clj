(ns kixi.comms.components.kafka
  (:require [cheshire.core :refer [parse-string]]
            [clojure.spec :as s]
            [clojure.core.async :as async]
            [com.stuartsierra.component :as component]
            [franzy.clients.consumer
             [callbacks :as callbacks]
             [client :as consumer]
             [defaults :as cd]
             [protocols :as cp]]
            [franzy.clients.producer
             [client :as producer]
             [defaults :as pd]
             [protocols :as pp]]
            [franzy.serialization
             [deserializers :as deserializers]
             [serializers :as serializers]]
            [kixi.comms :as comms]
            [kixi.comms.time :as t]
            [kixi.comms.schema :as ks]
            [kixi.comms.messages :as msg]
            [taoensso.timbre :as timbre :refer [error info]]
            [zookeeper :as zk])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [franzy.clients.consumer.client FranzConsumer]))

;; https://github.com/pingles/clj-kafka/blob/321f2d6a90a2860f4440431aa835de86a72e126d/src/clj_kafka/zk.clj#L8
(defn brokers
  "Get brokers from zookeeper"
  [path host port]
  (let [z (atom nil)]
    (try
      (info "Connecting to ZooKeeper...")
      (reset! z (zk/connect (str host ":" port)))
      (if-let [broker-ids (zk/children @z (str path "brokers/ids"))]
        (do
          (info "Broker IDS:" (vec broker-ids))
          (let [brokers (doall (map (comp #(parse-string % true)
                                          #(String. ^bytes %)
                                          :data
                                          #(zk/data @z (str path "brokers/ids/" %)))
                                    broker-ids))]
            (info "Brokers:" (vec brokers))
            (when (seq brokers)
              (mapv (fn [{:keys [host port]}] (str host ":" port)) brokers)))))
      (finally
        (when @z
          (zk/close @z))))))

(defn create-producer
  [in-chan topics origin broker-list]
  (let [key-serializer     (serializers/string-serializer)
        value-serializer   (serializers/string-serializer)
        pc                 {:bootstrap.servers broker-list}
        po                 (pd/make-default-producer-options)]
    (try
      (let [producer           (producer/make-producer
                                pc
                                key-serializer
                                value-serializer
                                po)]
        (async/go
          (loop []
            (let [msg (async/<! in-chan)]
              (if msg
                (let [[topic-key _ _ _ _ opts] msg
                      topic     (get topics topic-key)
                      formatted (apply msg/format-message (conj (vec (butlast msg)) (assoc opts :origin origin)))
                      _ (when comms/*verbose-logging*
                          (info "Sending msg to Kafka topic" topic ":" formatted))
                      rm        (pp/send-sync! producer topic nil
                                               (or (:kixi.comms.command/id formatted)
                                                   (:kixi.comms.event/id formatted))
                                               (msg/clj->transit formatted) po)]
                  (recur))
                (.close producer))))))
      (catch Exception e
        (error e (str "Producer Exception1, pc=" pc ", po=" po))))))

(def custom-config-elements
  "These are some additional pieces of config that Kafka doesn't like"
  #{:heartbeat.interval.multiplier
    :heartbeat.interval.send.multiplier
    :heartbeat.interval.send.ms
    :consumer.topic
    :poll-timeout})

(defn dissoc-custom
  [config]
  (apply dissoc config custom-config-elements))

(def default-consumer-config
  {:session.timeout.ms 30000
   :auto.commit.interval.ms 5000
   :heartbeat.interval.multiplier 0.6
   :heartbeat.interval.send.multiplier 0.4
   :auto.offset.reset :earliest
   :enable.auto.commit true
   :max.poll.records 10})

(defn build-consumer-config
  [group-id topic broker-list base-config]
  (merge base-config
         {:bootstrap.servers broker-list
          :group.id group-id
          :consumer.topic topic
          :heartbeat.interval.ms (int (* (:session.timeout.ms base-config)
                                         (:heartbeat.interval.multiplier base-config)))
          :heartbeat.interval.send.ms (int (* (:session.timeout.ms base-config)
                                              (:heartbeat.interval.send.multiplier base-config)))
                                        ;  :client.id "host"
          }))

(defn consumer-options
  [config]
  (let [poll-timeout (:poll-timeout config 1000)
        listener (callbacks/consumer-rebalance-listener
                  (fn [tps]
                                        ;Don't really know what to do here yet
                    )
                  (fn [tps]
                                        ;Don't really know what to do here yet
                    ))]
    {:consumer-records-fn         seq
     :poll-timeout-ms             poll-timeout
     :offset-commit-callback      (callbacks/offset-commit-callback)
     :rebalance-listener-callback listener}))

(defn create-consumer
  [msg-handler process-msg?-fn kill-chan config]
  (async/thread
    (try
      (let [topic (:consumer.topic config)
            ^FranzConsumer consumer (consumer/make-consumer
                                     (dissoc-custom config)
                                     (deserializers/string-deserializer)
                                     (deserializers/string-deserializer)
                                     (consumer-options config))
            _ (cp/subscribe-to-partitions! consumer topic)]
        (loop []
          (let [pack (cp/poll! consumer)]
            (when-let [msgs (seq (keep (comp process-msg?-fn
                                             msg/transit->clj
                                             :value)
                                       (keep identity
                                             (into [] pack))))]
              (cp/pause! consumer (cp/assigned-partitions consumer))
              (doseq [msg msgs]
                (when comms/*verbose-logging*
                  (info "Received msg from Kafka topic" topic  ":" msg))
                (let [result-ch (msg-handler msg)]
                  (loop []
                    (let [[val port] (async/alts!! [result-ch
                                                    (async/timeout (:heartbeat.interval.send.ms config))])]
                      (when-not (= port
                                   result-ch)
                        (cp/poll! consumer)
                        (recur))))))
              (cp/resume! consumer (cp/assigned-partitions consumer)))
            (if-not (async/poll! kill-chan)
              (recur)
              (do
                (cp/commit-offsets-sync! consumer)
                (cp/clear-subscriptions! consumer)
                (.close consumer)
                :done)))))
      (catch Exception e
        (error e (str "Consumer for " (:group.id config) " has died. Full config: " config))))))

(defrecord Kafka [host port zk-path topics origin consumer-config
                  consumer-kill-ch consumer-kill-mult broker-list consumer-loops]
  comms/Communications


  (send-event! [{:keys [producer-in-ch]} event version payload opts]
    (when producer-in-ch
      (async/put! producer-in-ch [:event event version nil payload opts])))

  (send-command! [comms command version user payload]
    (comms/send-command! comms command version user payload {}))

  (send-command! [{:keys [producer-in-ch]} command version user payload opts]
    (when producer-in-ch
      (async/put! producer-in-ch [:command command version user payload opts])))
  (attach-event-with-key-handler!
      [this group-id map-key handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)
          handler (create-consumer (msg/msg-handler-fn handler
                                                       (partial msg/handle-result this :event))
                                   (msg/process-msg? :event #(contains? % map-key))
                                   kill-chan
                                   (build-consumer-config
                                    group-id
                                    (:event topics)
                                    broker-list
                                    (:consumer-config this)))]
      (swap! consumer-loops assoc handler kill-chan)
      handler))
  (attach-event-handler!
      [this group-id event version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)
          handler (create-consumer (msg/msg-handler-fn handler
                                                       (partial msg/handle-result this :event))
                                   (msg/process-msg? :event event version)
                                   kill-chan
                                   (build-consumer-config
                                    group-id
                                    (:event topics)
                                    broker-list
                                    (:consumer-config this)))]
      (swap! consumer-loops assoc handler kill-chan)
      handler))
  (attach-command-handler!
      [this group-id command version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)
          handler (create-consumer (msg/msg-handler-fn handler
                                                       (partial msg/handle-result this :command))
                                   (msg/process-msg? :command command version)
                                   kill-chan
                                   (build-consumer-config
                                    group-id
                                    (:command topics)
                                    broker-list
                                    (:consumer-config this)))]
      (swap! consumer-loops assoc handler kill-chan)
      handler))
  (detach-handler! [this handler]
    (when-let [kill-chan (get @consumer-loops handler)]
      (swap! consumer-loops dissoc handler)
      (async/untap consumer-kill-mult kill-chan)
      (async/>!! kill-chan :done)
      (async/close! kill-chan)
      (async/<!! handler)))
  component/Lifecycle
  (start [component]
    (timbre/merge-config! {:ns-blacklist ["org.apache.kafka.*"
                                          "org.apache.zookeeper.*"]})
    (if-not (:producer-in-ch component)
      (let [topics (or topics {:command "command" :event "event"})
            origin (or origin (try (.. java.net.InetAddress getLocalHost getHostName)
                                   (catch Throwable _ "<unknown>")))
            broker-list        (brokers (or zk-path "/") host port)
            producer-chan      (async/chan)
            consumer-kill-chan (async/chan)
            consumer-kill-mult (async/mult consumer-kill-chan)]
        (info "Starting Kafka Producer/Consumer")
        (create-producer producer-chan
                         topics
                         origin
                         broker-list)
        (assoc component
               :topics topics
               :origin origin
               :broker-list broker-list
               :consumer-loops (atom {})
               :producer-in-ch producer-chan
               :consumer-kill-ch consumer-kill-chan
               :consumer-kill-mult consumer-kill-mult
               :consumer-config (merge default-consumer-config
                                       consumer-config)))
      component))
  (stop [component]
    (let [{:keys [producer-in-ch
                  consumer-kill-ch]} component]
      (if (:producer-in-ch component)
        (do
          (info "Stopping Kafka Producer/Consumer")
          (async/close! producer-in-ch)
          (async/>!! consumer-kill-ch :done)
          (doseq [c (keys @consumer-loops)]
            (async/<!! c))
          (async/close! consumer-kill-ch)
          (dissoc component
                  :topics
                  :origin
                  :broker-list
                  :producer-in-ch
                  :consumer-loops
                  :consumer-kill-ch
                  :consumer-kill-mult))
        component))))
