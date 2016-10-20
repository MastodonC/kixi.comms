(ns kixi.comms.components.kafka
  (:require [cheshire.core :refer [parse-string]]
            [clojure.spec :as s]
            [clojure.core.async :as async]
            [cognitect.transit :as transit]
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
            [taoensso.timbre :as timbre :refer [error info]]
            [zookeeper :as zk])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [franzy.clients.consumer.client FranzConsumer]))

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

(defn clj->transit
  ([m]
   (clj->transit m :json))
  ([m t]
   (let [out (ByteArrayOutputStream. 4096)
         writer (transit/writer out t)]
     (transit/write writer m)
     (.toString out))))

(defn transit->clj
  ([s]
   (transit->clj s :json))
  ([s t]
   (let [in (ByteArrayInputStream. (.getBytes s))
         reader (transit/reader in t)]
     (transit/read reader))))

(defmulti format-message (fn [t _ _ _ _] t))

(defmethod format-message
  :command
  [_ command-key command-version payload _]
  {:kixi.comms.message/type       :command
   :kixi.comms.command/id         (str (java.util.UUID/randomUUID))
   :kixi.comms.command/key        command-key
   :kixi.comms.command/version    command-version
   :kixi.comms.command/created-at (t/timestamp)
   :kixi.comms.command/payload    payload})

(defmethod format-message
  :event
  [_ event-key event-version payload {:keys [origin command-id]}]
  (let [r {:kixi.comms.message/type     :event
           :kixi.comms.event/id         (str (java.util.UUID/randomUUID))
           :kixi.comms.event/key        event-key
           :kixi.comms.event/version    event-version
           :kixi.comms.event/created-at (t/timestamp)
           :kixi.comms.event/payload    payload
           :kixi.comms.event/origin     origin}]
    (if command-id
      (assoc r :kixi.comms.command/id command-id)
      r)))

(defn create-producer
  [in-chan topics origin broker-list]
  (async/go
    (try
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
          (let [msg (async/<! in-chan)]
            (if msg
              (let [[topic-key _ _ _ opts] msg
                    topic     (get topics topic-key)
                    formatted (apply format-message (conj (vec (butlast msg)) (assoc opts :origin origin)))
                    rm        (pp/send-sync! producer topic nil
                                             (or (:kixi.comms.command/id formatted)
                                                 (:kixi.comms.event/id formatted))
                                             (clj->transit formatted) po)]
                (recur))
              (.close producer)))))
      (catch Exception e
        (error e "Producer Exception")))))

(defn process-msg?
  [msg-type event version]
  (fn [msg]
    (and
     (= msg-type
        (:kixi.comms.message/type msg))
     (= event
        (or (:kixi.comms.command/key msg)
            (:kixi.comms.event/key msg)))
     (= version
        (or (:kixi.comms.command/version msg)
            (:kixi.comms.event/version msg))))))

(defn handle-result
  [kafka msg-type original result]
  (when (or (= msg-type :command) result)
    (if-not (s/valid? ::ks/event-result result)
      (throw (Exception. (str "Handler must return a valid event result: "
                              (s/explain-data ::ks/event-result result))))
      (letfn [(send-event-fn! [{:keys [kixi.comms.event/key
                                       kixi.comms.event/version
                                       kixi.comms.event/payload] :as f}]
                (comms/send-event! kafka key version payload (:kixi.comms.command/id original)))]
        (if (sequential? result)
          (run! send-event-fn! result)
          (send-event-fn! result))))))

(defn msg-handler-fn
  [component-handler result-handler]
  (fn [raw-msg msg]
    (async/thread
      (try
        (result-handler msg (component-handler msg))
        (catch Exception e
          (error e (str "Consumer exception processing msg. Raw: " (:value raw-msg) ". Demarshalled: " msg)))))))

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
  []
  (let [poll-timeout 100
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
      (let [^FranzConsumer consumer (consumer/make-consumer
                                     config
                                     (deserializers/string-deserializer)
                                     (deserializers/string-deserializer)
                                     (consumer-options))
            _ (cp/subscribe-to-partitions! consumer (:consumer.topic config))]
        (loop []
          (let [pack (cp/poll! consumer)]
            (doseq [raw-msg (into [] pack)]
              (when-let [msg (some-> raw-msg
                                     :value
                                     transit->clj)]
                (when (process-msg?-fn msg)
                  (cp/pause! consumer (cp/assigned-partitions consumer))
                  (let [result-ch (msg-handler raw-msg msg)]
                    (loop []
                      (let [[val port] (async/alts!! [result-ch
                                                      (async/timeout (:heartbeat.interval.send.ms config))])]
                        (when-not (= port
                                     result-ch)
                          (cp/poll! consumer)
                          (recur)))))
                  (cp/resume! consumer (cp/assigned-partitions consumer)))))
            (if-not (async/poll! kill-chan)
              (recur)
              (do
                (cp/clear-subscriptions! consumer)
                (.close consumer)
                :done)))))
      (catch Exception e
        (error e (str "Consumer for " (:group.id config) " has died. Full config: " config))))))

(defrecord Kafka [host port topics origin consumer-config
                  consumer-kill-ch consumer-kill-mult broker-list consumer-loops]
  comms/Communications
  (send-event! [{:keys [producer-in-ch]} event version payload]
    (when producer-in-ch
      (async/put! producer-in-ch [:event event version payload nil])))
  (send-event! [{:keys [producer-in-ch]} event version payload command-id]
    (when producer-in-ch
      (async/put! producer-in-ch [:event event version payload {:command-id command-id}])))
  (send-command! [{:keys [producer-in-ch]} command version payload]
    (when producer-in-ch
      (async/put! producer-in-ch [:command command version payload nil])))
  (attach-event-handler! [this group-id event version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)]
      (->> (create-consumer (msg-handler-fn handler
                                            (partial handle-result this :event))
                            (process-msg? :event event version)
                            kill-chan
                            (build-consumer-config
                             group-id
                             (:event topics)
                             broker-list
                             (:consumer-config this)))
           (swap! consumer-loops conj))))
  (attach-command-handler! [this group-id command version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)]
      (->> (create-consumer (msg-handler-fn handler
                                            (partial handle-result this :command))
                            (process-msg? :command command version)
                            kill-chan
                            (build-consumer-config
                             group-id
                             (:command topics)
                             broker-list
                             (:consumer-config this)))
           (swap! consumer-loops conj))))
  component/Lifecycle
  (start [component]
    (let [topics (or topics {:command "command" :event "event"})
          origin (or origin (.. java.net.InetAddress getLocalHost getHostName))
          broker-list        (brokers host port)
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
             :consumer-loops (atom [])
             :producer-in-ch producer-chan
             :consumer-kill-ch consumer-kill-chan
             :consumer-kill-mult consumer-kill-mult
             :consumer-config (merge default-consumer-config
                                     consumer-config))))
  (stop [component]
    (let [{:keys [producer-in-ch
                  consumer-kill-ch]} component]
      (info "Stopping Kafka Producer/Consumer")
      (async/close! producer-in-ch)
      (async/>!! consumer-kill-ch :done)
      (doseq [c @consumer-loops]
        (async/<!! c))
      (dissoc component
              :topics
              :origin
              :broker-list
              :producer-in-ch
              :consumer-loops
              :consumer-kill-ch
              :consumer-kill-mult))))
