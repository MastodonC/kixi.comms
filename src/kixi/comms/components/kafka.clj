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
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

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
  [_ event-key event-version payload {:keys [origin]}]
  {:kixi.comms.message/type     :event
   :kixi.comms.event/id         (str (java.util.UUID/randomUUID))
   :kixi.comms.event/key        event-key
   :kixi.comms.event/version    event-version
   :kixi.comms.event/created-at (t/timestamp)
   :kixi.comms.event/payload    payload
   :kixi.comms.event/origin     origin})

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
              (let [[topic-key message-type message-version payload] msg
                    topic     (get topics topic-key)
                    formatted (apply format-message (conj msg {:origin origin}))
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
  [kafka msg-type result]
  (when (or (= msg-type :command)
            result)
    (when-not (s/valid? ::ks/event-result result)
      (throw (Exception. (str "Handler must return a valid event result: "
                              (s/explain-data ::ks/event-result result))))))
  (letfn [(send-event-fn! [{:keys [kixi.comms.event/key
                                   kixi.comms.event/version
                                   kixi.comms.event/payload] :as f}]
            (comms/send-event! kafka key version payload))]
    (if (sequential? result)
      (run! send-event-fn! result)
      (send-event-fn! result))))

(defn create-consumer
  [handle-result-fn process-msg?-fn kill-chan group-id topics broker-list handler]
  (let [timeout 100
        cc {:bootstrap.servers       broker-list
            :group.id                group-id
            :auto.offset.reset       :earliest
            :enable.auto.commit      false
            :auto.commit.interval.ms timeout}
        listener (callbacks/consumer-rebalance-listener
                  (fn [tps]
                    (info "topic partitions assigned:" tps))
                  (fn [tps]
                    (info "topic partitions revoked:" tps)))
        co (cd/make-default-consumer-options
            {:rebalance-listener-callback listener})
        consumer (consumer/make-consumer
                  cc
                  (deserializers/string-deserializer)
                  (deserializers/string-deserializer))
        _ (cp/subscribe-to-partitions! consumer (vals topics))]
    (async/go-loop []
      (let [[val port] (async/alts! [(async/thread
                                       (cp/poll! consumer {:poll-timeout-ms timeout}))
                                     kill-chan])]
        (when-not (= port kill-chan)
          (doseq [raw-msg (into [] val)]
            (when-let [msg (some-> raw-msg
                                   :value
                                   transit->clj)]
              (when (process-msg?-fn msg)
                (async/<! (async/thread
                            (try
                              (handle-result-fn (handler msg))
                              (catch Exception e
                                (error e (str "Consumer exception processing msg. Raw: " (:value raw-msg) ". Demarshalled: " msg))))))
                (cp/commit-offsets-async! consumer {(select-keys raw-msg [:topic :partition])
                                                    {:offset (inc (:offset raw-msg))
                                                     :metadata (str "Consumer stopping - "(java.util.Date.))}})))))
        (if-not (= port kill-chan)
          (recur)
          (do
            (cp/clear-subscriptions! consumer)
            (.close consumer)))))))

(defrecord Kafka [host port topics origin
                  consumer-kill-ch consumer-kill-mult broker-list]
  comms/Communications
  (send-event! [{:keys [producer-in-ch]} event version payload]
    (when producer-in-ch
      (async/put! producer-in-ch [:event event version payload])))
  (send-command! [{:keys [producer-in-ch]} command version payload]
    (when producer-in-ch
      (async/put! producer-in-ch [:command command version payload])))
  (attach-event-handler! [this group-id event version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)]
      (create-consumer (partial handle-result this :event)
                       (process-msg? :event event version)
                       kill-chan
                       group-id
                       topics
                       broker-list
                       handler)))
  (attach-command-handler! [this group-id command version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)]
      (create-consumer (partial handle-result this :command)
                       (process-msg? :command command version)
                       kill-chan
                       group-id
                       topics
                       broker-list
                       handler)))
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
             :producer-in-ch producer-chan
             :consumer-kill-ch consumer-kill-chan
             :consumer-kill-mult consumer-kill-mult)))
  (stop [component]
    (let [{:keys [producer-in-ch
                  consumer-kill-ch]} component]
      (info "Stopping Kafka Producer/Consumer")
      (async/close! producer-in-ch)
      (async/close! consumer-kill-ch)
      (dissoc component
              :topics
              :origin
              :broker-list
              :producer-in-ch
              :consumer-kill-ch
              :consumer-kill-mult))))
