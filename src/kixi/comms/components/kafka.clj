(ns kixi.comms.components.kafka
  (:require [cheshire.core :refer [parse-string]]
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

(defn process-msg
  [msg-type event version msg]
  (and
   (= msg-type 
      (:kixi.comms.message/type msg))
   (= event
      (or (:kixi.comms.command/key msg)
          (:kixi.comms.event/key msg)))
   (= version 
      (or (:kixi.comms.command/version msg)
          (:kixi.comms.event/version msg)))))

(defn create-consumer
  [kill-chan group-id {:keys [commands events] :as topics} broker-list 
   msg-type event version handler]
  (async/go
    (try
      (let [timeout            100
            key-deserializer   (deserializers/string-deserializer)
            value-deserializer (deserializers/string-deserializer)
            cc                 {:bootstrap.servers       broker-list
                                :group.id                group-id
                                :auto.offset.reset       :earliest
                                :enable.auto.commit      false
                                :auto.commit.interval.ms timeout}
            listener            (callbacks/consumer-rebalance-listener
                                 (fn [tps]
                                   (info "topic partitions assigned:" tps))
                                 (fn [tps]
                                   (info "topic partitions revoked:" tps)))
            co                 (cd/make-default-consumer-options
                                {:rebalance-listener-callback listener})
           
            running?            (atom true)]
        (async/go (async/<! kill-chan) (reset! running? false))
        (with-open  [consumer (consumer/make-consumer
                               cc
                               key-deserializer
                               value-deserializer)]
          (cp/subscribe-to-partitions! consumer (vals topics))
          (loop []
            (let [cr (into [] (cp/poll! consumer {:poll-timeout-ms timeout}))]              
              (loop [cr' cr]
                (when-let [process (some identity cr')]
                  (when @running?    
                    (let [msg ((comp transit->clj :value) process)]
                      (when (process-msg msg-type event version msg)
                        (handler msg))
                      (cp/commit-offsets-sync! consumer {(select-keys process [:topic :partition])
                                                         {:offset (inc (:offset process))
                                                          :metadata (str "Consumer stopping - "(java.util.Date.))}})
                      (recur (rest cr')))))))
            (if @running?
              (recur)
              (cp/clear-subscriptions! consumer)))))
      (catch Exception e 
        (error e "Consumer exception")))))

(defn start-listening!
  [handlers {:keys [command event]} consumer-out-ch]
  (async/go-loop []
    (let [msg (async/<! consumer-out-ch)]
      (if msg
        (let [handlers' (get-in @handlers [(:kixi.comms.message/type msg)
                                           (or (:kixi.comms.command/key msg)
                                               (:kixi.comms.event/key msg))
                                           (or (:kixi.comms.command/version msg)
                                               (:kixi.comms.event/version msg))])]
          (run! (fn [f] (f msg)) handlers')
          (recur))))))

(defrecord Kafka [host port topics origin 
                  consumer-kill-ch consumer-kill-mult broker-list]
  comms/Communications
  (send-event! [{:keys [producer-in-ch]} event version payload]
    (when producer-in-ch
      (async/put! producer-in-ch [:event event version payload])))
  (send-command! [{:keys [producer-in-ch]} command version payload]
    (when producer-in-ch
      (async/put! producer-in-ch [:command command version payload])))
  (attach-event-handler! [_ group-id event version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)]
      (create-consumer kill-chan
                       group-id
                       topics
                       broker-list
                       :event
                       event
                       version
                       handler)))
  (attach-command-handler! [_ group-id command version handler]
    (let [kill-chan (async/chan)
          _ (async/tap consumer-kill-mult kill-chan)]
      (create-consumer kill-chan
                       group-id
                       topics
                       broker-list
                       :command
                       command
                       version
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
