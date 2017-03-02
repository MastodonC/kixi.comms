(ns kixi.comms.components.kinesis
  (:require [cheshire.core :refer [parse-string]]
            [clojure.spec :as s]
            [clojure.core.async :as async]
            [cognitect.transit :as transit]
            [com.stuartsierra.component :as component]
            [kixi.comms :as comms]
            [kixi.comms.time :as t]
            [kixi.comms.schema :as ks]
            [kixi.comms.messages :as msg]
            [taoensso.timbre :as timbre :refer [error info]]
            [amazonica.aws.kinesis :as kinesis]
            [byte-streams :as bs]))

(defn sanitize-app-name
  [s]
  (-> s
      (clojure.string/replace #"\:" "")
      (clojure.string/replace #"\/" "_")))

(defn list-streams
  [endpoint]
  (kinesis/list-streams {:endpoint endpoint}))

(defn create-streams!
  [endpoint streams]
  (let [{:keys [stream-names]} (list-streams endpoint)
        stream-names-set (set stream-names)
        shards 1]
    (run! (fn [stream-name]
            (when-not (contains? stream-names-set stream-name)
              (info "Creating stream" stream-name "with" shards "shard(s)!")
              (kinesis/create-stream {:endpoint endpoint} stream-name 1)
              (deref
               (future
                 (loop [n 0
                        {:keys [stream-names]} (list-streams endpoint)]
                   (when-not (contains? (set stream-names) stream-name)
                     (if (< n 10)
                       (do
                         (Thread/sleep 500)
                         (recur (inc n) (list-streams endpoint)))
                       (throw (Exception. "Failed to create stream:" stream-name))))))))) streams)))
(defn create-worker
  [msg-handler process-msg?-fn
   {:keys [stream-name app-name kinesis-endpoint region checkpoint]
    :or {checkpoint 60}}]
  (info "Creating worker" stream-name kinesis-endpoint)
  (first (kinesis/worker :app app-name
                         :stream stream-name
                         :endpoint kinesis-endpoint
                         :checkpoint checkpoint
                         :processor (fn [records]
                                      (doseq [row records]
                                        (println ">>>>"
                                                 (:data row)
                                                 (:sequence-number row)
                                                 (:partition-key row)))))))

(defn run-worker!
  [w]
  (info "Running worker" w)
  (future (.run w)))

(defn shutdown-worker!
  [w]
  (info "Shutting down worker" w)
  (force (.shutdown w)))

(defn create-producer
  [endpoint stream-names origin in-chan]
  (async/go
    (loop []
      (let [msg (async/<! in-chan)]
        (if msg
          (let [_ (info "Sending message" msg)
                [stream-name-key _ _ _ _ opts] msg
                stream-name (get stream-names stream-name-key)
                formatted (apply msg/format-message (conj (vec (butlast msg)) (assoc opts :origin origin)))]
            (kinesis/put-record {:endpoint endpoint}
                                stream-name
                                formatted
                                (str (java.util.UUID/randomUUID)))
            (recur)))))))

(defrecord Kinesis [kinesis-endpoint dynamodb-endpoint region stream-names
                    origin checkpoint
                    producer-in-chan]
  comms/Communications
  (send-event! [comms event version payload]
    (comms/send-event! comms event version payload {}))

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
    ;; TODO
    )
  (attach-event-handler! [this group-id event version handler]
    ;; TODO
    )
  (attach-command-handler! [{:keys [stream-names workers] :as this}
                            group-id command version handler]
    (info "Attaching command handler for" command version)
    (let [worker (create-worker (msg/msg-handler-fn handler
                                                    (partial msg/handle-result this :command))
                                (msg/process-msg? :command command version)
                                {:stream-name (:command stream-names)
                                 :app-name (sanitize-app-name group-id)
                                 :kinesis-endpoint kinesis-endpoint
                                 :checkpoint checkpoint
                                 :region region})]
      (swap! workers conj worker)
      (run-worker! worker)
      nil))

  (detach-handler! [this handler]
    ;; TODO
    )
  component/Lifecycle
  (start [component]
    (timbre/merge-config! {:ns-blacklist ["org.apache.http.*"
                                          "com.amazonaws.http.*"
                                          "com.amazonaws.request"
                                          "com.amazonaws.requestId"
                                          "com.amazonaws.auth"
                                          "com.amazonaws.auth.*"
                                          "com.amazonaws.services.kinesis.metrics.*"
                                          "com.amazonaws.services.kinesis.leases.*"
                                          "com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardConsumer"]})
    (if-not (:producer-in-ch component)
      (let [stream-names (or stream-names {:command "command" :event "event"})
            origin (or origin (try (.. java.net.InetAddress getLocalHost getHostName)
                                   (catch Throwable _ "<unknown>")))
            producer-chan      (async/chan)]
        (info "Starting Kinesis Producer/Consumer")
        (create-streams! kinesis-endpoint (vals stream-names))
        (create-producer kinesis-endpoint stream-names origin producer-chan)
        (assoc component
               :workers (atom [])
               :stream-names stream-names
               :origin origin
               :producer-in-ch producer-chan))
      component))
  (stop [component]
    (let [{:keys [producer-in-ch workers]} component]
      (if (:producer-in-ch component)
        (do
          (info "Stopping Kinesis Producer/Consumer")
          (async/close! producer-in-ch)
          (run! shutdown-worker! @workers)
          (dissoc component
                  :workers
                  :stream-names
                  :origin
                  :producer-in-ch))
        component))))
