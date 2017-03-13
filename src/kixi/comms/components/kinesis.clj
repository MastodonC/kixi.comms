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
            [taoensso.timbre :as timbre :refer [error info debug]]
            [amazonica.aws.kinesis :as kinesis]
            [byte-streams :as bs]))

(defn sanitize-app-name
  [profile s]
  (str "kixi-comms-"
       (name profile) "-app-"
       (-> s
           (clojure.string/replace #"\:" "")
           (clojure.string/replace #"\/" "_"))))

(defn get-group-ids-seen
  [{:keys [group-ids-seen]}]
  @group-ids-seen)

(defn list-streams
  [endpoint]
  (kinesis/list-streams {:endpoint endpoint}))

(defn get-stream-status
  [endpoint stream-name]
  (get-in (kinesis/describe-stream {:endpoint endpoint} stream-name)
          [:stream-description :stream-status]))

(defn create-streams!
  [endpoint streams create-delay]
  (let [{:keys [stream-names]} (list-streams endpoint)
        create-delay (or create-delay 500)
        stream-names-set (set stream-names)
        shards 1]
    (run! (fn [stream-name]
            (when-not (contains? stream-names-set stream-name)
              (info "Creating stream" stream-name "with" shards "shard(s)!")
              (kinesis/create-stream {:endpoint endpoint} stream-name 1)
              (Thread/sleep create-delay)
              (deref
               (future
                 (loop [n 0
                        status (get-stream-status endpoint stream-name)]
                   (when (not (= "ACTIVE" status))
                     (if (< n 50)
                       (do
                         (info "Waiting for" stream-name "status to be ACTIVE:" status)
                         (Thread/sleep 500)
                         (recur (inc n) (get-stream-status endpoint stream-name)))
                       (throw (Exception. (str "Failed to create stream " stream-name)))))))))) streams)))

(defn delete-streams!
  [endpoint streams]
  (run! (fn [stream-name]
          (kinesis/delete-stream {:endpoint endpoint} stream-name)
          (deref
           (future
             (loop []
               (let [running-streams (-> endpoint
                                         (list-streams)
                                         :stream-names
                                         (set))]
                 (when (contains? running-streams stream-name)
                   (recur)))))))
        streams))

(defn create-and-run-worker!
  [msg-handler process-msg?-fn
   {:keys [stream-name app-name kinesis-endpoint dynamodb-endpoint
           region checkpoint initial-position-in-stream]
    :or {checkpoint false
         initial-position-in-stream :LATEST}}]
  (info "Creating worker" stream-name kinesis-endpoint initial-position-in-stream)
  (let [[w id] (kinesis/worker :app app-name
                               :stream stream-name
                               :region-name region
                               :endpoint kinesis-endpoint
                               :dynamodb-endpoint dynamodb-endpoint
                               :checkpoint checkpoint
                               :initial-position-in-stream initial-position-in-stream
                               :processor (fn [records]
                                            (doseq [{:keys [data]} records]
                                              (when (process-msg?-fn data)
                                                (when comms/*verbose-logging*
                                                  (info "Received msg from Kinesis stream" stream-name ":" data))
                                                (msg-handler data)))
                                            true))]
    (info "Running worker" id w)
    [(future (.run w)) w id]))

(defn shutdown-worker!
  [[f w id]]
  (info "Shutting down worker" id w)
  (.shutdown w)
  (deref f))

(defn create-producer
  [endpoint stream-names origin in-chan]
  (async/go
    (loop []
      (let [msg (async/<! in-chan)]
        (if msg
          (let [[stream-name-key _ _ _ _ opts] msg
                stream-name (get stream-names stream-name-key)
                formatted (apply msg/format-message (conj (vec (butlast msg)) (assoc opts :origin origin)))]
            (when comms/*verbose-logging*
              (info "Sending msg to Kinesis stream" stream-name ":" formatted))
            (kinesis/put-record {:endpoint endpoint}
                                stream-name
                                formatted
                                (str (java.util.UUID/randomUUID)))
            (recur)))))))

(defrecord Kinesis [kinesis-endpoint dynamodb-endpoint region stream-names
                    origin checkpoint create-delay profile
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
    (comms/attach-event-with-key-handler! this group-id map-key handler {}))
  (attach-event-with-key-handler!
    [{:keys [stream-names workers group-ids-seen] :as this}
     group-id map-key handler opts]
    (info "Attaching event-with-key handler for" map-key)
    (let [sanitized-app-name (sanitize-app-name profile group-id)
          [_ _ id :as worker]
          (create-and-run-worker!
           (msg/msg-handler-fn handler
                               (partial msg/handle-result this :event))
           (msg/process-msg? :event #(contains? % map-key))
           (merge {:stream-name (:event stream-names)
                   :app-name sanitized-app-name
                   :dynamodb-endpoint dynamodb-endpoint
                   :kinesis-endpoint kinesis-endpoint
                   :checkpoint checkpoint
                   :region region}
                  opts))]
      (swap! workers assoc id worker)
      (swap! group-ids-seen conj sanitized-app-name)
      id)
    )
  (attach-event-handler!
    [this group-id event version handler]
    (comms/attach-event-handler! this group-id event version handler {}))
  (attach-event-handler!
    [{:keys [stream-names workers group-ids-seen] :as this}
     group-id event version handler opts]
    (info "Attaching event handler for" event version)
    (let [sanitized-app-name (sanitize-app-name profile group-id)
          [_ _ id :as worker]
          (create-and-run-worker!
           (msg/msg-handler-fn handler
                               (partial msg/handle-result this :event))
           (msg/process-msg? :event event version)
           (merge {:stream-name (:event stream-names)
                   :app-name sanitized-app-name
                   :dynamodb-endpoint dynamodb-endpoint
                   :kinesis-endpoint kinesis-endpoint
                   :checkpoint checkpoint
                   :region region}
                  opts))]
      (swap! workers assoc id worker)
      (swap! group-ids-seen conj sanitized-app-name)
      id)
    )
  (attach-command-handler!
    [this group-id command version handler]
    (comms/attach-command-handler! this group-id command version handler {}))
  (attach-command-handler!
    [{:keys [stream-names workers group-ids-seen] :as this}
     group-id command version handler opts]
    (info "Attaching command handler for" command version)
    (let [sanitized-app-name (sanitize-app-name profile group-id)
          [_ _ id :as worker]
          (create-and-run-worker!
           (msg/msg-handler-fn handler
                               (partial msg/handle-result this :command))
           (msg/process-msg? :command command version)
           (merge {:stream-name (:command stream-names)
                   :app-name sanitized-app-name
                   :dynamodb-endpoint dynamodb-endpoint
                   :kinesis-endpoint kinesis-endpoint
                   :checkpoint checkpoint
                   :region region}
                  opts))]
      (swap! workers assoc id worker)
      (swap! group-ids-seen conj sanitized-app-name)
      id))

  (detach-handler! [{:keys [workers] :as this} worker-id]
    (when-let [worker (get @workers worker-id)]
      (shutdown-worker! worker)
      (swap! workers dissoc worker-id)))
  component/Lifecycle
  (start [component]
    (timbre/merge-config! {:ns-blacklist ["org.apache.http.*"
                                          "com.amazonaws.http.*"
                                          "com.amazonaws.request"
                                          "com.amazonaws.requestId"
                                          "com.amazonaws.auth"
                                          "com.amazonaws.auth.*"
                                          "com.amazonaws.internal.SdkSSLSocket"
                                          "com.amazonaws.services.kinesis.metrics.*"
                                          "com.amazonaws.services.kinesis.leases.*"
                                          "com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardConsumer"
                                          "com.amazonaws.services.kinesis.clientlibrary.lib.worker.ProcessTask"]})
    (if-not (:producer-in-ch component)
      (let [stream-names (or stream-names {:command "command" :event "event"})
            origin (or origin (try (.. java.net.InetAddress getLocalHost getHostName)
                                   (catch Throwable _ "<unknown>")))
            producer-chan      (async/chan)]
        (info "Starting Kinesis Producer/Consumer")
        (create-streams! kinesis-endpoint (vals stream-names) create-delay)
        (create-producer kinesis-endpoint stream-names origin producer-chan)
        (assoc component
               :group-ids-seen (atom [])
               :workers (atom {})
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
          (run! shutdown-worker! (vals @workers))
          (dissoc component
                  :group-ids-seen
                  :workers
                  :stream-names
                  :origin
                  :producer-in-ch))
        component))))
