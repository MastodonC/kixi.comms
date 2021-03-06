(ns kixi.comms.components.kinesis
  (:require [amazonica.aws.kinesis :as kinesis]
            [com.stuartsierra.component :as component]
            [kixi.comms :as comms]
            [kixi.comms.messages :as msg]
            [taoensso.timbre :as timbre :refer [debug info error fatal]]
            [clojure.core.async :as async]
            [clj-time.format :as f]
            [clj-time.coerce :as time-coerce])
  (:import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker))

(def generic-event-worker-postfix "-event-generic-processor")
(def generic-command-worker-postfix "-command-generic-processor")

(defn sanitize-app-name
  [profile s]
  (str "kixi-comms-"
       (name profile) "-app-"
       (-> s
           (clojure.string/replace #"\:" "")
           (clojure.string/replace #"\/" "_"))))

(defn list-streams
  [conn]
  (kinesis/list-streams conn))

(defn get-stream-status
  [conn stream-name]
  (get-in (kinesis/describe-stream conn stream-name)
          [:stream-description :stream-status]))

(defn create-streams!
  [conn streams]
  (let [{:keys [stream-names]} (list-streams conn)
        missing-streams (remove (set stream-names) streams)
        shards 2]
    (doseq [stream-name missing-streams]
      (info "Creating stream" stream-name "with" shards "shard(s)!")
      (kinesis/create-stream conn stream-name shards))
    (doseq [stream-name missing-streams]
      (loop [n 0
             status (get-stream-status conn stream-name)]
        (when (not (= "ACTIVE" status))
          (if (< n 50)
            (do
              (info "Waiting for" stream-name "status to be ACTIVE:" status)
              (Thread/sleep 500)
              (recur (inc n) (get-stream-status conn stream-name)))
            (throw (Exception. (str "Failed to create stream " stream-name)))))))))

(defn stream-exists
  [conn stream]
  (try
    (kinesis/describe-stream conn stream)
    true
    (catch Exception e
      false)))

(defn delete-streams!
  [conn streams]
  (doseq [stream-name streams]
    (kinesis/delete-stream conn stream-name))
  (doseq [stream-name streams]
    (loop []
      (info "Waiting for" stream-name " to be deleted")
      (when (stream-exists conn stream-name)
        (Thread/sleep 100)
        (recur)))))

(def client-config-kws
  #{:app
    :stream
    :worker-id
    :endpoint
    :dynamodb-endpoint
    :initial-position-in-stream
    :initial-position-in-stream-date
    :failover-time-millis
    :shard-sync-interval-millis
    :max-records
    :idle-time-between-reads-in-millis
    :call-process-records-even-for-empty-record-list
    :parent-shard-poll-interval-millis
    :cleanup-leases-upon-shard-completion
    :common-client-config
    :kinesis-client-config
    :dynamodb-client-config
    :cloud-watch-client-config
    :user-agent
    :task-backoff-time-millis
    :metrics-level
    :metrics-buffer-time-millis
    :metrics-max-queue-size
    :validate-sequence-number-before-checkpointing
    :region-name
    :initial-lease-table-read-capacity
    :initial-lease-table-write-capacity})

(def default-client-config
  {:checkpoint false
   :initial-position-in-stream-date (java.util.Date.)})

(defn ensure-arg-types
  [client-config]
  (if (:initial-position-in-stream-date client-config)
    (update client-config
            :initial-position-in-stream-date
            #(time-coerce/to-date (f/parse (f/formatters :basic-date-time-no-ms) %)))
    client-config))

(defn create-and-run-worker!
  [msg-handler client-config]
  (let [shutdown-chan (async/chan)
        processor (fn [records]
                    (reduce
                     (fn [_ data]
                       (try
                         (msg-handler data)
                         true
                         (catch Throwable e
                           (fatal e "Processing exception, shutting down Kinesis processor: " data)
                           (async/>!! shutdown-chan e)
                           (reduced false))))
                     true
                     (map :data records)))
        full-config (merge
                     default-client-config
                     (ensure-arg-types client-config)
                     {:processor processor})
        _ (info "Creating worker" full-config)
        [^Worker w id] (kinesis/worker full-config)]
    (async/go
      (async/<! shutdown-chan)
      (.shutdown w))
    (debug "Running worker" id w)
    [(future (.run w)) w id]))

(defn shutdown-workers!
  [workers]
  (doseq [[f ^Worker w id] workers]
    (info "Shutting down worker" id w)
    (.shutdown w))
  (doseq [[f ^Worker w id] workers]
    (deref f)))

(defn send-old-format
  [conn stream-names origin msg]
  (let [[stream-name-key _ _ _ _ opts] msg
        stream-name (get stream-names stream-name-key)
        formatted (apply msg/format-message (conj (vec (butlast msg)) (assoc opts :origin origin)))
        partition-key (or (:kixi.comms.event/partition-key opts)
                          (:kixi.comms.command/partition-key opts))
        seq-num (:seq-num opts)
        cmd-id (:kixi.comms.command/id opts)]
    (when comms/*verbose-logging*
      (info "Sending msg to Kinesis stream" stream-name ":" formatted))
    (when-not partition-key
      (throw (ex-info "Partition key must be specified" {:event formatted
                                                         :opts opts})))
    (kinesis/put-record conn
                        stream-name
                        formatted
                        partition-key
                        (some-> seq-num str))))

(defn send-new-format
  [conn stream-names origin [stream-name-key msg opts]]
  (let [stream-name (get stream-names stream-name-key)
        seq-num (:seq-num opts)]
    (when comms/*verbose-logging*
      (info "Sending msg to Kinesis stream" stream-name ":" msg))
    (kinesis/put-record conn
                        stream-name
                        (assoc msg
                               :kixi.message/origin origin)
                        (:partition-key opts)
                        (some-> seq-num str))))

(defn attach-generic-processing-switch
  [config id->handle-msg-and-process-msg-atom]
  (create-and-run-worker!
   (fn [msg]
     (when comms/*verbose-logging*
       (info "Received msg from Kinesis" (:stream config) "stream:" msg))
     (doseq [{:keys [process-msg? handle-msg app-name]} (vals @id->handle-msg-and-process-msg-atom)]
                                        ;should process in parrel
       (if (process-msg? msg)
         (do (debug "# Forwarding last message from" (:stream config)" to handler" app-name)
             (handle-msg msg))
         (debug "# NOT forwarding last message from" (:stream config)" to handler" app-name))))
   config))

(defn event-worker-app-name
  [app profile]
  (str profile "-" app generic-event-worker-postfix))

(defn command-worker-app-name
  [app profile]
  (str profile "-" app generic-command-worker-postfix))

(defrecord Kinesis [app-name endpoint dynamodb-endpoint region-name streams
                    origin checkpoint profile kinesis-options
                    producer-fn-old-format
                    producer-fn-new-format
                    id->handle-msg-and-process-msg-atom
                    id->command-handle-msg-and-process-msg-atom]
  comms/Communications

  (send-event! [{:keys [producer-fn-old-format]} event version payload opts]
    (when producer-fn-old-format
      (producer-fn-old-format [:event event version nil payload opts])))

  (-send-event! [{:keys [producer-fn-new-format]} event opts]
    (when producer-fn-new-format
      (debug "# Putting event: " event)
      (producer-fn-new-format [:event event opts])))

  (send-command! [{:keys [producer-fn-old-format]} command version user payload opts]
    (when producer-fn-old-format
      (producer-fn-old-format [:command command version user payload opts])))

  (-send-command! [{:keys [producer-fn-new-format]} command opts]
    (when producer-fn-new-format
      (debug "# Putting command: " command)
      (producer-fn-new-format [:command command opts])))

  (attach-event-with-key-handler!
    [{:keys [stream-names workers] :as this}
     group-id map-key handler]
    (info "Attaching event-with-key handler for" map-key)
    (let [sanitized-app-name (sanitize-app-name profile group-id)
          id (java.util.UUID/randomUUID)]
      (swap! id->handle-msg-and-process-msg-atom assoc
             id {:app-name sanitized-app-name
                 :process-msg? (msg/process-msg? :event #(contains? % map-key))
                 :handle-msg  (msg/msg-handler-fn handler
                                                  (partial msg/handle-result this :event))})
      id))
  (attach-event-handler!
    [{:keys [stream-names workers] :as this}
     group-id event version handler]
    (info "Attaching event handler for" event version)
    (let [sanitized-app-name (sanitize-app-name profile group-id)
          id (java.util.UUID/randomUUID)]
      (swap! id->handle-msg-and-process-msg-atom assoc
             id {:app-name sanitized-app-name
                 :process-msg? (msg/process-msg? :event event version)
                 :handle-msg  (msg/msg-handler-fn handler
                                                  (partial msg/handle-result this :event))})
      id))

  (attach-validating-event-handler!
    [{:keys [stream-names workers] :as this}
     group-id event version handler]
    (info "Attaching event handler for" event version)
    (let [sanitized-app-name (sanitize-app-name profile group-id)
          id (java.util.UUID/randomUUID)]
      (swap! id->handle-msg-and-process-msg-atom assoc
             id {:app-name sanitized-app-name
                 :process-msg? (msg/process-msg? :event event version)
                 :handle-msg  (msg/event-handler this handler)})
      id))

  (attach-command-handler!
    [{:keys [stream-names workers] :as this}
     group-id command version handler]
    (info "Attaching command handler for" command version)

    (let [sanitized-app-name (sanitize-app-name profile group-id)
          id (java.util.UUID/randomUUID)]
      (swap! id->command-handle-msg-and-process-msg-atom assoc
             id {:app-name sanitized-app-name
                 :process-msg? (msg/process-msg? :command command version)
                 :handle-msg  (msg/msg-handler-fn handler
                                                  (partial msg/handle-result this :command))})
      id))

  (attach-validating-command-handler!
    [{:keys [stream-names workers] :as this}
     group-id command version handler]
    (info "Attaching command handler for" command version)
    (let [sanitized-app-name (sanitize-app-name profile group-id)
          id (java.util.UUID/randomUUID)]
      (swap! id->command-handle-msg-and-process-msg-atom assoc
             id {:app-name sanitized-app-name
                 :process-msg? (msg/process-msg? :command command version)
                 :handle-msg  (msg/command-handler this handler)})
      id))

  (detach-handler! [{:keys [id->handle-msg-and-process-msg-atom
                            id->command-handle-msg-and-process-msg-atom] :as this} worker-id]
    (when id->handle-msg-and-process-msg-atom
      (swap! id->handle-msg-and-process-msg-atom dissoc worker-id))
    (when id->command-handle-msg-and-process-msg-atom
      (swap! id->command-handle-msg-and-process-msg-atom dissoc worker-id)))
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
    (if-not (:producer-fn-new-format component)
      (let [streams (or streams {:command "command" :event "event"})
            origin (or origin
                       (try (.. java.net.InetAddress
                                getLocalHost getHostName)
                            (catch Throwable _ "<unknown>")))
            id->handle-msg-and-process-msg-atom (atom {})
            id->command-handle-msg-and-process-msg-atom (atom {})
            conn {:endpoint endpoint
                  :region-name region-name}
            producer-f-new-format (partial send-new-format
                                           conn streams origin)
            producer-f-old-format (partial send-old-format
                                           conn streams origin)]
        (info "Starting Kinesis Producer/Consumer")
        (create-streams! conn (vals streams))
        (merge
         (assoc component
                :streams streams
                :origin origin
                :producer-fn-new-format producer-f-new-format
                :producer-fn-old-format producer-f-old-format
                :conn conn)
         (when (:event streams)
           {:id->handle-msg-and-process-msg-atom id->handle-msg-and-process-msg-atom
            :generic-event-worker (attach-generic-processing-switch
                                   (-> (select-keys component client-config-kws)
                                       (assoc :stream (:event streams))
                                       (update :app
                                               (fn [n] (event-worker-app-name n profile))))
                                   id->handle-msg-and-process-msg-atom)})
         (when (:command streams)
           {:id->command-handle-msg-and-process-msg-atom id->command-handle-msg-and-process-msg-atom
            :generic-command-worker (attach-generic-processing-switch
                                     (-> (select-keys component client-config-kws)
                                         (assoc :stream (:command streams))
                                         (update :app
                                                 (fn [n] (command-worker-app-name n profile))))
                                     id->command-handle-msg-and-process-msg-atom)})))
      component))
  (stop [component]
    (let [{:keys [producer-fn]} component]
      (if (:producer-fn-new-format component)
        (do
          (info "Stopping Kinesis Producer/Consumer")
          (shutdown-workers! (keep identity
                                   [(:generic-event-worker component)
                                    (:generic-command-worker component)]))
          (dissoc component
                  :workers
                  :streams
                  :origin
                  :producer-fn-new-format
                  :producer-fn-old-format))
        component))))
