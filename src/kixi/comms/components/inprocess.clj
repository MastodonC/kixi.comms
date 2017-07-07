(ns kixi.comms.components.inprocess
  (:require [clojure.core.async :as async]
            [com.stuartsierra.component :as component]
            [kixi.comms :as comms]
            [kixi.comms.messages :as msg]
            [taoensso.timbre :as timbre :refer [debug info error]]))


(defn sanitize-app-name
  [profile s]
  (str "kixi-comms-"
       (name profile) "-app-"
       (-> s
           (clojure.string/replace #"\:" "")
           (clojure.string/replace #"\/" "_"))))

(defn put-msg
  [raw-msg id->handle-msg-and-process-msg-atom]
  (let [opts (last raw-msg)
        msg (apply msg/format-message (conj (vec (butlast raw-msg)) opts))]
    (when comms/*verbose-logging*
      (info "Received msg in process: " msg))
    (doseq [{:keys [process-msg? handle-msg app-name]} (vals @id->handle-msg-and-process-msg-atom)]
      (if (process-msg? msg)
        (try
          (debug "# Forwarding last message to handler" app-name)
          (handle-msg msg)
          (catch Throwable e
            (error e "Handler threw an exception:" app-name msg)))
        (debug "# NOT forwarding last message to handler" app-name)))))

(defrecord InProcess
    [app-name profile buffersize
     event-chan cmd-chan
     id->handle-msg-and-process-msg-atom
     id->command-handle-msg-and-process-msg-atom]
    comms/Communications

    (send-event! [comms event version payload]
      (comms/send-event! comms event version payload {}))

    (send-event! [_ event version payload opts]
      (debug "# Putting event: " event version)
      (put-msg [:event event version nil payload opts]
               id->handle-msg-and-process-msg-atom))

    (send-command! [comms command version user payload]
      (comms/send-command! comms command version user payload {}))

    (send-command! [_ command version user payload opts]
      (debug "# Putting command: " command version)
      (put-msg [:command command version user payload opts]
               id->command-handle-msg-and-process-msg-atom))

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

    (detach-handler! [{:keys [id->handle-msg-and-process-msg-atom
                              id->command-handle-msg-and-process-msg-atom] :as this} worker-id]
      (when id->handle-msg-and-process-msg-atom
        (swap! id->handle-msg-and-process-msg-atom dissoc worker-id))
      (when id->command-handle-msg-and-process-msg-atom
        (swap! id->command-handle-msg-and-process-msg-atom dissoc worker-id)))
    component/Lifecycle
    (start [component]
      (if-not (:id->handle-msg-and-process-msg-atom component)
        (let [id->handle-msg-and-process-msg-atom (atom {})
              id->command-handle-msg-and-process-msg-atom (atom {})]
          (info "Starting In Process Communications Layer")
          (assoc component
                 :id->handle-msg-and-process-msg-atom id->handle-msg-and-process-msg-atom
                 :id->command-handle-msg-and-process-msg-atom id->command-handle-msg-and-process-msg-atom))
        component))
    (stop [component]
      (let [{:keys [cmd-chan event-chan]} component]
        (if (:id->handle-msg-and-process-msg-atom component)
          (do
            (info "Stopping In Process Communication Layer")
            (dissoc component
                    :streams
                    :origin))
          component))))

