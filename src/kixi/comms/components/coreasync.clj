(ns kixi.comms.components.coreasync
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

(defn attach-generic-processing-switch
  [target-chan id->handle-msg-and-process-msg-atom]
  (async/go-loop [msg (async/<! target-chan)]
    (when msg
      (when comms/*verbose-logging*
        (info "Received msg from Core Async: " msg))
      (doseq [{:keys [process-msg? handle-msg app-name]} (vals @id->handle-msg-and-process-msg-atom)]
                                        ;should process in parrel
        (if (process-msg? msg)
          (try
            (debug "# Forwarding last message to handler" app-name)
            (handle-msg msg)
            (catch Throwable e
              (error e "Handler threw an exception:" app-name msg)))
          (debug "# NOT forwarding last message to handler" app-name)))
      (recur (async/<! target-chan)))))

(def buffersize-default 100)

(defrecord CoreAsync [app-name profile buffersize
                      event-chan cmd-chan
                      id->handle-msg-and-process-msg-atom
                      id->command-handle-msg-and-process-msg-atom]
  comms/Communications

  (send-event! [comms event version payload]
    (comms/send-event! comms event version payload {}))

  (send-event! [{:keys [event-chan]} event version payload opts]
    (when event-chan
      (debug "# Putting event: " event version)
      (async/put! event-chan 
                  (msg/format-message :event event version nil payload opts))))

  (-send-event! [{:keys [event-chan]} event opts]    
    (when event-chan
      (debug "# Putting event: " event)
      (async/put! event-chan event)))

  (send-command! [comms command version user payload]
    (comms/send-command! comms command version user payload {}))

  (send-command! [{:keys [cmd-chan]} command version user payload opts]
    (when cmd-chan
      (debug "# Putting command: " command version)
      (async/put! cmd-chan 
                  (msg/format-message :command command version user payload opts))))

  (-send-command! [{:keys [cmd-chan]} command opts]
    (when cmd-chan
      (debug "# Putting command: " command)
      (async/put! cmd-chan command)))
  
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
    (if-not (:event-chan component)
      (let [cmd-chan (async/chan (or buffersize buffersize-default))
            event-chan (async/chan (or buffersize buffersize-default))
            id->handle-msg-and-process-msg-atom (atom {})
            id->command-handle-msg-and-process-msg-atom (atom {})]
        (info "Starting Core Async Communications Layer")
        (assoc component
               :event-chan event-chan
               :cmd-chan cmd-chan
               :id->handle-msg-and-process-msg-atom id->handle-msg-and-process-msg-atom
               :generic-event-worker (attach-generic-processing-switch
                                      event-chan
                                      id->handle-msg-and-process-msg-atom)
               :id->command-handle-msg-and-process-msg-atom id->command-handle-msg-and-process-msg-atom
               :generic-command-worker (attach-generic-processing-switch
                                        cmd-chan
                                        id->command-handle-msg-and-process-msg-atom)))
      component))
  (stop [component]
    (let [{:keys [cmd-chan event-chan]} component]
      (if (:event-chan component)
        (do
          (info "Stopping Core Async Communication Layer")
          (async/close! event-chan)
          (async/close! cmd-chan)
          (dissoc component
                  :workers
                  :streams
                  :origin
                  :producer-in-ch))
        component))))

