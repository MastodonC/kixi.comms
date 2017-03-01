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
            [taoensso.timbre :as timbre :refer [error info]]))

(defrecord Kinesis [host port stream-names origin consumer-config
                    consumer-kill-ch consumer-kill-mult broker-list consumer-loops]
  comms/Communications
  (send-event! [comms event version payload]
    (comms/send-event! comms event version payload {}))

  (send-event! [{:keys []} event version payload opts]
    ;; TODO
    )

  (send-command! [comms command version user payload]
    (comms/send-command! comms command version user payload {}))

  (send-command! [{:keys []} command version user payload opts]
    ;; TODO
    )

  (attach-event-with-key-handler!
    [this group-id map-key handler]
    ;; TODO
    )
  (attach-event-handler! [this group-id event version handler]
    ;; TODO
    )
  (attach-command-handler! [this group-id command version handler]
    ;; TODO
    )
  (detach-handler! [this handler]
    ;; TODO
    )
  component/Lifecycle
  (start [component]
    (if-not (:producer-in-ch component)
      (let [stream-names (or stream-names {:command "command" :event "event"})
            origin (or origin (try (.. java.net.InetAddress getLocalHost getHostName)
                                   (catch Throwable _ "<unknown>")))
            producer-chan      (async/chan)]
        (info "Starting Kinesis Producer/Consumer")
        (assoc component
               :stream-names stream-names
               :origin origin
               :producer-in-ch producer-chan))
      component))
  (stop [component]
    (let [{:keys [producer-in-ch]} component]
      (if (:producer-in-ch component)
        (do
          (info "Stopping Kinesis Producer/Consumer")
          (async/close! producer-in-ch)
          (dissoc :stream-names
                  :origin
                  :producer-in-ch))
        component))))
