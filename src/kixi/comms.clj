(ns kixi.comms)

(def ^:dynamic *verbose-logging* false)

(defn set-verbose-logging!
  [v]
  (alter-var-root #'*verbose-logging* (fn [_] v)))

(defprotocol Communications
  "send-event opts: command-id
   send-command opts: origin, id"
  (send-event!
    [this event version payload]
    [this event version payload opts])
  (send-command!
    [this command version user payload]
    [this command version user payload opts])
  (attach-event-handler!
    [this group-id event version handler])
  (attach-event-with-key-handler!
    [this group-id map-key handler])
  (attach-command-handler!
    [this group-id event version handler])
  (detach-handler!
    [this handler]))
