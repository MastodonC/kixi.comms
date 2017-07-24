(ns kixi.comms
  (:require [clojure.spec :as s]
            [com.gfredericks.schpec :as sh]
            [kixi.data-types :as t]))

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
  (-send-event!
    [this event opts])
  (send-command!
    [this command version user payload]
    [this command version user payload opts])
  (-send-command!
    [this command opts])
  (attach-event-handler!
    [this group-id event version handler])
  (attach-event-with-key-handler!
    [this group-id map-key handler])
  (attach-validating-event-handler!
    [this group-id event version handler])
  (attach-command-handler!
    [this group-id event version handler])
  (attach-validating-command-handler!
    [this group-id event version handler])
  (detach-handler!
    [this handler]))


(s/def ::partition-key string?)

(sh/alias 'command 'kixi.command)
(sh/alias 'msg 'kixi.message)
(sh/alias 'event 'kixi.event)


(defmulti command-payload (juxt ::command/type
                                ::command/version))

(s/def ::command/payload
  (s/multi-spec command-payload
                (fn [gend-val dispatch-val]
                  (assoc gend-val
                         ::command/type (first dispatch-val)
                         ::command/version (second dispatch-val)))))

(s/def :kixi/command
  (s/and 
   (s/merge ::command/payload
            (s/keys :req [::msg/type
                          ::command/id
                          ::command/type
                          ::command/version
                          :kixi/user]
                    :opt [::event/id]))
   #(= :command (::msg/type %))))

(s/def ::command/options
  (s/keys :req-un [::partition-key]))

(defn send-valid-command!
  [impl command opts]
  (let [cmd-with-id (assoc command ::command/id 
                           (or (::command/id command)
                               (str (java.util.UUID/randomUUID)))
                           :kixi.message/type :command)]
    (when-not (s/valid? :kixi/command cmd-with-id)
      (throw (ex-info "Invalid command" (s/explain-data :kixi/command cmd-with-id))))
    (when-not (s/valid? ::command/options opts)
      (throw (ex-info "Invalid command options" (s/explain-data ::command/options opts))))
    (-send-command! impl
                    cmd-with-id
                    opts)))

(defmulti event-payload (juxt ::event/type
                              ::event/version))

(s/def ::event/payload
  (s/multi-spec event-payload
                (fn [gend-val dispatch-val]
                  (assoc gend-val
                         ::event/type (first dispatch-val)
                         ::event/version (second dispatch-val)))))

(s/def :kixi/event
  (s/and
   (s/merge ::event/payload
            (s/keys :req [::msg/type
                          ::event/type
                          ::event/version
                          ::command/id
                          :kixi/user]))
   #(= :event (::msg/type %))))

(s/def ::event/options
  (s/keys :req-un [::partition-key]))

(defn send-valid-event!
  [impl event opts]
  (when-not (s/valid? :kixi/event event)
    (throw (ex-info "Invalid event" (s/explain-data :kixi/event event))))
  (when-not (s/valid? ::event/options opts)
    (throw (ex-info "Invalid event options" (s/explain-data ::event/options opts))))
  (-send-event! impl
                event
                opts))

(defmulti command-type->event-types
  "Services must define the relationship between a command type and a set of event types it can result in"
  (juxt ::command/type ::command/version))

(defmulti event-type->command-types
  "Event handlers may emmit commands, such relationships must be defined"
  (juxt ::event/type ::event/version))
