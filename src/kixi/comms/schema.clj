(ns kixi.comms.schema
  (:require [clojure.spec    :as s]
            [kixi.comms.time :as t]))

(defn uuid?
  [s]
  (re-find #"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" s))

(defn semver?
  [s]
  (re-find #"^\d+\.\d+\.\d+$" s))

(defn -keyword?
  [x]
  (cond
    (clojure.core/keyword? x) x
    (clojure.core/string? x) (clojure.core/keyword x)
    :else :clojure.spec/invalid))

(def kixi-keyword?
  (s/conformer -keyword?))

(s/def :kixi.comms.command/id uuid?)
(s/def :kixi.comms.command/key kixi-keyword?)
(s/def :kixi.comms.command/version semver?)
(s/def :kixi.comms.command/receipt uuid?)
(s/def :kixi.comms.command/created-at t/timestamp?)
(s/def :kixi.comms.command/payload (constantly true))

(s/def :kixi.comms.event/id uuid?)
(s/def :kixi.comms.event/key kixi-keyword?)
(s/def :kixi.comms.event/version semver?)
(s/def :kixi.comms.event/created-at t/timestamp?)
(s/def :kixi.comms.event/payload (constantly true))
(s/def :kixi.comms.event/origin string?)

(s/def :kixi.comms.query/id uuid?)
(s/def :kixi.comms.query/body (constantly true))

(defmulti message-type :kixi.comms.message/type)

(defmethod message-type "command" [_]
  (s/keys :req [:kixi.comms.message/type
                :kixi.comms.command/id
                :kixi.comms.command/key
                :kixi.comms.command/version
                :kixi.comms.command/created-at
                :kixi.comms.command/payload]))

(defmethod message-type "event" [_]
  (s/keys :req [:kixi.comms.message/type
                :kixi.comms.event/id
                :kixi.comms.event/key
                :kixi.comms.event/version
                :kixi.comms.event/created-at
                :kixi.comms.event/payload
                :kixi.comms.event/origin]
          :opt [:kixi.comms.command/id]))

(defmethod message-type "query" [_]
  (s/keys :req [:kixi.comms.message/type
                :kixi.comms.query/id
                :kixi.comms.query/body]))

(s/def :kixi.comms.message/message
  (s/multi-spec message-type :kixi.comms.message/type))

(s/def :kixi.comms.message/command
  (s/and #(= (:kixi.comms.message/type %) "command")
         :kixi.comms.message/message))

(s/def :kixi.comms.message/event
  (s/and #(= (:kixi.comms.message/type %) "event")
         :kixi.comms.message/message))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::partial-event
  (s/keys :req [:kixi.comms.event/key
                :kixi.comms.event/version
                :kixi.comms.event/payload]))

(s/def ::event-result
  (s/or :result ::partial-event
        :results (s/coll-of ::partial-event)))
