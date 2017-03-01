(ns kixi.comms.messages
  (:require [cognitect.transit :as transit]
            [kixi.comms.time :as t])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

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
  ([^String s t]
   (let [in (ByteArrayInputStream. (.getBytes s))
         reader (transit/reader in t)]
     (transit/read reader))))

(defmulti format-message (fn [t _ _ _ _ _] t))

(defmethod format-message
  :command
  [_ command-key command-version user payload {:keys [id created-at]}]
  {:kixi.comms.message/type       "command"
   :kixi.comms.command/id         (or id (str (java.util.UUID/randomUUID)))
   :kixi.comms.command/key        command-key
   :kixi.comms.command/version    command-version
   :kixi.comms.command/created-at (or created-at (t/timestamp))
   :kixi.comms.command/payload    payload
   :kixi.comms.command/user       user})

(defmethod format-message
  :event
  [_ event-key event-version _ payload {:keys [origin command-id]}]
  (let [r {:kixi.comms.message/type     "event"
           :kixi.comms.event/id         (str (java.util.UUID/randomUUID))
           :kixi.comms.event/key        event-key
           :kixi.comms.event/version    event-version
           :kixi.comms.event/created-at (t/timestamp)
           :kixi.comms.event/payload    payload
           :kixi.comms.event/origin     origin}]
    (if command-id
      (assoc r :kixi.comms.command/id command-id)
      r)))
