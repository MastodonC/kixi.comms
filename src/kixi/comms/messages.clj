(ns kixi.comms.messages
  (:require [cognitect.transit :as transit]
            [clojure.spec :as s]
            [clojure.core.async :as async]
            [taoensso.timbre :as timbre :refer [error info]]
            [kixi.comms.time :as t]
            [kixi.comms :as comms]
            [kixi.comms.schema :as ks])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn process-msg?
  ([msg-type pred]
   (fn [msg]
     (when (and (= (name msg-type)
                   (:kixi.comms.message/type msg))
                (pred msg))
       msg)))
  ([msg-type event version]
   (fn [msg]
     (when (and
            (= (name msg-type)
               (:kixi.comms.message/type msg))
            (= event
               (or (:kixi.comms.command/key msg)
                   (:kixi.comms.event/key msg)))
            (= version
               (or (:kixi.comms.command/version msg)
                   (:kixi.comms.event/version msg))))
       msg))))

(defn handle-result
  [comms-component msg-type original result]
  (when (or (= msg-type :command) result)
    (if-not (s/valid? ::ks/event-result result)
      (throw (Exception. (str "Handler must return a valid event result: "
                              (s/explain-data ::ks/event-result result))))
      (letfn [(send-event-fn! [{:keys [kixi.comms.event/key
                                       kixi.comms.event/version
                                       kixi.comms.event/payload] :as f}]
                (comms/send-event! comms-component key version payload
                                   {:command-id (:kixi.comms.command/id original)}))]
        (if (sequential? result)
          (run! send-event-fn! result)
          (send-event-fn! result))))))

(defn msg-handler-fn
  [component-handler result-handler]
  (fn [msg]
    (async/thread
      (try
        (result-handler msg (component-handler msg))
        (catch Exception e
          (error e (str "Consumer exception processing msg. Msg: " msg)))))))

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
