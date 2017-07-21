(ns kixi.comms.messages
  (:require [cognitect.transit :as transit]
            [clojure.spec :as s]
            [clojure.core.async :as async]
            [taoensso.timbre :as timbre :refer [error info]]
            [kixi.comms.time :as t]
            [kixi.comms :as comms]
            [kixi.comms.schema :as ks])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio ByteBuffer]))

(defn process-msg?
  ([msg-type pred]
   (fn [msg]
     (when (and (= (name msg-type)
                   (:kixi.comms.message/type msg))
                (pred msg))
       msg)))
  ([msg-type event version]
   (fn [msg]
     (or
                                        ;old format
      (when (and
             (= (name msg-type)
                (:kixi.comms.message/type msg))
             (= event
                (or (:kixi.comms.command/key msg)
                    (:kixi.comms.event/key msg)))
             (= version
                (or (:kixi.comms.command/version msg)
                    (:kixi.comms.event/version msg))))
        msg)
                                        ;new format
      (when (and
             (= msg-type
                (:kixi.message/type msg))
             (= event
                (or (:kixi.command/type msg)
                    (:kixi.event/type msg)))
             (= version
                (or (:kixi.command/version msg)
                    (:kixi.event/version msg))))
        msg)))))

(defn vec-if-not
  [x]
  (if (sequential? x)
    x
    [x]))

(defn unsafe-event
  [msg]
  (fn [resp]
    (when (and (= (:kixi.comms.event/key msg)
                  (:kixi.comms.event/key resp))
               (= (:kixi.comms.event/version msg)
                  (:kixi.comms.event/version resp)))
      (error (str "Infinte loop defeated: " msg))
      true)))

(defn handle-result
  [comms-component msg-type original result]
  (when (or (= msg-type :command) result)
    (if-not (s/valid? ::ks/event-result result)
      (throw (Exception. (str "Handler must return a valid event result: "
                              (s/explain-data ::ks/event-result result))))
      (letfn [(send-event-fn! [{:keys [kixi.comms.event/key
                                       kixi.comms.event/version
                                       kixi.comms.event/partition-key
                                       kixi.comms.event/payload] :as f}]
                (comms/send-event! comms-component key version payload
                                   (merge
                                    {:kixi.comms.command/id (:kixi.comms.command/id original)
                                     :seq-num (:seq-num f)}
                                    (when partition-key
                                      {:kixi.comms.event/partition-key partition-key}))))]
        (->> result
             vec-if-not
             (remove (unsafe-event original))
             (map-indexed #(assoc %2 :seq-num %1))
             (run! send-event-fn!))))))

(defn msg-handler-fn
  [component-handler result-handler]
  (fn [msg]
    (try
      (result-handler msg (component-handler msg))
      (catch Exception e
        (error e (str "Consumer exception processing msg. Msg: " msg))))))

(s/def ::command-result
  (let [single-result (s/cat :event :kixi/event
                             :opts :kixi.event/options)]
    (s/or :single single-result
          :multi (s/coll-of single-result))))

(defn command-handler
  [comms-component service-cmd-handler]
  (fn [command]
    (when-not (s/valid? :kixi/command command)
      (throw (ex-info "Invalid command" (s/explain-data :kixi/command command))))
    (let [result (service-cmd-handler command)]
      (when-not (s/valid? ::command-result result)
        (throw (ex-info "Invalid command result" (s/explain-data ::command-result result))))
      (let [conformed-result (apply hash-map (s/conform ::command-result result))]
        (doseq [{:keys [event opts]} (or (:multi conformed-result)
                                            [(:single conformed-result)])]
          (comms/send-valid-event! comms-component
                                   event
                                   opts))))))

(s/def ::event-result
  (let [single-result (s/cat :cmd :kixi/command
                             :opts :kixi.command/options)]
    (s/or :nil nil?
          :single single-result
          :multi (s/coll-of single-result))))

(defn event-handler
  [comms-component service-event-handler]
  (fn [event]
    (when-not (s/valid? :kixi/event event)
      (throw (ex-info "Invalid event" (s/explain-data :kixi/event event))))
    (let [result (service-event-handler event)]
      (when-not (s/valid? ::event-result result)
        (throw (ex-info "Invalid event result" (s/explain-data ::event-result result))))
      (let [conformed-result (apply hash-map (s/conform ::event-result result))]
        (doseq [{:keys [cmd opts]} (or (:multi conformed-result)
                                       [(:single conformed-result)])]
          (comms/send-valid-command! comms-component
                                     cmd
                                     opts))))))

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
  [_ command-key command-version user payload {:keys [kixi.comms.command/id created-at]}]
  {:kixi.comms.message/type       "command"
   :kixi.comms.command/id         (or id (str (java.util.UUID/randomUUID)))
   :kixi.comms.command/key        command-key
   :kixi.comms.command/version    command-version
   :kixi.comms.command/created-at (or created-at (t/timestamp))
   :kixi.comms.command/payload    payload
   :kixi.comms.command/user       user})

(defmethod format-message
  :event
  [_ event-key event-version _ payload {:keys [origin kixi.comms.command/id]}]
  (let [r {:kixi.comms.message/type     "event"
           :kixi.comms.event/id         (str (java.util.UUID/randomUUID))
           :kixi.comms.event/key        event-key
           :kixi.comms.event/version    event-version
           :kixi.comms.event/created-at (t/timestamp)
           :kixi.comms.event/payload    payload
           :kixi.comms.event/origin     origin}]
    (if id
      (assoc r :kixi.comms.command/id id)
      r)))

(defn edn-to-bytebuffer
  [edn]
  (let [data (str edn)
        buf (ByteBuffer/wrap (.getBytes data))]
    buf))

(defn bytebuffer-to-edn [^ByteBuffer byte-buffer]
  (let [b (byte-array (.remaining byte-buffer))]
    (.get byte-buffer b)
    (clojure.edn/read-string (String. b))))
