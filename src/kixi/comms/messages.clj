(ns kixi.comms.messages
  (:require [cognitect.transit :as transit]
            [clojure.spec.alpha :as s]
            [com.gfredericks.schpec :as sh]
            [clojure.core.async :as async]
            [taoensso.timbre :as timbre :refer [error info]]
            [kixi.comms.time :as t]
            [kixi.comms :as comms]
            [kixi.comms.schema :as ks])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio ByteBuffer]))


(sh/alias 'event 'kixi.event)
(sh/alias 'cmd 'kixi.command)


(defn process-msg?
  ([msg-type pred]
   (fn [msg]
     (when (and (or (= (name msg-type)
                       (:kixi.comms.message/type msg))
                    (= msg-type
                       (:kixi.message/type msg)))
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
                (or (::cmd/type msg)
                    (::event/type msg)))
             (= version
                (or (::cmd/version msg)
                    (::event/version msg))))
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
                (if-not partition-key
                  (throw (ex-info "Handler must return an explicit partition key"
                                  f))
                  (comms/send-event! comms-component key version payload
                                     (merge
                                      {:kixi.comms.command/id (:kixi.comms.command/id original)
                                       :seq-num (:seq-num f)}
                                      (when partition-key
                                        {:kixi.comms.event/partition-key partition-key})))))]
        (->> result
             vec-if-not
             (remove (unsafe-event original))
             (map-indexed #(assoc %2 :seq-num %1))
             (run! send-event-fn!))))))

(defn msg-handler-fn
  [component-handler result-handler]
  (fn [msg]
    (result-handler msg (component-handler msg))))

(s/def ::command-result
  (let [single-result (s/cat :event map?
                             :opts :kixi.event/options)]
    (s/or :single single-result
          :multi (s/coll-of single-result))))

(defn cmd-result->events
  [result]
  (let [conformed-result (apply hash-map (s/conform ::command-result result))]
    (or (:multi conformed-result)
        [(:single conformed-result)])))

(defn validate-events
  [command events]
  (let [allowed-event-types (comms/command-type->event-types command)]
    (doseq [{:keys [event]} events]
      (when-not (allowed-event-types ((juxt ::event/type ::event/version) event))
        (throw (ex-info "Invalid command result" {:allowed-events-types allowed-event-types
                                                  :command-type ((juxt ::cmd/type ::cmd/version) command)
                                                  :returned-event-type ((juxt ::event/type ::event/version) event)})))
      (when-not (s/valid? :kixi/event event)
        (throw (ex-info "Invalid event" (s/explain-data :kixi/event event)))))))

(defn uuid
  []
   (str (java.util.UUID/randomUUID)))

(defn tag-event
  [cmd event-opts]
  (update event-opts
          :event
          (fn [event]
            (merge event
                   (select-keys cmd
                                [::cmd/id
                                 :kixi/user])
                   {:kixi.message/type :event
                    ::event/id (uuid)
                    ::event/created-at (comms/timestamp)}))))

(defn command-handler
  [comms-component service-cmd-handler]
  (fn [command]
    (let [result (service-cmd-handler command)]
      (when-not (s/valid? ::command-result result)
        (throw (ex-info "Invalid command result" (s/explain-data ::command-result result))))
      (let [events (map (partial tag-event command)
                        (cmd-result->events result))]
        (validate-events command events)
        (doseq [{:keys [event opts]} events]
          (comms/send-valid-event! comms-component
                                   event
                                   opts))))))

(s/def ::event-result
  (let [single-result (s/cat :cmd map?
                             :opts :kixi.command/options)]
    (s/or :single single-result
          :multi (s/coll-of single-result))))

(defn validate-commands
  [event commands]
  (let [allowed-cmd-types (comms/event-type->command-types event)]
    (doseq [{:keys [cmd]} commands]
      (when-not (allowed-cmd-types ((juxt ::cmd/type ::cmd/version) cmd))
        (throw (ex-info "Invalid event result" {:allowed-command-types allowed-cmd-types
                                                :event-type ((juxt ::event/type ::event/version) event)
                                                :returned-command-type ((juxt ::cmd/type ::cmd/version) cmd)}))))))

(defn event-result->commands
  [result]
  (let [conformed-result (apply hash-map (s/conform ::event-result result))]
    (or (:multi conformed-result)
        [(:single conformed-result)])))

(defn tag-command
  [event command+opts]
  (update command+opts
          :cmd
          (fn [command]
            (merge command
                   (select-keys event
                                [:kixi/user
                                 ::event/id])))))

(defn event-handler
  [comms-component service-event-handler]
  (fn [event]
    (when-not (s/valid? :kixi/event event)
      (throw (ex-info "Invalid event" (s/explain-data :kixi/event event))))
    (let [result (service-event-handler event)]
      (when (s/valid? ::event-result result)
        (when-let [conformed-result (map (partial tag-command event)
                                         (event-result->commands result))]
          (validate-commands event conformed-result)
          (doseq [{:keys [cmd opts]} conformed-result]
            (println "OMG WE ARE SENDING THIS" cmd)
            (comms/send-valid-command! comms-component
                                       cmd
                                       opts)))))))

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
