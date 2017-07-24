(ns kixi.comms.components.test-base
  (:require [com.stuartsierra.component :as component]
            [com.gfredericks.schpec :as sh]
            [taoensso.timbre :as timbre :refer [error info]]
            [environ.core :refer [env]]))

(def ^:dynamic *wait-tries* (Integer/parseInt (env :wait-tries "160")))
(def ^:dynamic *wait-per-try* (Integer/parseInt (env :wait-per-try "100")))

(sh/alias 'msg 'kixi.message)
(sh/alias 'command 'kixi.command)
(sh/alias 'event 'kixi.event)

(defn old-format?
  [msg]
  (not (::msg/type msg)))

(defn wait
  [ms]
  (Thread/sleep ms))

(defn uuid []
  (str (java.util.UUID/randomUUID)))

(def user {:kixi.user/id (uuid)
           :kixi.user/groups [(uuid)]})

(defn cycle-system-fixture
  [system-func system-atom]
  (fn [all-tests]
    [all-tests]
    (timbre/with-merged-config
      {:level :info}
      (system-func system-atom)
      (all-tests)
      (component/stop-system @system-atom)
      (reset! system-atom nil))))

(defn wait-for-atom
  ([a]
   (wait-for-atom a *wait-tries*))
  ([a tries]
   (wait-for-atom a tries *wait-per-try*))
  ([a tries ms]
   (wait-for-atom a tries ms identity))
  ([a tries ms predicate]
   (loop [try tries]
     (if (pos? try)
       (let [result (when @a (predicate @a))]
         (if (and @a result)
           result
           (do
             (Thread/sleep ms)
             (recur (dec try)))))
       nil))))

(defn reset-as-event!
  [a cmd]
  (reset! a cmd)
  {:kixi.comms.event/key (-> (or (:kixi.comms.command/key cmd)
                                 (:kixi.comms.event/key cmd))
                             (str)
                             (subs 1)
                             (str "-event")
                             (keyword))
   :kixi.comms.event/version (or (:kixi.comms.command/version cmd)
                                 (:kixi.comms.event/version cmd))
   :kixi.comms.event/payload cmd})

(defn cmd->event
  [cmd]
  (if (old-format? cmd)
    {:kixi.comms.event/key (-> (or (:kixi.comms.command/key cmd)
                                   (:kixi.comms.event/key cmd))
                               (str)
                               (subs 1)
                               (str "-event")
                               (keyword))
     :kixi.comms.event/version (or (:kixi.comms.command/version cmd)
                                   (:kixi.comms.event/version cmd))
     :kixi.comms.event/payload cmd}
    [(merge {::event/type (-> cmd
                              ::command/type
                              (str)
                              (subs 1)
                              (str "-event")
                              (keyword))
             ::event/version (::command/version cmd)}
            (select-keys cmd
                         [:id :test]))
     {:partition-key "1"}]))

(defn event->cmd
  [event]
  [(merge {::command/type (-> event
                              ::event/type
                              (str)
                              (subs 1)
                              (str "-cmd")
                              (keyword))
           ::command/version (::event/version event)}
          (select-keys event
                       [:id :test]))
   {:partition-key "1"}])

(defn swap-conj-as-event!
  [a cmd]
  (swap! a conj cmd)
  (cmd->event cmd))

(defn swap-conj-as-cmd!
  [a event]
  (swap! a conj event)
  (event->cmd event))

(defn swap-conj-as-multi-events!
  [cnt a cmd]
  (swap! a conj cmd)
  (do
    (mapv #(-> cmd
               cmd->event
               (assoc :kixi.comms.event/partition-key cnt)
               (assoc-in [:kixi.comms.event/payload :create-order] %))
          (range cnt))))
