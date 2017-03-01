(ns kixi.comms.components.test-base
  (:require [com.stuartsierra.component :as component]))

(def wait-tries 160)
(def wait-per-try 100)

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
    (system-func system-atom)
    (all-tests)
    (component/stop-system @system-atom)
    (reset! system-atom nil)))

(defn wait-for-atom
  ([a]
   (wait-for-atom a wait-tries))
  ([a tries]
   (wait-for-atom a tries wait-per-try))
  ([a tries ms]
   (wait-for-atom a tries ms identity))
  ([a tries ms predicate]
   (loop [try tries]
     (when (pos? try)
       (if (and @a
                (predicate @a))
         @a
         (do
           (Thread/sleep ms)
           (recur (dec try))))))))

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

(defn swap-conj-as-event!
  [a cmd]
  (swap! a conj cmd)
  {:kixi.comms.event/key (-> (or (:kixi.comms.command/key cmd)
                                 (:kixi.comms.event/key cmd))
                             (str)
                             (subs 1)
                             (str "-event")
                             (keyword))
   :kixi.comms.event/version (or (:kixi.comms.command/version cmd)
                                 (:kixi.comms.event/version cmd))
   :kixi.comms.event/payload cmd})
