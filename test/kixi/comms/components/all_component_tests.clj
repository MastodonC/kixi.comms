(ns kixi.comms.components.all-component-tests
  (:require  [clojure.test :refer :all]
             [kixi.comms :as comms]
             [kixi.comms.components.test-base :refer :all]))

(def long-session-timeout 10000)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn contains-event-id?
  [id]
  (fn [events]
    (some (fn [c] (when (= id (get-in c [:kixi.comms.event/payload :id])) c)) events)))

(defn contains-command-id?
  [id]
  (fn [commands]
    (some (fn [c] (when (= id (get-in c [:kixi.comms.command/payload :id])) c)) commands)))

(defn command-roundtrip-test
  [component opts]
  (let [result (atom [])
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-command-handler! component :component-a :test/foo "1.0.0"
                                   (partial swap-conj-as-event! result) opts)
    (comms/send-command! component :test/foo "1.0.0" user {:test "command-roundtrip-test" :id id})
    (is (wait-for-atom
         result *wait-tries* *wait-per-try*
         (contains-command-id? id)) id)))

(defn event-roundtrip-test
  [component opts]
  (let [result (atom [])
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-b :test/foo-b "1.0.0" (partial swap-conj-as-event! result) opts)
    (comms/send-event! component :test/foo-b "1.0.0" {:test "event-roundtrip-tes" :id id})
    (is (wait-for-atom
         result *wait-tries* *wait-per-try*
         (contains-event-id? id)) id)))

(defn only-correct-handler-gets-message
  [component opts]
  (let [result (atom [])
        fail (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-c :test/foo-c "1.0.0" (partial swap-conj-as-event! result) opts)
    (comms/attach-event-handler! component :component-d :test/foo-c "1.0.1" (partial reset-as-event! fail) opts)
    (comms/send-event! component :test/foo-c "1.0.0" {:test "only-correct-handler-gets-message" :id id})
    (is (wait-for-atom
         result *wait-tries* *wait-per-try*
         (contains-event-id? id)) id)
    (is (not @fail))))

(defn multiple-handlers-get-same-message
  [component opts]
  (let [result1 (atom [])
        result2 (atom [])
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-e :test/foo-e "1.0.0" (partial swap-conj-as-event! result1) opts)
    (comms/attach-event-handler! component :component-f :test/foo-e "1.0.0" (partial swap-conj-as-event! result2) opts)
    (comms/send-event! component :test/foo-e "1.0.0" {:test "multiple-handlers-get-same-message" :id id})
    (is (wait-for-atom
         result1 *wait-tries* *wait-per-try*
         (contains-event-id? id)) id)
    (is (wait-for-atom
         result2 *wait-tries* *wait-per-try*
         (contains-event-id? id)) id)))

(defn roundtrip-command->event
  [component opts]
  (let [c-result (atom [])
        e-result (atom [])
        id (str (java.util.UUID/randomUUID))
        event-finder-fn (fn [id events]
                          (some (fn [e] (when (= id (get-in e [:kixi.comms.event/payload :kixi.comms.command/payload :id])) e)) events))]
    (comms/attach-command-handler! component :component-g :test/test-a "1.0.0" (partial swap-conj-as-event! c-result) opts)
    (comms/attach-event-handler! component :component-h :test/test-a-event "1.0.0" (fn [x] (swap! e-result conj x) nil) opts)
    (comms/send-command! component :test/test-a "1.0.0" user {:test "roundtrip-command->event" :id id})
    (is (wait-for-atom
         c-result *wait-tries* *wait-per-try*
         (contains-command-id? id)) id)
    (is (wait-for-atom
         e-result *wait-tries* *wait-per-try*
         (partial event-finder-fn id)))
    (let [event (event-finder-fn id @e-result)
          command ((contains-command-id? id) @c-result)]
      (is (= :test/test-a (get-in event [:kixi.comms.event/payload :kixi.comms.command/key])))
      (is (= (:kixi.comms.command/id command) (:kixi.comms.command/id event))))))

(defn roundtrip-command->event-with-key
  [component opts]
  (let [c-result (atom [])
        e-result (atom [])
        id (str (java.util.UUID/randomUUID))
        event-finder-fn (fn [id events]
                          (some (fn [e] (when (= id (get-in e [:kixi.comms.event/payload :kixi.comms.command/payload :id])) e)) events))]
    (comms/attach-command-handler! component :component-j :test/test-xyz "1.0.0" (partial swap-conj-as-event! c-result) opts)
    (comms/attach-event-with-key-handler! component
                                          :component-k
                                          :kixi.comms.command/id
                                          (fn [x] (swap! e-result conj x) nil) opts)
    (comms/send-command! component :test/test-xyz "1.0.0" user {:test "roundtrip-command->event-with-key" :id id})
    (is (wait-for-atom
         c-result *wait-tries* *wait-per-try*
         (contains-command-id? id)) id)
    (is (wait-for-atom
         e-result *wait-tries* *wait-per-try*
         (partial event-finder-fn id)))
    (let [event (event-finder-fn id @e-result)
          command ((contains-command-id? id) @c-result)]
      (is (= :test/test-xyz (get-in event [:kixi.comms.event/payload :kixi.comms.command/key])))
      (is (= (:kixi.comms.command/id command) (:kixi.comms.command/id event))))))

(defn processing-time-gt-session-timeout
  [component opts]
  (comment "If processing time is greater than the session time out, kafka will boot the consumer. Our consumer needs to pause the paritions and continue to call poll while a large job is processing.")
  (let [result (atom [])
        id (str (java.util.UUID/randomUUID))
        id2 (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-i :test/foo-f "1.0.0" #(do (wait long-session-timeout)
                                                                                 (swap-conj-as-event! result %)) opts)
    (comms/send-event! component :test/foo-f "1.0.0" {:test "processing-time-gt-session-timeout" :id id})
    (is (wait-for-atom
         result *wait-tries* *wait-per-try*
         (contains-event-id? id)))
    (comms/send-event! component :test/foo-f "1.0.0" {:test "processing-time-gt-session-timeout-2" :id id2})
    (is (wait-for-atom
         result *wait-tries* *wait-per-try*
         (contains-event-id? id2)))))

(defn detaching-a-handler
  [component opts]
  (let [c-result (atom [])
        e-result (atom [])
        id (str (java.util.UUID/randomUUID))
        event-finder-fn (fn [id events]
                          (some (fn [e] (when (= id (get-in e [:kixi.comms.event/payload :kixi.comms.command/payload :id])) e)) events))]
    (comms/attach-command-handler! component :component-l :test/test-a "1.0.0"
                                   (partial swap-conj-as-event! c-result) opts)
    (let [eh (comms/attach-event-handler! component :component-m :test/test-a-event "1.0.0" (fn [x] (swap! e-result conj x) nil) opts)]
      (comms/send-command! component :test/test-a "1.0.0" user {:test "detaching-a-handler" :id id})
      (is (wait-for-atom
           c-result *wait-tries* *wait-per-try*
           (contains-command-id? id)) id)
      (is (wait-for-atom
           e-result  *wait-tries* *wait-per-try*
           (partial event-finder-fn id)) id)
      (reset! e-result [])
      (comms/detach-handler! component eh)
      (comms/send-command! component :test/test-a "1.0.0" user {:test "detaching-a-handler-2" :id id})
      (is (nil? (wait-for-atom
                 e-result *wait-tries* *wait-per-try*
                 (contains-event-id? id))) (pr-str @e-result)))))