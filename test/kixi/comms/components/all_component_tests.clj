(ns kixi.comms.components.all-component-tests
  (:require  [clojure.test :refer :all]
             [kixi.comms :as comms]
             [kixi.comms.components.test-base :refer :all]))

(def long-session-timeout 10000)

(defn component-name
  []
  (keyword (str (java.util.UUID/randomUUID))))

(defn attach-command-handler!
  [component event handler]
  (comms/attach-command-handler! component
                                 (component-name)
                                 event
                                 "1.0.0"
                                 handler))

(defn attach-event-handler!
  [component event handler]
  (comms/attach-event-handler! component
                               (component-name)
                               event
                               "1.0.0"
                               handler))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn command-roundtrip-test
  [component]
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-command-handler! component :component-a :test/foo "1.0.0" (partial reset-as-event! result))
    (comms/send-command! component :test/foo "1.0.0" user {:test "command-roundtrip-test" :id id})
    (wait-for-atom result)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.command/payload :id])))))

(defn event-roundtrip-test
  [component]
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-b :test/foo-b "1.0.0" (partial reset-as-event! result))
    (comms/send-event! component :test/foo-b "1.0.0" {:test "event-roundtrip-tes" :id id})
    (wait-for-atom result)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.event/payload :id])))))

(defn only-correct-handler-gets-message
  [component]
  (let [result (atom nil)
        fail (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-c :test/foo-c "1.0.0" (partial reset-as-event! result))
    (comms/attach-event-handler! component :component-d :test/foo-c "1.0.1" (partial reset-as-event! fail))
    (comms/send-event! component :test/foo-c "1.0.0" {:test "only-correct-handler-gets-message" :id id})
    (wait-for-atom result)
    (is (not @fail))
    (is @result)
    (is (= id (get-in @result [:kixi.comms.event/payload :id])))))

(defn multiple-handlers-get-same-message
  [component]
  (let [result (atom [])
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-e :test/foo-e "1.0.0" (partial swap-conj-as-event! result))
    (comms/attach-event-handler! component :component-f :test/foo-e "1.0.0" (partial swap-conj-as-event! result))
    (comms/send-event! component :test/foo-e "1.0.0" {:test "multiple-handlers-get-same-message" :id id})
    (wait-for-atom result *wait-tries* *wait-per-try* #(<= 2 (count %)))
    (is @result)
    (is (= 2 (count @result)))
    (is (= (first @result) (second @result)))))

(defn roundtrip-command->event
  [component]
  (let [c-result (atom nil)
        e-result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-command-handler! component :component-g :test/test-a "1.0.0" (partial reset-as-event! c-result))
    (comms/attach-event-handler! component :component-h :test/test-a-event "1.0.0" (fn [x] (reset! e-result x) nil))
    (comms/send-command! component :test/test-a "1.0.0" user {:test "roundtrip-command->event" :id id})
    (wait-for-atom c-result)
    (wait-for-atom e-result)
    (is @c-result)
    (is @e-result)
    (is (= id (get-in @c-result [:kixi.comms.command/payload :id])))
    (is (= id (get-in @e-result [:kixi.comms.event/payload :kixi.comms.command/payload :id])))
    (is (= :test/test-a (get-in @e-result [:kixi.comms.event/payload :kixi.comms.command/key])))
    (is (= (:kixi.comms.command/id @c-result) (:kixi.comms.command/id @e-result)))))

(defn roundtrip-command->event-with-key
  [component]
  (let [c-result (atom nil)
        e-result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-command-handler! component :component-j :test/test-xyz "1.0.0" (partial reset-as-event! c-result))
    (comms/attach-event-with-key-handler! component
                                          :component-k
                                          :kixi.comms.command/id
                                          (fn [x] (reset! e-result x) nil))
    (comms/send-command! component :test/test-xyz "1.0.0" user {:test "roundtrip-command->event-with-key" :id id})
    (wait-for-atom c-result)
    (wait-for-atom e-result)
    (is @c-result)
    (is @e-result)
    (is (= id (get-in @c-result [:kixi.comms.command/payload :id])))
    (is (= id (get-in @e-result [:kixi.comms.event/payload :kixi.comms.command/payload :id])))
    (is (= :test/test-xyz (get-in @e-result [:kixi.comms.event/payload :kixi.comms.command/key])))
    (is (= (:kixi.comms.command/id @c-result) (:kixi.comms.command/id @e-result)))))

(defn processing-time-gt-session-timeout
  [component]
  (comment "If processing time is greater than the session time out, kafka will boot the consumer. Our consumer needs to pause the paritions and continue to call poll while a large job is processing.")
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))
        id2 (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! component :component-i :test/foo-f "1.0.0" #(do (wait long-session-timeout)
                                                                                 (reset-as-event! result %)))
    (comms/send-event! component :test/foo-f "1.0.0" {:test "processing-time-gt-session-timeout" :id id})
    (wait-for-atom result)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.event/payload :id])))
    (comms/send-event! component :test/foo-f "1.0.0" {:test "processing-time-gt-session-timeout-2" :id id2})
    (wait-for-atom result *wait-tries* *wait-per-try* #(= id2 (get-in % [:kixi.comms.event/payload :id])))
    (is @result)
    (is (= id2 (get-in @result [:kixi.comms.event/payload :id])))))

(defn detaching-a-handler
  [component]
  (let [c-result (atom nil)
        e-result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (attach-command-handler! component :test/test-a
                             (partial reset-as-event! c-result))
    (let [eh (attach-event-handler! component :test/test-a-event
                                    (fn [x] (reset! e-result x) nil))]
      (comms/send-command! component :test/test-a "1.0.0" user {:test "detaching-a-handler" :id id})
      (wait-for-atom c-result)
      (wait-for-atom e-result)
      (if-not (and @c-result @e-result)
        (is false
            (str "Results not received: cmd: " @c-result ". event: " @e-result))
        (do
          (reset! e-result nil)
          (comms/detach-handler! component eh)
          (comms/send-command! component :test/test-a "1.0.0" user {:test "detaching-a-handler-2" :id id})
          (wait-for-atom e-result)
          (is (nil? @e-result)))))))
