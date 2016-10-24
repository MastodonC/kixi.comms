(ns kixi.comms.components.kafka-test
  (:require [clojure
             [spec :as s]
             [test :refer :all]]
            [com.stuartsierra.component :as component]
            [kixi.comms.schema]
            [kixi.comms :as comms]
            [kixi.comms.components.kafka :refer :all]))

(def zookeeper-ip "127.0.0.1")
(def zookeeper-port 2181)
(def group-id "test-group")

(def system (atom nil))

(defn start-kafka-system
  []
  (when-not @system
    (reset! system
            (component/start-system
             (component/system-map
              :kafka
              (map->Kafka {:host zookeeper-ip
                           :port zookeeper-port
                           :consumer-config {;to set this lower, need to configure the brokers group.min.session.timeout.ms
                                             :session.timeout.ms 6000
                                             :auto.commit.interval.ms 1500}}))))))
(defn cycle-system-fixture
  [all-tests]
  (start-kafka-system)
  (all-tests)
  (component/stop-system @system)
  (reset! system nil))

(use-fixtures :once cycle-system-fixture)

(defn wait-for-atom
  ([a]
   (wait-for-atom a 65))
  ([a tries]
   (wait-for-atom a tries 500))
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

(deftest formatting-tests
  (is (not
       (s/explain-data :kixi.comms.message/command
                       (format-message :command :test/foo "1.0.0" {:foo :bar} nil))))
  (is (not
       (s/explain-data :kixi.comms.message/event
                       (format-message :event :test/foo "1.0.0" {:foo :bar} {:origin "local"})))))

(deftest brokers-list-test
  (let [bl (first (brokers zookeeper-ip zookeeper-port))]
    (is (re-find #"\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{4,5}" bl))))

(deftest handle-result-tests
  (is (successful? (handle-result (:kafka @system)
                                  :command
                                  {}
                                  {:kixi.comms.event/key :received-result
                                   :kixi.comms.event/version "1.0.0"
                                   :kixi.comms.event/payload {}})))
  (is (successful? (handle-result (:kafka @system)
                                  :event
                                  {}
                                  {:kixi.comms.event/key :received-result
                                   :kixi.comms.event/version "1.0.0"
                                   :kixi.comms.event/payload {}})))
  (is (successful? (handle-result (:kafka @system)
                                  :event
                                  {}
                                  [{:kixi.comms.event/key :received-result
                                    :kixi.comms.event/version "1.0.0"
                                    :kixi.comms.event/payload {}}
                                   {:kixi.comms.event/key :received-result2
                                    :kixi.comms.event/version "1.0.0"
                                    :kixi.comms.event/payload {}}])))
  (is (successful? (handle-result (:kafka @system)
                                  :event {} nil)))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :command {} nil)))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :command {} 123)))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :command {} {:foo 123})))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :event {} {:foo 123})))
  (is (thrown-with-msg? Exception #"^Handler must return a valid event result"
                        (handle-result (:kafka @system)
                                       :event {} [{:foo 123}
                                                  {:kixi.comms.event/key :received-result
                                                   :kixi.comms.event/version "1.0.0"
                                                   :kixi.comms.event/payload {}}]))))

(deftest command-roundtrip-test
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-command-handler! (:kafka @system) :component-a :test/foo "1.0.0" (partial reset-as-event! result))
    (comms/send-command! (:kafka @system) :test/foo "1.0.0" {:foo "123" :id id})
    (wait-for-atom result)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.command/payload :id])))))

(deftest event-roundtrip-test
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! (:kafka @system) :component-b :test/foo-b "1.0.0" (partial reset-as-event! result))
    (comms/send-event! (:kafka @system) :test/foo-b "1.0.0" {:foo "123" :id id})
    (wait-for-atom result)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.event/payload :id])))))

(deftest only-correct-handler-gets-message
  (let [result (atom nil)
        fail (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! (:kafka @system) :component-c :test/foo-c "1.0.0" (partial reset-as-event! result))
    (comms/attach-event-handler! (:kafka @system) :component-d :test/foo-c "1.0.1" (partial reset-as-event! fail))
    (comms/send-event! (:kafka @system) :test/foo-c "1.0.0" {:foo "123" :id id})
    (wait-for-atom result)
    (is (not @fail))
    (is @result)
    (is (= id (get-in @result [:kixi.comms.event/payload :id])))))


(deftest multiple-handlers-get-same-message
  (let [result (atom [])
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! (:kafka @system) :component-e :test/foo-e "1.0.0" (partial swap-conj-as-event! result))
    (comms/attach-event-handler! (:kafka @system) :component-f :test/foo-e "1.0.0" (partial swap-conj-as-event! result))
    (comms/send-event! (:kafka @system) :test/foo-e "1.0.0" {:foo "123" :id id})
    (wait-for-atom result 65 500 #(<= 2 (count %)))
    (is @result)
    (is (= 2 (count @result)))
    (is (= (first @result) (second @result)))))

(deftest roundtrip-command->event
  (let [c-result (atom nil)
        e-result (atom nil)
        id (str (java.util.UUID/randomUUID))]
    (comms/attach-command-handler! (:kafka @system) :component-g :test/test-a "1.0.0" (partial reset-as-event! c-result))
    (comms/attach-event-handler! (:kafka @system) :component-h :test/test-a-event "1.0.0" (fn [x] (reset! e-result x) nil))
    (comms/send-command! (:kafka @system) :test/test-a "1.0.0" {:foo "123" :id id})
    (wait-for-atom c-result)
    (wait-for-atom e-result)
    (is @c-result)
    (is @e-result)
    (is (= id (get-in @c-result [:kixi.comms.command/payload :id])))
    (is (= id (get-in @e-result [:kixi.comms.event/payload :kixi.comms.command/payload :id])))
    (is (= :test/test-a (get-in @e-result [:kixi.comms.event/payload :kixi.comms.command/key])))
    (is (= (:kixi.comms.command/id @c-result) (:kixi.comms.command/id @e-result)))))

(defn wait
  [ms]
  (Thread/sleep ms))

(deftest processing-time-gt-session-timeout
  (comment "If processing time is greater than the session time out, kafka will boot the consumer. Our consumer needs to pause the paritions and continue to call poll while a large job is processing.")
  (let [result (atom nil)
        id (str (java.util.UUID/randomUUID))
        id2 (str (java.util.UUID/randomUUID))]
    (comms/attach-event-handler! (:kafka @system) :component-i :test/foo-f "1.0.0" #(do (wait 7000)
                                                                                        (reset-as-event! result %)))
    (comms/send-event! (:kafka @system) :test/foo-f "1.0.0" {:foo "123" :id id})
    (wait-for-atom result 20 500)
    (is @result)
    (is (= id (get-in @result [:kixi.comms.event/payload :id])))
    (comms/send-event! (:kafka @system) :test/foo-f "1.0.0" {:foo "123" :id id2})
    (wait-for-atom result 20 500 #(= id2 (get-in % [:kixi.comms.event/payload :id])))
    (is @result)
    (is (= id2 (get-in @result [:kixi.comms.event/payload :id])))))
