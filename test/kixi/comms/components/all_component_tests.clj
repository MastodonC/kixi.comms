(ns kixi.comms.components.all-component-tests
  (:require  [clojure.test :refer :all]
             [clojure.spec.alpha :as s]
             [clojure.spec.test.alpha :as st]
             [com.gfredericks.schpec :as sh]
             [kixi.comms :as comms]
             [kixi.comms.components.test-base :refer :all]))

(def long-session-timeout 10000)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn contains-event-id?
  [id]
  (fn [events]
    (some (fn [c] (when (= id (or (get-in c [:kixi.comms.event/payload :id])
                                  (:id c)))
                    c))
          events)))

(defn contains-command-id?
  [id]
  (fn [commands]
    (some (fn [c] (when (= id (or (get-in c [:kixi.comms.command/payload :id])
                                  (:id c))) c))
          commands)))

(sh/alias 'command 'kixi.command)
(sh/alias 'event 'kixi.event)

(st/instrument ['kixi.comms/send-valid-command!
                'kixi.comms/send-valid-event!])

(s/def ::test string?)
(s/def ::id string?)

(defmethod kixi.comms/command-payload
  [:test/vfoo "1.0.0"]
  [_]
  (s/keys :req-un [::test
                   ::id]))

(defmethod kixi.comms/event-payload
  [:test/vfoo-event "1.0.0"]
  [_]
  (s/keys :req-un [::test
                   ::id]))

(defmethod kixi.comms/command-type->event-types
  [:test/vfoo "1.0.0"]
  [_]
  #{[:test/vfoo-event "1.0.0"]})

(defmethod kixi.comms/command-payload
  [:test/cmd-event-cond "1.0.0"]
  [_]
  (s/keys :req-un [::test
                   ::id]))

(defmethod kixi.comms/event-payload
  [:test/cmd-event-cond-event "1.0.1"]
  [_]
  (s/keys :req-un [::test
                   ::id]))

(defmethod kixi.comms/command-type->event-types
  [:test/cmd-event-cond "1.0.0"]
  [_]
  #{[:test/cmd-event-cond-event "1.0.0"]})

(defmethod kixi.comms/event-payload
  [:test/vfoo-b "1.0.0"]
  [_]
  (s/keys :req-un [::test
                   ::id]))

(defmethod kixi.comms/command-payload
  [:test/vfoo-b-cmd "1.0.0"]
  [_]
  (s/keys :req-un [::test
                   ::id]))

(defn command-roundtrip-test
  [component opts]
  (testing "Unvalidated command send"
    (let [result (atom [])
          id (new-uuid)]
      (comms/attach-command-handler! component :component-a :test/foo "1.0.0"
                                     (partial swap-conj-as-event! result))
      (comms/send-command! component :test/foo "1.0.0" user {:test "command-roundtrip-test" :id id})
      (is (wait-for-atom
           result *wait-tries* *wait-per-try*
           (contains-command-id? id)) id)))
  (testing "Validated command send"
    (let [result (atom [])
          id (new-uuid)]
      (comms/attach-validating-command-handler! component :component-aa :test/vfoo "1.0.0"
                                                (partial swap-conj-as-event! result))
      (comms/send-valid-command! component
                                 {:kixi.message/type :command
                                  ::command/type :test/vfoo
                                  ::command/version "1.0.0"
                                  :kixi/user user
                                  :test "validated-command-roundtrip-test"
                                  :id id}
                                 {:partition-key id})
      (is (wait-for-atom
           result *wait-tries* *wait-per-try*
           (contains-command-id? id)) id)))
  (testing "Validated command send - invalid command"
    (let [id (new-uuid)]
      (is (thrown-with-msg?
           Exception
           #"Invalid command"
           (comms/send-valid-command! component
                                      {::command/type :test/vfoo
                                       ::command/version "1.0.0"
                                       :kixi/user (dissoc user
                                                          :kixi.user/id)
                                       :test "command-invalid-test"
                                       :id id}
                                      {:partition-key id})))))
  (testing "Validated command type to event type conditions are applied"
    (let [result (atom [])
          id (new-uuid)]
      (comms/attach-validating-command-handler! component :component-aaa :test/cmd-event-cond "1.0.0"
                                                #(update (swap-conj-as-event! result %)
                                                         0
                                                         (fn [e] (assoc e ::event/version "1.0.1"))))
      (comms/send-valid-command! component
                                 {:kixi.message/type :command
                                  ::command/type :test/cmd-event-cond
                                  ::command/version "1.0.0"
                                  :kixi/user user
                                  :test "validated-command-type-condition-applied"
                                  :id id}
                                 {:partition-key id})
      (is (wait-for-atom
           result *wait-tries* *wait-per-try*
           (contains-command-id? id)) id))))

(defn event-roundtrip-test
  [component opts]
  (testing "Unvalidated event send"
    (let [result (atom [])
          id (new-uuid)]
      (comms/attach-event-handler! component :component-b :test/foo-b "1.0.0" (partial swap-conj-as-event! result))
      (comms/send-event! component :test/foo-b "1.0.0" {:test "event-roundtrip-test" :id id})
      (is (wait-for-atom
           result *wait-tries* *wait-per-try*
           (contains-event-id? id)) id)))
  (testing "Validated event send"
    (let [result (atom [])
          id (new-uuid)]
      (comms/attach-validating-event-handler! component :component-b :test/vfoo-b "1.0.0" (partial swap-conj! result))
      (comms/send-valid-event! component
                               {:kixi.message/type :event
                                ::event/type :test/vfoo-b
                                ::event/version "1.0.0"
                                ::command/id id
                                :kixi/user user
                                :test "event-validating-roundtrip-test"
                                :id id}
                               {:partition-key id})
      (is (wait-for-atom
           result *wait-tries* *wait-per-try*
           (contains-event-id? id)) id)))
  (testing "Validated event send - invalid event"
    (let [id (new-uuid)]
      (is (thrown-with-msg?
           Exception
           #"Invalid event"
           (comms/send-valid-event! component
                                    {::event/type :test/vfoo-b
                                     ::event/version "1.0.0"
                                     ::command/id "not-valid-cmd-id"
                                     :kixi/user user
                                     :test "event-invalid-roundtrip-test"
                                     :id id}
                                    {:partition-key id}))))))

(defn only-correct-handler-gets-message
  [component opts]
  (let [result (atom [])
        fail (atom nil)
        id (new-uuid)]
    (comms/attach-event-handler! component :component-c :test/foo-c "1.0.0" (partial swap-conj-as-event! result))
    (comms/attach-event-handler! component :component-d :test/foo-c "1.0.1" (partial reset-as-event! fail))
    (comms/send-event! component :test/foo-c "1.0.0" {:test "only-correct-handler-gets-message" :id id})
    (is (wait-for-atom
         result *wait-tries* *wait-per-try*
         (contains-event-id? id)) id)
    (is (not @fail))))

(defn multiple-handlers-get-same-message
  [component opts]
  (let [result1 (atom [])
        result2 (atom [])
        id (new-uuid)]
    (comms/attach-event-handler! component :component-e :test/foo-e "1.0.0" (partial swap-conj-as-event! result1))
    (comms/attach-event-handler! component :component-f :test/foo-e "1.0.0" (partial swap-conj-as-event! result2))
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
        id (new-uuid)
        event-finder-fn (fn [id events]
                          (some (fn [e] (when (= id (get-in e [:kixi.comms.event/payload :kixi.comms.command/payload :id])) e)) events))]
    (comms/attach-command-handler! component :component-g :test/test-a "1.0.0" (partial swap-conj-as-event! c-result))
    (comms/attach-event-handler! component :component-h :test/test-a-event "1.0.0" (fn [x] (swap! e-result conj x) nil))
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

(defn roundtrip-command->multi-event
  [component opts]
  (let [event-count 20
        c-result (atom [])
        e-result (atom [])
        id (new-uuid)
        events-finder-fn (fn [id events]
                           (filter (fn [e] (= id (get-in e [:kixi.comms.event/payload :kixi.comms.command/payload :id]))) events))]
    (comms/attach-command-handler! component :component-n :test/test-b "1.0.0" (partial swap-conj-as-multi-events! event-count c-result))
    (comms/attach-event-handler! component :component-o :test/test-b-event "1.0.0" (fn [x] (swap! e-result conj x) nil))
    (comms/send-command! component :test/test-b "1.0.0" user {:test "roundtrip-command->multi-events" :id id})
    (is (wait-for-atom
         c-result *wait-tries* *wait-per-try*
         (contains-command-id? id)))
    (is (wait-for-atom
         e-result *wait-tries* *wait-per-try*
         #(= event-count (count (events-finder-fn id %)))))
    (let [events (events-finder-fn id @e-result)
          event-keys (map #(get-in % [:kixi.comms.event/payload :kixi.comms.command/key]) events)
          event-command-ids (map :kixi.comms.command/id events)
          event-index-create-order (map-indexed #(vector %1 (get-in %2 [:kixi.comms.event/payload :create-order])) events)
          command ((contains-command-id? id) @c-result)]
      (is (every? #{:test/test-b} event-keys))
      (is (every? #{(:kixi.comms.command/id command)} event-command-ids))
      (is (every? (fn [[dex event-num]] (= dex event-num)) event-index-create-order)))))

(defn roundtrip-command->event-with-key
  [component opts]
  (let [c-result (atom [])
        e-result (atom [])
        id (new-uuid)
        event-finder-fn (fn [id events]
                          (some (fn [e] (when (= id (get-in e [:kixi.comms.event/payload :kixi.comms.command/payload :id])) e)) events))]
    (comms/attach-command-handler! component :component-j :test/test-xyz "1.0.0" (partial swap-conj-as-event! c-result))
    (comms/attach-event-with-key-handler! component
                                          :component-k
                                          :kixi.comms.command/id
                                          (fn [x] (swap! e-result conj x) nil))
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
        id2 (new-uuid)]
    (comms/attach-event-handler! component :component-i :test/foo-f "1.0.0" #(do (wait long-session-timeout)
                                                                                 (swap-conj-as-event! result %)))
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
        id (new-uuid)
        event-finder-fn (fn [id events]
                          (some (fn [e] (when (= id (get-in e [:kixi.comms.event/payload :kixi.comms.command/payload :id])) e)) events))]
    (comms/attach-command-handler! component :component-l :test/test-a "1.0.0"
                                   (partial swap-conj-as-event! c-result))
    (let [eh (comms/attach-event-handler! component :component-m :test/test-a-event "1.0.0" (fn [x] (swap! e-result conj x) nil))]
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

(defn infinite-loop-defended
  [component opts]
  (let [result (atom [])
        id (new-uuid)]
    (comms/attach-event-handler! component :component-p :test/foo-c "1.0.0" #(do (swap! result conj %) %))
    (comms/send-event! component :test/foo-c "1.0.0" {:test "event-infinte-loop-test" :id id})
    (is (wait-for-atom
         result *wait-tries* *wait-per-try*
         (contains-event-id? id)) id)))

(defn events-are-partitioned
  "Tests that the option partition-key is respected.
   The test stream *MUST* have multiple streams/paritions for this test to prove anything!"
  [component opts]
  (let [result (atom [])
        values (range 0 50)
        partition-key (new-uuid)]
    (comms/attach-event-handler! component :component-q :test/foo-d "1.0.0" #(do (swap! result conj (get-in % [:kixi.comms.event/payload :val])) %))
    (doseq [v values]
      (comms/send-event! component :test/foo-d "1.0.0"
                         {:test "events-are-partitioned"
                          :val v}
                         {:kixi.comms.event/partition-key partition-key}))
    (wait-for-atom
     result *wait-tries* *wait-per-try*
     #(= (count %) (count values)))
    (is (= (count @result)
           (count values)))
    (is (true? (apply < @result)))))

(defn commands-are-partitioned
  "Tests that the option partition-key is respected.
   The test stream *MUST* have multiple shards/paritions for this test to prove anything!"
  [component opts]
  (let [result (atom [])
        values (range 0 50)
        partition-key (new-uuid)]
    (comms/attach-command-handler! component :component-r :test/foo-e "1.0.0" #(do (swap! result conj (get-in % [:kixi.comms.command/payload :val]))
                                                                                   (cmd->event %)))
    (doseq [v values]
      (comms/send-command! component :test/foo-e "1.0.0"
                           user
                           {:test "commands-are-partitioned"
                            :val v}
                           {:kixi.comms.command/partition-key partition-key}))
    (wait-for-atom
     result *wait-tries* *wait-per-try*
     #(= (count %) (count values)))
    (is (= (count @result)
           (count values)))
    (is (true? (apply < @result)))))

(defn command-produced-events-are-partitioned
  "Tests that the option partition-key is respected.
   The test stream *MUST* have multiple shards/paritions for this test to prove anything!"
  [component opts]
  (let [result (atom [])
        values (range 0 50)
        partition-key (new-uuid)]
    (comms/attach-command-handler! component :component-s :test/foo-f "1.0.0" #(assoc (cmd->event %)
                                                                                      :kixi.comms.event/partition-key partition-key))
    (comms/attach-event-handler! component :component-t :test/foo-f-event "1.0.0"
                                 #(do (swap! result conj (get-in % [:kixi.comms.event/payload :kixi.comms.command/payload :val])) nil))
    (doseq [v values]
      (comms/send-command! component :test/foo-f "1.0.0"
                           user
                           {:test "command-produced-events-are-partitioned"
                            :val v}
                           {:kixi.comms.command/partition-key partition-key}))
    (wait-for-atom
     result *wait-tries* *wait-per-try*
     #(= (count %) (count values)))
    (is (= (count @result)
           (count values)))
    (is (true? (apply < @result)))))
