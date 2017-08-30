(ns kixi.comms.messages-test
  (:require [kixi.comms.messages :refer :all :exclude [uuid]]
            [kixi.comms :as comms]
            [kixi.comms.components.test-base :refer :all]
            [clojure.spec.alpha :as s]
            [clojure
             [test :refer :all]]
            [com.gfredericks.schpec :as sh]))

(deftest formatting-tests
  (is (not
       (s/explain-data :kixi.comms.message/command
                       (format-message :command :test/foo "1.0.0" user {:foo :bar} nil))))
  (is (not
       (s/explain-data :kixi.comms.message/event
                       (format-message :event :test/foo "1.0.0" user {:foo :bar} {:origin "local"})))))


(sh/alias 'event 'kixi.event)
(sh/alias 'cmd 'kixi.command)

(defn validate-cmd-type->event-types
  [cmd events]
  ((command-handler (reify comms/Communications
                      (send-event!
                          [this event version payload])
                      (send-event!
                          [this event version payload opts])
                      (-send-event!
                          [this event opts])
                      (send-command!
                          [this command version user payload])
                      (send-command!
                          [this command version user payload opts])
                      (-send-command!
                          [this command opts])
                      (attach-event-handler!
                          [this group-id event version handler])
                      (attach-event-with-key-handler!
                          [this group-id map-key handler])
                      (attach-validating-event-handler!
                          [this group-id event version handler])
                      (attach-command-handler!
                          [this group-id event version handler])
                      (attach-validating-command-handler!
                          [this group-id event version handler])
                      (detach-handler!
                          [this handler]))
                    (constantly events))
   (assoc cmd
          ::cmd/id (uuid)
          :kixi/user {:kixi.user/id (uuid)
                      :kixi.user/groups [(uuid)]}
          :kixi.message/type :command
          ::cmd/created-at (comms/timestamp))))

(defn validate-event-type->command-types
  [event cmds]
  ((event-handler (reify comms/Communications
                      (send-event!
                          [this event version payload])
                      (send-event!
                          [this event version payload opts])
                      (-send-event!
                          [this event opts])
                      (send-command!
                          [this command version user payload])
                      (send-command!
                          [this command version user payload opts])
                      (-send-command!
                          [this command opts])
                      (attach-event-handler!
                          [this group-id event version handler])
                      (attach-event-with-key-handler!
                          [this group-id map-key handler])
                      (attach-validating-event-handler!
                          [this group-id event version handler])
                      (attach-command-handler!
                          [this group-id event version handler])
                      (attach-validating-command-handler!
                          [this group-id event version handler])
                      (detach-handler!
                          [this handler]))
                    (constantly cmds))
   (assoc event
          ::event/id (uuid)
          :kixi/user {:kixi.user/id (uuid)
                      :kixi.user/groups [(uuid)]}
          :kixi.message/type :event
          ::cmd/id (uuid)
          ::event/created-at (comms/timestamp))))

(defn exception-from
  [f]
  (try
    (f)
    (catch Exception e
      e)))

(deftest validate-cmd-type->event-types-test
  (defmethod comms/command-type->event-types
    [:cmd-test "1.0.0"]
    [_]
    #{[:event-test "1.0.0"] [:event-test2 "1.0.0"]})
  (defmethod comms/command-payload
    [:cmd-test "1.0.0"]
    [_]
    (s/keys :req []))
  (defmethod comms/event-payload
    [:event-test "1.0.0"]
    [_]
    (s/keys :req []))
  (defmethod comms/event-payload
    [:event-test2 "1.0.0"]
    [_]
    (s/keys :req []))
  (testing "Finds match"
    (is (nil? (validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"}
                                              [{::event/type :event-test ::event/version "1.0.0"} {:partition-key (uuid)}])))
    (is (nil? (validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"}
                                              [{::event/type :event-test2 ::event/version "1.0.0"} {:partition-key (uuid)}])))
    (is (nil? (validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"}
                                              [[{::event/type :event-test2 ::event/version "1.0.0"} {:partition-key (uuid)}]
                                               [{::event/type :event-test2 ::event/version "1.0.0"} {:partition-key (uuid)}]]))))
  (testing "Exception on non match"
    (let [ex (exception-from #(validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"}
                                                              [{::event/type :un-regd ::event/version "1.0.0"} {:partition-key (uuid)}]))]
      (is (= (ex-data ex)
             {:allowed-events-types #{[:event-test2 "1.0.0"] [:event-test "1.0.0"]}
              :command-type [:cmd-test "1.0.0"]
              :returned-event-type [:un-regd "1.0.0"]})))
    (let  [ex (exception-from #(validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"}
                                                               [[{::event/type :event-test2 ::event/version "1.0.0"} {:partition-key (uuid)}]
                                                                [{::event/type :un-regd ::event/version "1.0.0"} {:partition-key (uuid)}]]))]
      (is (= (ex-data ex)
             {:allowed-events-types #{[:event-test2 "1.0.0"] [:event-test "1.0.0"]}
              :command-type [:cmd-test "1.0.0"]
              :returned-event-type [:un-regd "1.0.0"]}))))
  (remove-method comms/command-type->event-types [:cmd-test "1.0.0"])
  (remove-method comms/command-payload [:cmd-test "1.0.0"])
  (remove-method comms/event-payload [:event-test "1.0.0"])
  (remove-method comms/event-payload [:event-test2 "1.0.0"]))


(deftest validate-event-type->command-types-test
  (defmethod comms/event-type->command-types
    [:event-test "1.0.0"]
    [_]
    #{[:cmd-test "1.0.0"] [:cmd-test2 "1.0.0"]})
  (defmethod comms/event-payload
    [:event-test "1.0.0"]
    [_]
    (s/keys :req []))
  (defmethod comms/command-payload
    [:cmd-test "1.0.0"]
    [_]
    (s/keys :req []))
  (defmethod comms/command-payload
    [:cmd-test2 "1.0.0"]
    [_]
    (s/keys :req []))
  (testing "Finds match"
    (is (nil? (validate-event-type->command-types {::event/type :event-test ::event/version "1.0.0"}
                                                  [{::cmd/type :cmd-test ::cmd/version "1.0.0"} {:partition-key (uuid)}])))
    (is (nil? (validate-event-type->command-types {::event/type :event-test ::event/version "1.0.0"}
                                                  [{::cmd/type :cmd-test2 ::cmd/version "1.0.0"} {:partition-key (uuid)}])))
    (is (nil? (validate-event-type->command-types {::event/type :event-test ::event/version "1.0.0"}
                                                  [[{::cmd/type :cmd-test2 ::cmd/version "1.0.0"} {:partition-key (uuid)}]
                                                   [{::cmd/type :cmd-test ::cmd/version "1.0.0"} {:partition-key (uuid)}]]))))
  (testing "Exception on non match"
    (let [ex (exception-from #(validate-event-type->command-types {::event/type :event-test ::event/version "1.0.0"}
                                                                  [{::cmd/type :un-regd ::cmd/version "1.0.0"} {:partition-key (uuid)}]))]
      (is (= (ex-data ex)
             {:allowed-command-types #{[:cmd-test2 "1.0.0"] [:cmd-test "1.0.0"]}
              :event-type [:event-test "1.0.0"]
              :returned-command-type [:un-regd "1.0.0"]})))
    (let [ex (exception-from #(validate-event-type->command-types {::event/type :event-test ::event/version "1.0.0"}
                                                                  [[{::cmd/type :un-regd ::cmd/version "1.0.0"} {:partition-key (uuid)}]
                                                                   [{::cmd/type :cmd-test ::cmd/version "1.0.0"} {:partition-key (uuid)}]]))]
      (is (= (ex-data ex)
             {:allowed-command-types #{[:cmd-test2 "1.0.0"] [:cmd-test "1.0.0"]},
              :event-type [:event-test "1.0.0"],
              :returned-command-type [:un-regd "1.0.0"]}))))
  (remove-method comms/event-type->command-types [:event-test "1.0.0"])
  (remove-method comms/command-payload [:cmd-test "1.0.0"])
  (remove-method comms/command-payload [:cmd-test2 "1.0.0"])
  (remove-method comms/event-payload [:event-test "1.0.0"]))
