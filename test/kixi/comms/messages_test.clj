(ns kixi.comms.messages-test
  (:require [kixi.comms.messages :refer :all]
            [kixi.comms :as comms]
            [kixi.comms.components.test-base :refer :all]
            [clojure
             [spec :as s]
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

(deftest validate-cmd-type->event-types-test
  (defmethod comms/command-type->event-types
    [:cmd-test "1.0.0"]
    [_]
    #{[:event-test "1.0.0"] [:event-test2 "1.0.0"]})
  (testing "Finds match"
    (is (nil? (validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"} [{:event {::event/type :event-test ::event/version "1.0.0"}}])))
    (is (nil? (validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"} [{:event {::event/type :event-test2 ::event/version "1.0.0"}}]))))
  (testing "Exception on non match"
    (is (thrown?
         Exception
         (validate-cmd-type->event-types {::cmd/type :cmd-test ::cmd/version "1.0.0"} [{:event {::event/type :un-regd ::event/version "1.0.0"}}]))))
  (remove-method comms/command-type->event-types [:cmd-test "1.0.0"]))


(deftest validate-event-type->command-types-test
  (defmethod comms/event-type->command-types
    [:event-test "1.0.0"]
    [_]
    #{[:cmd-test "1.0.0"] [:cmd-test2 "1.0.0"]})
  (testing "Finds match"
    (is (nil? (validate-event-type->command-types {::event/type :event-test ::event/version "1.0.0"} [{:cmd {::cmd/type :cmd-test ::cmd/version "1.0.0"}}])))
    (is (nil? (validate-event-type->command-types {::event/type :event-test ::event/version "1.0.0"} [{:cmd {::cmd/type :cmd-test2 ::cmd/version "1.0.0"}}]))))
  (testing "Exception on non match"
    (is (thrown?
         Exception
         (validate-cmd-type->event-types {::event/type :cmd-test ::event/version "1.0.0"} [{:cmd {::cmd/type :un-regd ::cmd/version "1.0.0"}}]))))
  (remove-method  comms/event-type->command-types [:event-test "1.0.0"]))
