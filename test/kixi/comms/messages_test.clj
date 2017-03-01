(ns kixi.comms.messages-test
  (:require [kixi.comms.messages :refer :all]
            [kixi.comms.components.test-base :refer :all]
            [clojure
             [spec :as s]
             [test :refer :all]]))

(deftest formatting-tests
  (is (not
       (s/explain-data :kixi.comms.message/command
                       (format-message :command :test/foo "1.0.0" user {:foo :bar} nil))))
  (is (not
       (s/explain-data :kixi.comms.message/event
                       (format-message :event :test/foo "1.0.0" user {:foo :bar} {:origin "local"})))))
