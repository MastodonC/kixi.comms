(ns kixi.comms.schema-test
  (:require [clojure.test :refer :all]
            [kixi.comms.schema :refer :all]
            [kixi.comms.time :as t]
            [clojure.spec :as s]))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(deftest kixi-keyword?-test
  (is (s/valid? kixi-keyword? :foo))
  (is (s/valid? kixi-keyword? "foo"))
  (is (not (s/valid? kixi-keyword? 123))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest message-type-test
  (is (s/valid? :kixi.comms.message/type :command))
  (is (not (s/valid? :kixi.comms.message/type :foobar)))

(deftest message-conform-test
  (let [msg {:kixi.comms.message/type :command
             :kixi.comms.command/key  "gateway/ping"
             :kixi.comms.command/version "1.0.0"
             :kixi.comms.command/id   (uuid)
             :kixi.comms.command/created-at (t/timestamp)
             :kixi.comms.command/payload {}}]
    (is (not (= :clojure.spec/invalid (s/conform :kixi.comms.message/message msg)))
        (pr-str (s/explain-data :kixi.comms.message/message msg)))))

(deftest message-fail-test
  (let [msg {:kixi.comms.message/type :foobar
             :kixi.comms.command/key  "gateway/ping"
             :kixi.comms.command/version "1.0.0"
             :kixi.comms.command/id   (uuid)
             :kixi.comms.command/created-at (t/timestamp)
             :kixi.comms.command/payload {}}]
    (is (= :clojure.spec/invalid (s/conform :kixi.comms.message/message msg)))))
