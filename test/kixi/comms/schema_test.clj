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

(deftest uuid-test
  (is (s/valid? uuid? (uuid)))
  (is (not (s/valid? uuid? 123))))

(deftest semver-test
  (is (s/valid? semver? "1.2.3"))
  (is (s/valid? semver? "1111111.2222222222.333333333"))
  (is (not (s/valid? semver? "1")))
  (is (not (s/valid? semver? "1.")))
  (is (not (s/valid? semver? "1.2")))
  (is (not (s/valid? semver? "1.2.")))
  (is (not (s/valid? semver? "1.2.3a")))
  (is (not (s/valid? semver? "x.y.z")))
  (is (not (s/valid? semver? 1.2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest query-response-results-or-error
  (is (s/valid? :kixi.comms.message/query-response {:kixi.comms.message/type "query-response"
                                                    :kixi.comms.query/id (uuid)
                                                    :kixi.comms.query/results [1 2 3]}))
  (is (s/valid? :kixi.comms.message/query-response {:kixi.comms.message/type "query-response"
                                                    :kixi.comms.query/id (uuid)
                                                    :kixi.comms.query/error "Foobar"}))
  (is (not (s/valid? :kixi.comms.message/query-response {:kixi.comms.message/type "query-response"
                                                         :kixi.comms.query/id (uuid)
                                                         :kixi.comms.query/error "Foobar"
                                                         :kixi.comms.query/results [1 2 3]})))
  (is (not (s/valid? :kixi.comms.message/query-response {:kixi.comms.message/type "query-response"
                                                         :kixi.comms.query/id (uuid)}))))

(deftest message-conform-test
  (let [msg {:kixi.comms.message/type "command"
             :kixi.comms.command/key  "gateway/ping"
             :kixi.comms.command/version "1.0.0"
             :kixi.comms.command/id   (uuid)
             :kixi.comms.command/created-at (t/timestamp)
             :kixi.comms.command/payload {}}]
    (is (not (= :clojure.spec/invalid (s/conform :kixi.comms.message/message msg)))
        (pr-str (s/explain-data :kixi.comms.message/message msg)))))

(deftest message-fail-test
  (let [msg {:kixi.comms.message/type "foobar"
             :kixi.comms.command/key  "gateway/ping"
             :kixi.comms.command/version "1.0.0"
             :kixi.comms.command/id   (uuid)
             :kixi.comms.command/created-at (t/timestamp)
             :kixi.comms.command/payload {}}]
    (is (= :clojure.spec/invalid (s/conform :kixi.comms.message/message msg)))))
