(def slf4j-version "1.7.21")
(defproject kixi/kixi.comms "0.2.26"
  :description "FIXME: write description"
  :url "https://github.com/MastodonC/kixi.comms"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]
                 [org.clojure/core.async "0.3.443"]
                 [com.cognitect/transit-clj "0.8.300"]
                 [com.gfredericks/schpec "0.1.2"]
                 [com.taoensso/encore "2.92.0"]
                 [com.stuartsierra/component "0.3.2"]
                 [com.fzakaria/slf4j-timbre "0.3.7"]
                 [com.taoensso/timbre "4.10.0" :exclusions [com.taoensso/encore]]
                 [com.fasterxml.jackson.core/jackson-core "2.9.1"]
                 [com.fasterxml.jackson.dataformat/jackson-dataformat-cbor "2.9.1"]
                 [cheshire "5.8.0" :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [clj-time "0.14.0"]
                 [org.slf4j/log4j-over-slf4j ~slf4j-version]
                 [org.slf4j/jul-to-slf4j ~slf4j-version]
                 [org.slf4j/jcl-over-slf4j ~slf4j-version]
                 [environ "1.1.0"]
                 ;; Kafka
                 [mastondonc/franzy "0.0.3" :exclusions [org.slf4j/slf4j-log4j12]]
                 [zookeeper-clj "0.9.4" :exclusions [org.slf4j/slf4j-log4j12 log4j]]
                 ;; Kinesis
                 [amazonica "0.3.92" :exclusions [com.taoensso/encore
                                                  com.fasterxml.jackson.core/jackson-core
                                                  com.fasterxml.jackson.dataformat/jackson-dataformat-cbor]]
                 [byte-streams "0.2.3"]]
  :test-selectors {:default (complement :kafka)}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]
                 ["snapshots" {:url "https://clojars.org/repo"
                               :creds :gpg}]])
