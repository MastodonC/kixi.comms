(defproject kixi/kixi.comms "0.1.5"
  :description "FIXME: write description"
  :url "https://github.com/MastodonC/kixi.comms"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha13"]
                 [org.clojure/core.async "0.2.391"]
                 [ymilky/franzy "0.0.1"]
                 [com.cognitect/transit-clj "0.8.290"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/timbre "4.7.0"]
                 [zookeeper-clj "0.9.4"]
                 [cheshire "5.6.3"]
                 [clj-time "0.12.0"]]
  :main ^:skip-aot kixi.comms
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[org.clojure/test.check "0.9.0"]]}}
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]
                 ["snapshots" {:url "https://clojars.org/repo"
                               :creds :gpg}]])
