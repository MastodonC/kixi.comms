(defproject kixi.comms "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha13"]
                 [org.clojure/core.async "0.2.391"]
                 [ymilky/franzy "0.0.1"]
                 [com.stuartsierra/component "0.3.1"]
                 [zookeeper-clj "0.9.4"]
                 [cheshire "5.6.3"]]
  :main ^:skip-aot kixi.comms
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {}})
