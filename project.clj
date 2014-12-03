(defproject feedworker "0.1.0"
  :description "muncher of feeds"
  :url "https://github.com/innoq/feedworker"
  :license {:name "Apache 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [adamwynne/feedparser-clj "0.5.2"]
                 [clj-http "1.0.0"]
                 [pandect "0.4.1"]
                 [ring/ring-core "1.3.1"]
                 [ring/ring-jetty-adapter "1.3.1"]
                 [metrics-clojure "2.4.0"]
                 [metrics-clojure-ring "2.4.0"]])
