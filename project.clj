(defproject feedworker "0.1.0-SNAPSHOT"
  :description "muncher of feeds"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [adamwynne/feedparser-clj "0.5.2"]
                 [clj-http "1.0.0"]
                 [pandect "0.4.1"]
                 [ring/ring-core "1.3.1"]
                 [ring/ring-jetty-adapter "1.3.1"]
                 [metrics-clojure "2.4.0"]
                 [metrics-clojure-ring "2.4.0"]]
  :main feedworker.core)
