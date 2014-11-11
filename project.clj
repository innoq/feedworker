(defproject feedworker "0.1.0-SNAPSHOT"
  :description "muncher of feeds"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [adamwynne/feedparser-clj "0.5.2"]
                 [clj-http "1.0.0"]
                 [pandect "0.4.1"]
                 [org.clojure/data.json "0.2.5"]] ;; just for statuses use case
  :main feedworker.statuses
  :aot [feedworker.statuses])
