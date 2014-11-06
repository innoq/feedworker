(ns feedworker.statuses
  (:require [clj-http.client :as http]
            [feedworker.core :as feedworker :refer [log]]))

(defn linkified-mentions [text]
  (let [group-matches (re-seq #"@<a[^>]*>(\w+)</a>" text)]
    ;; re-seq returns a seq of vectors [<full-match> <author>]
    (map second group-matches)))

(defn extract-mentions [entry]
  (->> (:contents entry)
       (map :value)
       (mapcat linkified-mentions)))

(defn subject [_]
  "[statuses] You were mentioned!")

(defn body [entry]
  (str (-> entry :contents first :value) " (" (:link entry) ")"))

(defn naveed-req [mentions subject body naveed-conf]
  {:form-params {:recipient mentions
                 :subject subject
                 :body body}
   :headers {"Authorization" (str "Bearer " (:token naveed-conf))}
   :throw-exceptions false
   :conn-timeout (:conn-timeout naveed-conf 2000)
   :socket-timeout (:socket-timeout naveed-conf 2000)})

(defn handler [entry worker-id conf]
  (log "received" entry)
  (let [mentions (extract-mentions entry)]
    (log "extracted mentions" mentions)
    (if (seq mentions)
      (let [req (naveed-req mentions
                            (subject entry)
                            (body entry)
                            (:naveed conf))
            resp (http/post (-> conf :naveed :url) req)]
        (if (= 503 (:status resp))
          :break
          {:response resp
           :request req}))
      (str "no mentions found"))))

(def conf-example {:workers
                   {:statuses-mentions {:url "http://<statuseshost>/statuses/updates?format=atom"
                                        :handler 'feedworker.statuses/handler
                                        :processing-strategy :at-most-once
                                        :repeat 10000}}
                   :processed-entries-dir "processedentries"
                   :naveed {:url "http://<naveedhost>/outbox"
                            :token "<token>"
                            :conn-timeout 2000
                            :socket-timeout 2000}})

(defn map-vals
  "applies function f to every value in map m"
  [f m]
  (->> m
       (map (fn [[k v]]
              [k (f v)]))
       (into {})))

(defn create-handlers [config]
  (update-in config [:workers]
             (fn [workers]
               (map-vals (fn [worker]
                           (update-in worker [:handler] eval))
                         workers))))

(defn parse-config [filepath]
  (-> filepath
      slurp
      read-string
      create-handlers))

(defn -main [& [config]]
  (if config
    (feedworker/run! (parse-config config))
    (do
      (println "usage: first argument must be a file which contains a config like this:")
      (clojure.pprint/pprint conf-example))))
