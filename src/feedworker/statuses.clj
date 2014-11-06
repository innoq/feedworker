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
  {:form-params {:recipient mentions ;; TODO verify if this is the correct way to set multiple values for a parameter
                 :subject subject
                 :body body}
   :headers {"Authorization" (str "Bearer " (:token naveed-conf))}
   :conn-timeout (:conn-timeout naveed-conf 2000)
   :socket-timeout (:socket-timeout naveed-conf 2000)})

(defn handler [entry worker-id conf]
  (log "received" entry)
  (let [mentions (extract-mentions entry)]
    (log "extracted mentions" mentions)
    (when (seq mentions)
      (let [resp (println (-> conf :naveed :url) ;; TODO http/post
                          (naveed-req mentions 
                                      (subject entry)
                                      (body entry)
                                      (:naveed conf)))]
        (if (= 503 (:status resp))
          :break
          resp)))))

(def conf {:workers
           {:statuses-mentions {:url "http://localhost:8080/statuses/updates?format=atom"
                                :handler handler
                                :processing-strategy :at-least-once
                                :repeat 10000}}
           :processed-entries-dir "processedentries"
           :naveed {:url "<url>"
                    :token "<token>"
                    :conn-timeout 2000
                    :socket-timeout 2000}})

(defn -main [& args]
  (feedworker/run! conf))
