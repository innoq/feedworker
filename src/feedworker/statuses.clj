(ns feedworker.statuses
  (:require [clj-http.client :as http]))

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
   :conn-timeout (:conn-timeout naveed-conf 2000)
   :socket-timeout (:socket-timeout naveed-conf 2000)})

(defn handler [entry worker-id conf]
  (let [mentions (extract-mentions entry)]
    (when (seq mentions)
      (let [resp (println (-> conf :naveed :url) ;; TODO http/post
                          (naveed-req mentions 
                                      (subject entry)
                                      (body entry)
                                      (:naveed conf)))]
        (if (= 503 (:status resp))
          :retry
          resp)))))

(def statuses "https://intern.innoq.com/statuses/updates?format=atom")

(def mention "@<a href='/statuses/updates?author=st'>st</a> Kandidat @<a>foo</a>")




(def iblog "https://internal.innoq.com/blogging/index-all.atom")

;; (process-feed falo (parse-secure-feed iblog <usr> <pwd>) #(-> % :uri println))

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
