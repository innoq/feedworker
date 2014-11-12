(ns feedworker.statuses
  (:gen-class)
  (:require [clj-http.client :as http]
            [feedworker.core :as feedworker :refer [log]]
            [clojure.data.json :as json]))

(defn base64 [s]
  (javax.xml.bind.DatatypeConverter/printBase64Binary (.getBytes s "UTF-8")))

(defn load-url
  "loads content at url as an InputStream - supports http(s), file URLs etc. and allows embedding basic auth credentials in URL"
  [url conn-timeout read-timeout]
  (let [u (java.net.URL. url)
        conn (if-let [userinfo (.getUserInfo u)]
               (doto (.openConnection u)
                 (.setRequestProperty "Authorization" (str "Basic " (base64 userinfo))))
               (.openConnection u))]
    (.getContent (doto conn
                   (.setConnectTimeout conn-timeout)
                   (.setReadTimeout read-timeout)))))

(defn create-cache [source interval]
  (let [cache (atom nil)]
    (feedworker/schedule interval
                         #(when-let [n (feedworker/on-exception nil (source))]
                            (reset! cache n)))
    cache))

(defn create-lookup [cache]
  (fn this
    ([key]
       (this key nil))
    ([key default]
       (get @cache key default))))

(defn build-idx [json-idx]
  (let [members (-> json-idx (json/read-str :key-fn keyword) :member)]
    (into {} (map (fn [member]
                    [(:uid member) (:displayName member)])
                  members))))

(defn create-user-lookup [conf]
  (if (contains? conf :user-index)
      (let [idx-conf (:user-index conf)
            cache (create-cache
                   #(-> (load-url
                         (:url idx-conf)
                         (:conn-timeout idx-conf)
                         (:read-timeout idx-conf))
                        slurp
                        build-idx)
                   (:interval idx-conf))
            lookup (create-lookup cache)]
        (fn [shorthand]
          (lookup shorthand shorthand)))
      identity))

(defn linkified-mentions [text]
  (let [group-matches (re-seq #"@<a[^>]*>(\w+)</a>" text)]
    ;; re-seq returns a seq of vectors [<full-match> <author>]
    (map second group-matches)))

(defn unlinkify [text]
  (clojure.string/replace text #"<a[^>]*>([^<]+)</a>" "$1"))

(defn extract-mentions [entry]
  (->> (:contents entry)
       (map :value)
       (mapcat linkified-mentions)
       distinct))

(defn subject [_]
  "[statuses] You were mentioned!")

(defn body [entry user-lookup]
  (let [msg (-> entry :contents first :value unlinkify)
        author (-> entry :authors first :name user-lookup)
        link (:link entry)]
    (str
"Hello,

You were mentioned by @" author ":

\"" msg "\"

" link)))

(defn naveed-req [mentions subject body token naveed-conf]
  {:form-params {:recipient mentions
                 :subject subject
                 :body body}
   :headers {"Authorization" (str "Bearer " token)}
   :throw-exceptions false
   :conn-timeout (:conn-timeout naveed-conf 2000)
   :socket-timeout (:read-timeout naveed-conf 2000)})

(defn handler [entry worker-id conf]
  (log "received" entry)
  (let [mentions (extract-mentions entry)]
    (log "extracted mentions" mentions)
    (if (seq mentions)
      (let [req (naveed-req mentions
                            (subject entry)
                            (body entry (::user-lookup conf))
                            (-> conf :workers worker-id :naveed-token)
                            (:naveed conf))
            resp (http/post (-> conf :naveed :url) req)]
        (if (= 503 (:status resp))
          :break
          {:response resp
           :request req}))
      (str "no mentions found"))))

(defn map-vals
  "applies function f to every value in map m"
  [f m]
  (->> m
       (map (fn [[k v]]
              [k (f v)]))
       (into {})))

(defn add-user-lookup [config]
  (assoc config ::user-lookup (create-user-lookup config)))

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
      add-user-lookup
      create-handlers))

(defn -main [& [config]]
  (if config
    (feedworker/run! (parse-config config))
    (do
      (println "usage: first argument must be a file which contains a config like this:")
      (clojure.pprint/pprint feedworker/conf-example))))
