(ns feedworker.core
  (:require [feedparser-clj.core :as feedparser]
            [clj-http.client :as http]
            [pandect.core :as pandect]
            [metrics.core :as metrics]
            [metrics.utils :refer [all-metrics]]
            [metrics.timers :refer [time!]]
            [metrics.ring.expose :refer [render-to-basic]]
            [ring.adapter.jetty :refer [run-jetty]])
  (:import [java.io File]
           [java.util.concurrent ScheduledThreadPoolExecutor TimeUnit]))

(defn pprint-str [x]
  (let [sw (java.io.StringWriter.)]
    (clojure.pprint/pprint x sw)
    (str sw)))

(defn log [& msgs]
  (let [now (java.util.Date.)]
    (apply println (str now) "---" msgs)
    (doseq [msg msgs]
      (when (instance? Exception msg)
        (.printStackTrace msg)))))

(defmacro on-shutdown [& body]
  `(.addShutdownHook (Runtime/getRuntime) (Thread. #(do ~@body))))

(defmacro on-exception [exceptional-result & body]
  `(try
     ~@body
     (catch Exception e#
       (log e#)
       ~exceptional-result)))

(defprotocol ProcessingStrategy
  (already-processed? [this id])
  (start-processing? [this id])
  (after-processing [this id])
  (mark-for-retry [this id]))

(defn file-for [processed-dir id]
  (let [hash (pandect/sha1 id)
        dir (File. processed-dir)]
    (File. dir hash)))

(defrecord FileAtMostOnce [processed-dir]
  ProcessingStrategy
  (already-processed? [_ id]
    (.exists (file-for processed-dir id)))
  (start-processing? [_ id]
    (.createNewFile (file-for processed-dir id)))
  (after-processing [_ _])
  (mark-for-retry [_ id]
    (on-exception nil
      (.delete (file-for processed-dir id)))))

(defrecord FileAtLeastOnce [processed-dir]
  ProcessingStrategy
  (already-processed? [_ id]
    (.exists (file-for processed-dir id)))
  (start-processing? [_ id]
    (not (.exists (file-for processed-dir id))))
  (after-processing [_ id]
    (.createNewFile (file-for processed-dir id)))
  (mark-for-retry [_ id]
    (on-exception nil
      (.delete (file-for processed-dir id)))))

(defprotocol Tracer
  (trace [this entry-id msg]))

(defrecord FileTracer [processed-dir]
  Tracer
  (trace [_ entry-id msg]
    (spit (file-for processed-dir entry-id) (pr-str msg))))

(defn parse-feed
  ([url]
     (parse-feed url nil))
  ([url user-and-pwd]
     (-> url
         (http/get {:basic-auth user-and-pwd
                    :conn-timeout 5000
                    :socket-timeout 5000
                    :as :stream})
         :body
         feedparser/parse-feed)))

(defn trace-msg [result entry]
  {:result result
   :entry entry
   :processed-on (java.util.Date.)})

(defn process-entry [entry handler worker-id conf processing-strategy]
  (let [entry-identifier (-> conf :workers worker-id ::entry-identifier)
        result (try
                 (handler entry worker-id conf)
                 (catch Exception e
                   (log "failed processing" (pr-str entry) e)
                   e))]
    (after-processing processing-strategy (entry-identifier entry))
    result))

(defn find-unprocessed-entries [feed-pages processing-strategy entry-identifier]
  (->> feed-pages
       (mapcat :entries)
       (take-while #(not (already-processed? processing-strategy (entry-identifier %))))))

(defn process-feed [processing-strategy tracer feed-pages handler worker-id conf]
  (let [entry-identifier (-> conf :workers worker-id ::entry-identifier)]
    (when-let [entries (seq (find-unprocessed-entries feed-pages processing-strategy entry-identifier))] ;; find entries which so far have not been processed
      (log (str "found " (count entries) " entry/entries for processing in feed " (-> conf :workers worker-id :url)))
      (loop [[entry & remaining] (reverse entries)] ;; process entries starting from the oldest one
        (when (start-processing? processing-strategy (entry-identifier entry)) ;; check again to make sure no one else processed the entry
                                                                        ;; (and possibly persist the fact that the entry has been processed)
          (let [timer (.timer (::metrics-registry conf) (str (name worker-id) ".entry.processing.timer"))
                result (time! timer
                              (process-entry entry handler worker-id conf processing-strategy))]
            (if (= :break result)
              (mark-for-retry processing-strategy (entry-identifier entry)) ;; break out of loop and process entry again on next run
              (do
                (trace tracer (entry-identifier entry) (trace-msg result entry))
                (when (seq remaining)
                  (recur remaining))))))))))

(defn metric-name [worker suffix]
  (-> (::id worker)
      name
      (str "." suffix)))

(defn map-workers [conf f]
  (update-in conf [:workers]
             (fn [workers]
               (into {} (map (fn [[id worker]]
                               [id (f worker)])
                             workers)))))

(defn add-worker-ids [conf]
  (update-in conf [:workers]
             (fn [workers]
               (into {} (map (fn [[id worker]]
                               [id (assoc worker ::id id)])
                             workers)))))

(defn create-metrics-registry [conf]
  (assoc conf ::metrics-registry (metrics/new-registry)))

(defn processing-strategy [strategy dir]
  (case strategy
    :at-least-once (FileAtLeastOnce. (.getPath dir))
    :at-most-once (FileAtMostOnce. (.getPath dir))))

(defn worker-dir [worker-id conf]
  (let [entries-dir (File. (:processed-entries-dir conf))
        workername (name worker-id)]
    (File. entries-dir workername)))

(defn create-worker-dirs [conf]
  (map-workers conf
               (fn [worker]
                 (let [dir (worker-dir (::id worker) conf)]
                   (.mkdirs dir)
                   (assoc worker ::dir dir)))))

(defn unique-id [entry]
  (pandect/sha1 (pr-str entry)))

(defn create-entry-identifiers [conf]
  (map-workers conf
               (fn [worker]
                 (let [identifier (get worker :entry-identifier unique-id)]
                   (assoc worker ::entry-identifier identifier)))))

(defn create-processing-strategies [conf]
    (map-workers conf 
                 (fn [worker]
                     (assoc worker ::processing-strategy
                            (processing-strategy 
                             (:processing-strategy worker) 
                             (::dir worker))))))

(defn create-feed-loaders [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::feed-loader
                        (fn []
                          (time! (.timer (::metrics-registry conf) (metric-name worker "feed.load.timer"))
                                 (try
                                   ;; TODO load seq of all pages of feed
                                   [(parse-feed (:url worker) (:basic-auth worker))] ;; :basic-auth may be nil
                                   (catch Exception e
                                     (log (str "failed to load feed with url " (:url worker) ": " e))
                                     nil))))))))

(defn create-tracers [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::tracer
                        (FileTracer. (.getAbsolutePath (::dir worker)))))))

(defn files-from-new-to-old [dir]
  (->> dir
       .listFiles
       (filter #(.isFile %))
       (sort-by #(.lastModified %) >)))

(defn cleanup [timer dir keep max]
  (let [files (files-from-new-to-old dir)
        c (count files)]
    (when (and (> c max)
               (not= (.lastModified (first files)) ;; only clean up if unambiguous
                     (.lastModified (second files))))
      (log "cleaning up" dir (str "keeping " keep " of " c " files"))
      (time! timer
             (doseq [f (drop keep files)]
               (.delete f))))))

(defn create-cleanups [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::cleanup
                        (fn []
                          (when (contains? conf :cleanup)
                            (let [{:keys [keep max]} (:cleanup conf)
                                  dir (::dir worker)]
                              (cleanup (.timer (::metrics-registry conf) (metric-name worker "cleanup.timer"))
                                       dir keep max))))))))

(defn create-tasks [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::task
                        (fn []
                          (try
                            (let [s (::processing-strategy worker)
                                  t (::tracer worker)
                                  h (:handler worker)
                                  feed-pages ((::feed-loader worker))]
                              (when (seq feed-pages)
                                (process-feed s t feed-pages h (::id worker) conf)
                                ((::cleanup worker))))
                            (catch Exception e
                              (log "task failed" e))))))))

(defn create-scheduler []
  (let [s (ScheduledThreadPoolExecutor. 1)]
    (on-shutdown (.shutdownNow s))
    s))

(defn create-schedulers [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::scheduler (create-scheduler)))))

(defprotocol Enrichable
  (enrich [this basic]))

(extend-type Object
  Enrichable
  (enrich [_ basic]
    basic))

(extend-type com.codahale.metrics.Timer
  Enrichable
  (enrich [this basic]
    (assoc basic :count (.getCount this))))

(defn reg->map [registry]
  (into {} (map (fn [[name metric]] [name (enrich metric (render-to-basic metric))]) 
                (all-metrics registry))))

(defn metrics-handler [path registry]
  (fn [req]
    (when (and (= :get (:request-method req))
               (.startsWith (:uri req) path))
      {:status 200
       :body (str "<html><head><title>feedworker metrics</title></head><body>"
                  "<h1>metrics</h1>"
                  "<pre>" (pprint-str (reg->map registry)) "</pre>"
                  "<h1>your request</h1>"
                  "<pre>" (pprint-str req) "</pre>"
                  "</body></html>")
       :headers {"Content-Type" "text/html"}})))

(defn publish-metrics [conf]
  (if-let [metrics-conf (get-in conf [:metrics :http])]
    (let [jetty (run-jetty (metrics-handler (:path metrics-conf) (::metrics-registry conf))
                           {:port (:port metrics-conf) :host (:host metrics-conf) :join? false})]
      (log "metrics published" metrics-conf)
      (on-shutdown (.stop jetty))
      (assoc-in conf [:metrics :http ::jetty] jetty))
    conf))

(defn schedule
  ([period f]
     (schedule (create-scheduler) 0 period f))
  ([scheduler initial-delay period f]
     (.scheduleAtFixedRate scheduler f initial-delay period (TimeUnit/MILLISECONDS))))

(defn schedule-tasks!
  "schedules all tasks for execution and returns a map of worker-id to java.util.concurrent.ScheduledFuture"
  [conf]
  (into 
   {}
   (map
    (fn [[id worker]]
      (let [s (::scheduler worker)
            t (::task worker)
            initial-delay (long (rand-int 5000))
            period (:interval worker)
            scheduled-future (schedule s initial-delay period t)]
        [id scheduled-future]))
    (:workers conf))))

(defn prepare [conf]
  (-> conf
      add-worker-ids
      create-metrics-registry
      create-worker-dirs
      create-processing-strategies
      create-entry-identifiers
      create-feed-loaders
      create-tracers
      create-cleanups
      create-tasks))

(defn schedule! [prepared-conf]
  (-> prepared-conf
      create-schedulers
      publish-metrics
      schedule-tasks!))

(defn run! [conf]
  (let [log-step (fn [step msg-fn]
                   (log (msg-fn step))
                   step)]
    (-> conf
        (log-step #(str "running conf: " %))
        prepare
        (log-step #(str "prepared conf: " %))
        schedule!
        (log-step #(str "scheduled: " %)))))

(defn example-handler [entry worker-id conf]
  (log "worker" worker-id "processing entry with title" (:title entry)))

(def example-conf {:workers
                   {:dilbert {:url "http://feed.dilbert.com/dilbert/most_popular?format=xml"
                              :handler example-handler
                              :processing-strategy :at-most-once
                              ;; optional: :entry-identifier :uri
                              :interval 10000}}
                   :processed-entries-dir "processedentries"
                   :cleanup {:keep 10 :max 200}
                   :metrics {:http {:port 8080
                                    :path "/feedworker/metrics"}}})
