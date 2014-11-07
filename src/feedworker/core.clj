(ns feedworker.core
  (:require [feedparser-clj.core :refer [parse-feed]]
            [clj-http.client :as http]
            [pandect.core :as pandect])
  (:import [java.io File]
           [java.util.concurrent ScheduledThreadPoolExecutor TimeUnit]))

(defn log [& msgs]
  (let [now (java.util.Date.)]
    (apply println (str now) "---" msgs)
    (doseq [msg msgs]
      (when (instance? Exception msg)
        (.printStackTrace msg)))))

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

(defn parse-secure-feed [url user-and-pwd]
  (-> url
      (http/get {:basic-auth user-and-pwd
                 :conn-timeout 5000
                 :socket-timeout 5000
                 :as :stream})
      :body
      parse-feed))

(defn trace-msg [result entry]
  {:result result
   :entry entry
   :processed-on (java.util.Date.)})

(defn process-entry [entry handler worker-id conf processing-strategy]
  (let [result (try
                 (handler entry worker-id conf)
                 (catch Exception e
                   (log "failed processing" (pr-str entry) e)))]
    (after-processing processing-strategy (:uri entry))
    result))

(defn find-unprocessed-entries [feed-pages processing-strategy]
  (->> feed-pages
       (mapcat :entries)
       (take-while #(not (already-processed? processing-strategy (:uri %))))))

(defn process-feed [processing-strategy tracer feed-pages handler worker-id conf]
  (let [entries (find-unprocessed-entries feed-pages processing-strategy)] ;; find entries which so far have not been processed
    (loop [[entry & remaining] (reverse entries)] ;; process entries starting from the oldest one
      (when (start-processing? processing-strategy (:uri entry)) ;; check again to make sure no one else processed the entry
                                                                 ;; (and possibly persist the fact that the entry has been processed)
        (let [result (process-entry entry handler worker-id conf processing-strategy)]
          (if (= :break result)
            (mark-for-retry processing-strategy (:uri entry)) ;; break out of loop and process entry again on next run
            (do
              (trace tracer (:uri entry) (trace-msg result entry))
              (when (seq remaining)
                (recur remaining)))))))))

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
                          (try
                            (if (contains? worker :basic-auth)
                              ;; TODO load seq of all pages of feed
                              [(parse-secure-feed (:url worker) (:basic-auth worker))]
                              [(parse-feed (:url worker))])
                            (catch Exception e
                              (log (str "failed to load feed with url " (:url worker) ": " e))
                              nil)))))))

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

(defn cleanup [dir keep max]
  (let [files (files-from-new-to-old dir)
        c (count files)]
    (when (> c max)
      (log "cleaning up" dir (str "keeping " keep " of " c " files"))
      (doseq [f (drop keep files)]
        (.delete f)))))

(defn create-cleanups [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::cleanup
                        (fn []
                          (when (contains? conf :cleanup)
                            (let [{:keys [keep max]} (:cleanup conf)
                                  dir (::dir worker)]
                              (cleanup dir keep max))))))))

(defn create-tasks [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::task
                        (fn []
                          (let [s (::processing-strategy worker)
                                t (::tracer worker)
                                h (:handler worker)
                                feed-pages ((::feed-loader worker))]
                            (when (seq feed-pages)
                              (process-feed s t feed-pages h (::id worker) conf)
                              ((::cleanup worker)))))))))

(defn create-schedulers [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::scheduler
                        (ScheduledThreadPoolExecutor. 1)))))

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
            period (:repeat worker)
            time-unit (TimeUnit/MILLISECONDS)
            scheduled-future (.scheduleAtFixedRate s t initial-delay period time-unit)]
        (.addShutdownHook (Runtime/getRuntime) (Thread. #(.shutdownNow s)))
        [id scheduled-future]))
    (:workers conf))))

(defn prepare [conf]
  (-> conf
      add-worker-ids
      create-worker-dirs
      create-processing-strategies
      create-feed-loaders
      create-tracers
      create-cleanups
      create-tasks))

(defn schedule! [prepared-conf]
  (-> prepared-conf
      create-schedulers
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

(def conf-example {:workers
                   {:statuses-mentions {:url "http://<statuseshost>/statuses/updates?format=atom"
                                        :handler 'feedworker.statuses/handler
                                        :processing-strategy :at-most-once
                                        :repeat 10000}}
                   :processed-entries-dir "processedentries"
                   :cleanup {:keep 10 :max 50}
                   :naveed {:url "http://<naveedhost>/outbox"
                            :token "<token>"
                            :conn-timeout 2000
                            :socket-timeout 2000}})
