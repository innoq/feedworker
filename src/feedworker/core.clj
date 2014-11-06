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

(defprotocol ProcessingStrategy
  (should-be-processed? [this id])
  (mark-processed [this id])
  (mark-for-retry [this id]))

(defn file-for [processed-dir id]
  (let [hash (pandect/sha1 id)
        dir (File. processed-dir)]
    (File. dir hash)))

(defrecord FileAtMostOnce [processed-dir]
  ProcessingStrategy
  (should-be-processed? [_ id]
    (try
      (.createNewFile (file-for processed-dir id))
      (catch Exception _ false)))
  (mark-processed [_ _])
  (mark-for-retry [_ id]
    (try
      (.delete (file-for processed-dir id))
      (catch Exception _))))

(defrecord FileAtLeastOnce [processed-dir]
  ProcessingStrategy
  (should-be-processed? [_ id]
    (try 
      (not (.exists (file-for processed-dir id)))
      (catch Exception _)))
  (mark-processed [_ id]
    (try
      (.createNewFile (file-for processed-dir id))
      (catch Exception _ false)))
  (mark-for-retry [_ id]
    (try
      (.delete (file-for processed-dir id))
      (catch Exception _))))

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
  (try
    (let [result (handler entry worker-id conf)]
      (mark-processed processing-strategy (:uri entry))
      result)
    (catch Exception e
      (log "failed to handle" (pr-str entry) e))))

(defn process-feed [processing-strategy tracer feed handler worker-id conf]
    (loop [[entry & remaining] (reverse (:entries feed))]
      (if (should-be-processed? processing-strategy (:uri entry)) ;; not using filter to avoid chunked lazyness
        (let [result (process-entry entry handler worker-id conf processing-strategy)]
          (if (= :break result)
            (mark-for-retry processing-strategy (:uri entry))
            (do
              (trace tracer (:uri entry) (trace-msg result entry))
              (when (seq remaining)
                (recur remaining)))))
        (when (seq remaining)
          (recur remaining)))))

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
                              (parse-secure-feed (:url worker) (:basic-auth worker))
                              (parse-feed (:url worker)))
                            (catch Exception e
                              (log (str "failed to load feed with url " (:url worker) ": " e))
                              nil)))))))

(defn create-tracers [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::tracer
                        (FileTracer. (.getAbsolutePath (::dir worker)))))))

(defn create-tasks [conf]
  (map-workers conf
               (fn [worker]
                 (assoc worker ::task
                        (fn []
                          (let [s (::processing-strategy worker)
                                t (::tracer worker)
                                h (:handler worker)
                                feed ((::feed-loader worker))]
                            (when feed
                              (process-feed s t feed h (::id worker) conf))))))))

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
