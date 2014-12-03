# feedworker

Library for processing RSS/Atom feeds.

## Setup

Add this to you project.clj:

     [feedworker "0.1.0"]

## What's this?

The purpose of feedworker is to make processing feeds easy. It manages which entries have been processed already and schedules periodic processing.

It's configured with a Clojure data structure like this:

    {:workers
     {:dilbert {:url "http://feed.dilbert.com/dilbert/most_popular?format=xml"
                :handler example-handler
                :processing-strategy :at-most-once
                :interval 10000}}
     :processed-entries-dir "processedentries"
     :cleanup {:keep 10 :max 200}
     :metrics {:http {:port 8080
                      :path "/feedworker/metrics"}}}

All durations are given in milliseconds. The actual processing of each feed entry is done by the given handler (here: example-handler). It's a function of three arguments:

* A single feed entry as parsed by [feedparser-clj](https://github.com/scsibug/feedparser-clj).
* The ID of the worker (e.g. :dilbert).
* The entire configuration.

Paginated feeds are not properly supported, yet. Also, feedworker needs to be able to track which entries it already processed. By default, this is currently done by hashing each entry to generate a unique id which is then stored on disk. This means, that feedworker cannot handle feeds that contain entries which are exact duplicates (not only regarding the content but considering all metadata, e.g. timestamps). Optionally, the configuration of a worker may contain an :entry-identifier function which is then applied to each entry to determine the id. Use ':uri' to access the unique id of an atom feed entry.

## License

Copyright 2014 innoQ Deutschland GmbH. Published under the Apache 2.0 license.
