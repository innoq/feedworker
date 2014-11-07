# feedworker

Utility for processing RSS/Atom feeds.

## Setup

Install [Leiningen](http://leiningen.org/) and a recent JDK. Then:

    lein run config.example.clj

Log messages are written to stdout, exceptions are written to stderr. So:

    lein run config.example.clj 2> exceptions 1> log

## What's this?

The purpose of feedworker is to make processing feeds easy. It manages which entries have been processed already and schedules periodic processing.

Its configured with a Clojure data structure like this:

    {:workers {:statuses-mentions {:url "http://localhost:8080/statuses/updates?format=atom"
                                   :handler notify-mentions-via-naveed
                                   :processing-strategy :at-least-once
                                   :repeat 10000}}
     :processed-entries-dir "processedentries"
     :cleanup {:keep 10 :max 50}
     :naveed {:url "<url>"
              :token "<token>"
              :conn-timeout 2000
              :socket-timeout 2000}}

All durations are given in milliseconds. The actuall processing of each feed entry is done by the given handler (here: notify-mentions-via-naveed). It's a function of three arguments:

* A single feed entry as parsed by [feedparser-clj](https://github.com/scsibug/feedparser-clj).
* The ID of the worker (e.g. :statuses-mentions).
* The entire configuration.

Paginated feeds are not properly supported, yet.

## License

Copyright 2014 innoQ Deutschland GmbH. Published under the Apache 2.0 license.
