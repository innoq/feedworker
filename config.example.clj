{:workers {:statuses-mentions {:url "http://localhost:8080/statuses/updates?format=atom"
                               :handler feedworker.statuses/handler
                               :processing-strategy :at-most-once
                               :repeat 30000
                               :naveed-token "<token>"}}
 :processed-entries-dir "processedentries"
 :cleanup {:keep 10 :max 50}
 :naveed {:conn-timeout 2000
          :socket-timeout 2000
          :url "http://<naveed>/outbox"}}
