{:workers {:statuses-mentions {:url "http://localhost:8080/statuses/updates?format=atom"
                               :handler feedworker.statuses/handler
                               :processing-strategy :at-most-once
                               :repeat 30000}}
 :processed-entries-dir "processedentries"
 :naveed {:conn-timeout 2000
          :socket-timeout 2000
          :token "<token>"
          :url "http://<naveed>/outbox"}}
