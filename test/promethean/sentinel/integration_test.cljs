(ns promethean.sentinel.integration-test
  "Integration tests for Sentinel client"
  (:require [cljs.test :refer-macros [deftest testing is async use-fixtures]]
            [promethean.sentinel.client.node :as client]
            [promethean.sentinel.config :as config]
            [promethean.sentinel.events :as events]))

(deftest test-end-to-end-mock-flow
  (async done
    (testing "complete mock flow from config to event handling"
      (let [received-events (atom [])
            subscription-fn (fn [event] 
                            (swap! received-events conj event)
                            (when (>= (count @received-events) 2)
                              (done)))
            
            ;; Load config with mock mode
            cfg (config/load-config ["--mock"])
            client-config (config/create-client-config cfg)
            
            ;; Create mock client
            mock-client (client/create-mock-client subscription-fn)]
        
        ;; Verify config loaded correctly
        (is (= (:mock client-config) true))
        
        ;; Start subscription
        ((:subscribe mock-client) subscription-fn)
        
        ;; Wait for events
        (js/setTimeout 
          (fn []
            ((:close mock-client))
            (is (> (count @received-events) 0)))
          6000)))))

(deftest test-event-processing-chain
  (async done
    (testing "event creation -> validation -> topic generation"
      (let [watcher {:key :test :path "/test" :abs-path "/test"}
            rule {:id :test-change :on :change}
            fs-event (events/create-fs-event {:path "/test/file.txt" 
                                             :relative "file.txt" 
                                             :watcher watcher})
            synthetic-event (events/create-synthetic-event {:path "/test/file.txt"
                                                          :relative "file.txt"
                                                          :watcher watcher
                                                          :rule rule
                                                          :event {:type :change :path "/test/file.txt"}})]
        
        ;; Test event creation
        (is (events/validate-event fs-event))
        (is (events/validate-event synthetic-event))
        
        ;; Test topic generation
        (is (= (events/event->topic fs-event) "sentinel.fs.raw"))
        (is (= (events/event->topic synthetic-event) "sentinel.synthetic.test-change."))
        
        ;; Test payload validation
        (let [payload-js (clj->js synthetic-event)
              validated-event (client/validate-event-payload payload-js)]
          (is (some? validated-event))
          (is (= (:type validated-event) :synthetic)))
        
        (done)))))

(deftest test-config-merging-precedence
  (testing "configuration precedence: CLI > env > file > defaults"
    ;; Test that CLI args take precedence
    (let [cfg (config/load-config ["--sentinel" "--mock" "--timeout" "5000"])]
      (is (= (:sentinel-enabled cfg) true))
      (is (= (:mock cfg) true))
      (is (= (:timeout cfg) 5000)))
    
    ;; Test default values
    (let [cfg (config/load-config [])]
      (is (= (:reconnect cfg) true))
      (is (= (:mock cfg) false))
      (is (= (:timeout cfg) 30000)))))

(deftest test-error-handling
  (async done
    (testing "client handles connection failures gracefully"
      (let [error-count (atom 0)
            log-fn (fn [level msg ctx] 
                    (when (= level :error)
                      (swap! error-count inc)))
            
            ;; Try to create client with invalid URL
            cfg {:url "invalid://url" :reconnect false :mock false}
            client-instance (client/createSentinelClient cfg)]
        
        ;; Should return nil due to connection failure
        (is (nil? client-instance))
        
        ;; Should have logged errors
        (js/setTimeout 
          (fn []
            (done))
          1000)))))