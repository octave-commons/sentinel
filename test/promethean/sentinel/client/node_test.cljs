(ns promethean.sentinel.client.node-test
  "Tests for Sentinel Node client"
  (:require [cljs.test :refer-macros [deftest testing is async use-fixtures]]
            [promethean.sentinel.client.node :as client]
            [promethean.sentinel.events :as events]
            [promethean.sentinel.config :as config]))

(deftest test-event-validation
  (testing "valid fs event"
    (let [event {:type :fs
                 :path "/test/path"
                 :relative "test/path"
                 :watcher {:key :test :path "/test" :abs-path "/test"}
                 :ts 1234567890}]
      (is (events/validate-event event))))
  
  (testing "valid synthetic event"
    (let [event {:type :synthetic
                 :path "/test/path"
                 :relative "test/path"
                 :watcher {:key :test :path "/test" :abs-path "/test"}
                 :rule {:id :test :on :change}
                 :event {:type :change :path "/test/path"}
                 :ts 1234567890}]
      (is (events/validate-event event))))
  
  (testing "invalid event - missing type"
    (let [event {:path "/test/path"
                 :watcher {:key :test}
                 :ts 1234567890}]
      (is (not (events/validate-event event)))))
  
  (testing "invalid event - wrong type"
    (let [event {:type :invalid
                 :path "/test/path"
                 :watcher {:key :test}
                 :ts 1234567890}]
      (is (not (events/validate-event event)))))

(deftest test-event-creation
  (testing "create fs event"
    (let [watcher {:key :test :path "/test" :abs-path "/test"}
          event (events/create-fs-event {:path "/test/path" :relative "test/path" :watcher watcher})]
      (is (= (:type event) :fs))
      (is (= (:path event) "/test/path"))
      (is (= (:relative event) "test/path"))
      (is (= (:watcher event) watcher))
      (is (number? (:ts event)))))
  
  (testing "create synthetic event"
    (let [watcher {:key :test :path "/test" :abs-path "/test"}
          rule {:id :test :on :change}
          sub-event {:type :change :path "/test/path"}
          event (events/create-synthetic-event {:path "/test/path" 
                                               :relative "test/path" 
                                               :watcher watcher 
                                               :rule rule 
                                               :event sub-event})]
      (is (= (:type event) :synthetic))
      (is (= (:path event) "/test/path"))
      (is (= (:relative event) "test/path"))
      (is (= (:watcher event) watcher))
      (is (= (:rule event) rule))
      (is (= (:event event) sub-event))
      (is (number? (:ts event))))))

(deftest test-topic-generation
  (testing "fs event topic"
    (let [event {:type :fs}]
      (is (= (events/event->topic event) "sentinel.fs.raw"))))
  
  (testing "synthetic event topic"
    (let [event {:type :synthetic :rule {:id :test}}]
      (is (= (events/event->topic event) "sentinel.synthetic.test."))))
  
  (testing "topic patterns"
    (is (= (events/topic-pattern :fs) "sentinel.fs.raw"))
    (is (= (events/topic-pattern :synthetic) "sentinel.synthetic.*"))))

(deftest test-config-loading
  (testing "default config"
    (let [cfg (config/load-config [])]
      (is (= (:topics cfg) ["sentinel.synthetic.*"]))
      (is (= (:reconnect cfg) true))
      (is (= (:mock cfg) false))
      (is (= (:timeout cfg) 30000))))
  
  (testing "cli args parsing"
    (let [cfg (config/parse-cli-args ["--sentinel" "--mock" "--no-reconnect"])]
      (is (= (:sentinel-enabled cfg) true))
      (is (= (:mock cfg) true))
      (is (= (:reconnect cfg) false))))
  
  (testing "env config loading"
    (let [env-config (config/load-env-config)]
      (is (contains? env-config :url))
      (is (contains? env-config :topics))
      (is (contains? env-config :reconnect)))))

(deftest test-backoff-calculation
  (testing "exponential backoff"
    (let [client-state @client/client-state]
      (is (= (client/create-backoff-delay 1000 30000) 2000))
      (is (= (client/create-backoff-delay 2000 30000) 4000))
      (is (= (client/create-backoff-delay 20000 30000) 30000)))))

(deftest test-mock-client
  (async done
    (testing "mock client creation and subscription"
      (let [received-events (atom [])
            subscription-fn (fn [event] (swap! received-events conj event))
            mock-client (client/create-mock-client subscription-fn)]
        
        (is (contains? mock-client :subscribe))
        (is (contains? mock-client :close))
        
        ;; Test subscription
        ((:subscribe mock-client) subscription-fn)
        
        ;; Wait a bit then close
        (js/setTimeout 
          (fn []
            ((:close mock-client))
            (done))
          1000)))))

(deftest test-mock-client-with-events
  (async done
    (testing "mock client with predefined events"
      (let [received-events (atom [])
            subscription-fn (fn [event] (swap! received-events conj event))
            mock-events [{:type :synthetic :path "/test1" :ts 123}
                        {:type :synthetic :path "/test2" :ts 124}]
            mock-client (client/create-mock-client-with-events subscription-fn mock-events)]
        
        ((:subscribe mock-client) subscription-fn)
        
        ;; Wait for events to be processed
        (js/setTimeout 
          (fn []
            ((:close mock-client))
            (is (>= (count @received-events) 1))
            (done))
          3000)))))

(deftest test-client-config-validation
  (testing "valid config"
    (let [cfg {:url "amqp://localhost" 
               :topics ["sentinel.synthetic.*"] 
               :timeout 30000}]
      (is (empty? (config/validate-config cfg)))))
  
  (testing "invalid config - wrong url type"
    (let [cfg {:url 123 :topics ["sentinel.synthetic.*"]}]
      (is (seq (config/validate-config cfg)))))
  
  (testing "invalid config - wrong topics type"
    (let [cfg {:url "amqp://localhost" :topics "not-an-array"}]
      (is (seq (config/validate-config cfg))))))

(deftest test-client-state-management
  (testing "initial state"
    (let [state @client/client-state]
      (is (= (:connected state) false))
      (is (= (:messaging-ctx state) nil))
      (is (= (:subscriptions state) []))
      (is (= (:reconnect-timer state) nil))
      (is (= (:backoff-ms state) 1000))
      (is (= (:max-backoff-ms state) 30000)))))

(deftest test-event-payload-validation
  (testing "valid payload conversion"
    (let [payload-js (clj->js {:type :synthetic 
                               :path "/test" 
                               :watcher {:key :test} 
                               :rule {:id :test} 
                               :event {:type :change} 
                               :ts 1234567890})
          event (client/validate-event-payload payload-js)]
      (is (some? event))
      (is (= (:type event) :synthetic))
      (is (= (:path event) "/test"))))
  
  (testing "invalid payload"
    (let [payload-js (clj->js {:invalid "payload"})]
      (is (nil? (client/validate-event-payload payload-js))))))

(deftest test-topic-subscription
  (testing "topic subscription creation"
    ;; This would require mocking the messaging context
    ;; For now, just test that the function exists
    (is (fn? client/subscribe-to-topic)))))