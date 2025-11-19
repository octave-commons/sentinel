(ns promethean.sentinel.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [promethean.sentinel.core :as core]))

(defn reset-state! []
  (reset! core/sentinel-state {:root "/"
                               :watcher nil
                               :anchor-watcher nil
                               :backend :uninitialized
                               :watchers []
                               :registry {}
                               :config-path ""
                               :debounce-cache {}
                               :messaging nil
                               :rpc-closers []}))

(use-fixtures :each (fn [f]
                      (reset-state!)
                      (f)))

(deftest keyword->path-basic
  (is (= "foo/bar" (core/keyword->path :foo/bar)))
  (is (= "foo" (core/keyword->path :foo)))
  (is (= "ns/foo" (core/keyword->path :ns/foo))))

(deftest sentinel-pack-names-merge
  (is (= [:a :b :c]
         (core/sentinel-pack-names {:packs [:a] :use [:b :c]})))
  (is (nil? (core/sentinel-pack-names nil))))

(deftest match-rule-basic
  (testing "glob and type"
    (is (core/match-rule? {:on :add :glob "**/*.txt"} :add "foo/bar.txt" "/tmp/foo/bar.txt"))
    (is (not (core/match-rule? {:on :change} :add "foo" "/tmp/foo"))))
  (testing "size over"
    (with-redefs [core/file-size (fn [_] 2e6)]
      (is (core/match-rule? {:on :change :when {:size-over 1e6}}
                            :change "big.bin" "/tmp/big.bin"))
      (is (not (core/match-rule? {:on :change :when {:size-over 3e6}}
                                 :change "big.bin" "/tmp/big.bin"))))))

(deftest debounce-behavior
  (let [rule {:id :x :debounce-ms 10}
        p "/tmp/file"]
    (is (core/should-emit? rule p 1000))
    (is (not (core/should-emit? rule p 1005)))
    (is (core/should-emit? rule p 1015))))

(deftest recompute-watchers
  (core/registry-assoc "a" [{:path "p1"}])
  (core/registry-assoc "b" [{:path "p2"}])
  (is (= #{"p1" "p2"} (set (map :path (:watchers @core/sentinel-state))))))

(deftest normalize-watch-node-basic
  (let [base "/tmp/root"
        w (core/normalize-watch-node base :docs {:path "docs" :synthetic [{:id :s}]
                                               :ignored ["**/.git/**"]})]
    (is (= :docs (:key w)))
    (is (= "docs" (:path w)))
    (is (= ["**/.git/**"] (:ignored w)))
    (is (re-find #"/tmp/root/docs" (:abs-path w)))
    (is (= [{:id :s}] (:synthetic w)))))

(deftest handle-fs-event-emits-synthetic
  (let [events (atom [])
        watcher {:key :docs :path "docs" :abs-path "/tmp/root"
                 :synthetic [{:id :ping :on :add :glob "**/*.md"}]}
        publish (fn [topic payload]
                  (swap! events conj [topic payload]))]
    (reset! core/sentinel-state {:root "/tmp/root"
                                 :watcher nil
                                 :anchor-watcher nil
                                 :backend :test
                                 :watchers [watcher]
                                 :registry {}
                                 :config-path ""
                                 :debounce-cache {}
                                 :messaging nil
                                 :rpc-closers []})
    (with-redefs [core/publish-event! publish
                  core/log (fn
                             ([a b] nil)
                             ([a b c] nil))]
      (core/handle-fs-event :add "/tmp/root/readme.md"))
    (is (= 1 (count @events)))
    (is (= "sentinel.synthetic.ping" (ffirst @events)))
    (is (= :ping (get-in (second (first @events)) [:rule :id])))))
