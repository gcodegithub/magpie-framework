(ns magpie-framework-clj.task-executor
  (:import [java.io File IOException])
  (:require [taoensso.timbre :as timbre]
            [com.jd.bdp.magpie.util.utils :as utils]))

(defn execute
  "
  run-fn: 这个是系统正常执行的方法，它会被连续无休眠地被调用，即当它执行结束后，这个方法就会被再次调用，直到收到其它命令。
          为了保证任务能收到系统的其它指令，这个方法里最好不要有耗时操作，更不可在此执行死循环。
  prepare-fn: 初始化application，任务里初始化的操作都在这里执行，比如queue的连接、系统的配置等。
              任务在第一次开始执行run-fun前，会先执行这个方法。
  reload-fn: 当对系统任务执行reload命令时，会调用这个方法。
  pause-fn: 当对系统任务执行pause命令时，会调用这个方法。
  close-fn: 当对系统任务执行kill命令时，会调用这个方法。
  "
  [{run-fn} & {:keys [prepare-fn reload-fn pause-fn close-fn]}]
  (let [zk-servers (System/getProperty "zookeeper.servers")
        zk-root (System/getProperty "zookeeper.root")
        pids-dir (System/getProperty "pids.dir")
        job-id (System/getProperty "job.id")
        job-node (System/getProperty "job.node")]
    (let [file (File. pids-dir)]
        (if-not (.isDirectory file)
          (try
            (.mkdirs file)
            (catch IOException e
              (timbre/error (.toString e))
              (System/exit -1))))
        (let [pid-file (File. file (utils/get-pid))]
          (try
            (.createNewFile pid-file)
            (catch IOException e
              (timbre/error (.toString e))
              (System/exit -1)))))))
