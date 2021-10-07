

# Running

```
poetry install
poetry run python main.py
```

On Windows via WSL, I get

```
Stage 0:   0%|          | 0/1 [00:00<?, ?it/s]
Traceback (most recent call last):
  File "main.py", line 31, in <module>
    main_pipeline_actor()
  File "main.py", line 24, in main_pipeline_actor
    results = ray.get([actor.log_from_pipeline.remote() for actor in actors])
  File "/home/anthony/.cache/pypoetry/virtualenvs/hello-ray-O1YdFran-py3.8/lib/python3.8/site-packages/ray/_private/client_mode_hook.py", line 82, in wrapper
    return func(*args, **kwargs)
  File "/home/anthony/.cache/pypoetry/virtualenvs/hello-ray-O1YdFran-py3.8/lib/python3.8/site-packages/ray/worker.py", line 1621, in get
    raise value.as_instanceof_cause()
ray.exceptions.RayTaskError: ray::RemotePipelineActor.log_from_pipeline() (pid=22753, ip=172.28.74.251, repr=<main.RemotePipelineActor object at 0x7f4cd8079100>)
  File "main.py", line 14, in log_from_pipeline
    for df in self.pipeline.iter_batches(batch_size=1000, batch_format="pandas"):
  File "/home/anthony/.cache/pypoetry/virtualenvs/hello-ray-O1YdFran-py3.8/lib/python3.8/site-packages/ray/data/dataset_pipeline.py", line 97, in gen_batches
    for batch in ds.iter_batches(
  File "/home/anthony/.cache/pypoetry/virtualenvs/hello-ray-O1YdFran-py3.8/lib/python3.8/site-packages/ray/data/dataset.py", line 1016, in iter_batches
    block = ray.get(block_window[0])
ray.exceptions.ObjectLostError: Object 69a6825d641b4613ffffffffffffffffffffffff0100000001000000 is lost due to node failure.
2021-10-07 10:44:51,234 WARNING worker.py:1215 -- A worker died or was killed while executing a task by an unexpected system error. To troubleshoot the problem, check the logs for the dead worker. RayTask ID: 082653e59a59fdd2eece02761b7c13e76e282c05723325f9 Worker ID: 3f6649cf1f6d124f69ad4ab451ce71fbdde71d4654ac4ffe0b5f6ed7 Node ID: 80cbfab28d3fe4ef07f03cc79921e4ac6525b0e5c17c50e2833dfca2 Worker IP address: 172.28.74.251 Worker port: 43659 Worker PID: 22751
(pid=22751) *** SIGSEGV received at time=1633628691 on cpu 1 ***
(pid=22751) PC: @     0x7f8b75dce8e0  (unknown)  ray::core::CoreWorkerDirectTaskReceiver::HandleTask()::{lambda()#1}::operator()()
(pid=22751)     @     0x7f8b776a7210  (unknown)  (unknown)
(pid=22751)     @     0x7f8b75dcf05a         80  std::_Function_handler<>::_M_invoke()
(pid=22751)     @     0x7f8b75d50705        448  ray::core::NormalSchedulingQueue::ScheduleRequests()
(pid=22751)     @     0x7f8b7610e1b6        112  boost::asio::detail::completion_handler<>::do_complete()
(pid=22751)     @     0x7f8b76210a28        112  boost::asio::detail::scheduler::do_run_one()
(pid=22751)     @     0x7f8b762115e1        160  boost::asio::detail::scheduler::run()
(pid=22751)     @     0x7f8b76213130         64  boost::asio::io_context::run()
(pid=22751)     @     0x7f8b75dbafd5        144  ray::core::CoreWorkerProcess::RunTaskExecutionLoop()
(pid=22751)     @     0x7f8b75c57c17         32  __pyx_pw_3ray_7_raylet_10CoreWorker_9run_task_loop()
(pid=22751)     @           0x50506b  (unknown)  (unknown)
(pid=22751)     @           0x908780  (unknown)  (unknown)
(pid=22753) 2021-10-07 10:44:51,166     WARNING worker.py:1619 -- Local object store memory usage:
(pid=22753)
(pid=22753) (global lru) capacity: 6987585945
(pid=22753) (global lru) used: 0.170365%
(pid=22753) (global lru) num objects: 1
(pid=22753) (global lru) num evictions: 0
(pid=22753) (global lru) bytes evicted: 0
(pid=22753)

```
