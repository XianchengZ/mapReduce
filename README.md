# mapReduce

Andrew's implementation of MapReduce according to [mapReduce paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

It is suppose to be running in distributed network. We simulate this by running multiple process on a single host.

## Code Structure

`/mr` folder contains the implmenetation of mapReduce framework.

`/main` contains the programs that utilize the underlying mapReduce framework and some test cases.

- To run testing, run `./test-mr.sh` inside the `main` folder.

`/mrapps` contains bunch of user defined map reduce functions.

## Fault Tolerant

If the master or coordinator failed, the whole MapReduce job will be re-run.

If a worker fails, the master/coordinator will be responsible for re-running the task.

- If the task is a map task, we will re-run it even if it is in complete state because the underlying disk is not accessible.
- If the task is a reduce task and it has not been completed yet, we will re-run it.

There mechanism used for checking if a worker is still alive is not the perfect solution, we simply check if each task has been completed within 10 seconds. If the task has not been completed yet, we re-assign the job to other workers.

## Sample Testing

We can run the word count example described in the paper above for testing purpose.

1. Go to the `main` directory, e.g. `cd main`
2. To remove any previous outputs, run `rm mr-out*`
3. To start the coordinator, run `go run mrcoordinator.go pg-*.txt`
4. We need to build the `wc.go` program as plugin first, run `go build -buildmode=plugin ../mrapps/wc.go`
5. Open a few other terminals to run some workers, run `go run mrworker.go wc.so`
6. The output files are stored inside `main/mr-out-<partitionNumber>`
