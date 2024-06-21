const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');
const { EventEmitter } = require('events');

const numCPUs = 2 || os.cpus().length;

class Pipeline {
    constructor(stages) {
        this.stages = stages;
    }

    async process(input) {
        let result = input;
        for (const stage of this.stages) {
            result = await stage(result);
        }
        return result;
    }
}

class WorkerPool extends EventEmitter {
    constructor(numWorkers, workerScript) {
        super();
        this.workers = [];
        this.taskQueue = [];
        this.freeWorkers = [];

        for (let i = 0; i < numWorkers; i++) {
            this.addNewWorker(workerScript);
        }
    }

    // ... [previous WorkerPool methods remain the same]

    addNewWorker(workerScript) {
        const worker = new Worker(workerScript, {
            workerData: { workerId: this.workers.length }
        });

        worker.on('message', (message) => {
            this.handleWorkerMessage(worker, message);
        });

        worker.on('error', (error) => {
            console.error(`Worker ${worker.threadId} error:`, error);
            this.removeWorker(worker);
            this.addNewWorker(workerScript);
        });

        worker.on('exit', (code) => {
            if (code !== 0) {
                console.error(`Worker ${worker.threadId} exited with code ${code}`);
            }
            this.removeWorker(worker);
        });

        this.workers.push(worker);
        this.freeWorkers.push(worker);
        this.emit('workerAdded', worker);
    }

    removeWorker(worker) {
        const index = this.workers.indexOf(worker);
        if (index !== -1) {
            this.workers.splice(index, 1);
        }
        const freeIndex = this.freeWorkers.indexOf(worker);
        if (freeIndex !== -1) {
            this.freeWorkers.splice(freeIndex, 1);
        }
        this.emit('workerRemoved', worker);
    }

    async runPipeline(pipeline, initialInput) {
        return new Promise((resolve, reject) => {
            const runStage = async (stageIndex, input) => {
                if (stageIndex >= pipeline.stages.length) {
                    resolve(input);
                    return;
                }

                const stage = pipeline.stages[stageIndex];
                try {
                    const result = await this.runTask({ stage, input });
                    runStage(stageIndex + 1, result);
                } catch (error) {
                    reject(error);
                }
            };

            runStage(0, initialInput);
        });
    }

    runTask(task) {
        return new Promise((resolve, reject) => {
            const wrappedTask = {
                ...task,
                resolve,
                reject
            };

            if (this.freeWorkers.length > 0) {
                const worker = this.freeWorkers.pop();
                worker.postMessage({ type: 'task', data: wrappedTask });
            } else {
                this.taskQueue.push(wrappedTask);
            }
        });
    }

    handleWorkerMessage(worker, message) {
        if (message.type === 'task_completed') {
            const { taskId, result } = message.data;
            this.emit('taskCompleted', { taskId, result });
            this.freeWorkers.push(worker);
            this.runNextTask();

            // Resolve the promise for the completed task
            if (this.taskQueue[taskId]) {
                this.taskQueue[taskId].resolve(result);
                delete this.taskQueue[taskId];
            }
        } else if (message.type === 'task_error') {
            const { taskId, error } = message.data;
            this.emit('taskError', { taskId, error });
            this.freeWorkers.push(worker);
            this.runNextTask();

            // Reject the promise for the failed task
            if (this.taskQueue[taskId]) {
                this.taskQueue[taskId].reject(error);
                delete this.taskQueue[taskId];
            }
        } else if (message.type === 'broadcast') {
            this.emit('broadcast', message.data);
        } else {
            this.emit('message', message);
        }
    }
}

if (isMainThread) {
    const workerPool = new WorkerPool(numCPUs, __filename);

    workerPool.on('taskCompleted', ({ taskId, result }) => {
        console.log(`Task ${taskId} completed:`, result);
    });

    workerPool.on('taskError', ({ taskId, error }) => {
        console.error(`Task ${taskId} failed:`, error);
    });

    workerPool.on('broadcast', (message) => {
        console.log('Broadcast received:', message);
    });

    // 定义流水线阶段
    const pipeline = new Pipeline([
        async (input) => input * 2,
        async (input) => input + 10,
        async (input) => input ** 2
    ]);

    // 运行流水线
    workerPool.runPipeline(pipeline, 5)
        .then(result => console.log('Pipeline result:', result))
        .catch(error => console.error('Pipeline error:', error));

} else {
    // 工作线程代码
    console.log(`Worker ${workerData.workerId} started`);

    parentPort.on('message', async (message) => {
        if (message.type === 'task') {
            const { stage, input, taskId } = message.data;
            try {
                const result = await stage(input);
                parentPort.postMessage({
                    type: 'task_completed',
                    data: { taskId, result }
                });
            } catch (error) {
                parentPort.postMessage({
                    type: 'task_error',
                    data: { taskId, error: error.message }
                });
            }
        } else if (message.type === 'broadcast') {
            console.log(`Worker ${workerData.workerId} received broadcast:`, message.data);
        }
    });

    // 工作线程可以主动发送消息
    setInterval(() => {
        parentPort.postMessage({
            type: 'broadcast',
            data: `Hello from worker ${workerData.workerId}`
        });
    }, 5000);
}

/*
https://developer.mozilla.org/en-US/docs/Web/API/DedicatedWorkerGlobalScope/postMessage

不支持跨线程通信

Pipeline error: DOMException [DataCloneError]: async (input) => input * 2 could not be cloned.
    at new DOMException (node:internal/per_context/domexception:53:5)
    at Worker.postMessage (node:internal/worker:378:5)
    at I:\javascript\workthreads\pipeline.js:105:24
    at new Promise (<anonymous>)
    at WorkerPool.runTask (I:\javascript\workthreads\pipeline.js:96:16)
    at runStage (I:\javascript\workthreads\pipeline.js:84:47)
    at I:\javascript\workthreads\pipeline.js:91:13
    at new Promise (<anonymous>)
    at WorkerPool.runPipeline (I:\javascript\workthreads\pipeline.js:75:16)
    at Object.<anonymous> (I:\javascript\workthreads\pipeline.js:166:16)
Worker 0 started
Worker 1 started
Broadcast received: Hello from worker 1
Broadcast received: Hello from worker 0

*/