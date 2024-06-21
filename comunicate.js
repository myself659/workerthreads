const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');
const { EventEmitter } = require('events');

const numCPUs = os.cpus().length;

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

    handleWorkerMessage(worker, message) {
        if (message.type === 'task_completed') {
            this.emit('taskCompleted', message.data);
            this.freeWorkers.push(worker);
            this.runNextTask();
        } else if (message.type === 'broadcast') {
            this.emit('broadcast', message.data);
        } else {
            this.emit('message', message);
        }
    }

    runTask(task) {
        if (this.freeWorkers.length > 0) {
            const worker = this.freeWorkers.pop();
            worker.postMessage({ type: 'task', data: task });
        } else {
            this.taskQueue.push(task);
        }
    }

    runNextTask() {
        if (this.taskQueue.length > 0 && this.freeWorkers.length > 0) {
            const task = this.taskQueue.shift();
            const worker = this.freeWorkers.pop();
            worker.postMessage({ type: 'task', data: task });
        }
    }

    broadcast(message) {
        for (const worker of this.workers) {
            worker.postMessage({ type: 'broadcast', data: message });
        }
    }
}

if (isMainThread) {
    const workerPool = new WorkerPool(numCPUs, __filename);

    workerPool.on('taskCompleted', (result) => {
        console.log('Task completed:', result);
    });

    workerPool.on('broadcast', (message) => {
        console.log('Broadcast received:', message);
    });

    workerPool.on('message', (message) => {
        console.log('Message from worker:', message);
    });

    // 示例：运行任务
    for (let i = 0; i < 10; i++) {
        workerPool.runTask({ id: i, data: `Task ${i}` });
    }

    // 示例：广播消息
    setTimeout(() => {
        workerPool.broadcast('Hello from main thread!');
    }, 1000);

} else {
    // 工作线程代码
    console.log(`Worker ${workerData.workerId} started`);

    parentPort.on('message', (message) => {
        if (message.type === 'task') {
            // 执行任务
            console.log(`Worker ${workerData.workerId} received task:`, message.data);
            // 模拟任务处理
            setTimeout(() => {
                parentPort.postMessage({
                    type: 'task_completed',
                    data: { taskId: message.data.id, result: Math.random() }
                });
            }, Math.random() * 1000);
        } else if (message.type === 'broadcast') {
            console.log(`Worker ${workerData.workerId} received broadcast:`, message.data);
            // 可以在这里处理广播消息
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