const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');
const { EventEmitter } = require('events');
const winston = require('winston');

const numCPUs = 2 || os.cpus().length;

// 创建日志记录器
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.File({ filename: 'error.log', level: 'error' }),
        new winston.transports.File({ filename: 'combined.log' }),
        new winston.transports.Console()
    ]
});

class Pipeline {
    constructor(stages, name = 'UnnamedPipeline') {
        this.stages = stages;
        this.name = name;
    }

    async process(input) {
        let result = input;
        for (let i = 0; i < this.stages.length; i++) {
            const stage = this.stages[i];
            logger.info(`Pipeline ${this.name}: Starting stage ${i + 1}`);
            const startTime = Date.now();
            result = await stage(result);
            const duration = Date.now() - startTime;
            logger.info(`Pipeline ${this.name}: Completed stage ${i + 1} in ${duration}ms`);
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
        this.metrics = {
            totalTasks: 0,
            completedTasks: 0,
            failedTasks: 0,
            avgTaskDuration: 0
        };

        for (let i = 0; i < numWorkers; i++) {
            this.addNewWorker(workerScript);
        }

        // 定期记录指标
        setInterval(() => this.logMetrics(), 60000); // 每分钟记录一次
    }

    addNewWorker(workerScript) {
        const worker = new Worker(workerScript, {
            workerData: { workerId: this.workers.length }
        });

        worker.on('message', (message) => {
            this.handleWorkerMessage(worker, message);
        });

        worker.on('error', (error) => {
            logger.error(`Worker ${worker.threadId} error:`, error);
            this.removeWorker(worker);
            this.addNewWorker(workerScript);
        });

        worker.on('exit', (code) => {
            if (code !== 0) {
                logger.error(`Worker ${worker.threadId} exited with code ${code}`);
            }
            this.removeWorker(worker);
        });

        this.workers.push(worker);
        this.freeWorkers.push(worker);
        logger.info(`Added new worker. Total workers: ${this.workers.length}`);
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
        logger.info(`Removed worker. Remaining workers: ${this.workers.length}`);
    }

    async runPipeline(pipeline, initialInput) {
        logger.info(`Starting pipeline: ${pipeline.name}`);
        const startTime = Date.now();
        try {
            const result = await pipeline.process(initialInput);
            const duration = Date.now() - startTime;
            logger.info(`Completed pipeline: ${pipeline.name} in ${duration}ms`);
            return result;
        } catch (error) {
            logger.error(`Error in pipeline ${pipeline.name}:`, error);
            throw error;
        }
    }

    runTask(task) {
        return new Promise((resolve, reject) => {
            const taskId = this.metrics.totalTasks++;
            const wrappedTask = {
                ...task,
                taskId,
                startTime: Date.now(),
                resolve,
                reject
            };

            if (this.freeWorkers.length > 0) {
                const worker = this.freeWorkers.pop();
                worker.postMessage({ type: 'task', data: wrappedTask });
                logger.debug(`Assigned task ${taskId} to worker ${worker.threadId}`);
            } else {
                this.taskQueue.push(wrappedTask);
                logger.debug(`Queued task ${taskId}. Queue length: ${this.taskQueue.length}`);
            }
        });
    }

    handleWorkerMessage(worker, message) {
        if (message.type === 'task_completed' || message.type === 'task_error') {
            const { taskId, result, error } = message.data;
            const task = this.taskQueue[taskId];
            if (task) {
                const duration = Date.now() - task.startTime;
                this.updateMetrics(duration, message.type === 'task_completed');

                if (message.type === 'task_completed') {
                    logger.info(`Task ${taskId} completed in ${duration}ms`);
                    task.resolve(result);
                } else {
                    logger.error(`Task ${taskId} failed after ${duration}ms:`, error);
                    task.reject(error);
                }

                delete this.taskQueue[taskId];
            }

            this.freeWorkers.push(worker);
            this.runNextTask();
        } else if (message.type === 'broadcast') {
            logger.debug('Broadcast received:', message.data);
            this.emit('broadcast', message.data);
        } else {
            logger.debug('Message from worker:', message);
            this.emit('message', message);
        }
    }

    runNextTask() {
        if (this.taskQueue.length > 0 && this.freeWorkers.length > 0) {
            const task = this.taskQueue.shift();
            const worker = this.freeWorkers.pop();
            worker.postMessage({ type: 'task', data: task });
            logger.debug(`Assigned queued task ${task.taskId} to worker ${worker.threadId}`);
        }
    }

    updateMetrics(taskDuration, isCompleted) {
        if (isCompleted) {
            this.metrics.completedTasks++;
        } else {
            this.metrics.failedTasks++;
        }

        // 更新平均任务持续时间
        const totalTasks = this.metrics.completedTasks + this.metrics.failedTasks;
        this.metrics.avgTaskDuration = (this.metrics.avgTaskDuration * (totalTasks - 1) + taskDuration) / totalTasks;
    }

    logMetrics() {
        logger.info('Worker Pool Metrics', this.metrics);
    }
}

if (isMainThread) {
    const workerPool = new WorkerPool(numCPUs, __filename);

    // 定义流水线阶段
    const pipeline = new Pipeline([
        async (input) => {
            logger.debug('Stage 1: Doubling input');
            return input * 2;
        },
        async (input) => {
            logger.debug('Stage 2: Adding 10');
            return input + 10;
        },
        async (input) => {
            logger.debug('Stage 3: Squaring');
            return input ** 2;
        }
    ], 'MathPipeline');

    // 运行流水线
    workerPool.runPipeline(pipeline, 5)
        .then(result => logger.info('Pipeline result:', result))
        .catch(error => logger.error('Pipeline error:', error));

} else {
    // 工作线程代码
    logger.info(`Worker ${workerData.workerId} started`);

    parentPort.on('message', async (message) => {
        if (message.type === 'task') {
            const { stage, input, taskId } = message.data;
            try {
                logger.debug(`Worker ${workerData.workerId} starting task ${taskId}`);
                const result = await stage(input);
                logger.debug(`Worker ${workerData.workerId} completed task ${taskId}`);
                parentPort.postMessage({
                    type: 'task_completed',
                    data: { taskId, result }
                });
            } catch (error) {
                logger.error(`Worker ${workerData.workerId} error in task ${taskId}:`, error);
                parentPort.postMessage({
                    type: 'task_error',
                    data: { taskId, error: error.message }
                });
            }
        } else if (message.type === 'broadcast') {
            logger.debug(`Worker ${workerData.workerId} received broadcast:`, message.data);
        }
    });
}