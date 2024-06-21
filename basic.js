const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');

const numCPUs = os.cpus().length;

if (isMainThread) {
    // 主线程代码
    console.log(`主线程启动，CPU核心数: ${numCPUs}`);

    const workers = new Set();

    // 创建工作线程
    for (let i = 0; i < numCPUs; i++) {
        const worker = new Worker(__filename, {
            workerData: { workerId: i }
        });

        worker.on('message', (message) => {
            console.log(`来自工作线程 ${i} 的消息:`, message);
        });

        worker.on('error', (error) => {
            console.error(`工作线程 ${i} 错误:`, error);
        });

        worker.on('exit', (code) => {
            if (code !== 0) {
                console.error(`工作线程 ${i} 退出，退出码: ${code}`);
            }
            workers.delete(worker);
            if (workers.size === 0) {
                console.log('所有工作线程已完成');
            }
        });

        workers.add(worker);
    }

    // 这里可以添加主线程的其他逻辑
    // 例如: 分发任务给工作线程

} else {
    // 工作线程代码
    console.log(`工作线程 ${workerData.workerId} 启动`);

    // 这里添加工作线程的具体任务
    // 例如:
    function performTask() {
        // 模拟一些耗时操作
        const result = Math.random() * 1000;
        parentPort.postMessage(`任务完成，结果: ${result}`);
    }

    performTask();

    // 如果有持续运行的任务，可以使用 setInterval 等方法
    // setInterval(performTask, 1000);
}