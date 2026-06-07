// console.log("1: sync");

// setTimeout(() => console.log("4: macrotask"), 0);

// Promise.resolve().then(() => console.log("3: microtask"));

// console.log("2: sync");

queue = [];

function addTask(task) {
  queue.push(task);
}

function runEventLoop() {
  while (queue.length > 0) {
    const task = queue.shift();
    task();
  }
  setTimeout(runEventLoop, 10);
}

addTask(() => console.log("task 1"));
addTask(() => console.log("task 2"));
addTask(() => console.log("task 3"));

runEventLoop();

setTimeout(() => addTask(() => console.log("task 4 added late")), 500);
