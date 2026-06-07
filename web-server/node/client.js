const net = require("net");

const client = net.createConnection({ port: 4000 }, () => {
  console.log("Connected to server");
});

client.on("data", (data) => {
  console.log(`Received data from server: ${data}`);
});

client.on("end", () => {
  console.log("Disconnected from server");
});
