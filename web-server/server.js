// import { createServer } from "net";

// const server = createServer({ allowHalfOpen: true }, (socket) => {
//   socket
//     .on("data", (data) => {
//       console.log(`Received data from client: ${data}`);
//       socket.write(`Echo: ${data}`);
//     })
//     .on("end", () => {
//       console.log("Client disconnected");
//     })
//     .on("error", (err) => {
//       console.log("Something went wrong:", err.message);
//     });
// });

// server.listen(4000, () => console.log("Server is listening on port 4000"));

import { createServer } from "net";

const server = createServer((socket) => {
  socket.setTimeout(5000); // wait 5 seconds of inactivity then close

  socket.on("data", (data) => {
    const request = data.toString();
    const firstLine = request.split("\r\n")[0];
    const [method, path] = firstLine.split(" ");

    console.log(`${method} ${path}`);

    const body = `<h1>Hello</h1><p>You hit: ${path}</p>`;

    socket.write(
      `HTTP/1.1 200 OK\r\n` +
        `Connection: keep-alive\r\n` + // ← tell browser don't close
        `Keep-Alive: timeout=5\r\n` + // ← tell browser wait 5 seconds
        `Content-Type: text/html\r\n` +
        `Content-Length: ${Buffer.byteLength(body)}\r\n` +
        `\r\n` +
        body,
    );
    // notice no socket.end() ← we don't close anymore
  });

  socket.on("timeout", () => {
    console.log("Connection timed out, closing");
    socket.end();
  });

  socket.on("end", () => console.log("Client disconnected"));

  socket.on("error", (err) => {
    if (err.code === "ECONNRESET") {
      console.log("Client forcefully closed the connection");
    } else {
      console.log("Socket error:", err.message);
    }
  });
});

server.listen(4000, () => console.log("Listening on port 4000"));
