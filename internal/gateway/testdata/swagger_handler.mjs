import http from "node:http";
import { once } from "node:events";
import { createRequire } from "node:module";

const framework = createRequire(process.env.FRAMEWORK_PACKAGE_JSON);
const express = framework("express");
const swaggerUI = framework("swagger-ui-express");
const application = express();
application.use("/api/example/docs", swaggerUI.serve, swaggerUI.setup({
  openapi: "3.0.0",
  info: { title: "EventBus unchanged Swagger fixture", version: "1.0.0" },
  paths: {},
}));

// Only the Lambda HTTP adapter belongs to this fixture. Express and Swagger
// middleware receive the original native event path and handle redirects/assets.
export async function docs(event) {
  const path = event.rawPath ?? event.path;
  const method = event.requestContext.http?.method ?? event.httpMethod;
  const query = event.rawQueryString ?? new URLSearchParams(event.queryStringParameters).toString();
  const server = http.createServer(application);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    const response = await new Promise((resolve, reject) => {
      const request = http.request({
        host: "127.0.0.1", port: server.address().port,
        path: path + (query ? "?" + query : ""), method, agent: false,
      }, incoming => {
        const body = [];
        incoming.on("data", chunk => body.push(chunk));
        incoming.on("end", () => resolve({
          statusCode: incoming.statusCode,
          headers: {
            ...incoming.headers,
            "x-fixture-native-path": path,
            "x-fixture-native-route": event.routeKey ?? `${method} ${event.resource}`,
          },
          body: Buffer.concat(body).toString("utf8"),
        }));
        incoming.on("error", reject);
      });
      request.on("error", reject);
      request.end();
    });
    return response;
  } finally {
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
}

export async function authorize(event) {
  return { isAuthorized: event.headers.authorization === "Bearer owned", context: { fixture: "swagger" } };
}

export async function fallback(event) {
  return { statusCode: 404, headers: { "x-fixture-default": "selected" }, body: "protected default integration" };
}
