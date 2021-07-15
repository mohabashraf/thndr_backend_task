const keys = require("./keys");

// Express App Setup
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const mqtt = require("mqtt");
const app = express();
app.use(cors());

app.use(bodyParser.json());

// // Postgres Client Setup
// const { Pool } = require("pg");
// const pgClient = new Pool({
//   user: keys.pgUser,
//   host: keys.pgHost,
//   database: keys.pgDatabase,
//   password: keys.pgPassword,
//   port: keys.pgPort,
// });

// pgClient.on("connect", (client) => {
//   client
//     .query(
//       "CREATE TABLE IF NOT EXISTS admin_technical_analysis(id SERIAL PRIMARY KEY, stock_id text, target text, type text, time timestamp)"
//     )
//     .catch((err) => console.error(err));
// });

const mqttClient = mqtt.connect(`mqtt:${keys.mqttHost}:${keys.mqttPort}`);

mqttClient.on("connect", function () {
  mqttClient.subscribe("thndr-trading", (err) => {
    console.log(err);
  });
});

mqttClient.on("message", function (topic, message) {
  // message is Buffer
  const stock = JSON.parse(message.toString());
  redisClient.hset("stocks", stock.stock_id, message);
  redisPublisher.publish("insert", stock.stock_id);
});

// Redis Client Setup
const redis = require("redis");
const redisClient = redis.createClient({
  host: keys.redisHost,
  port: keys.redisPort,
  retry_strategy: () => 1000,
});
const redisPublisher = redisClient.duplicate();

// Express route handlers

app.get("/", (req, res) => {
  res.send("Hi");
});

app.get("/stocks/all", async (req, res) => {
  const values = await pgClient
    .query("SELECT * from stocks")
    .catch((err) => console.log(err));
  res.send(values.rows);
});

app.get("/analysis/all", async (req, res) => {
  const values = await pgClient
    .query("SELECT * from stock_analysis")
    .catch((err) => console.log(err));
  res.send(values.rows);
});

app.get("/techanalysis/all", async (req, res) => {
  const values = await pgClient
    .query("SELECT * from admin_technical_analysis")
    .catch((err) => console.log(err));
  res.send(values.rows);
});



app.get("/stocks", async (req, res) => {
  await redisClient.hgetall("stocks", (err, values) => {
    if (err) {
      console.log("error in fetching values" + err);
      return res.status(422).send("Stock connection lost");
    }

    let stocks = [];
    try {
      console.log(values);
      console.log(typeof values);
      stocks = Object.values(values);
    } catch (err) {
      console.log("error looping through stock values" + err);
      return res.status(422).send("Stock connection lost");
    }
    return res.json(stocks);
  });
});

app.get("/analysis", async (req, res) => {
  await redisClient.hgetall("technical_analysis", (err, values) => {
    if (err) {
      console.log("error in fetching values" + err);
      return res.status(422).send("Stock connection lost");
    }

    let stocks = [];
    try {
      console.log(values);
      console.log(typeof values);
      stocks = Object.values(values);
    } catch (err) {
      console.log("error looping through stock values" + err);
      return res.status(422).send("Stock connection lost");
    }
    return res.json(stocks);
  });
});

app.post("/admin/stocks/:stock_id/analysis", async (req, res) => {
  const { target, type } = req.body;

  console.log("The param " + req.params.stock_id);

  let stock = "";
  stock = await new Promise((resolve) => {
    redisClient.hget("stocks", req.params.stock_id, (err, value) => {
      if (err) {
        reject(err);
        return res.status(422).send("error while retrieving value");
      }

      resolve(JSON.parse(value.toString()));
    });
  }).catch((err) => {
    console.log("Errot", err);
    return res.status(422).send("error while retrieving value");
  });

  console.log("the stock " + stock);

  if (stock === "") {
    return res.status(422).send("Stock is empty");
  }

  let target_hit = true;
  if (stock.price > target && type === "UP") {
    target_hit = true;
  } else {
    target_hit = false;
  }

  let output = {
    target,
    type,
    target_hit,
  };

  // pgClient.query(
  //   "INSERT INTO admin_technical_analysis(stock_id, target, type, time) VALUES($1, $2, $3, $4)",
  //   [req.params.stock_id, target, type, new Date().toISOString()]
  // );
  // redisPublisher.publish("insert", {target, type});

  redisClient.hset(
    "technical_analysis",
    stock.stock_id,
    JSON.stringify({ target, type })
  );

  res.json(output);
});

app.get("/stocks/:stock_id", async (req, res) => {
  console.log("The param " + req.params.stock_id);

  let stock = "";
  stock = await new Promise((resolve) => {
    redisClient.hget("stocks_analysis", req.params.stock_id, (err, value) => {
      if (err) {
        reject(err);
        return res.status(422).send("error while retrieving value");
      }
      if (value) {
        resolve(value);
      } else {
        return resolve("");
      }
    });
  }).catch((err) => {
    console.log("Errot", err);
    return res.status(422).send("error while retrieving value");
  });

  if (stock === "") {
    return res.status(422).send("Stock is empty");
  }

  res.json(JSON.parse(stock));
});

app.listen(5000, (err) => {
  console.log("Listening");
});
