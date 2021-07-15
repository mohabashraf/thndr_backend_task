const keys = require("./keys");

// Express App Setup
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const mqtt = require("mqtt");
const app = express();
app.use(cors());

app.use(bodyParser.json());

const mqttClient = mqtt.connect(`mqtt:${keys.mqttHost}:${keys.mqttPort}`);

mqttClient.on("connect", function () {
  mqttClient.subscribe("thndr-trading", (err) => {
    console.log(err);
  });
});

mqttClient.on("message", function (topic, message) {
  // message is Buffer
  const stock = JSON.parse(message.toString());
  redisClient.hset("values", stock.stock_id, message);
});

// Postgres Client Setup
const { Pool } = require("pg");
const pgClient = new Pool({
  user: keys.pgUser,
  host: keys.pgHost,
  database: keys.pgDatabase,
  password: keys.pgPassword,
  port: keys.pgPort,
});

pgClient.on("connect", (client) => {
  client
    .query("CREATE TABLE IF NOT EXISTS values (number INT)")
    .catch((err) => console.error(err));
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

app.get("/values/all", async (req, res) => {
  const values = await pgClient.query("SELECT * from values");

  res.send(values.rows);
});

app.get("/stocks", async (req, res) => {
  redisClient.hgetall("values", (err, values) => {
    if (err) {
      console.log("error in fetching values" + err);
      res.status(422).send("Stock connection lost");
    }

    let stocks = [];
    try {
      console.log(values)
      console.log(typeof values)
      stocks = Object.values(values);
    } catch (err) {
      console.log("error looping through stock values" + err);
      res.status(422).send("Stock connection lost");
    }
    res.json(stocks);
  });
});

app.post("/admin/stocks/:stock_id/analysis", async (req, res) => {
  const { target, type } = req.body;

  const stock = redisClient.get(req.params.stock_id);

  console.log("stock  " + stock);
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

  // pgClient.query("INSERT INTO values(number) VALUES($1)", ["inde"]);
  // redisPublisher.publish("insert", {target, type});

  res.json(output);
});

app.post("/values", async (req, res) => {
  const index = req.body.index;

  if (parseInt(index) > 40) {
    return res.status(422).send("Index too high");
  }

  redisClient.hset("values", index, "Nothing yet!");
  redisPublisher.publish("insert", index);
  pgClient.query("INSERT INTO values(number) VALUES($1)", [index]);

  res.send({ working: true });
});

app.listen(5000, (err) => {
  console.log("Listening");
});
