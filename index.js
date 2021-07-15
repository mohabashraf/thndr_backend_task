const keys = require("./keys");

// Express App Setup
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const mqtt = require("mqtt");
const app = express();
app.use(cors());

app.use(bodyParser.json());


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
    .query("CREATE TABLE IF NOT EXISTS stocks(stock_id text, name text, price INT, availability INT)")
    .query("CREATE TABLE IF NOT EXISTS technical_analysis(stock_id text, target text, type text)")
    .catch((err) => console.error(err));
});


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

app.get("/values/all", async (req, res) => {
  const values = await pgClient.query("SELECT * from technical_analysis");

  res.send(values.rows);
});

app.get("/stocks", async (req, res) => {
  await redisClient.hgetall("values", (err, values) => {
    if (err) {
      console.log("error in fetching values" + err);
      return res.status(422).send("Stock connection lost");
    }

    let stocks = [];
    try {
      console.log(values)
      console.log(typeof values)
      stocks =  Object.values(values);
    } catch (err) {
      console.log("error looping through stock values" + err);
      return res.status(422).send("Stock connection lost");
    }
    return res.json(values);
  });
});

app.post("/admin/stocks/:stock_id/analysis",  (req, res) => {
  const { target, type } = req.body;

  console.log("The param " + req.params.stock_id)

  let stock = "" 
  const resolve =  redisClient.hget("values",req.params.stock_id, (err, value) => {
    if (err) { 
      console.log("error while retrieving stock value" + err);
      return res.status(422).send("Stock connection lost");
    }
    stock = value;
    console.log("the stock " + stock )
  });

  console.log("the resolve " + resolve);

  if(stock === ""){
    return res.status(422).send("Stock is empty");
  }

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

  pgClient.query("INSERT INTO technical_analysis(stock_id, target, type) VALUES($1, $2, $3)", [req.params.stock_id, target, type]);
  // redisPublisher.publish("insert", {target, type});

  return res.json(output);
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
