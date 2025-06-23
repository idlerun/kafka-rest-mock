#!/usr/bin/env node

import express, { Request, Response } from "express";
import yaml from "js-yaml";

const app = express();
const port = 18081;

app.use(
  express.json({
    type: ["application/json", "application/vnd.kafka.binary.v2+json"],
  })
);

interface KafkaRecord {
  key: string; // base64 encoded
  value: string; // base64 encoded JSON string of a generic object
}

interface KafkaRequestBody {
  records: KafkaRecord[];
}

app.post("/topics/:topicName", (req: Request, res: Response) => {
  const requestBody = req.body as KafkaRequestBody;

  if (requestBody.records && Array.isArray(requestBody.records)) {
    for (const record of requestBody.records) {
      try {
        const decodedKeyString = Buffer.from(record.key, "base64").toString(
          "utf-8"
        );
        const decodedValueString = Buffer.from(record.value, "base64").toString(
          "utf-8"
        );
        const recordValue = JSON.parse(decodedValueString) as Record<
          string,
          unknown
        >;
        const data = {
          key: decodedKeyString,
          value: recordValue,
        };

        console.log(yaml.dump(data));
        console.log("--------------------------------\n");
      } catch (e) {
        console.error("Error processing record:", e);
        console.log("Problematic record:", JSON.stringify(record, null, 2));
      }
    }
  } else {
    console.log("Request body does not contain a records array. Full body:");
    console.log(JSON.stringify(req.body, null, 2));
  }

  res.status(200).send("Message received");
});

app.listen(port, () => {
  console.log(`Server listening on port ${port}\n\n\n`);
});
