import express from "express";
import bodyParser from "body-parser";

const app = express();
app.use(bodyParser.json({ type: "*/*" })); // SNS sends weird Content-Types

// SNS WEBHOOK ENDPOINT
app.post("/webhooks/s3-upload", (req, res) => {
  const snsType = req.body.Type;

  // 1️⃣ STEP: Handle SNS subscription confirmation
  if (snsType === "SubscriptionConfirmation") {
    console.log("SNS Subscription Confirmation URL:", req.body.SubscribeURL);
    // Optional: auto-confirm
    fetch(req.body.SubscribeURL).then(() => {
      console.log("SNS subscription confirmed!");
    });
    return res.sendStatus(200);
  }

  // 2️⃣ STEP: Handle actual S3 upload event
  if (snsType === "Notification") {
    const message = JSON.parse(req.body.Message);
    const record = message.Records[0];

    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key);
    const size = record.s3.object.size;

    console.log("🎬 New video uploaded:");
    console.log("Bucket:", bucket);
    console.log("Key:", key);
    console.log("Size:", size);

    // TODO: Start processing (ECS / Lambda / Queue / DB update)
    // await startProcessingVideo(key);

    return res.sendStatus(200);
  }

  res.sendStatus(400);
});

// Start server
app.listen(4000, () => console.log("Webhook server running on port 4000"));
