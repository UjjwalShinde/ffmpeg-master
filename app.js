var AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const M3U8FileParser = require("m3u8-file-parser");
const fs = require("fs");
const readline = require("readline");
AWS.config.update({ region: "ap-south-1" });
const { Consumer } = require("sqs-consumer");
var sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

const { getVideoDurationInSeconds } = require("get-video-duration");
// const { ConfigurationServicePlaceholders } = require("aws-sdk/lib/config_service_placeholders");
const { Console } = require("console");

const cancelJob = async (reason, i) => {
  var params = {
    jobId: process.env.AWS_BATCH_JOB_ID,
    reason: reason,
  };
  const response = await batch.cancelJob(params).promise().catch( async ()=>{
    await cancelJob(reason, 1);
  })
    console.log(response);
};

const deleteSQSqueue = async (url) => {
  if (url) {
    var params = {
      QueueUrl: url /* required */,
    };
   const response = await sqs.deleteQueue(params).promise().catch(e=>{console.log(e)});
   console.log(response);
  }
};

var sendJobId = new Set();
var receivedJobId = new Set();
var sentVideosNames = [];
const mergeviddeos = async (path, secondfile, i, n) => {
  const content = fs.readFileSync(path, { encoding: "utf-8" });

  const reader = new M3U8FileParser();
  const stream = fs.createReadStream(path, { encoding: "utf-8" });
  const interface = readline.createInterface({ input: stream });

  interface.on("line", async (line) => {
    reader.read(line);
    if (i == 0 && !line.includes("#EXT-X-ENDLIST")) {
      fs.appendFileSync(secondfile, line + "\r\n");
    } else {
      if (
        !(
          line.includes("#EXTM3U") ||
          line.includes("#EXT-X-VERSION") ||
          line.includes("#EXT-X-TARGETDURATION") ||
          line.includes("#EXT-X-MEDIA-SEQUENCE") ||
          line.includes("#EXT-X-PLAYLIST-TYPE") ||
          line.includes("#EXT-X-ENDLIST")
        )
      ) {
        fs.appendFileSync(secondfile, line + "\r\n");
      }
    }
    if (line.includes("#EXT-X-ENDLIST") && i == n) {
      fs.appendFileSync(secondfile, line + "\r\n");
    }
    if (line.includes("#EXT-X-ENDLIST") && i < n) {
      fs.appendFileSync(secondfile, "#EXT-X-DISCONTINUITY" + "\r\n");
    }
  }); // Read line by line
  interface.on(
    "close",
    () => {}
    // reader.getResult()
  ); // Get result after file ends
};

const submitJob = (initialTime, endTime, i = 0, url,attempt) => {
  console.log(initialTime, " ", endTime);
  const name = uuidv4();
  sentVideosNames.push(i + process.env.outputVideoName);

  var params = {
    jobDefinition:
      "arn:aws:batch:ap-south-1:907610416451:job-definition/toCheckParallelProcessing:1",
    jobName: name.toString(),
    jobQueue:
      "arn:aws:batch:ap-south-1:907610416451:job-queue/helloworldjobqueue",
    containerOverrides: {
      environment: [
        {
          name: "initialTime",
          value: initialTime,
        },
        {
          name: "endTime",
          value: endTime,
        },
        {
          name: "inputVideoName",
          value: process.env.inputVideoName,
        },
        {
          name: "outputVideoName",
          value: i + process.env.outputVideoName,
        },
        {
          name: "url", //queue url
          value: url,
        },

        /* more items */
      ],
    },
  };
  var batch = new AWS.Batch();
  batch.submitJob(params, function (err, data) {
    console.log(data);
    if(err){
      console.log(err,"error while submitting job")
      if(attempt<1)
      submitJob(initialTime, endTime, i , url,attempt+1);
    }
  });
};

const formatTime = (seconds) => {
  //convert seconds to HH/MM/SS form
  var measuredTime = new Date(null);
  measuredTime.setSeconds(seconds);
  var MHSTime = measuredTime.toISOString().substr(11, 8);
  return MHSTime;
};

const videoDuration = async (path) => {
  const duration = await getVideoDurationInSeconds(path);
  return duration;
};

const init = async () => {
  var params = {
    QueueName: uuidv4() /* required */,
  };
  const data = await sqs.createQueue(params).promise();
  console.log(data);

  const url = data.QueueUrl;
  console.log(url);
  const app = Consumer.create({
    queueUrl: url,
    handleMessage: async (message) => {
      if (sendJobId.has(message.Body)) {
        receivedJobId.add(message.Body);
        if (receivedJobId.size == sendJobId.size) {
          for (k = 0; k < sentVideosNames.length; k++) {
            path = "./" + sentVideosNames[k];
            secondpath = "./" + process.env.outputVideoName;
            mergeviddeos(path, secondpath, k, sentVideosNames.length - 1);
          }
          app.stop();
          await deleteSQSqueue(url);
        }
      }
    },
  });

  app.on("error", (err) => {
    console.error(err.message);
  });

  app.on("processing_error", (err) => {
    console.error(err.message);
  });

  app.start();

  const promise = videoDuration("./sample.mkv");
  promise
    .then((duration) => {
      var initialTime = 0;
      var endTime = 600; //to process videos in 10 min duration.
      var totalChildBatch = 0;

      do {
        const response = submitJob(
          formatTime(initialTime),
          formatTime(endTime),
          totalChildBatch,
          url,
          0
        );

        initialTime = initialTime + 600; //to go for next 10 mins.
        endTime = endTime + 600;
        totalChildBatch++;
      } while (endTime < duration);
      if (initialTime < duration) {
        submitJob(
          formatTime(initialTime),
          formatTime(endTime),
          totalChildBatch,
          url,
          0
        );
        totalChildBatch++;
      }
    })
    .catch((e) => {
      if (app) {
        app.stop();
      }
      await deleteSQSqueue(url);

     await  cancelJob("video duration not found", 0);
    });
};
// programm start here **************************

init();
