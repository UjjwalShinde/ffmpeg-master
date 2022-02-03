var AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const M3U8FileParser = require("m3u8-file-parser");
const fs = require("fs");
const readline = require("readline");
AWS.config.update({ region: "ap-south-1" });
const { Consumer } = require("sqs-consumer");
var sqs = new AWS.SQS({ apiVersion: "2012-11-05" });
var lambda = new AWS.Lambda({ apiVersion: "2015-03-31" });

const { getVideoDurationInSeconds } = require("get-video-duration");
// const { ConfigurationServicePlaceholders } = require("aws-sdk/lib/config_service_placeholders");

var sendJobId = new Set();
var receivedJobId = new Set();
var sentVideosNames = [];
var murgedVideos=new Set();

const cancelJob = async (reason, i) => {
  console.log("cancel job trigger");
  var params = {
    jobId: process.env.AWS_BATCH_JOB_ID,
    reason: reason,
  };
  const response = await batch
    .cancelJob(params)
    .promise()
    .catch(async () => {
      await cancelJob(reason, 1);
    });
  console.log(response);
};

const deleteSQSqueue = async (url) => {
  console.log("deleting queque");

  if (url) {
    console.log("url found");

    var params = {
      QueueUrl: url /* required */,
    };
    const response = await sqs
      .deleteQueue(params)
      .promise()
      .catch((e) => {
        console.log(e,"error while deleting queue");
      });
    console.log(response);
  }
};

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

const sendSuccessResponse = async () => {
  var params = {
    FunctionName: "lambda_function_to_call_api" /* required */,
    Payload: JSON.stringify({
      inputVideoName: process.env.inputVideoName,
      outputVideoName: process.env.outputVideoName,
    }),
  };
  await lambda.invoke(params).promise();
};

const submitJob = (initialTime, endTime, i = 0, url, attempt) => {
  console.log(initialTime, " ", endTime);
  const name = uuidv4();
  sentVideosNames.push(i + process.env.outputVideoName); // kepp track of output video names of child batch jobs

  var params = {
    jobDefinition:
      "arn:aws:batch:ap-south-1:907610416451:job-definition/slave-ffmpeg-definition:1",
    jobName: name.toString(),
    jobQueue:
      "arn:aws:batch:ap-south-1:907610416451:job-queue/slave-ffmpeg-jobqueue",
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
    if (data) {
      console.log("batch resonse submit", data);
      sendJobId.add(data.jobId);
    }
    if (err) {
      console.log(err, "error while submitting job");
      if (attempt < 1) submitJob(initialTime, endTime, i, url, attempt + 1); //reattempt to submit job
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
  var i = 0;
  var params = {
    QueueName: uuidv4() /* required */,
  };
  console.log("before queue");

  const data = await sqs.createQueue(params).promise();
  console.log(data);

  const url = data.QueueUrl;
  console.log(url);
  const app = Consumer.create({
    queueUrl: url,
    handleMessage: async (message) => {
      if (i == 0) {
        console.log("sendjobid", Array.from(sendJobId.values()));
        i++;
      }
      console.log("received message", message);
      console.log("receivedjobid", Array.from(receivedJobId.values()));
      if (sendJobId.has(message.Body)) {
        console.log("message found in sendJobID");
        receivedJobId.add(message.Body);
        if (receivedJobId.size == sendJobId.size) {
          console.log("size of received and sent id array are equal");
          for (index = 0; index < sentVideosNames.length; index++) {
            if(! murgedVideos.has(sentVideosNames[index])){ // check if video is merged already.

            path = process.env.S3_MOUNT_DIRECTORY_OUTPUT + "/" + sentVideosNames[index];
            secondpath = process.env.S3_MOUNT_DIRECTORY_OUTPUT +"/" + process.env.outputVideoName;
            await mergeviddeos(
              path,
              secondpath,
              index,
              sentVideosNames.length - 1
            );
            murgedVideos.add(sentVideosNames[index]); //to track merged videos.
          
          }
          }
          await sendSuccessResponse();
          app.stop();
          await deleteSQSqueue(url);
        }
      } else {
        console.log("wrong message from slave");
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
  var videoPath =
    process.env.S3_MOUNT_DIRECTORY_INPUT + "/" + process.env.inputVideoName;

  const promise = videoDuration(videoPath);
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
        // for submitting last batch job
        submitJob(
          formatTime(initialTime),
          formatTime(endTime),
          totalChildBatch,
          url,
          0
        );
        totalChildBatch++;
      }
      console.log("sendjobid", Array.from(sendJobId.values()));
    })
    .catch(async (e) => {
      //failed to get video duration
      if (app) {
        app.stop();
      }
      await deleteSQSqueue(url);

      await cancelJob("video duration not found", 0);
    });
};
// programm start here **************************

init();

// flow of program :
//     create queue
//  -> get video duration of video.
//  -> submit multiple jobs in 10 mins interval.
//  -> get response as id of submitted jobs from child batch jobs.
//  -> check if all response came .
//  -> merge all m3u8 output files to single m3u8 files.
//  -> delete queque and trigger success lambda function.
