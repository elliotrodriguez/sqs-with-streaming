const express = require('express')
const AWS = require('aws-sdk')
const { v4: uuidv4 } = require('uuid')

// aws config 
AWS.config.update({
    region: 'us-east-1'
})
const sqs = new AWS.SQS({
    apiVersio: '2012-11-05'
})

const app = express()
const port = 3000
const QUEUE_URL = 'https://sqs.REGION_ID.amazonaws.com/ACCOUNT_ID/streaming-queue'

// helper function to send message
async function sendToSQS(correlationId, messageType, content) {
    const params = {
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify({
            correlationId,
            messageType,
            timestamp: new Date().toISOString(),
            content
        }),
        MessageAttributes: {
            "CorrelationId": {
                DataType: "String",
                StringValue: correlationId
            },
            "MessageType": {
                DataType: "String",
                StringValue: messageType
            }
        }
    };

    try {
        const data = await sqs.sendMessage(params).promise()
        console.log(`Message sent to SQS: ${data.MessageId}`)
        return data.MessageId
    } catch (err) {
        console.err(`Error! ${err}`)
        throw err
    }
}

app.get('/stream', async(req, res) => {
    // set headers for streaming
    res.setHeader('Content-Type', 'text/plain')
    res.setHeader('Transfer-Encoding', 'chunked')

    const correlationId = uuidv4()
    console.log(`Starting stream with correlationId: ${correlationId}`);

    // send first message, stream started
    try {
        await sendToSQS(correlationId, 'STREAM_STARTED', `Stream has started for correlationId ${correlationId}`)
    } catch(err) {
        res.status(500).end('Error sending message to queue')
        return;
    }

    //Simulating chunk sent
    const chunks = [
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n",
    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.\n",
    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.\n",
    "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\n",
    "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam.\n",
    "Eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo.\n",
    "Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt.\n",
    "Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem.\n",
    "Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur?\n",
    "Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?\n"
    ]

    let fullResponse = '';

    // send chunks with delay to simulate streaming
    const sendChunk = (index) => {
        if (index < chunks.length) {
            const chunk = chunks[index];
            res.write(chunk);
            fullResponse += chunk;


            // schedule the next chunk
            setTimeout(() => sendChunk(index + 1), 1000)
        } else {
            // finnished
            res.end()

            // send that its finished
            sendToSQS(correlationId, 'STREAM_COMPLETED', fullResponse)
                .then(() => {
                    console.log(`Stream completed for correlationId ${correlationId}`)
                })
                .catch((err) => {
                    console.error(`Error sending completion message ${err}`)
                })
        }
    }

    // go
    sendChunk(0)
})

app.listen(port, () => {
    console.log(`Listening on port ${port}`)
})