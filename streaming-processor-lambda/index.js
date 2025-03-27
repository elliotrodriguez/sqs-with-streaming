const AWS = require('aws-sdk')
const dynamoDB = new AWS.DynamoDB.DocumentClient()

exports.handler = async(event) => {
    console.log(`Received event:`, JSON.stringify(event, null, 2))

    const recordPromises = event.Records.map(async(record) => {
        const messageBody = JSON.parse(record.body)

        const { correlationId, messageType, timestamp, content } = messageBody

        console.log(`Processing message ${messageType} for correlationId ${correlationId}`)

        try {
            // First, check if we already have a record for this correlationId
            const existingItem = await dynamoDB.get({
              TableName: 'streaming-responses',
              Key: { correlationId }
            }).promise();
            
            let item;
            
            if (!existingItem.Item) {
              // No existing record, create a new one
              item = {
                correlationId,
                streamStarted: messageType === 'STREAM_STARTED' ? timestamp : null,
                streamCompleted: messageType === 'STREAM_COMPLETED' ? timestamp : null,
                content: messageType === 'STREAM_COMPLETED' ? content : null
              };
            } else {
              // Update existing record
              item = existingItem.Item;
              
              if (messageType === 'STREAM_STARTED') {
                item.streamStarted = timestamp;
              } else if (messageType === 'STREAM_COMPLETED') {
                item.streamCompleted = timestamp;
                item.content = content;
              }
            }
            
            await dynamoDB.put({
              TableName: 'streaming-responses',
              Item: item
            }).promise();
            
            console.log(`Successfully processed ${messageType} for correlationId: ${correlationId}`);
            return { status: 'success', correlationId };
          } catch (err) {
            console.error(`Error processing message: ${err}`);
            throw err;
          }
        });
        
        return Promise.all(recordPromises);
    
}