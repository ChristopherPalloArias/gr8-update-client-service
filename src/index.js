import express from 'express';
import AWS from 'aws-sdk';
import cors from 'cors';
import swaggerUi from 'swagger-ui-express';
import swaggerJsDoc from 'swagger-jsdoc';
import amqp from 'amqplib';

// AWS region and Lambda function configuration
const region = "us-east-2";
const lambdaFunctionName = "fetchSecretsFunction_gr8";

// Function to invoke Lambda and fetch secrets
async function getSecretFromLambda() {
  const lambda = new AWS.Lambda({ region: region });
  const params = {
    FunctionName: lambdaFunctionName,
  };

  try {
    const response = await lambda.invoke(params).promise();
    const payload = JSON.parse(response.Payload);
    if (payload.errorMessage) {
      throw new Error(payload.errorMessage);
    }
    const body = JSON.parse(payload.body);
    return JSON.parse(body.secret);
  } catch (error) {
    console.error('Error invoking Lambda function:', error);
    throw error;
  }
}

// Function to start the service
async function startService() {
  let secrets;
  try {
    secrets = await getSecretFromLambda();
  } catch (error) {
    console.error(`Error starting service: ${error}`);
    return;
  }

  const app = express();
  const port = 8096;

  app.use(cors());
  app.use(express.json());

  // Configure AWS DynamoDB
  AWS.config.update({
    region: region,
    accessKeyId: secrets.AWS_ACCESS_KEY_ID,
    secretAccessKey: secrets.AWS_SECRET_ACCESS_KEY,
  });

  const dynamoDB = new AWS.DynamoDB.DocumentClient();

  // Swagger setup
  const swaggerOptions = {
    swaggerDefinition: {
      openapi: '3.0.0',
      info: {
        title: 'Update Client Service API',
        version: '1.0.0',
        description: 'API for updating clients',
      },
    },
    apis: ['./src/index.js'],
  };

  const swaggerDocs = swaggerJsDoc(swaggerOptions);
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocs));

  // Connect to RabbitMQ
  let channel;
  async function connectRabbitMQ() {
    try {
      const connection = await amqp.connect('amqp://3.136.72.14:5672/');
      channel = await connection.createChannel();
      await channel.assertQueue('client-events', { durable: true });
      console.log('Connected to RabbitMQ');
    } catch (error) {
      console.error('Error connecting to RabbitMQ:', error);
    }
  }

  // Publish event to RabbitMQ
  const publishEvent = async (eventType, data) => {
    const event = { eventType, data };
    try {
      if (channel) {
        channel.sendToQueue('client-events', Buffer.from(JSON.stringify(event)), { persistent: true });
        console.log('Event published to RabbitMQ:', event);
      } else {
        console.error('Channel is not initialized');
      }
    } catch (error) {
      console.error('Error publishing event to RabbitMQ:', error);
    }
  };

  await connectRabbitMQ();

  /**
   * @swagger
   * /clients/{ci}:
   *   put:
   *     summary: Update an existing client
   *     description: Update an existing client by CI
   *     parameters:
   *       - in: path
   *         name: ci
   *         required: true
   *         description: CI of the client to update
   *         schema:
   *           type: string
   *     requestBody:
   *       description: Client object that needs to be updated
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             properties:
   *               firstName:
   *                 type: string
   *               lastName:
   *                 type: string
   *               phone:
   *                 type: string
   *               address:
   *                 type: string
   *     responses:
   *       200:
   *         description: Client updated
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 message:
   *                   type: string
   *       404:
   *         description: Client not found
   *       500:
   *         description: Error updating client
   */
  app.put('/clients/:ci', async (req, res) => {
    const { ci } = req.params;
    const { firstName, lastName, phone, address } = req.body;

    const params = {
      TableName: 'ClientsUpdate_gr8',
      Key: { ci },
      UpdateExpression: 'set firstName = :firstName, lastName = :lastName, phone = :phone, address = :address',
      ExpressionAttributeValues: {
        ':firstName': firstName,
        ':lastName': lastName,
        ':phone': phone,
        ':address': address
      },
      ReturnValues: 'UPDATED_NEW'
    };

    try {
      const result = await dynamoDB.update(params).promise();
      const updatedClient = {
        ci,
        firstName,
        lastName,
        phone,
        address,
      };

      // Publish client updated event to RabbitMQ
      await publishEvent('ClientUpdated', updatedClient);

      res.send({ message: 'Client updated', result });
    } catch (error) {
      console.error('Error updating client:', error);
      res.status(500).send({ message: 'Error updating client', error });
    }
  });

  app.get('/', (req, res) => {
    res.send('Update Client Service Running');
  });

  app.listen(port, () => {
    console.log(`Update Client service listening at http://localhost:${port}`);
  });
}

startService();
