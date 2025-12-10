import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';

dotenv.config();

const app = express();
const port = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

app.get('/health', (_, res) => {
  res.json({ status: 'ok', message: 'Server skeleton is running.' });
});

app.listen(port, () => {
  console.log(`API server ready at http://localhost:${port}`);
});
