import 'dotenv/config';
import {createApplication} from './app.js';
let runtime;
try {
if(process.env.NODE_ENV==='production'&&!process.env.MONGODB_URI)throw new Error('Missing backend configuration');
runtime=await createApplication({mongoUri:process.env.MONGODB_URI||'mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true',dbName:process.env.MONGODB_DB||'herenow',demoMode:process.env.DEMO_MODE==='true',origins:(process.env.WEB_ORIGINS||'').split(',').map(x=>x.trim()).filter(Boolean)});
await runtime.listen(Number(process.env.PORT||8000));
console.log(`HereNow Node API and WebSocket /ws listening on port ${process.env.PORT||8000}`);
} catch { console.error('Backend startup failed. Check database, Firebase and proxy configuration in backend secrets.');process.exit(1); }
let stopping=false;async function shutdown(){if(stopping)return;stopping=true;await runtime.close();process.exit(0);}
process.on('SIGINT',shutdown);process.on('SIGTERM',shutdown);
