import 'dotenv/config';
import {connectDatabase} from './database.js';
import {Service} from './service.js';
const {client,db}=await connectDatabase(process.env.MONGODB_URI||'mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true',process.env.MONGODB_DB||'herenow');
const service=new Service(client,db);const [command,...v]=process.argv.slice(2);
try{
 if(command==='reports')console.log(JSON.stringify(await db.collection('reports').find({status:'open',expiresAt:{$gt:new Date()}}).toArray(),null,2));
 else if(command==='resolve')await db.collection('reports').updateOne({_id:v[0]},{$set:{status:'resolved'}});
 else if(command==='ban')await service.ban(v[0]);
 else if(command==='cleanup')await service.cleanup();
 else throw new Error('Usage: npm run manage -- reports | resolve REPORT_ID | ban USER_ID | cleanup');
}finally{await client.close();}
