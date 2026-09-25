import 'dotenv/config';
import {connectDatabase,transaction} from './database.js';
import {Service} from './service.js';
import {z} from 'zod';
const {client,db}=await connectDatabase(process.env.MONGODB_URI||'mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true',process.env.MONGODB_DB||'herenow');
const service=new Service(client,db);const [command,...v]=process.argv.slice(2);
try{
 if(command==='reports')console.log(JSON.stringify(await db.collection('reports').find({status:'open',expiresAt:{$gt:new Date()}}).toArray(),null,2));
 else if(command==='resolve')await db.collection('reports').updateOne({_id:v[0]},{$set:{status:'resolved'}});
 else if(command==='ban')await service.ban(v[0]);
 else if(command==='cleanup')await service.cleanup();
 else if(command==='add-place'){
  const [key,name,description,lat,lon,radius,landmark]=v;
  const p=z.object({_id:z.string().min(1).max(80),name:z.string().min(1).max(100),description:z.string().max(150),lat:z.number().finite().min(-90).max(90),lon:z.number().finite().min(-180).max(180),radius:z.number().int().min(50).max(2000),landmark:z.string().min(1).max(200)}).parse({_id:key,name,description,lat:Number(lat),lon:Number(lon),radius:Number(radius),landmark});
  await transaction(client,db,true,async t=>{await t.insert('places',{...p,location:{type:'Point',coordinates:[p.lon,p.lat]}});await service.emit(t,'places.changed',null);});
 }else throw new Error('Usage: npm run manage -- reports | resolve REPORT_ID | ban USER_ID | cleanup | add-place ID NAME DESCRIPTION LAT LON RADIUS_METRES LANDMARK');
}finally{await client.close();}
