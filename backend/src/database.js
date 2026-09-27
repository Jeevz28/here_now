import {observeDatabase,measured} from './performance.js';
import { MongoClient } from 'mongodb';
export async function connectDatabase(uri, name) {
  const client=new MongoClient(uri,{serverSelectionTimeoutMS:10000,monitorCommands:process.env.PERF_METRICS==='1'});
  observeDatabase(client);
  await client.connect();
  const hello=await client.db('admin').command({hello:1});
  if(!hello.setName && hello.msg!=='isdbgrid'){await client.close();throw new Error('HereNow needs a MongoDB replica set for transactions and live change streams. Use the included Docker Compose setup or MongoDB Atlas.');}
  const db=client.db(name);
  for(const collection of ['users','tokens','places','presence','conversations','messages','activities','blocks','reports','events','control','locations','locationRisks','pushDevices','pushJobs']) {
    try{await db.createCollection(collection);}catch(e){if(e.code!==48)throw e;}
  }
  await db.collection('pushDevices').createIndex({token:1},{unique:true});
  await db.collection('pushDevices').createIndex({userId:1});
  await db.collection('pushDevices').createIndex({expiresAt:1},{expireAfterSeconds:0});
  await db.collection('pushJobs').createIndex({status:1,nextAt:1});
  await db.collection('pushJobs').createIndex({expiresAt:1},{expireAfterSeconds:0});
  await db.collection('users').createIndex({email:1},{unique:true});
  await db.collection('tokens').createIndex({expiresAt:1},{expireAfterSeconds:0});
  await db.collection('tokens').createIndex({userId:1});
  await db.collection('presence').createIndex({place_id:1});
  await db.collection('presence').createIndex({location:'2dsphere'});
  await db.collection('places').createIndex({location:'2dsphere'});
  await db.collection('locations').createIndex({expiresAt:1},{expireAfterSeconds:0});
  await db.collection('locationRisks').createIndex({expiresAt:1},{expireAfterSeconds:0});
  // Presence has no TTL index: application cleanup must also remove chats/memberships.
  await db.collection('conversations').createIndex({a:1,b:1},{unique:true});
  await db.collection('conversations').createIndex({b:1});
  await db.collection('messages').createIndex({conversationId:1,sender:1,clientId:1},{unique:true});
  await db.collection('messages').createIndex({conversationId:1,created:1,_id:1});
  await db.collection('messages').createIndex({conversationId:1,sender:1,read:1});
  await db.collection('messages').createIndex({conversationId:1,read:1,sender:1});
  await db.collection('messages').createIndex({sender:1,created:1});
  await db.collection('activities').createIndex({place_id:1});
  await db.collection('blocks').createIndex({owner:1,target:1},{unique:true});
  await db.collection('reports').createIndex({expiresAt:1},{expireAfterSeconds:0});
  await db.collection('events').createIndex({expiresAt:1},{expireAfterSeconds:0});
  await db.collection('control').updateOne({_id:'consistency'},{$setOnInsert:{revision:0}},{upsert:true});
  for(const p of [
    {_id:'kelvingrove',name:'Kelvingrove Park',description:'Open lawns, new connections.',lat:55.8685,lon:-4.2840,radius:700,landmark:'Main park entrance on Kelvin Way'},
    {_id:'glasgow-green',name:'Glasgow Green',description:'A little fresh air. A new circle.',lat:55.851,lon:-4.238,radius:850,landmark:'Outside the People’s Palace entrance'},
    {_id:'george-square',name:'George Square',description:'A city break, together.',lat:55.8612,lon:-4.2502,radius:350,landmark:'Main entrance to the City Chambers'}
  ])await db.collection('places').updateOne({_id:p._id},{$setOnInsert:p},{upsert:true});
  // Idempotent migration of existing curated places.
  for(const p of await db.collection('places').find({}).toArray()) {
    await db.collection('places').updateOne({_id:p._id},{$set:{location:{type:'Point',coordinates:[p.lon,p.lat]}}});
  }
  return {client,db};
}
// A database-backed revision fence serializes cross-document writes for this MVP.
// MongoDB retries conflicting transactions; reads use a consistent snapshot.
export async function transaction(client, db, write, callback) {
  const session=client.startSession();
  try{return await measured(write?'transaction.write':'transaction.read',()=>session.withTransaction(async()=>{
    if(write)await db.collection('control').updateOne({_id:'consistency'},{$inc:{revision:1}},{session});
    const t={
      one:(name,filter,options={})=>db.collection(name).findOne(filter,{...options,session}),
      all:(name,filter={},options={})=>db.collection(name).find(filter,{...options,session}).toArray(),
      aggregate:(name,pipeline)=>db.collection(name).aggregate(pipeline,{session}).toArray(),
      count:(name,filter)=>db.collection(name).countDocuments(filter,{session}),
      insert:(name,doc)=>db.collection(name).insertOne(doc,{session}),
      update:(name,filter,update,options={})=>db.collection(name).updateOne(filter,update,{...options,session}),
      updateMany:(name,filter,update)=>db.collection(name).updateMany(filter,update,{session}),
      remove:(name,filter)=>db.collection(name).deleteMany(filter,{session}),
    };
    return callback(t);
  },{readConcern:{level:'snapshot'},writeConcern:{w:'majority'},maxCommitTimeMS:10000}));}
  finally{await session.endSession();}
}
