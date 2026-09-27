import {z} from 'zod';
import {digest,id} from './core.js';
// Structured stages contain IDs/codes only: never credentials, tokens or message text.
export function pushLog(stage,fields={}){console.info(JSON.stringify({component:'herenow.push',stage,...fields}));}

const registration=z.object({installationId:z.string().regex(/^[a-zA-Z0-9_-]{8,100}$/),token:z.string().min(20).max(4096),preview:z.boolean().default(false)});
export async function registerPush(service,p,input){
 const data=registration.parse(input);
 const result=await service.execute(p,true,async(t,u)=>{
  // A refreshed FCM token or account switch replaces the old installation binding.
  await t.remove('pushDevices',{$or:[{installationId:data.installationId},{token:data.token}]});
  const auth=await t.one('tokens',{_id:p.hash});
  await t.insert('pushDevices',{_id:digest(data.installationId),installationId:data.installationId,userId:u._id,tokenHash:p.hash,token:data.token,preview:data.preview,expiresAt:auth.expiresAt});
  return {ok:true};
 });
 pushLog('device_registered',{recipientUserId:p.id,deviceId:digest(data.installationId)});return result;
}
export async function removePush(service,p,input){
 const {installationId}=registration.pick({installationId:true}).parse(input);
 return service.execute(p,true,async(t,u)=>{await t.remove('pushDevices',{installationId,userId:u._id,tokenHash:p.hash});return {ok:true};});
}
// Called inside the same transaction as the message insert. HTTP retries and WS
// fallback return the original message before reaching this enqueue operation.
export async function enqueuePush(t,c,message,now){
 const recipientId=c.a===message.sender?c.b:c.a;
 const devices=await t.all('pushDevices',{userId:recipientId,expiresAt:{$gt:new Date(now*1000)}});
 // Transaction attempts may log more than once; messageId is the correlation key.
 pushLog('token_lookup',{recipientUserId:recipientId,messageId:message._id,conversationId:c._id,deviceCount:devices.length,transactionPending:true});
 for(const device of devices)await t.insert('pushJobs',{_id:message._id+':'+device._id,deviceId:device._id,tokenHash:device.tokenHash,recipientId,conversationId:c._id,messageId:message._id,attempts:0,status:'pending',nextAt:new Date(),expiresAt:new Date(Date.now()+300000)});
}
export async function createFcmSender(){
 if(process.env.PUSH_ENABLED!=='true'){pushLog('disabled',{reason:'PUSH_ENABLED is not true'});return null;}
 const {initializeApp,cert,applicationDefault,getApps}=await import('firebase-admin/app');
 const {getMessaging}=await import('firebase-admin/messaging');
 const credential=process.env.FIREBASE_SERVICE_ACCOUNT_JSON?cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON)):applicationDefault();
 const app=getApps().find(a=>a.name==='herenow-push')||initializeApp({credential},'herenow-push');
 pushLog('configured',{projectId:app.options.projectId||app.options.credential?.projectId||process.env.GOOGLE_CLOUD_PROJECT||'application-default'});
 return payload=>getMessaging(app).send(payload);
}
export function startPushWorker(service,send,{intervalMs=1000}={}){
 const db=service.db;let busy=false,stopped=false,current=Promise.resolve();
 async function deliver(job){
  const now=new Date(),device=await db.collection('pushDevices').findOne({_id:job.deviceId,userId:job.recipientId,tokenHash:job.tokenHash,expiresAt:{$gt:now}});
  const fields={recipientUserId:job.recipientId,deviceId:job.deviceId,messageId:job.messageId,conversationId:job.conversationId,attempt:job.attempts};
  if(!device){pushLog('skipped',{...fields,reason:'device_registration_missing'});return {outcome:'skipped',reason:'device_registration_missing'};}
  let payload;
  // Recheck authorization, blocks, live expiry and message visibility before delivery.
  try{payload=await service.execute({id:job.recipientId,hash:job.tokenHash},false,async(t,u)=>{
   const c=await service.getChat(t,job.conversationId,u._id);
   const m=await t.one('messages',{_id:job.messageId,conversationId:c._id,read:{$ne:true}});
   if(!m||m.sender===u._id)return null;
   const sender=await t.one('users',{_id:m.sender});if(!sender)return null;
   return {token:device.token,notification:{title:sender.alias+' · herenow',body:device.preview?(Array.from(m.body).slice(0,160).join('')+(Array.from(m.body).length>160?'…':'')):'New message. Open herenow to read it.'},data:{type:'chat.message',conversationId:c._id,messageId:m._id,senderId:m.sender,recipientId:u._id},android:{priority:'high',ttl:Math.max(0,job.expiresAt.getTime()-Date.now()),collapseKey:c._id,notification:{channelId:'messages',icon:'herenow_notification',color:'#6D28D9',tag:m._id,eventTimestamp:new Date(m.created*1000),visibility:'private',sound:'default'}}};
  });}catch(e){if([401,403,404,409].includes(e.status)){pushLog('skipped',{...fields,reason:'session_or_conversation_ineligible'});return {outcome:'skipped',reason:'session_or_conversation_ineligible'};}throw e;}
  if(!payload){pushLog('skipped',{...fields,reason:'message_read_or_unavailable'});return {outcome:'skipped',reason:'message_read_or_unavailable'};}
  try{pushLog('fcm_attempt',fields);const providerMessageId=await send(payload);pushLog('fcm_accepted',{...fields,providerMessageId});return {outcome:'accepted',providerMessageId,acceptedAt:new Date()};}catch(e){e.deviceToken=device.token;throw e;}
 }
 async function drain(){
  if(busy||stopped||!send)return;busy=true;
  try{for(let i=0;i<20&&!stopped;i++){
   const lease=id(),now=new Date();
   const job=await db.collection('pushJobs').findOneAndUpdate({status:{$in:['pending','sending']},nextAt:{$lte:now},expiresAt:{$gt:now}},{$set:{status:'sending',lease,nextAt:new Date(Date.now()+60000)},$inc:{attempts:1}},{sort:{nextAt:1},returnDocument:'after'});
   if(!job)break;
   try{const result=await deliver(job);await db.collection('pushJobs').updateOne({_id:job._id,lease},{$set:{status:'done',...result},$unset:{lease:'',errorCode:''}});}
   catch(e){
    pushLog('fcm_failed',{recipientUserId:job.recipientId,messageId:job.messageId,deviceId:job.deviceId,attempt:job.attempts,code:safePushCode(e.code)});
    if(['messaging/registration-token-not-registered','messaging/invalid-registration-token'].includes(e.code))await db.collection('pushDevices').deleteOne({_id:job.deviceId,tokenHash:job.tokenHash,token:e.deviceToken});
    const retry=job.attempts<5&&!['messaging/registration-token-not-registered','messaging/invalid-registration-token','messaging/invalid-argument'].includes(e.code);
    await db.collection('pushJobs').updateOne({_id:job._id,lease},{$set:{status:retry?'pending':'failed',nextAt:new Date(Date.now()+Math.min(60000,1000*2**job.attempts)),errorCode:safePushCode(e.code)},$unset:{lease:''}});
   }
  }}finally{busy=false;}
 }
 const run=()=>{if(busy||stopped)return;current=drain().catch(e=>console.error('Push worker unavailable:',e.name));};
 const timer=setInterval(run,intervalMs);timer.unref();run();
 return {drain,async close(){stopped=true;clearInterval(timer);await current;}};
}

export function safePushCode(value){return typeof value==='string'&&/^[a-zA-Z0-9_/-]{1,100}$/.test(value)?value:'delivery-unavailable';}
export async function pushStatus(service,p){
 return service.execute(p,false,async(t,u)=>{
  const devices=await t.all('pushDevices',{userId:u._id,tokenHash:p.hash,expiresAt:{$gt:new Date()}});
  const jobs=await service.db.collection('pushJobs').find({recipientId:u._id,tokenHash:p.hash},{projection:{tokenHash:0,lease:0}}).sort({expiresAt:-1}).limit(10).toArray();
  return {enabled:!!service.pushConfigured,registeredDevices:devices.map(d=>({deviceId:d._id,preview:d.preview})),recent:jobs.map(j=>({messageId:j.messageId,conversationId:j.conversationId,status:j.status,outcome:j.outcome||null,reason:j.reason||null,attempts:j.attempts,providerMessageId:j.providerMessageId||null,errorCode:j.errorCode||null})),note:'FCM acceptance does not prove Android display. Check device logs and notification panel.'};
 });
}
