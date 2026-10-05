import {z} from 'zod';
import {id,fail,identifier,schemas,anonymous,text} from './core.js';
export const ACTIVITY_CATEGORIES=['Sports','Coffee','Walk/Run','Gaming','Study','Food','Music','Friends/Chat','Dog Walk','Other'];
const inputSchema=z.object({title:text(3,70),description:text(0,240).default(''),category:z.enum(ACTIVITY_CATEGORIES),maxParticipants:z.number().int().min(2).max(30),minutes:z.union([z.literal(0),z.literal(30),z.literal(60),z.literal(120)]),clientId:z.string().regex(/^[a-zA-Z0-9_-]{8,100}$/)}).strict();
const active={status:{$in:['active','full']}};
export const activityMethods={
 async activityEvent(t,a,event='activities.changed'){
  const nearby=a.location?await t.all('locations',{location:this.geo(a.location),status:'trusted',expiresAt:{$gt:new Date(this.clock()*1000)}},{projection:{_id:1}}):[];
  await this.emit(t,event,[...new Set([...a.participantIds,...nearby.map(x=>x._id)])],{activityId:a._id});
 },
 async finishActivity(t,a,status='ended'){
  if(!['active','full'].includes(a.status))return;
  await t.update('activities',{_id:a._id},{$set:{status,endedAt:this.clock(),purgeAt:new Date((this.clock()+86400)*1000)},$unset:{location:''},$inc:{revision:1}});
  await this.activityEvent(t,a);
 },
 async removeActivityMember(t,a,uid){
  if(!a.participantIds.includes(uid))return;
  const count=a.participantCount-1;
  await t.update('activities',{_id:a._id},{$pull:{participantIds:uid},$inc:{participantCount:-1,revision:1},$set:{status:count>=a.maxParticipants?'full':'active'}});
  await this.activityEvent(t,a);
 },
 async reconcileActivities(t,onlyUser){
  const query={...active,...(onlyUser?{participantIds:onlyUser}:{})};
  for(const a of await t.all('activities',query)){
   const owner=await t.one('presence',{_id:a.creatorId,...this.active()});
   if(a.expiresAt<=this.clock()||!owner||!this.closeEnough(a,owner)){await this.finishActivity(t,a,a.expiresAt<=this.clock()?'expired':'ended');continue;}
   for(const uid of a.participantIds){
    if(uid===a.creatorId)continue;
    const member=await t.one('presence',{_id:uid,...this.active()});
    if(!member||!this.closeEnough(a,member)){
     const current=await t.one('activities',{_id:a._id});await this.removeActivityMember(t,current,uid);
    }
   }
   if(a.untilLeave){await t.update('activities',{_id:a._id},{$set:{expiresAt:Math.min(owner.expires,owner.checked+420)}});}
  }
 },
 async activityAccess(t,p,aid,{member=false,allowEnded=false}={}){
  identifier.parse(aid);const a=await t.one('activities',{_id:aid});
  if(!a)fail(404,'Activity unavailable.');
  const blocked=await this.blockedIds(t,p.id);
  if(a.participantIds.some(uid=>blocked.has(uid)))fail(404,'Activity unavailable.');
  if(member&&!a.participantIds.includes(p.id))fail(403,'Join this activity first.');
  if(allowEnded&&a.participantIds.includes(p.id)&&!['active','full'].includes(a.status))return a;
  if(!['active','full'].includes(a.status)||a.expiresAt<=this.clock())fail(410,'This activity has ended.');
  const loc=await this.requireLocation(t,p),owner=await t.one('presence',{_id:a.creatorId,...this.active()});
  if(!owner||!this.closeEnough(a,owner)||!this.closeEnough(a,loc))fail(404,'Activity unavailable nearby.');
  return a;
 },
 activityDTO(a,uid){return {id:a._id,creatorId:a.creatorId,title:a.title,description:a.description,category:a.category,maxParticipants:a.maxParticipants,participantCount:a.participantCount,createdAt:a.createdAt,expiresAt:a.expiresAt,untilLeave:a.untilLeave,status:a.status,revision:a.revision,joined:a.participantIds.includes(uid),isOwner:a.creatorId===uid};},
 async activitiesView(t,p){
  const loc=await this.locationFor(t,p);if(!loc)return [];
  const blocked=await this.blockedIds(t,p.id),result=[];
  for(const a of await t.all('activities',{...active,expiresAt:{$gt:this.clock()},location:this.geo(loc.location)},{sort:{createdAt:-1},limit:100})){
   if(a.participantIds.some(uid=>blocked.has(uid)))continue;
   const owner=await t.one('presence',{_id:a.creatorId,...this.active()});
   if(owner&&this.closeEnough(a,owner))result.push(this.activityDTO(a,p.id));
  }
  return result;
 },
 async activities(p){return this.execute(p,false,t=>this.activitiesView(t,p));},
 async createActivity(p,input){
  const d=inputSchema.parse(input);await this.limits.take('createActivity:'+p.id,10,60000);
  return this.execute(p,true,async(t,u)=>{
   const loc=await this.requireLocation(t,p),here=await this.live(t,u._id);
   if(here.tokenHash!==p.hash)fail(403,'Use the account session that started Live.');
   const previous=await t.one('activities',{creatorId:u._id,clientId:d.clientId});
   if(previous)return this.activityDTO(previous,u._id);
   // execute(write=true) acquires the shared MongoDB revision fence before this read.
   // Concurrent requests therefore retry against the committed activity, not a stale count.
   await this.reconcileActivities(t,u._id);
   if(await t.count('activities',{creatorId:u._id,...active,expiresAt:{$gt:this.clock()}})>=1)fail(409,'You already have something going. End your current activity before starting another.');
   const a={_id:id(),creatorId:u._id,clientId:d.clientId,category:d.category,title:d.title,description:d.description,location:loc.location,maxParticipants:d.maxParticipants,participantIds:[u._id],participantCount:1,createdAt:this.clock(),expiresAt:d.minutes===0?Math.min(here.expires,here.checked+420):this.clock()+d.minutes*60,untilLeave:d.minutes===0,status:'active',revision:1,messageSeq:0};
   await t.insert('activities',a);await this.activityEvent(t,a);return this.activityDTO(a,u._id);
  });
 },
 async activityDetails(p,aid){return this.execute(p,false,async(t,u)=>{
  const a=await this.activityAccess(t,p,aid,{allowEnded:true});
  const users=await t.all('users',{_id:{$in:a.participantIds},banned:false});
  return {...this.activityDTO(a,u._id),participants:users.map(anonymous)};
 });},
 async joinActivity(p,aid){
  await this.limits.take('joinActivity:'+p.id,30,60000);
  return this.execute(p,true,async(t,u)=>{
   const a=await this.activityAccess(t,p,aid),here=await this.live(t,u._id);
   if(here.tokenHash!==p.hash)fail(403,'Use your current Live session.');
   if(a.participantIds.includes(u._id))return this.activityDTO(a,u._id); // idempotent retry
   const result=await t.update('activities',{_id:a._id,...active,expiresAt:{$gt:this.clock()},participantIds:{$ne:u._id},$expr:{$lt:['$participantCount','$maxParticipants']}},{$addToSet:{participantIds:u._id},$inc:{participantCount:1,revision:1}});
   if(result.modifiedCount!==1)fail(409,'This activity is full.');
   const next=await t.one('activities',{_id:a._id});
   next.status=next.participantCount===next.maxParticipants?'full':'active';
   await t.update('activities',{_id:a._id},{$set:{status:next.status}});
   await this.activityEvent(t,next);return this.activityDTO(next,u._id);
  });
 },
 async leaveActivity(p,aid){identifier.parse(aid);return this.execute(p,true,async(t,u)=>{
  const a=await t.one('activities',{_id:aid});
  if(!a||!a.participantIds.includes(u._id))fail(404,'Activity unavailable.');
  if(['active','full'].includes(a.status)){if(a.creatorId===u._id)await this.finishActivity(t,a);else await this.removeActivityMember(t,a,u._id);}
  return {ok:true};
 });},
 async endActivity(p,aid){identifier.parse(aid);return this.execute(p,true,async(t,u)=>{
  const a=await t.one('activities',{_id:aid,creatorId:u._id});if(!a)fail(404,'Activity unavailable.');
  await this.finishActivity(t,a);return {ok:true};
 });},
 async activityMessages(p,aid,after='0'){
  if(typeof after!=='string'||!/^\d{1,12}$/.test(after))fail(422,'Invalid cursor.');
  return this.execute(p,false,async(t,u)=>{
   const a=await this.activityAccess(t,p,aid,{member:true,allowEnded:true});
   const blocked=await this.blockedIds(t,u._id),cursor=Number(after);
   const rows=await t.all('activityMessages',{activityId:aid,seq:{$gt:cursor},sender:{$nin:[...blocked]}},{sort:{seq:cursor?1:-1},limit:100});
   if(!cursor)rows.reverse();
   return {messages:rows.map(m=>({id:m._id,clientId:m.clientId,body:m.body,created:m.created,seq:m.seq,mine:m.sender===u._id,alias:m.alias})),hasMore:cursor>0&&rows.length===100,status:a.status};
  });
 },
 async sendActivityMessage(p,aid,input){
  const d=schemas.message.parse(input);await this.limits.take('activityMessage:'+p.id,60,60000);
  return this.execute(p,true,async(t,u)=>{
   const a=await this.activityAccess(t,p,aid,{member:true});await this.live(t,u._id);
   const previous=await t.one('activityMessages',{activityId:aid,sender:u._id,clientId:d.clientId});
   if(previous&&previous.body!==d.body)fail(409,'This message was already sent with different content.');
   if(previous)return {id:previous._id,clientId:previous.clientId,body:previous.body,created:previous.created,seq:previous.seq,mine:true,alias:previous.alias};
   const m={_id:id(),activityId:aid,sender:u._id,alias:u.alias,...d,seq:(a.messageSeq||0)+1,created:this.clock(),expiresAt:new Date((this.clock()+86400)*1000)};
   await t.insert('activityMessages',m);await t.update('activities',{_id:aid},{$inc:{messageSeq:1}});
   await this.emit(t,'activity.message',a.participantIds,{activityId:aid});
   return {id:m._id,clientId:m.clientId,body:m.body,created:m.created,seq:m.seq,mine:true,alias:m.alias};
  });
 },
};
