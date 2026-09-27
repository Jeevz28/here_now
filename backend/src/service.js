import {registerPush,removePush,enqueuePush,pushLog} from './push.js';
import { randomBytes } from 'node:crypto';
import { transaction } from './database.js';
import { id,digest,fail,schemas,hashPassword,verifyPassword,age,own,anonymous,distance } from './core.js';
// Five-minute updates plus two minutes of OS/network scheduling grace.
export const LOCATION_LEASE_SECONDS=420;
export class Service {
  constructor(client,db,{demoMode=false,clock=()=>Date.now()/1000}={}){this.client=client;this.db=db;this.demoMode=demoMode;this.clock=clock;}
  registerPush(p,input){return registerPush(this,p,input);}
  removePush(p,input){return removePush(this,p,input);}
  active(){return {expires:{$gt:this.clock()},checked:{$gt:this.clock()-LOCATION_LEASE_SECONDS},location:{$exists:true}};}
  async emit(t,event,users,extra={}){
    await t.insert('events',{_id:id(),event,scope:users===null?'all':'users',users:users||[],...extra,createdAt:new Date(),expiresAt:new Date(Date.now()+600000)});
  }
  async circleEvent(t,places){
    const people=await t.all('presence',{place_id:{$in:[...new Set(places)]},...this.active()});
    if(people.length)await this.emit(t,'circle.changed',people.map(p=>p._id));
    await this.emit(t,'places.changed',null);
  }
  async assertPrincipal(t,principal){
    const token=await t.one('tokens',{_id:principal.hash,userId:principal.id,expiresAt:{$gt:new Date(this.clock()*1000)}});
    const user=await t.one('users',{_id:principal.id,banned:false});
    if(!token||!user)fail(401,'Session expired. Please sign in again.');
    return user;
  }
  async authenticate(token){
    if(typeof token!=='string'||token.length<20||token.length>200)fail(401,'Please sign in.');
    return this.authenticateHash(digest(token));
  }
  async authenticateHash(hash){
    const token=await this.db.collection('tokens').findOne({_id:hash,expiresAt:{$gt:new Date(this.clock()*1000)}});
    if(!token)fail(401,'Session expired. Please sign in again.');
    const user=await this.db.collection('users').findOne({_id:token.userId,banned:false});
    if(!user)fail(401,'Session expired. Please sign in again.');
    return {id:user._id,hash};
  }
  async execute(principal,write,fn){
    return transaction(this.client,this.db,write,async t=>{
      const u=await this.assertPrincipal(t,principal);
      // The scheduled cleaner owns global expiry work; access checks enforce expiry.
      // Do not scan all sessions/reports/tokens on every message write.
      return fn(t,u);
    });
  }
  async issue(t,userId){
    const token=randomBytes(32).toString('base64url');
    await t.insert('tokens',{_id:digest(token),userId,expiresAt:new Date((this.clock()+7*86400)*1000)});
    return {token};
  }
  async register(input){
    const data=schemas.register.parse(input);if(age(data.dob)<18||age(data.dob)>110)fail(422,'This version is for adults aged 18 and over.');
    const password=await hashPassword(data.password);
    return transaction(this.client,this.db,true,async t=>{
      if(await t.one('users',{email:data.email}))fail(409,'Unable to register with these details. Try signing in.');
      const uid=id();const words=['Curious Otter','Sunny Fox','Kind Panda','Cosmic Robin','Mellow Koala','Bright Owl'];
      await t.insert('users',{_id:uid,email:data.email,password,name:data.name,dob:data.dob,gender:data.gender,interests:data.interests,alias:words[randomBytes(1)[0]%words.length]+' '+randomBytes(2).toString('hex').toUpperCase(),banned:false});
      return this.issue(t,uid);
    });
  }
  async login(input){
    const data=schemas.credentials.parse(input);const u=await this.db.collection('users').findOne({email:data.email});
    const saved=u?.password||('00'.repeat(16)+':'+'00'.repeat(64));
    if(!await verifyPassword(data.password,saved)||!u||u.banned)fail(401,'Email or password is incorrect.');
    return transaction(this.client,this.db,true,async t=>{
      if(!await t.one('users',{_id:u._id,banned:false,password:saved}))fail(401,'Unable to sign in.');
      return this.issue(t,u._id);
    });
  }
  async blocked(t,a,b){return !!await t.one('blocks',{$or:[{owner:a,target:b},{owner:b,target:a}]});}
  async live(t,uid){const p=await t.one('presence',{_id:uid,...this.active()});if(!p)fail(409,'Go live at a place to continue.');return p;}
  async endPresence(t,ids){
    if(!ids.length)return;
    const old=await t.all('presence',{_id:{$in:ids}});
    const chats=await t.all('conversations',{$or:[{a:{$in:ids}},{b:{$in:ids}}]});
    const cids=chats.map(c=>c._id);
    await t.remove('messages',{conversationId:{$in:cids}});
    await t.remove('conversations',{_id:{$in:cids}});
    await t.remove('activities',{owner:{$in:ids}});
    await t.updateMany('activities',{},{$pull:{members:{$in:ids}}});
    await t.remove('presence',{_id:{$in:ids}});
    for(const c of chats)await this.emit(t,'chat.ended',[c.a,c.b],{conversationId:c._id});
    await this.emit(t,'presence.ended',ids);
    await this.circleEvent(t,old.map(p=>p.place_id));
  }
  async prune(t){
    await t.remove('locations',{expiresAt:{$lte:new Date(this.clock()*1000)}});
    const expired=await t.all('presence',{$or:[{location:{$exists:false}},{expires:{$lte:this.clock()}},{checked:{$lte:this.clock()-LOCATION_LEASE_SECONDS}}]});
    await this.endPresence(t,expired.map(p=>p._id));
    const codes=await t.all('conversations',{codeExpires:{$lte:this.clock()}});
    for(const c of codes){await t.update('conversations',{_id:c._id},{$set:{readyA:false,readyB:false,code:null,codeExpires:null},$inc:{revision:1}});await this.emit(t,'meetup.changed',[c.a,c.b],{conversationId:c._id});}
    await t.remove('reports',{expiresAt:{$lte:new Date(this.clock()*1000)}});
    await t.remove('tokens',{expiresAt:{$lte:new Date(this.clock()*1000)}});
  }
  async cleanup(){return transaction(this.client,this.db,true,t=>this.prune(t));}
  async me(p){return this.execute(p,false,async(t,u)=>own(u));}
  async profile(p,input){const d=schemas.profile.parse(input);return this.execute(p,true,async(t,u)=>{
    await t.update('users',{_id:u._id},{$set:d});const here=await t.one('presence',{_id:u._id});
    if(here)await this.circleEvent(t,[here.place_id]);
    const chats=await t.all('conversations',{$or:[{a:u._id},{b:u._id}]});
    for(const c of chats)await this.emit(t,'chat.changed',[c.a,c.b],{conversationId:c._id});
    await this.emit(t,'profile.changed',[u._id]);return {ok:true};
  });}
  async logout(p){return this.execute(p,true,async(t,u)=>{await this.endPresence(t,[u._id]);await t.remove('locations',{_id:u._id});await t.remove('pushDevices',{tokenHash:p.hash});await t.remove('pushJobs',{tokenHash:p.hash});await t.remove('tokens',{_id:p.hash});await this.emit(t,'session.revoked',[u._id],{tokenHash:p.hash});return {ok:true};});}
  async deleteMe(p){return this.execute(p,true,async(t,u)=>{
    await t.remove('pushDevices',{userId:u._id});await t.remove('pushJobs',{recipientId:u._id});
    await this.endPresence(t,[u._id]);await t.remove('locations',{_id:u._id});await t.remove('tokens',{userId:u._id});await t.remove('blocks',{$or:[{owner:u._id},{target:u._id}]});await t.remove('users',{_id:u._id});await t.remove('locationRisks',{_id:u._id});await this.emit(t,'session.revoked',[u._id]);return {ok:true};
  });}
  geo(location){return {$geoWithin:{$centerSphere:[location.coordinates,1000/6371000]}};}
  closeEnough(a,b){return !!a?.location&&!!b?.location&&distance(a.location.coordinates[1],a.location.coordinates[0],b.location.coordinates[1],b.location.coordinates[0])<=1000;}
  async locationFor(t,p){return t.one('locations',{_id:p.id,tokenHash:p.hash,status:'trusted',checked:{$gt:this.clock()-LOCATION_LEASE_SECONDS},expiresAt:{$gt:new Date(this.clock()*1000)}});}
  async requireLocation(t,p){const loc=await this.locationFor(t,p);if(!loc)fail(403,'Location verification required. Enable real device location and try again.');return loc;}
  async revokeLocation(p,input={}){return this.execute(p,true,async(t,u)=>{
    if(input?.liveSessionId){const live=await t.one('presence',{_id:u._id,liveSessionId:input.liveSessionId,tokenHash:p.hash});if(!live)return {ok:true};}
    await t.remove('locations',{_id:u._id});await this.endPresence(t,[u._id]);return {ok:true};
  });}
  async updateLocation(p,input){
    const d=schemas.location.parse(input);
    // Commit revocation before reporting a rejected fix. Throwing inside the
    // transaction would roll it back and leave the person visible.
    const result=await this.execute(p,true,async(t,u)=>{
      const now=this.clock(),old=await t.one('locations',{_id:u._id}),risk=await t.one('locationRisks',{_id:u._id});
      if(input.liveSessionId&&!await t.one('presence',{_id:u._id,liveSessionId:input.liveSessionId,tokenHash:p.hash,...this.active()}))fail(409,'This Live session has ended.');
      let status='trusted',detail='';
      if(!d.servicesEnabled||!d.permissionGranted){status='suspicious';detail='Enable location services and grant precise location permission.';}
      else if(d.mocked){status='highRisk';detail='Live Mode requires your real device location. Disable mock/fake location and try again.';}
      else if(risk?.blockedUntil>now){status='highRisk';detail='Location verification failed. Wait two minutes, then try again with your real location.';}
      else if(now*1000-d.timestamp>30000||d.timestamp-now*1000>10000||d.accuracy>100){status='suspicious';detail='Location is old or inaccurate. Move to a clearer area and try again.';}
      else if(old?.location&&old.checked>now-LOCATION_LEASE_SECONDS){
        const elapsed=(d.timestamp-old.timestamp)/1000;
        const metres=distance(old.location.coordinates[1],old.location.coordinates[0],d.lat,d.lon);
        if(elapsed<=0){status='suspicious';detail='A new location fix is required. Try again.';}
        else if(metres>500+d.accuracy+old.accuracy&&metres/elapsed>60){status='highRisk';detail='An implausible location jump was detected. Recheck your real location in two minutes.';}
      }
      if(status!=='trusted'){
        if(status==='highRisk'&&!(risk?.blockedUntil>now))await t.update('locationRisks',{_id:u._id},{$set:{blockedUntil:now+120,expiresAt:new Date((now+900)*1000)},$inc:{failures:1}},{upsert:true});
        await t.remove('locations',{_id:u._id});await this.endPresence(t,[u._id]);return {status,detail};
      }
      const loc={location:{type:'Point',coordinates:[d.lon,d.lat]},accuracy:d.accuracy,timestamp:d.timestamp,checked:now,tokenHash:p.hash,status:'trusted',expiresAt:new Date((now+LOCATION_LEASE_SECONDS)*1000)};
      await t.update('locations',{_id:u._id},{$set:loc},{upsert:true});
      const here=await t.one('presence',{_id:u._id});let ended=false;
      if(here){
        const place=here.place_id?await t.one('places',{_id:here.place_id}):null;
        if(here.tokenHash!==p.hash||(here.place_id&&(!place||distance(d.lat,d.lon,place.lat,place.lon)>Math.min(place.radius,1000)))){await this.endPresence(t,[u._id]);ended=true;}
        else {await t.update('presence',{_id:u._id},{$set:{location:loc.location,checked:now}});await this.circleEvent(t,[here.place_id]);
          for(const c of await t.all('conversations',{$or:[{a:u._id},{b:u._id}]})){
            const other=await t.one('presence',{_id:c.a===u._id?c.b:c.a,...this.active()});
            if(!this.closeEnough(loc,other)){await t.remove('messages',{conversationId:c._id});await t.remove('conversations',{_id:c._id});await this.emit(t,'chat.ended',[c.a,c.b],{conversationId:c._id});}
          }
        }
      }
      return {status:'trusted',expiresIn:LOCATION_LEASE_SECONDS,presenceEnded:ended};
    });
    if(result.status!=='trusted')fail(403,result.detail);
    return result;
  }
  async blockedIds(t,uid){
    if(t.blockedFor===uid)return t.blockedIds;
    const rows=await t.all('blocks',{$or:[{owner:uid},{target:uid}]});
    t.blockedFor=uid;t.blockedIds=new Set(rows.map(b=>b.owner===uid?b.target:b.owner));return t.blockedIds;
  }
  async nearbyPeople(t,uid,loc,category){
    const blocked=await this.blockedIds(t,uid);
    const rows=(await t.all('presence',{...this.active(),_id:{$ne:uid},location:this.geo(loc.location)})).filter(p=>!blocked.has(p._id)&&(!category||((p.category==='Dating')===(category==='Dating'))));
    const users=await t.all('users',{_id:{$in:rows.map(p=>p._id)},banned:false});const byId=new Map(users.map(u=>[u._id,u]));
    return rows.filter(p=>byId.has(p._id)).map(p=>({...anonymous(byId.get(p._id)),category:p.category}));
  }
  async placeList(t,p){
    const loc=await this.locationFor(t,p);if(!loc)return [];
    const ps=await t.all('places',{location:this.geo(loc.location)},{sort:{name:1}}),result=[];
    const blocked=await this.blockedIds(t,p.id);
    const members=await t.all('presence',{...this.active(),place_id:{$in:ps.map(x=>x._id)},location:this.geo(loc.location)});
    const activities=await t.all('activities',{place_id:{$in:ps.map(x=>x._id)},owner:{$in:members.filter(x=>!blocked.has(x._id)).map(x=>x._id)}});
    for(const place of ps){
      const categories={};
      for(const other of members){
        if(other.place_id!==place._id||other._id===p.id||blocked.has(other._id))continue;
        categories[other.category]=(categories[other.category]||0)+1;
      }
      result.push({id:place._id,name:place.name,description:place.description,landmark:place.landmark,count:Object.values(categories).reduce((a,b)=>a+b,0),categories,activityCount:activities.filter(a=>a.place_id===place._id).length});
    }return result;
  }
  async places(p){return this.execute(p,false,t=>this.placeList(t,p));}
  async verifyPlace(t,placeId,loc){if(!placeId)return;
    const place=await t.one('places',{_id:placeId});if(!place)fail(404,'Place not found.');
    if(distance(loc.location.coordinates[1],loc.location.coordinates[0],place.lat,place.lon)>Math.min(place.radius,1000))fail(403,'You need to be inside this place to enter its Live Circle. You can still go live Around you.');
  }
  async enter(p,input){const d=schemas.presence.parse(input);if(input.demo)fail(403,'Demo location bypass is no longer supported.');return this.execute(p,true,async(t,u)=>{
    const loc=await this.requireLocation(t,p);await this.verifyPlace(t,d.placeId,loc);await this.endPresence(t,[u._id]);
    const sessionId=id(),expiry=this.clock()+d.minutes*60;
    await t.insert('presence',{_id:u._id,user_id:u._id,place_id:d.placeId,category:d.category,expires:expiry,checked:loc.checked,location:loc.location,liveSessionId:sessionId,tokenHash:p.hash});
    await this.circleEvent(t,[d.placeId]);return {ok:true,liveSessionId:sessionId,expires:expiry};
  });}
  async heartbeat(p,input){return this.execute(p,true,async(t,u)=>{
    const here=await this.live(t,u._id),loc=await this.requireLocation(t,p);
    if(here.tokenHash!==p.hash)fail(403,'Live session belongs to another sign-in.');
    await this.verifyPlace(t,here.place_id,loc);await t.update('presence',{_id:u._id},{$set:{checked:loc.checked,location:loc.location}});return {ok:true};
  });}
  async leave(p){return this.execute(p,true,async(t,u)=>{await this.endPresence(t,[u._id]);return {ok:true};});}
  async circleView(t,uid){
    const here=await t.one('presence',{_id:uid,...this.active()});if(!here)return {presence:null,people:[]};
    let people=await this.nearbyPeople(t,uid,here,here.category);
    if(here.place_id){const members=await t.all('presence',{place_id:here.place_id,...this.active()});const ids=new Set(members.map(x=>x._id));people=people.filter(x=>ids.has(x.id));}
    const {user_id,place_id,category,expires,checked,liveSessionId}=here;
    return {presence:{user_id,place_id,category,expires,checked,liveSessionId},people};
  }
  async circle(p){return this.execute(p,false,async(t,u)=>{if(!await this.locationFor(t,p))return {presence:null,people:[]};return this.circleView(t,u._id);});}
  async getChat(t,cid,uid){
    const c=await t.one('conversations',{_id:cid,$or:[{a:uid},{b:uid}]});if(!c)fail(404,'This temporary conversation has ended.');
    const a=await t.one('presence',{_id:c.a,...this.active()}),b=await t.one('presence',{_id:c.b,...this.active()});
    if(!a||!b||!this.closeEnough(a,b)||await this.blocked(t,c.a,c.b))fail(404,'This temporary conversation has ended.');
    return c;
  }
  chatDTO(c,uid,other,place,latest,unreadCount){
    const mutual=c.vibeA&&c.vibeB,isA=c.a===uid,valid=c.codeExpires>this.clock();
    const result={id:c._id,revision:c.revision||0,summaryRevision:c.summaryRevision||0,person:anonymous(other),mutual,liked:isA?c.vibeA:c.vibeB,ready:valid&&(isA?c.readyA:c.readyB),meetup:null,vibeTransition:mutual?(c.vibeTransition||null):null,meetTransition:null,latestMessage:latest?{body:latest.body,created:latest.created,mine:latest.sender===uid}:null,unreadCount};
    if(mutual)result.person.name=other.name;
    if(mutual&&c.readyA&&c.readyB&&c.code&&valid){result.meetup={code:c.code,expires:c.codeExpires,landmark:place?.landmark||'Agree on a public meeting place in chat.'};result.meetTransition=c.meetTransition||null;}
    return result;
  }
  async chatView(t,c,uid,withMessages=false){
    const other=await t.one('users',{_id:c.a===uid?c.b:c.a},{projection:{name:1,alias:1,dob:1,gender:1,interests:1}});
    const place=c.code&&c.codeExpires>this.clock()?await t.one('places',{_id:c.place_id},{projection:{landmark:1}}):null;
    const latest=await t.all('messages',{conversationId:c._id},{sort:{created:-1,_id:-1},limit:1,projection:{body:1,created:1,sender:1}});
    const unread=await t.count('messages',{conversationId:c._id,read:{$ne:true},sender:{$ne:uid}});
    const result=this.chatDTO(c,uid,other,place,latest[0],unread);
    if(withMessages)Object.assign(result,await this.messagePage(t,c._id,uid));return result;
  }
  async messagePage(t,cid,uid,before){
    let filter={conversationId:cid};
    if(before){let cursor;try{cursor=JSON.parse(Buffer.from(before,'base64url').toString());}catch{fail(422,'Invalid message cursor.');}
      if(!Number.isFinite(cursor.created)||typeof cursor.id!=='string'||!/^[a-f0-9]{32}$/.test(cursor.id))fail(422,'Invalid message cursor.');
      filter={...filter,$or:[{created:{$lt:cursor.created}},{created:cursor.created,_id:{$lt:cursor.id}}]};
    }
    const rows=await t.all('messages',filter,{sort:{created:-1,_id:-1},limit:51}),hasMore=rows.length>50;
    const page=rows.slice(0,50),last=page.at(-1);
    return {messages:page.reverse().map(m=>({id:m._id,body:m.body,created:m.created,mine:m.sender===uid,clientId:m.clientId})),hasMore,nextCursor:hasMore&&last?Buffer.from(JSON.stringify({created:last.created,id:last._id})).toString('base64url'):null};
  }
  async olderMessages(p,cid,before){return this.execute(p,false,async(t,u)=>{await this.getChat(t,cid,u._id);await this.requireLocation(t,p);return this.messagePage(t,cid,u._id,before);});}
  async chatList(t,uid){
    const chats=await t.aggregate('conversations',[
      {$match:{$or:[{a:uid},{b:uid}]}},
      {$lookup:{from:'messages',localField:'_id',foreignField:'conversationId',pipeline:[{$sort:{created:-1,_id:-1}},{$limit:1},{$project:{body:1,created:1,sender:1}}],as:'latest'}}
    ]);
    if(!chats.length)return [];
    const ids=[...new Set(chats.flatMap(c=>[c.a,c.b]))];
    const presence=new Map((await t.all('presence',{_id:{$in:ids},...this.active()})).map(p=>[p._id,p]));
    const blocked=await this.blockedIds(t,uid);
    const eligible=chats.filter(c=>!blocked.has(c.a===uid?c.b:c.a)&&this.closeEnough(presence.get(c.a),presence.get(c.b)));
    if(!eligible.length)return [];
    const users=new Map((await t.all('users',{_id:{$in:ids}},{projection:{name:1,alias:1,dob:1,gender:1,interests:1}})).map(u=>[u._id,u]));
    const places=new Map((await t.all('places',{_id:{$in:[...new Set(eligible.filter(c=>c.code&&c.codeExpires>this.clock()).map(c=>c.place_id))]}},{projection:{landmark:1}})).map(p=>[p._id,p]));
    const unread=new Map((await t.aggregate('messages',[{$match:{conversationId:{$in:eligible.map(c=>c._id)},read:{$ne:true},sender:{$ne:uid}}},{$group:{_id:'$conversationId',count:{$sum:1}}}])).map(x=>[x._id,x.count]));
    return eligible.filter(c=>users.has(c.a===uid?c.b:c.a)).map(c=>this.chatDTO(c,uid,users.get(c.a===uid?c.b:c.a),places.get(c.place_id),c.latest[0],unread.get(c._id)||0));
  }
  async chats(p){return this.execute(p,false,async(t,u)=>await this.locationFor(t,p)?this.chatList(t,u._id):[]);}
  async chat(p,cid){return this.execute(p,false,async(t,u)=>{const c=await this.getChat(t,cid,u._id);await this.requireLocation(t,p);return this.chatView(t,c,u._id,true);});}
  async chatSummary(p,cid){return this.execute(p,false,async(t,u)=>{const c=await this.getChat(t,cid,u._id);await this.requireLocation(t,p);return this.chatView(t,c,u._id);});}
  async chatEvent(p,event){return this.execute(p,false,async(t,u)=>{
    const c=await this.getChat(t,event.conversationId,u._id);await this.requireLocation(t,p);
    if(!['chat.message','chat.read'].includes(event.event))return {chat:await this.chatView(t,c,u._id)};
    const unreadCount=await t.count('messages',{conversationId:c._id,read:{$ne:true},sender:{$ne:u._id}});
    const chatPatch={id:c._id,summaryRevision:c.summaryRevision||0,unreadCount};
    if(event.event==='chat.read')return {chatPatch};
    const m=await t.one('messages',{_id:event.messageId,conversationId:c._id});
    const latest=(await t.all('messages',{conversationId:c._id},{sort:{created:-1,_id:-1},limit:1,projection:{body:1,created:1,sender:1}}))[0];
    if(latest)chatPatch.latestMessage={body:latest.body,created:latest.created,mine:latest.sender===u._id};
    return {chatPatch,...(m?{message:{id:m._id,clientId:m.clientId,body:m.body,created:m.created,mine:m.sender===u._id}}:{})};
  });}
  async readMessages(p,cid,input){
    const {messageIds}=schemas.read.parse(input);
    return this.execute(p,true,async(t,u)=>{
      await this.getChat(t,cid,u._id);await this.requireLocation(t,p);
      // Acknowledge only messages actually displayed, never a timestamp that can
      // accidentally consume messages arriving concurrently or out of order.
      const result=await t.updateMany('messages',{conversationId:cid,_id:{$in:messageIds},sender:{$ne:u._id},read:{$ne:true}},{$set:{read:true}});
      if(result.modifiedCount){await t.update('conversations',{_id:cid},{$inc:{summaryRevision:1}});await this.emit(t,'chat.read',[u._id],{conversationId:cid});}return {ok:true};
    });
  }
  async startChat(p,input){const {target}=schemas.target.parse(input);return this.execute(p,true,async(t,u)=>{
    await this.requireLocation(t,p);if(target===u._id)fail(422,'Choose someone else.');const a=await this.live(t,u._id),b=await this.live(t,target);
    if(!this.closeEnough(a,b)||await this.blocked(t,u._id,target))fail(403,'This person is unavailable.');
    if((a.category==='Dating')!==(b.category==='Dating'))fail(403,'Both people must choose Dating.');
    const pair=[u._id,target].sort();let c=await t.one('conversations',{a:pair[0],b:pair[1]});
    if(!c){c={_id:id(),a:pair[0],b:pair[1],place_id:a.place_id,vibeA:false,vibeB:false,readyA:false,readyB:false,code:null,codeExpires:null};await t.insert('conversations',c);await this.emit(t,'chat.created',pair,{conversationId:c._id});}
    return {id:c._id};
  });}
  async sendMessage(p,cid,input){const d=schemas.message.parse(input);return this.execute(p,true,async(t,u)=>{
    const c=await this.getChat(t,cid,u._id);await this.requireLocation(t,p);const existing=await t.one('messages',{conversationId:cid,sender:u._id,clientId:d.clientId});
    if(existing){if(existing.body!==d.body)fail(409,'This message ID was already used for different text.');return {id:existing._id,clientId:d.clientId,created:existing.created};}
    if(await t.count('messages',{sender:u._id,created:{$gt:this.clock()-60}})>=20)fail(429,'You can send 20 messages a minute.');
    const mid=id(),created=this.clock();await t.insert('messages',{_id:mid,conversationId:cid,sender:u._id,clientId:d.clientId,body:d.body,created});
    await t.update('conversations',{_id:cid},{$inc:{summaryRevision:1}});
    await enqueuePush(t,c,{_id:mid,sender:u._id},this.clock());
    await this.emit(t,'chat.message',[c.a,c.b],{conversationId:cid,messageId:mid});return {id:mid,clientId:d.clientId,created};
  }).then(result=>{pushLog('message_committed',{messageId:result.id,conversationId:cid,senderUserId:p.id});return result;});}
  async vibe(p,cid,input){const {value}=schemas.choice.parse(input);return this.execute(p,true,async(t,u)=>{
    const c=await this.getChat(t,cid,u._id),key=c.a===u._id?'vibeA':'vibeB',wasMutual=c.vibeA&&c.vibeB;
    await this.requireLocation(t,p);if(c[key]===value)return {chat:await this.chatView(t,c,u._id)};c[key]=value;
    Object.assign(c,{readyA:false,readyB:false,code:null,codeExpires:null,revision:(c.revision||0)+1});
    if(!wasMutual&&c.vibeA&&c.vibeB)c.vibeTransition=id();
    await t.update('conversations',{_id:cid},{$set:{[key]:value,readyA:false,readyB:false,code:null,codeExpires:null,revision:c.revision,vibeTransition:c.vibeTransition||null}});
    // A one-sided private choice must not produce an event visible to the other person.
    await this.emit(t,'chat.changed',wasMutual||(c.vibeA&&c.vibeB)?[c.a,c.b]:[u._id],{conversationId:cid});return {chat:await this.chatView(t,c,u._id)};
  });}
  async ready(p,cid,input){const {value}=schemas.choice.parse(input);return this.execute(p,true,async(t,u)=>{
    const c=await this.getChat(t,cid,u._id);if(!c.vibeA||!c.vibeB)fail(409,'Both people must connect first.');
    await this.requireLocation(t,p);const before=!!c.code;
    if(!c.codeExpires||c.codeExpires<=this.clock()){c.readyA=false;c.readyB=false;c.code=null;c.codeExpires=this.clock()+600;}
    const key=c.a===u._id?'readyA':'readyB';
    if(c[key]===value)return {chat:await this.chatView(t,c,u._id)};
    c[key]=value;c.revision=(c.revision||0)+1;
    if(!value)c.code=null;
    else if(c.readyA&&c.readyB&&!c.code){c.code=randomBytes(3).toString('hex').toUpperCase();c.codeExpires=this.clock()+600;c.meetTransition=id();}
    await t.update('conversations',{_id:cid},{$set:{readyA:c.readyA,readyB:c.readyB,code:c.code,codeExpires:c.codeExpires,revision:c.revision,meetTransition:c.meetTransition||null}});
    await this.emit(t,'meetup.changed',before||c.code?[c.a,c.b]:[u._id],{conversationId:cid});return {chat:await this.chatView(t,c,u._id)};
  });}
  async endChat(p,cid){return this.execute(p,true,async(t,u)=>{
    const c=await t.one('conversations',{_id:cid,$or:[{a:u._id},{b:u._id}]});
    if(!c)fail(404,'This temporary conversation has ended.');
    await t.remove('messages',{conversationId:cid});await t.remove('conversations',{_id:cid});
    await this.emit(t,'chat.ended',[c.a,c.b],{conversationId:cid});return {ok:true};
  });}
  async activitiesView(t,uid){
    const here=await t.one('presence',{_id:uid,...this.active()});if(!here||!here.place_id)return [];
    const activities=await t.all('activities',{place_id:here.place_id,expires:{$gt:this.clock()}});if(!activities.length)return [];
    const ids=[...new Set(activities.flatMap(a=>[a.owner,...a.members]))];
    const active=new Map((await t.all('presence',{_id:{$in:ids},place_id:here.place_id,...this.active()})).map(p=>[p._id,p]));
    const blocked=await this.blockedIds(t,uid),place=await t.one('places',{_id:here.place_id});
    const result=[];for(const a of activities){const owner=active.get(a.owner);if(!owner||!this.closeEnough(here,owner))continue;
      const members=a.members.filter(m=>active.has(m));if(members.some(m=>blocked.has(m)))continue;
      result.push({id:a._id,title:a.title,category:a.category,capacity:a.capacity,count:members.length,joined:members.includes(uid),isOwner:a.owner===uid,landmark:place?.landmark||'Agree on a public meeting place in chat.'});
    }return result;
  }
  async activities(p){return this.execute(p,false,async(t,u)=>await this.locationFor(t,p)?this.activitiesView(t,u._id):[]);}
  async createActivity(p,input){const d=schemas.activity.parse(input);return this.execute(p,true,async(t,u)=>{
    await this.requireLocation(t,p);const here=await this.live(t,u._id);if(!here.place_id)fail(409,'Enter a Live Place to host a venue activity.');if(await t.count('activities',{owner:u._id})>=3)fail(409,'You can host up to three live activities.');
    const aid=id();await t.insert('activities',{_id:aid,owner:u._id,place_id:here.place_id,...d,expires:here.expires,members:[u._id]});
    await this.circleEvent(t,[here.place_id]);return {id:aid};
  });}
  async joinActivity(p,aid){return this.execute(p,true,async(t,u)=>{
    await this.requireLocation(t,p);const here=await this.live(t,u._id),a=await t.one('activities',{_id:aid,place_id:here.place_id});if(!a)fail(404,'Activity unavailable.');
    const owner=await t.one('presence',{_id:a.owner,...this.active()});if(!owner||!this.closeEnough(here,owner))fail(404,'Activity unavailable.');
    for(const member of a.members)if(await this.blocked(t,u._id,member))fail(404,'Activity unavailable.');
    if(a.members.includes(u._id))return {ok:true};if(a.members.length>=a.capacity)fail(409,'This activity is full.');
    await t.update('activities',{_id:aid},{$addToSet:{members:u._id}});await this.circleEvent(t,[here.place_id]);return {ok:true};
  });}
  async leaveActivity(p,aid){return this.execute(p,true,async(t,u)=>{
    const a=await t.one('activities',{_id:aid});if(a){if(a.owner===u._id)await t.remove('activities',{_id:aid});else await t.update('activities',{_id:aid},{$pull:{members:u._id}});await this.circleEvent(t,[a.place_id]);}return {ok:true};
  });}
  async blocksView(t,uid){const blocks=await t.all('blocks',{owner:uid});if(!blocks.length)return [];return (await t.all('users',{_id:{$in:blocks.map(b=>b.target)}},{projection:{alias:1}})).map(u=>({id:u._id,alias:u.alias}));}
  async blocks(p){return this.execute(p,false,(t,u)=>this.blocksView(t,u._id));}
  async block(p,input){const {target}=schemas.target.parse(input);return this.execute(p,true,async(t,u)=>{
    if(target===u._id||!await t.one('users',{_id:target}))fail(422,'Invalid person.');
    await t.update('blocks',{owner:u._id,target},{$setOnInsert:{_id:id(),owner:u._id,target}},{upsert:true});
    const pair=[u._id,target].sort(),c=await t.one('conversations',{a:pair[0],b:pair[1]});
    if(c){await t.remove('messages',{conversationId:c._id});await t.remove('conversations',{_id:c._id});await this.emit(t,'chat.ended',pair,{conversationId:c._id});}
    await t.updateMany('activities',{owner:u._id},{$pull:{members:target}});await t.updateMany('activities',{owner:target},{$pull:{members:u._id}});
    const here=await t.one('presence',{_id:u._id});if(here)await this.circleEvent(t,[here.place_id]);await this.emit(t,'safety.changed',pair);return {ok:true};
  });}
  async unblock(p,target){return this.execute(p,true,async(t,u)=>{await t.remove('blocks',{owner:u._id,target});const here=await t.one('presence',{_id:u._id});if(here)await this.circleEvent(t,[here.place_id]);await this.emit(t,'safety.changed',[u._id,target]);return {ok:true};});}
  async report(p,input){const d=schemas.report.parse(input);return this.execute(p,true,async(t,u)=>{
    if(d.target===u._id||!await t.one('users',{_id:d.target}))fail(422,'Invalid person.');
    const pair=[u._id,d.target].sort(),c=await t.one('conversations',{a:pair[0],b:pair[1]});
    const evidence=c?await t.all('messages',{conversationId:c._id},{sort:{created:-1},limit:30}):[];
    const rid=id();await t.insert('reports',{_id:rid,reporter:u._id,target:d.target,reason:d.reason,evidence:evidence.map(m=>({sender:m.sender,body:m.body,created:m.created})),created:this.clock(),expiresAt:new Date((this.clock()+30*86400)*1000),status:'open'});return {id:rid,message:'Report saved for review.'};
  });}
  async state(p){return this.execute(p,false,async(t,u)=>{
    const loc=await this.locationFor(t,p),here=await t.one('presence',{_id:u._id,...this.active()});
    const nearby=loc?await this.nearbyPeople(t,u._id,loc):[];
    const categories={};for(const person of nearby)categories[person.category]=(categories[person.category]||0)+1;
    return {me:own(u),locationReady:!!loc,nearby:{count:nearby.length,categories},places:await this.placeList(t,p),circle:loc?await this.circleView(t,u._id):{presence:null,people:[]},chats:loc?await this.chatList(t,u._id):[],activities:loc?await this.activitiesView(t,u._id):[],blocks:await this.blocksView(t,u._id)};
  });}
  async ban(uid){return transaction(this.client,this.db,true,async t=>{await t.update('users',{_id:uid},{$set:{banned:true}});await this.endPresence(t,[uid]);await t.remove('locations',{_id:uid});await t.remove('tokens',{userId:uid});await this.emit(t,'session.revoked',[uid]);});}
}
