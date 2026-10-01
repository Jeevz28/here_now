import {digest} from '../src/core.js';
import {test,before,after,beforeEach,afterEach} from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,mkdtemp,rm} from 'node:fs/promises';
import {MongoMemoryReplSet} from 'mongodb-memory-server';
import WebSocket from 'ws';
import {LOCATION_LEASE_SECONDS} from '../src/service.js';
import {createApplication} from '../src/app.js';
let mongo,runtime,base,folder,sequence=0;const sockets=[];let deliveries=[],pushFailure=null;
before(async()=>{await mkdir('.test-data',{recursive:true});folder=await mkdtemp(process.cwd()+'/.test-data/mongo-');mongo=await MongoMemoryReplSet.create({instanceOpts:[{dbPath:folder}],replSet:{count:1,args:['--nounixsocket','--setParameter','diagnosticDataCollectionEnabled=false']},binary:{version:'7.0.14'}});});
beforeEach(async()=>{deliveries=[];pushFailure=null;runtime=await createApplication({mongoUri:mongo.getUri(),dbName:'test'+(++sequence),demoMode:true,origins:['http://10.0.2.2:8000'],rateLimits:false,authTimeoutMs:300,pushIntervalMs:3600000,pushSender:async payload=>{if(pushFailure)throw pushFailure;deliveries.push(payload);return 'fcm-test-id';}});await runtime.listen(0,'127.0.0.1');base='http://127.0.0.1:'+runtime.server.address().port;});
afterEach(async()=>{for(const ws of sockets.splice(0))ws.terminate();await runtime.close();});
after(async()=>{await mongo?.stop();if(folder)await rm(folder,{recursive:true,force:true});});
async function api(path,{token,method='GET',body,status=200}={}){const r=await fetch(base+path,{method,headers:{'Content-Type':'application/json',...(token?{Authorization:'Bearer '+token}:{})},...(body!==undefined?{body:JSON.stringify(body)}:{})});const data=await r.json();assert.equal(r.status,status,JSON.stringify(data));return data;}
async function user(name){const {token}=await api('/auth/register',{method:'POST',body:{name,email:name.toLowerCase()+'@example.com',password:'Testing12345!',dob:'1997-06-15'},status:201});return {token,id:(await api('/me',{token})).id};}
const live={placeId:'kelvingrove',category:'Friends',minutes:60,lat:0,lon:0,demo:true};
async function fix(u,lat=55.8685,lon=-4.284,extra={},status=200){return api('/location',{token:u.token,method:'POST',body:{lat,lon,accuracy:10,timestamp:Date.now(),mocked:false,servicesEnabled:true,permissionGranted:true,...extra},status});}
async function enter(u,extra={}){const d={...live,...extra,demo:false};const coords={'kelvingrove':[55.8685,-4.284],'george-square':[55.8612,-4.2502]};await fix(u,...(coords[d.placeId]||[55.8685,-4.284]));return api('/presence',{token:u.token,method:'POST',body:d});}
async function pair(){const a=await user('Alice'),b=await user('Bob');await enter(a);await enter(b);const {id}=await api('/conversations',{token:a.token,method:'POST',body:{target:b.id},status:201});return {a,b,cid:id};}
function wsClient(token,{auth=true}={}){
 const ws=new WebSocket(base.replace('http:','ws:')+'/ws');sockets.push(ws);const buffer=[],waiters=[];ws.on('error',()=>{});
 ws.on('message',raw=>{const d=JSON.parse(raw);const i=waiters.findIndex(w=>w.predicate(d));if(i>=0){const w=waiters.splice(i,1)[0];clearTimeout(w.timer);w.resolve(d);}else buffer.push(d);});
 const next=(predicate,timeout=3000)=>{const i=buffer.findIndex(predicate);if(i>=0)return Promise.resolve(buffer.splice(i,1)[0]);return new Promise((resolve,reject)=>{const w={predicate,resolve,timer:setTimeout(()=>{const k=waiters.indexOf(w);if(k>=0)waiters.splice(k,1);reject(new Error('Timed out waiting for frame; buffer='+JSON.stringify(buffer)));},timeout)};waiters.push(w);});};
 const opened=new Promise(resolve=>ws.once('open',resolve));const ready=opened.then(async()=>{if(auth){ws.send(JSON.stringify({type:'auth',token}));await next(d=>d.type==='ready');}});
 return {ws,buffer,next,ready,opened,async command(type,cid,payload,expected='ack'){const requestId='req-'+Math.random();ws.send(JSON.stringify({type,requestId,conversationId:cid,payload}));return next(d=>d.requestId===requestId&&d.type===expected);}};
}
const event=(cid,name)=>d=>d.type==='event'&&d.event===name&&d.conversationId===cid;
const choice=(u,cid,kind,value=true,status=200)=>api(`/conversations/${cid}/${kind}`,{token:u.token,method:'POST',body:{value},status});
const send=(u,cid,body,clientId,status=201)=>api(`/conversations/${cid}/messages`,{token:u.token,method:'POST',body:{body,clientId},status});

test('choice acknowledgements return authoritative transitions and retries preserve consent',async()=>{
 const {a,b,cid}=await pair();const sa=wsClient(a.token),sb=wsClient(b.token);await Promise.all([sa.ready,sb.ready]);
 const one=(await sa.command('chat.vibe',cid,{value:true})).data.chat;assert.equal(one.liked,true);assert.equal(one.mutual,false);assert.equal(one.vibeTransition,null);
 const mutual=(await sb.command('chat.vibe',cid,{value:true})).data.chat;assert.equal(mutual.mutual,true);assert.ok(mutual.vibeTransition);await sa.next(event(cid,'chat.changed'));
 assert.equal((await api('/conversations/'+cid,{token:a.token})).vibeTransition,mutual.vibeTransition);
 const ready=(await sa.command('meetup.ready',cid,{value:true})).data.chat;assert.equal(ready.ready,true);assert.equal(ready.meetup,null);assert.equal(ready.meetTransition,null);
 const meet=(await sb.command('meetup.ready',cid,{value:true})).data.chat;assert.ok(meet.meetTransition);assert.ok(meet.meetup);assert.equal((await api('/conversations/'+cid,{token:a.token})).meetTransition,meet.meetTransition);
 const retried=(await choice(a,cid,'vibe')).chat;assert.equal(retried.meetTransition,meet.meetTransition);assert.equal(retried.ready,true);
 assert.equal((await choice(b,cid,'ready')).chat.meetTransition,meet.meetTransition);
 await choice(b,cid,'ready',false);const newMeet=(await choice(b,cid,'ready')).chat;assert.notEqual(newMeet.meetTransition,meet.meetTransition);
 await choice(b,cid,'vibe',false);const newVibe=(await choice(b,cid,'vibe')).chat;assert.notEqual(newVibe.vibeTransition,mutual.vibeTransition);assert.equal(newVibe.ready,false);
});
test('explicit leave chat deletes only that conversation, keeps Live presence, and notifies peer',async()=>{
 const {a,b,cid}=await pair(),eve=await user('Eve'),sb=wsClient(b.token);await sb.ready;await send(a,cid,'Good conversation','leave-msg-1');
 await api('/conversations/'+cid,{token:eve.token,method:'DELETE',status:404});await api('/conversations/'+cid,{token:a.token,method:'DELETE'});await sb.next(event(cid,'chat.ended'));
 await api('/conversations/'+cid,{token:b.token,status:404});assert.ok((await api('/state',{token:a.token})).circle.presence);assert.equal(await runtime.db.collection('messages').countDocuments({conversationId:cid}),0);
});

test('authentication, hashes, login and logout',async()=>{await api('/state',{status:401});const a=await user('Alice');await api('/auth/login',{method:'POST',body:{email:'alice@example.com',password:'wrongpassword'},status:401});await api('/auth/login',{method:'POST',body:{email:'ALICE@example.com',password:'Testing12345!'}});assert.notEqual((await runtime.db.collection('users').findOne({_id:a.id})).password,'Testing12345!');assert.equal(await runtime.db.collection('tokens').countDocuments({_id:a.token}),0);await api('/auth/logout',{token:a.token,method:'POST'});await api('/me',{token:a.token,status:401});});
test('adult date and input validation',async()=>{for(const dob of ['2015-01-01','1998-02-31'])await api('/auth/register',{method:'POST',body:{email:'young@example.com',password:'Testing12345!',name:'Young',dob},status:422});const a=await user('Alice');await api('/presence',{token:a.token,method:'POST',body:{...live,minutes:500},status:422});});
test('location required, demo bypass rejected, private temporary coordinates',async()=>{const a=await user('Alice');await api('/presence',{token:a.token,method:'POST',body:live,status:403});await api('/presence',{token:a.token,method:'POST',body:{...live,demo:false},status:403});await enter(a);const p=await runtime.db.collection('presence').findOne({_id:a.id});assert.deepEqual(p.location.coordinates,[-4.284,55.8685]);const out=JSON.stringify(await api('/state',{token:a.token}));assert.ok(!out.includes('coordinates'));assert.ok(!out.includes('tokenHash'));});
test('anonymous discovery and dating separation',async()=>{const {a,b}=await pair();const person=(await api('/circle',{token:a.token})).people[0];for(const key of ['name','email','dob','password','lat','lon'])assert.equal(person[key],undefined);await enter(b,{category:'Dating'});assert.deepEqual((await api('/circle',{token:a.token})).people,[]);await api('/conversations',{token:a.token,method:'POST',body:{target:b.id},status:403});});
test('WebSocket chat delivers only to participants and denies third-party commands',async()=>{const {a,b,cid}=await pair(),eve=await user('Eve'),sa=wsClient(a.token),sb=wsClient(b.token),se=wsClient(eve.token);await Promise.all([sa.ready,sb.ready,se.ready]);const ack=await sa.command('chat.send',cid,{body:'Hello over WebSocket',clientId:'message-001'});assert.ok(ack.data.id);await sb.next(event(cid,'chat.message'));await sa.next(event(cid,'chat.message'));const data=await api('/conversations/'+cid,{token:b.token});assert.equal(data.messages[0].body,'Hello over WebSocket');assert.equal(data.messages[0].mine,false);assert.equal(se.buffer.some(event(cid,'chat.message')),false);assert.equal((await se.command('chat.send',cid,{body:'Intrusion',clientId:'intrude-001'},'error')).status,404);await api('/conversations/'+cid,{token:eve.token,status:404});});
test('retry after reconnect and HTTP fallback create no duplicate messages',async()=>{const {a,b,cid}=await pair(),sa=wsClient(a.token);await sa.ready;const d={body:'Send once',clientId:'retry-msg-001'};const first=await sa.command('chat.send',cid,d);sa.ws.terminate();const again=wsClient(a.token);await again.ready;assert.equal((await again.command('chat.send',cid,d)).data.id,first.data.id);await send(a,cid,d.body,d.clientId);assert.equal((await api('/conversations/'+cid,{token:b.token})).messages.length,1);await send(a,cid,'Different',d.clientId,409);});
test('malformed frames and empty/oversized messages are rejected',async()=>{const {a,cid}=await pair(),sa=wsClient(a.token);await sa.ready;assert.equal((await sa.command('chat.send',cid,{body:' ',clientId:'blank-msg-1'},'error')).status,422);sa.ws.send('{broken');assert.equal((await sa.next(d=>d.type==='error')).status,422);await send(a,cid,'x'.repeat(1001),'long-msg-001',422);});
test('private vibes, mutual reveal, explicit meetup consent and withdrawal',async()=>{const {a,b,cid}=await pair(),sa=wsClient(a.token),sb=wsClient(b.token);await Promise.all([sa.ready,sb.ready]);await choice(a,cid,'ready',true,409);await sa.command('chat.vibe',cid,{value:true});await sa.next(event(cid,'chat.changed'));assert.equal(sb.buffer.some(event(cid,'chat.changed')),false);assert.equal((await api('/conversations/'+cid,{token:b.token})).person.name,undefined);await sb.command('chat.vibe',cid,{value:true});await sa.next(event(cid,'chat.changed'));assert.equal((await api('/conversations/'+cid,{token:a.token})).person.name,'Bob');await sa.command('meetup.ready',cid,{value:true});assert.equal((await api('/conversations/'+cid,{token:b.token})).meetup,null);await sb.command('meetup.ready',cid,{value:true});await sa.next(event(cid,'meetup.changed'));const v=await api('/conversations/'+cid,{token:a.token});assert.equal(v.meetup.code.length,6);assert.equal(v.meetup.lat,undefined);await sb.command('meetup.ready',cid,{value:false});assert.equal((await api('/conversations/'+cid,{token:a.token})).meetup,null);await sa.command('chat.vibe',cid,{value:false});assert.equal((await api('/conversations/'+cid,{token:b.token})).person.name,undefined);});
test('presence departure preserves chat and message history while removing discovery',async()=>{const {a,b,cid}=await pair(),sa=wsClient(a.token);await sa.ready;await send(a,cid,'Keep this','keep-chat-1');await api('/presence',{token:b.token,method:'DELETE'});await sa.next(d=>d.event==='circle.changed');assert.deepEqual((await api('/circle',{token:a.token})).people,[]);assert.equal((await api('/conversations/'+cid,{token:a.token})).messages.length,1);await send(b,cid,'Still here in chat','after-leave-1');assert.equal(sa.buffer.some(event(cid,'chat.ended')),false);});
test('stale presence expires without ending established conversation',async()=>{const {a,b,cid}=await pair();await runtime.db.collection('presence').updateOne({_id:b.id},{$set:{checked:runtime.service.clock()-LOCATION_LEASE_SECONDS-1}});await runtime.service.cleanup();assert.deepEqual((await api('/circle',{token:a.token})).people,[]);await api('/conversations/'+cid,{token:b.token});});
test('heartbeats cannot extend the original session deadline',async()=>{const a=await user('Alice');await enter(a,{minutes:30});const original=await runtime.db.collection('presence').findOne({_id:a.id});await api('/presence/heartbeat',{token:a.token,method:'POST',body:live});assert.equal((await runtime.db.collection('presence').findOne({_id:a.id})).expires,original.expires);await runtime.db.collection('presence').updateOne({_id:a.id},{$set:{expires:runtime.service.clock()-1}});await api('/presence/heartbeat',{token:a.token,method:'POST',body:live,status:409});assert.equal((await api('/circle',{token:a.token})).presence,null);});
test('meetup expiry pushes updates and requires fresh mutual consent',async()=>{const {a,b,cid}=await pair();for(const u of [a,b])await choice(u,cid,'vibe');for(const u of [a,b])await choice(u,cid,'ready');const sa=wsClient(a.token);await sa.ready;await runtime.db.collection('conversations').updateOne({_id:cid},{$set:{codeExpires:runtime.service.clock()-1}});await runtime.service.cleanup();await sa.next(event(cid,'meetup.changed'));const v=await api('/conversations/'+cid,{token:a.token});assert.equal(v.ready,false);assert.equal(v.meetup,null);await choice(a,cid,'ready');assert.equal((await api('/conversations/'+cid,{token:a.token})).meetup,null);});
test('transactional joins cannot overfill the last activity slot',async()=>{const {a,b}=await pair(),c=await user('Charlie');await enter(c);const {id}=await api('/activities',{token:a.token,method:'POST',body:{title:'Two-player game',capacity:2},status:201});const results=await Promise.all([b,c].map(u=>fetch(base+'/activities/'+id+'/join',{method:'POST',headers:{Authorization:'Bearer '+u.token}})));assert.deepEqual(results.map(r=>r.status).sort(),[200,409]);assert.equal((await api('/activities',{token:a.token}))[0].count,2);});
test('activities update live and close on owner departure',async()=>{const {a,b}=await pair(),sa=wsClient(a.token);await sa.ready;const {id}=await api('/activities',{token:a.token,method:'POST',body:{title:'Coffee in the park',capacity:5,category:'Friends'},status:201});await api(`/activities/${id}/join`,{token:b.token,method:'POST'});await sa.next(d=>d.event==='circle.changed');assert.equal((await api('/activities',{token:a.token}))[0].count,2);await api(`/activities/${id}/membership`,{token:b.token,method:'DELETE'});assert.equal((await api('/activities',{token:a.token}))[0].count,1);await api('/presence',{token:a.token,method:'DELETE'});assert.deepEqual(await api('/activities',{token:b.token}),[]);});
test('blocking enforces live chat closure and group isolation',async()=>{const {a,b,cid}=await pair(),sb=wsClient(b.token);await sb.ready;const act=await api('/activities',{token:a.token,method:'POST',body:{title:'Football game',capacity:5},status:201});await api('/blocks',{token:a.token,method:'POST',body:{target:b.id}});await sb.next(event(cid,'chat.ended'));assert.equal((await sb.command('chat.send',cid,{body:'Blocked send',clientId:'blocked-001'},'error')).status,404);await api('/activities/'+act.id+'/join',{token:b.token,method:'POST',status:404});assert.deepEqual((await api('/circle',{token:b.token})).people,[]);await api('/blocks/'+b.id,{token:a.token,method:'DELETE'});assert.equal((await api('/circle',{token:b.token})).people.length,1);});
test('reported evidence survives session end then expires',async()=>{const {a,b,cid}=await pair();await send(b,cid,'Evidence sample','report-msg-1');const r=await api('/reports',{token:a.token,method:'POST',body:{target:b.id,reason:'Unwanted contact'},status:201});await api('/presence',{token:a.token,method:'DELETE'});assert.equal((await runtime.db.collection('reports').findOne({_id:r.id})).evidence[0].body,'Evidence sample');await runtime.db.collection('reports').updateOne({_id:r.id},{$set:{expiresAt:new Date(0)}});await runtime.service.cleanup();assert.equal(await runtime.db.collection('reports').countDocuments(),0);});
test('logout immediately revokes WebSocket and HTTP access',async()=>{const a=await user('Alice'),sa=wsClient(a.token);await sa.ready;await api('/auth/logout',{token:a.token,method:'POST'});await sa.next(d=>d.type==='auth.expired');await api('/me',{token:a.token,status:401});});
test('account deletion cascades and operator bans revoke sockets',async()=>{const {a,b,cid}=await pair(),sb=wsClient(b.token);await sb.ready;await api('/me',{token:a.token,method:'DELETE'});await sb.next(event(cid,'chat.ended'));assert.equal(await runtime.db.collection('users').findOne({_id:a.id}),null);await runtime.service.ban(b.id);await sb.next(d=>d.type==='auth.expired');await api('/me',{token:b.token,status:401});});
test('invalid and expired tokens cannot authenticate WebSockets',async()=>{const s=wsClient('',{auth:false});await s.ready;s.ws.send(JSON.stringify({type:'auth',token:'invalid-token-invalid-token'}));await s.next(d=>d.type==='auth.expired');const a=await user('Alice');await runtime.db.collection('tokens').updateMany({userId:a.id},{$set:{expiresAt:new Date(0)}});const e=wsClient('',{auth:false});await e.ready;e.ws.send(JSON.stringify({type:'auth',token:a.token}));await e.next(d=>d.type==='auth.expired');});
test('authentication deadline and cross-origin socket protection',async()=>{const s=wsClient('',{auth:false});await s.ready;assert.equal(await new Promise(resolve=>s.ws.once('close',resolve)),4401);const ws=new WebSocket(base.replace('http:','ws:')+'/ws',{origin:'https://untrusted.example'});sockets.push(ws);ws.on('error',()=>{});assert.equal(await new Promise(resolve=>ws.on('unexpected-response',(req,res)=>{resolve(res.statusCode);req.destroy();})),403);});
test('reconnecting clients can fetch messages missed while offline',async()=>{const {a,b,cid}=await pair(),sb=wsClient(b.token);await sb.ready;sb.ws.terminate();await send(a,cid,'While offline','offline-001');const r=wsClient(b.token);await r.ready;assert.equal((await api('/conversations/'+cid,{token:b.token})).messages[0].body,'While offline');});
test('venue switches preserve established conversations',async()=>{const {a,b,cid}=await pair();runtime.service.clock=()=>Date.now()/1000+500;await runtime.db.collection('locations').deleteOne({_id:b.id});const timestamp=Date.now()+500000;await fix(b,55.8612,-4.2502,{timestamp});await api('/presence',{token:b.token,method:'POST',body:{...live,demo:false,placeId:'george-square'}});await api('/conversations/'+cid,{token:a.token});await api('/conversations',{token:a.token,method:'POST',body:{target:b.id},status:403});});
test('MongoDB change streams deliver between separate Node instances',async()=>{const {a,b,cid}=await pair();const second=await createApplication({mongoUri:mongo.getUri(),dbName:'test'+sequence,demoMode:true,rateLimits:false});await second.listen(0,'127.0.0.1');try{const sb=wsClient(b.token);await sb.ready;await second.service.sendMessage(await second.service.authenticate(a.token),cid,{body:'Across Node instances',clientId:'cross-node-1'});await sb.next(event(cid,'chat.message'));}finally{await second.close();}});
test('accounts and messages persist across API restarts',async()=>{const {a,b,cid}=await pair();await send(a,cid,'Persisted in MongoDB','persist-001');await runtime.close();runtime=await createApplication({mongoUri:mongo.getUri(),dbName:'test'+sequence,demoMode:true,rateLimits:false});await runtime.listen(0,'127.0.0.1');base='http://127.0.0.1:'+runtime.server.address().port;assert.equal((await api('/conversations/'+cid,{token:b.token})).messages[0].body,'Persisted in MongoDB');});
test('chat rate limits apply across both transports',async()=>{const {a,cid}=await pair(),s=wsClient(a.token);await s.ready;for(let i=0;i<20;i++)await s.command('chat.send',cid,{body:'Message '+i,clientId:'rate-msg-'+i});assert.equal((await s.command('chat.send',cid,{body:'Too many',clientId:'rate-msg-21'},'error')).status,429);await send(a,cid,'HTTP too','rate-http-1',429);});

test('1 km boundary works without a venue and hides all coordinate fields',async()=>{
 const a=await user('Alpha'),b=await user('Beta'),c=await user('Charlie');
 for(const [u,lat] of [[a,0],[b,0.0089],[c,0.0091]]){await fix(u,lat,0);await api('/presence',{token:u.token,method:'POST',body:{category:'Pets'}});}
 const state=await api('/state',{token:a.token});assert.equal(state.nearby.count,1);assert.equal(state.nearby.categories.Pets,1);assert.deepEqual(state.circle.people.map(x=>x.id),[b.id]);assert.equal(state.places.length,0);
 const json=JSON.stringify(state);for(const k of ['coordinates','accuracy','timestamp','tokenHash','distance'])assert.ok(!json.includes('"'+k+'"'));
 await api('/conversations',{token:a.token,method:'POST',body:{target:b.id},status:201});await api('/conversations',{token:a.token,method:'POST',body:{target:c.id},status:403});
});
test('curated venue discovery excludes a place at 1.4 km',async()=>{
 const a=await user('Alpha');await fix(a,0,0);
 for(const [key,lat] of [['inside',0.008],['outside',0.0126]])await runtime.db.collection('places').insertOne({_id:key,name:key,lat,lon:0,radius:100,location:{type:'Point',coordinates:[0,lat]}});
 const places=await api('/places',{token:a.token});assert.deepEqual(places.map(x=>x.id),['inside']);
 await api('/presence',{token:a.token,method:'POST',body:{placeId:'inside',category:'Friends'},status:403});
});
test('mock, inaccurate and stale fixes revoke live presence; profile stays accessible',async()=>{
 for(const [name,patch] of [['Mock',{mocked:true}],['Poor',{accuracy:101}],['Old',{timestamp:Date.now()-40000}],['Disabled',{servicesEnabled:false}]]){
 const a=await user(name);await enter(a);await fix(a,55.8685,-4.284,patch,403);
 assert.equal(await runtime.db.collection('presence').findOne({_id:a.id}),null);assert.equal((await api('/state',{token:a.token})).locationReady,false);await api('/me',{token:a.token});
 }
});
test('implausible jump blocks live and requires cooldown',async()=>{
 const a=await user('Alpha');await enter(a);await fix(a,9.9,76.3,{timestamp:Date.now()+1000},403);await fix(a,55.8685,-4.284,{timestamp:Date.now()+2000},403);
 assert.equal((await api('/state',{token:a.token})).circle.presence,null);
});
test('location freshness expires discovery even before Mongo TTL deletion',async()=>{
 const a=await user('Alpha');await enter(a);runtime.service.clock=()=>Date.now()/1000+LOCATION_LEASE_SECONDS+1;
 const state=await api('/state',{token:a.token});assert.equal(state.locationReady,false);assert.equal(state.nearby.count,0);assert.equal(state.circle.presence,null);
 await runtime.service.cleanup();assert.equal(await runtime.db.collection('locations').findOne({_id:a.id}),null);assert.equal(await runtime.db.collection('presence').findOne({_id:a.id}),null);
});
test('valid movement updates discovery and closes a venue session after leaving',async()=>{
 const a=await user('Alpha');await enter(a);const now=Date.now();runtime.service.clock=()=>now/1000+30;
 await fix(a,55.875,-4.284,{timestamp:now+30000});assert.equal((await api('/state',{token:a.token})).circle.presence,null);
});
test('location is bound to the authenticated sign-in; old samples cannot refresh it',async()=>{
 const a=await user('Alpha');await enter(a);const old=await runtime.db.collection('locations').findOne({_id:a.id});
 const login=await api('/auth/login',{method:'POST',body:{email:'alpha@example.com',password:'Testing12345!'}});
 assert.equal((await api('/state',{token:login.token})).locationReady,false);
 await api('/presence',{token:login.token,method:'POST',body:{category:'Friends'},status:403});
 await fix(a,55.8685,-4.284,{timestamp:old.timestamp},403);
});

test('moving beyond 1 km preserves established chats',async()=>{
 const a=await user('Alpha'),b=await user('Beta');
 for(const u of [a,b]){await fix(u,0,0);await api('/presence',{token:u.token,method:'POST',body:{category:'Social'}});}
 const {id}=await api('/conversations',{token:a.token,method:'POST',body:{target:b.id},status:201});
 const sb=wsClient(b.token);await sb.ready;const now=Date.now();runtime.service.clock=()=>now/1000+30;
 await fix(a,0.01,0,{timestamp:now+30000});
 await api('/conversations/'+id,{token:a.token});assert.ok(await runtime.db.collection('conversations').findOne({_id:id}));await send(a,id,'Still connected','moving-chat-1');
});

test('five-minute location refresh preserves chats and replaces a single coordinate record',async()=>{
 const {a,b,cid}=await pair();await send(a,cid,'Keep this while locked','lock-msg-001');
 const now=Date.now();runtime.service.clock=()=>now/1000+301;
 // No background/lock deletion request. The five-minute worker sends a fresh fix.
 for(const u of [a,b]){const p=await runtime.db.collection('presence').findOne({_id:u.id});await fix(u,55.8685,-4.284,{timestamp:now+301000,liveSessionId:p.liveSessionId});}
 assert.equal((await api('/conversations/'+cid,{token:a.token})).messages[0].body,'Keep this while locked');
 assert.equal(await runtime.db.collection('locations').countDocuments({_id:a.id}),1);
 runtime.service.clock=()=>now/1000+301+LOCATION_LEASE_SECONDS+1;await runtime.service.cleanup();
 assert.equal(await runtime.db.collection('messages').countDocuments({conversationId:cid}),1);
 assert.equal(await runtime.db.collection('locations').countDocuments({_id:a.id}),0);
});
test('an old background worker cannot revoke or update a replacement session',async()=>{
 const a=await user('Alpha');await enter(a);const old=await runtime.db.collection('presence').findOne({_id:a.id});await enter(a);const current=await runtime.db.collection('presence').findOne({_id:a.id});
 await api('/location',{token:a.token,method:'DELETE',body:{liveSessionId:old.liveSessionId}});
 assert.equal((await runtime.db.collection('presence').findOne({_id:a.id})).liveSessionId,current.liveSessionId);
 await fix(a,55.8685,-4.284,{liveSessionId:old.liveSessionId},409);
 assert.equal((await runtime.db.collection('presence').findOne({_id:a.id})).liveSessionId,current.liveSessionId);
});
test('message pages use stable cursors without overlap',async()=>{
 const {a,cid}=await pair();const rows=Array.from({length:61},(_,i)=>({_id:i.toString(16).padStart(32,'0'),conversationId:cid,sender:a.id,body:'Message '+i,clientId:'page-'+i,created:100+i}));await runtime.db.collection('messages').insertMany(rows);
 const first=await api('/conversations/'+cid,{token:a.token});assert.equal(first.messages.length,50);assert.equal(first.hasMore,true);
 const older=await api('/conversations/'+cid+'/messages?before='+first.nextCursor,{token:a.token});assert.equal(older.messages.length,11);assert.equal(older.hasMore,false);
 assert.equal(new Set([...older.messages,...first.messages].map(x=>x.id)).size,61);
 await api('/conversations/'+cid+'/messages?before=bad-cursor',{token:a.token,status:422});
});

test('leaving Live preserves verified discovery location and normal expiry is not a location failure',async()=>{
 const a=await user('Alice');await enter(a);await api('/presence',{token:a.token,method:'DELETE'});
 const state=await api('/state',{token:a.token});assert.equal(state.locationReady,true);assert.equal(state.circle.presence,null);assert.ok(state.places.length>0);
 await api('/presence',{token:a.token,method:'POST',body:{category:'Friends',minutes:30}});
 await runtime.db.collection('presence').updateOne({_id:a.id},{$set:{expires:runtime.service.clock()-1}});await runtime.service.cleanup();
 const expired=await api('/state',{token:a.token});assert.equal(expired.locationReady,true);assert.equal(expired.circle.presence,null);
});
test('unread counts survive retries/reconnect and acknowledgements cannot consume unseen messages',async()=>{
 const {a,b,cid}=await pair(),eve=await user('Eve');const s=wsClient(b.token);await s.ready;
 const one=await send(a,cid,'First unread','unread-msg-1');await send(a,cid,'First unread','unread-msg-1');await s.next(event(cid,'chat.message'));
 let chats=await api('/conversations',{token:b.token});assert.equal(chats[0].unreadCount,1);assert.equal(chats[0].latestMessage.body,'First unread');assert.equal((await api('/conversations',{token:a.token}))[0].unreadCount,0);
 const two=await send(a,cid,'Still unread','unread-msg-2');
 await api('/conversations/'+cid+'/read',{token:eve.token,method:'POST',body:{messageIds:[one.id]},status:404});
 await api('/conversations/'+cid+'/read',{token:b.token,method:'POST',body:{messageIds:[one.id]}});await s.next(event(cid,'chat.read'));
 await api('/conversations/'+cid+'/read',{token:b.token,method:'POST',body:{messageIds:[one.id]}});
 assert.equal((await api('/conversations',{token:b.token}))[0].unreadCount,1);s.ws.terminate();
 const reconnect=wsClient(b.token);await reconnect.ready;assert.equal((await api('/state',{token:b.token})).chats[0].unreadCount,1);
 await api('/conversations/'+cid+'/read',{token:b.token,method:'POST',body:{messageIds:[two.id]}});assert.equal((await api('/state',{token:b.token})).chats[0].unreadCount,0);
});
test('gender registration is stored and anonymous discovery reveals only allowed attributes',async()=>{
 const a=await user('Alice');const {token}=await api('/auth/register',{method:'POST',body:{name:'Private Name',email:'gender@example.com',dob:'1997-01-01',password:'Testing12345!',gender:'Non-binary'},status:201});
 const b=await api('/me',{token});assert.equal(b.gender,'Non-binary');await enter(a);await enter({...b,token});
 const p=(await api('/circle',{token:a.token})).people[0];assert.equal(p.gender,'Non-binary');assert.ok(p.ageRange);for(const k of ['name','email','dob','location','lat','lon'])assert.equal(p[k],undefined);
 await api('/auth/register',{method:'POST',body:{name:'Bad',email:'badgender@example.com',dob:'1997-01-01',password:'Testing12345!',gender:'unknown'},status:422});
});

test('nearby place activity counts include only active eligible owners',async()=>{
 const {a,b}=await pair();await api('/activities',{token:a.token,method:'POST',body:{title:'Coffee together',category:'Friends',capacity:6},status:201});
 assert.equal((await api('/places',{token:b.token})).find(p=>p.id==='kelvingrove').activityCount,1);
 await api('/blocks',{token:b.token,method:'POST',body:{target:a.id}});
 assert.equal((await api('/places',{token:b.token})).find(p=>p.id==='kelvingrove').activityCount,0);
});

test('direct message events are private, authoritative and keep content out of the outbox',async()=>{
 const {a,b,cid}=await pair(),eve=await user('Eve'),sa=wsClient(a.token),sb=wsClient(b.token),se=wsClient(eve.token);await Promise.all([sa.ready,sb.ready,se.ready]);
 await send(a,cid,'Private event content','direct-event-1');const incoming=await sb.next(event(cid,'chat.message'));
 assert.equal(incoming.message.body,'Private event content');assert.equal(incoming.message.mine,false);assert.equal(incoming.chatPatch.unreadCount,1);assert.ok(incoming.chatPatch.summaryRevision>0);
 const own=await sa.next(event(cid,'chat.message'));assert.equal(own.message.mine,true);assert.equal(own.chatPatch.unreadCount,0);assert.equal(se.buffer.some(event(cid,'chat.message')),false);
 const outbox=await runtime.db.collection('events').find({conversationId:cid}).toArray();assert.equal(JSON.stringify(outbox).includes('Private event content'),false);
 const summary=await api('/conversations/'+cid+'/summary',{token:b.token});assert.equal(summary.messages,undefined);assert.equal(summary.unreadCount,1);await api('/conversations/'+cid+'/summary',{token:eve.token,status:404});
 await runtime.db.collection('locations').deleteOne({_id:b.id});
 assert.ok(await runtime.service.chatEvent({id:b.id,hash:digest(b.token)},{event:'chat.message',conversationId:cid,messageId:incoming.message.id}));
});

const device=(u,installationId,preview=false,token='fcm-token-'+installationId+'-123456789',stableChatIntent=false)=>api('/push/devices',{token:u.token,method:'POST',body:{installationId,token,preview,stableChatIntent}});
test('durable push queues once per message/device; only recipient devices receive anonymous private payloads',async()=>{
 const {a,b,cid}=await pair();await device(a,'alice-phone');await device(b,'bob-phone');await device(b,'bob-tablet',false,undefined,true);
 const m=await send(a,cid,'Private hello','push-client-123');await send(a,cid,'Private hello','push-client-123');
 assert.equal(await runtime.db.collection('pushJobs').countDocuments({}),2);
 await Promise.all([runtime.push.drain(),runtime.push.drain()]);await runtime.push.drain();assert.equal(deliveries.length,2);
 for(const p of deliveries){assert.equal(p.data.messageId,m.id);assert.equal(p.data.recipientId,b.id);assert.equal(p.data.conversationId,cid);assert.equal(p.notification.body,'New message. Open herenow to read it.');assert.ok(!p.notification.title.includes('Alice'));assert.equal(p.android.notification.tag,m.id);assert.equal(p.android.notification.channelId,'messages');assert.equal(p.android.notification.clickAction,p.token.includes('bob-tablet')?'com.herenow.social.OPEN_CHAT':undefined);assert.ok(!('lat' in p.data));}
});
test('push token rotation replaces old registration and preview opt-in controls message text',async()=>{
 const {a,b,cid}=await pair();await device(b,'bob-phone',false);await device(b,'bob-phone',true,'fresh-token-12345678901234567890');
 assert.equal(await runtime.db.collection('pushDevices').countDocuments({userId:b.id}),1);
 await send(a,cid,'Preview allowed','push-preview-123');await runtime.push.drain();assert.equal(deliveries[0].token,'fresh-token-12345678901234567890');assert.equal(deliveries[0].notification.body,'Preview allowed');
 await api('/push/devices',{token:b.token,method:'DELETE',body:{installationId:'bob-phone'}});await send(a,cid,'No more pushes','push-disabled-123');await runtime.push.drain();assert.equal(deliveries.length,1);
});
test('read messages and ended conversations do not produce queued push; logout removes registrations',async()=>{
 const {a,b,cid}=await pair();await device(b,'bob-phone');const m=await send(a,cid,'Already seen','push-seen-123');
 await api('/conversations/'+cid+'/read',{token:b.token,method:'POST',body:{messageIds:[m.id]}});await runtime.push.drain();assert.equal(deliveries.length,0);
 await send(a,cid,'Before logout','push-logout-123');await api('/auth/logout',{token:b.token,method:'POST'});await runtime.push.drain();assert.equal(deliveries.length,0);assert.equal(await runtime.db.collection('pushDevices').countDocuments({userId:b.id}),0);
});
test('temporary provider failure retries durable jobs; invalid tokens are removed',async()=>{
 const {a,b,cid}=await pair();await device(b,'bob-phone');await send(a,cid,'Retry delivery','push-retry-123');pushFailure=Object.assign(Error('unavailable'),{code:'messaging/server-unavailable'});
 await runtime.push.drain();assert.equal(await runtime.db.collection('pushJobs').countDocuments({status:'pending'}),1);pushFailure=null;await runtime.db.collection('pushJobs').updateMany({},{$set:{nextAt:new Date(0)}});await runtime.push.drain();assert.equal(deliveries.length,1);
 await send(a,cid,'Expired token','push-invalid-123');pushFailure=Object.assign(Error('invalid'),{code:'messaging/registration-token-not-registered'});await runtime.push.drain();assert.equal(await runtime.db.collection('pushDevices').countDocuments({userId:b.id}),0);
});
test('revoked/foreign auth cannot remove another session push registration',async()=>{
 const {a,b}=await pair();await device(b,'bob-phone');await api('/push/devices',{token:a.token,method:'DELETE',body:{installationId:'bob-phone'}});assert.equal(await runtime.db.collection('pushDevices').countDocuments({userId:b.id}),1);
 await api('/auth/logout',{token:b.token,method:'POST'});await api('/push/devices',{token:b.token,method:'POST',status:401,body:{installationId:'bob-phone',token:'fcm-token-123456789012345'}});
});

test('push diagnostics retain provider ID, remain account-scoped and never expose registration tokens',async()=>{
 const {a,b,cid}=await pair();const secret='fcm-secret-123456789012345678901234';await device(b,'bob-phone',false,secret);
 const m=await send(a,cid,'Do not put this text in diagnostics','push-diagnostics-123');await runtime.push.drain();
 const status=await api('/push/status',{token:b.token});assert.equal(status.registeredDevices.length,1);assert.equal(status.recent[0].outcome,'accepted');assert.equal(status.recent[0].providerMessageId,'fcm-test-id');assert.equal(status.recent[0].messageId,m.id);
 const text=JSON.stringify(status);assert.ok(!text.includes(secret));assert.ok(!text.includes('Do not put this text'));assert.ok(!text.includes('tokenHash'));
 const other=await api('/push/status',{token:a.token});assert.equal(other.registeredDevices.length,0);assert.equal(other.recent.length,0);
 assert.equal(deliveries[0].data.senderId,a.id);
});
test('push diagnostics identify provider rejection and a skipped already-read message',async()=>{
 const {a,b,cid}=await pair();await device(b,'bob-phone');await send(a,cid,'Mismatch','push-mismatch-123');pushFailure=Object.assign(Error('FCM project mismatch'),{code:'messaging/mismatched-credential'});await runtime.push.drain();
 let status=await api('/push/status',{token:b.token});assert.equal(status.recent[0].errorCode,'messaging/mismatched-credential');assert.equal(status.recent[0].providerMessageId,null);
 pushFailure=null;const m=await send(a,cid,'Read first','push-read-trace-123');await api('/conversations/'+cid+'/read',{token:b.token,method:'POST',body:{messageIds:[m.id]}});await runtime.push.drain();status=await api('/push/status',{token:b.token});const skipped=status.recent.find(x=>x.messageId===m.id);assert.equal(skipped.outcome,'skipped');assert.equal(skipped.reason,'message_read_or_unavailable');
});

test('resume sync returns only missed messages, retries do not advance sequence, and validates access',async()=>{
 const {a,b,cid}=await pair();
 const initial=await api('/conversations/'+cid,{token:b.token});assert.equal(initial.syncSeq,0);
 const sent=await send(a,cid,'Before lock','resume-001');
 const snapshot=await api('/conversations/'+cid,{token:b.token});assert.equal(snapshot.syncSeq,1);
 await send(a,cid,'Before lock','resume-001');
 await Promise.all([send(a,cid,'While locked one','resume-002'),send(b,cid,'While locked two','resume-003')]);
 const delta=await api('/conversations/'+cid+'/sync?after=1',{token:b.token});
 assert.equal(delta.syncSeq,3);assert.equal(delta.messages.length,2);assert.equal(delta.hasMore,false);assert.ok(delta.messages.every(m=>m.id!==sent.id));
 const empty=await api('/conversations/'+cid+'/sync?after=3',{token:b.token});assert.equal(empty.messages.length,0);
 const eve=await user('Eve');await api('/conversations/'+cid+'/sync?after=0',{token:eve.token,status:404});
 await api('/conversations/'+cid+'/sync?after=999',{token:b.token,status:409});
 await api('/conversations/'+cid+'/sync?after=NaN',{token:b.token,status:422});
 await api('/conversations/'+cid,{token:a.token,method:'DELETE'});
 await api('/conversations/'+cid+'/sync?after=3',{token:b.token,status:404});
});
test('resume sync pages without skipping equal-timestamp messages; cached oldest ID paginates correctly',async()=>{
 const {a,b,cid}=await pair();const created=Date.now()/1000;
 const rows=Array.from({length:205},(_,i)=>({_id:(i+1).toString(16).padStart(32,'0'),conversationId:cid,sender:a.id,clientId:'seed-'+i,body:'Message '+i,created,seq:i+1}));
 await runtime.db.collection('messages').insertMany(rows);await runtime.db.collection('conversations').updateOne({_id:cid},{$set:{messageSeq:205}});
 let after=0,ids=[];for(let i=0;i<3;i++){const d=await api('/conversations/'+cid+'/sync?after='+after,{token:b.token});ids.push(...d.messages.map(m=>m.id));after=d.syncSeq;assert.equal(d.hasMore,i<2);}
 assert.equal(after,205);assert.equal(new Set(ids).size,205);
 const older=await api('/conversations/'+cid+'/messages?beforeId='+rows[5]._id,{token:b.token});assert.equal(older.messages.length,5);assert.equal(older.hasMore,false);
});

test('30 minute discoverability expiry preserves HTTP/WS messaging, history, read state and push',async()=>{
 const {a,b,cid}=await pair();await device(b,'expired-live-phone');await send(a,cid,'Before timer','timer-before-1');
 await runtime.db.collection('presence').updateMany({},{$set:{expires:runtime.service.clock()-1}});await runtime.service.cleanup();
 assert.equal((await api('/state',{token:b.token})).circle.presence,null);assert.equal((await api('/state',{token:b.token})).chats[0].id,cid);
 await runtime.db.collection('locations').deleteMany({});
 const sb=wsClient(b.token);await sb.ready;const ack=await sb.command('chat.send',cid,{body:'After timer',clientId:'timer-after-1'});assert.ok(ack.data.id);
 const history=await api('/conversations/'+cid,{token:a.token});assert.equal(history.messages.length,2);
 await send(a,cid,'Push after expiry','timer-push-1');await runtime.push.drain();assert.ok(deliveries.some(d=>d.data.conversationId===cid));
});
test('Until I leave refreshes safety lease, then expires stale presence without deleting chat',async()=>{
 const {a,b,cid}=await pair();await enter(a,{minutes:0});const initial=await runtime.db.collection('presence').findOne({_id:a.id});assert.equal(initial.untilLeave,true);
 const now=Date.now();runtime.service.clock=()=>now/1000+300;
 await fix(a,55.8685,-4.284,{timestamp:now+300000,liveSessionId:initial.liveSessionId});
 const fresh=await runtime.db.collection('presence').findOne({_id:a.id});assert.ok(fresh.expires>initial.expires+290);
 runtime.service.clock=()=>fresh.expires+1;await runtime.service.cleanup();assert.equal(await runtime.db.collection('presence').findOne({_id:a.id}),null);await api('/conversations/'+cid,{token:a.token});
});
test('Live extension adds thirty minutes once and rejects stale session identifiers',async()=>{
 const a=await user('Extension');await enter(a,{minutes:30});const before=await runtime.db.collection('presence').findOne({_id:a.id});
 const body={liveSessionId:before.liveSessionId,requestId:'extension-retry-1'};
 const first=await api('/presence/extend',{token:a.token,method:'POST',body});const retry=await api('/presence/extend',{token:a.token,method:'POST',body});assert.equal(first.expires,before.expires+1800);assert.equal(retry.expires,first.expires);
 await api('/presence/extend',{token:a.token,method:'POST',body:{...body,liveSessionId:'0'.repeat(32)},status:409});
});
test('modified client cannot spoof sender, peer vibe or ready, or mass-assign profile',async()=>{
 const {a,b,cid}=await pair();
 await api('/conversations/'+cid+'/messages',{token:a.token,method:'POST',body:{body:'Authenticated sender only',clientId:'security-sender-1',senderId:b.id,sender:b.id},status:422});
 assert.equal(await runtime.db.collection('messages').countDocuments({conversationId:cid}),0);
 await send(a,cid,'Authenticated sender only','security-sender-1');
 assert.equal((await runtime.db.collection('messages').findOne({conversationId:cid})).sender,a.id);
 await api('/me',{token:a.token,method:'PATCH',body:{name:'Alice',interests:'Music',userId:b.id,role:'admin',banned:false,verified:true},status:422});
 const saved=await runtime.db.collection('users').findOne({_id:a.id});assert.equal(saved.role,undefined);assert.equal(saved.verified,undefined);assert.equal((await api('/me',{token:b.token})).name,'Bob');
 await api('/conversations/'+cid+'/vibe',{token:a.token,method:'POST',body:{value:true,userId:b.id,mutual:true,vibeA:true,vibeB:true},status:422});
 assert.equal((await api('/conversations/'+cid,{token:b.token})).liked,false);
 await api('/conversations/'+cid+'/ready',{token:a.token,method:'POST',body:{value:true,userId:b.id,readyA:true,readyB:true},status:422});
 await choice(a,cid,'ready',true,409);
 const eve=await user('Eve');for(const action of ['vibe','ready'])await choice(eve,cid,action,true,404);
});
test('operator injection and outsider history/notification target IDs do not grant access',async()=>{
 const {a,cid}=await pair(),eve=await user('Eve');
 await api('/auth/login',{method:'POST',body:{email:{$ne:null},password:{$ne:null}},status:422});
 await api('/conversations',{token:a.token,method:'POST',body:{target:{$ne:a.id}},status:422});
 for(const path of ['', '/summary','/messages','/sync?after=0'])await api('/conversations/'+cid+path,{token:eve.token,status:404});
 await api('/conversations/'+cid+'/read',{token:eve.token,method:'POST',body:{messageIds:['0'.repeat(32)]},status:404});
 await api('/conversations/'+cid,{status:401});
});

test('security: foreign installation and FCM token cannot overwrite recipient registration',async()=>{
 const {a,b}=await pair();await device(b,'victim-installation',false,'victim-fcm-token-123456789');
 await api('/push/devices',{token:a.token,method:'POST',body:{installationId:'victim-installation',token:'attacker-fcm-token-123456789'},status:409});
 await api('/push/devices',{token:a.token,method:'POST',body:{installationId:'attacker-installation',token:'victim-fcm-token-123456789'},status:409});
 assert.equal((await runtime.db.collection('pushDevices').findOne({installationId:'victim-installation'})).userId,b.id);
 await api('/auth/logout',{token:b.token,method:'POST'});await device(a,'victim-installation',false,'victim-fcm-token-123456789');
 assert.equal((await runtime.db.collection('pushDevices').findOne({installationId:'victim-installation'})).userId,a.id);
});
test('security: malformed cursors and Live selector operators are rejected without changing state',async()=>{
 const {a,cid}=await pair();const here=await runtime.db.collection('presence').findOne({_id:a.id});
 await api('/conversations/'+cid+'/messages?before='+Buffer.from('null').toString('base64url'),{token:a.token,status:422});
 await api('/conversations/'+cid+'/messages?beforeId=a&beforeId=b',{token:a.token,status:422});
 await api('/location',{token:a.token,method:'DELETE',body:{liveSessionId:{$ne:null}},status:422});
 await fix(a,55.8685,-4.284,{liveSessionId:{$ne:null}},422);
 assert.equal((await runtime.db.collection('presence').findOne({_id:a.id})).liveSessionId,here.liveSessionId);
 await api('/presence',{token:a.token,method:'POST',body:{placeId:null,category:'Friends',expiresAt:9999999999999},status:422});
});
test('security: report abuse and choice spam limits share state across transports and instances',async()=>{
 const {a,b,cid}=await pair();runtime.service.limits.enabled=true;
 const s=wsClient(a.token);await s.ready;
 for(let i=0;i<20;i++){if(i%2)await choice(a,cid,'vibe',!!(i%3));else await s.command('chat.vibe',cid,{value:!!(i%3)});}
 assert.equal((await s.command('chat.vibe',cid,{value:true},'error')).status,429);
 await choice(a,cid,'vibe',true,429);
 // Last accepted Vibe was true; establish mutual consent before readiness spam.
 await choice(b,cid,'vibe',true);
 for(let i=0;i<20;i++){if(i%2)await choice(a,cid,'ready',!!(i%3));else await s.command('meetup.ready',cid,{value:!!(i%3)});}
 assert.equal((await s.command('meetup.ready',cid,{value:true},'error')).status,429);
 await choice(a,cid,'ready',true,429);
 for(let i=0;i<5;i++)await api('/reports',{token:a.token,method:'POST',body:{target:b.id,reason:'Report abuse test'},status:201});
 await api('/reports',{token:a.token,method:'POST',body:{target:b.id,reason:'Another report'},status:429});
 const {SharedLimiter}=await import('../src/security.js');const another=new SharedLimiter(runtime.db);
 await assert.rejects(another.take('report:'+a.id,5,3600000),e=>e.status===429);
 assert.equal(await runtime.db.collection('reports').countDocuments({reporter:a.id}),5);
});
test('security: login account budget cannot reset with a new limiter or changed source IP',async()=>{
 const a=await user('Budget');runtime.service.limits.enabled=true;
 // Consume the shared account budget without spending CPU on 20 password hashes.
 for(let i=0;i<20;i++)await runtime.service.limits.take('login:budget@example.com',20,900000);
 await api('/auth/login',{method:'POST',body:{email:'BUDGET@example.com',password:'Testing12345!'},status:429});
 const {SharedLimiter}=await import('../src/security.js');runtime.service.limits=new SharedLimiter(runtime.db);
 await api('/auth/login',{method:'POST',body:{email:'budget@example.com',password:'wrongpassword'},status:429});
 assert.equal((await api('/me',{token:a.token})).id,a.id);
});
test('security: forbidden WS event is suppressed after authorization fails',async()=>{
 const {a,cid}=await pair(),eve=await user('EventOutsider'),s=wsClient(eve.token);await s.ready;
 await runtime.db.collection('events').insertOne({_id:'f'.repeat(32),scope:'users',users:[eve.id],event:'chat.message',conversationId:cid,createdAt:new Date(),expiresAt:new Date(Date.now()+60000)});
 await assert.rejects(s.next(d=>d.event==='chat.message'&&d.conversationId===cid,250),/Timed out/);
 s.ws.send(JSON.stringify({type:'join',conversationId:cid,requestId:'illegal-join'}));assert.equal((await s.next(d=>d.requestId==='illegal-join')).status,422);
});
test('security: unsupported content type and oversized HTTP requests rejected safely',async()=>{
 const a=await user('Payload');
 const type=await fetch(base+'/me',{method:'PATCH',headers:{Authorization:'Bearer '+a.token,'Content-Type':'text/plain'},body:'hello'});assert.equal(type.status,415);
 const big=await fetch(base+'/me',{method:'PATCH',headers:{Authorization:'Bearer '+a.token,'Content-Type':'application/json'},body:JSON.stringify({name:'a'.repeat(20000),interests:''})});assert.equal(big.status,413);assert.equal(big.headers.get('cache-control'),'no-store');assert.ok(!(await big.text()).includes('stack'));
});
test('security: legacy password hashes upgrade only after valid password',async()=>{
 const a=await user('Legacy');const {scryptSync}=await import('node:crypto');const salt='01'.repeat(16);const legacy=salt+':'+scryptSync('Testing12345!',Buffer.from(salt,'hex'),64,{N:16384,r:8,p:1}).toString('hex');
 await runtime.db.collection('users').updateOne({_id:a.id},{$set:{password:legacy}});
 await api('/auth/login',{method:'POST',body:{email:'legacy@example.com',password:'Wrong12345!'},status:401});assert.equal((await runtime.db.collection('users').findOne({_id:a.id})).password,legacy);
 await api('/auth/login',{method:'POST',body:{email:'legacy@example.com',password:'Testing12345!'}});assert.ok((await runtime.db.collection('users').findOne({_id:a.id})).password.startsWith('scrypt$131072$8$1$'));
});

test('security: concurrent quota claims across instances remain atomic and private',async()=>{
 const {SharedLimiter}=await import('../src/security.js');const a=new SharedLimiter(runtime.db),b=new SharedLimiter(runtime.db);
 const results=await Promise.allSettled(Array.from({length:24},(_,i)=>(i%2?a:b).take('test-private-account@example.com',10,86400000)));
 assert.equal(results.filter(x=>x.status==='fulfilled').length,10);assert.ok(results.filter(x=>x.status==='rejected').every(x=>x.reason.status===429));
 const rows=await runtime.db.collection('rateLimits').find({}).toArray();assert.ok(!JSON.stringify(rows).includes('test-private-account'));assert.equal(rows[0].count,24);
});
test('security: malformed and oversized WS frames cannot invoke commands',async()=>{
 const a=await user('Frame');const s=wsClient(a.token);await s.ready;
 s.ws.send('null');assert.equal((await s.next(d=>d.type==='error')).status,422);
 s.ws.send(JSON.stringify({type:'ping',senderId:'spoof'}));assert.equal((await s.next(d=>d.type==='error')).status,422);
 const closed=new Promise(resolve=>s.ws.once('close',resolve));s.ws.send('x'.repeat(17*1024));assert.equal(await closed,1009);
});
test('security: protected routes deny absent authentication before data access',async()=>{
 const cid='0'.repeat(32);
 for(const [method,path] of [['GET','/me'],['PATCH','/me'],['DELETE','/me'],['GET','/state'],['GET','/circle'],['GET','/places'],['POST','/location'],['POST','/presence'],['DELETE','/presence'],['GET','/conversations'],['POST','/conversations'],['GET','/conversations/'+cid],['POST','/conversations/'+cid+'/messages'],['POST','/conversations/'+cid+'/vibe'],['POST','/conversations/'+cid+'/ready'],['POST','/blocks'],['POST','/reports'],['POST','/push/devices'],['GET','/push/status']])await api(path,{method,status:401});
});
