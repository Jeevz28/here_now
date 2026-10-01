import {clientAddress} from './security.js';
import {measured,metric} from './performance.js';
import {WebSocketServer,WebSocket} from 'ws';
import {z} from 'zod';
import {Limiter,errorData,fail,identifier} from './core.js';
const command=z.object({type:z.enum(['chat.send','chat.vibe','meetup.ready']),requestId:z.string().min(1).max(100),conversationId:identifier,payload:z.unknown()}).strict();
export async function attachRealtime(server,service,{origins=[],authTimeoutMs=5000,pingIntervalMs=25000,trust=()=>false}={}){
  const wss=new WebSocketServer({noServer:true,maxPayload:16*1024,perMessageDeflate:false});
  const connections=new Set(),limiter=new Limiter();
  let stopping=false;
  function send(ws,data){if(ws.readyState!==WebSocket.OPEN)return;if(ws.bufferedAmount>256*1024){ws.close(1013,'Client is too slow');return;}ws.send(JSON.stringify(data));}
  function expire(ws){send(ws,{type:'auth.expired'});ws.close(4401,'Session expired');}
  const upgrade=(req,socket,head)=>{
    if(req.url!=='/ws'){socket.write('HTTP/1.1 404 Not Found\r\n\r\n');socket.destroy();return;}
    const origin=req.headers.origin;
    // Native React Native clients send an Origin header (for example
    // http://10.0.2.2) even though they are not browser clients.  An empty
    // allow-list means local development, so do not reject that connection.
    // When WEB_ORIGINS is configured, enforce it for every origin-bearing
    // browser or native request.
    if(origins.length>0&&origin&&!origins.includes(origin)){socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');socket.destroy();return;}
    const ip=clientAddress(req,trust);
    try{limiter.take('upgrade:'+ip,30);if(connections.size>=500||[...connections].filter(s=>s.clientIp===ip).length>=20)fail(429,'Connection limit reached.');}
    catch{socket.write('HTTP/1.1 429 Too Many Requests\r\n\r\n');socket.destroy();return;}
    wss.handleUpgrade(req,socket,head,ws=>{ws.clientIp=ip;wss.emit('connection',ws,req);});
  };
  server.on('upgrade',upgrade);
  wss.on('connection',ws=>{
    connections.add(ws);ws.alive=true;ws.pending=0;let queue=Promise.resolve();
    const timer=setTimeout(()=>ws.close(4401,'Authenticate first'),authTimeoutMs);timer.unref();
    ws.on('error',()=>{});
    ws.on('pong',()=>{ws.alive=true;});
    ws.on('close',()=>{clearTimeout(timer);connections.delete(ws);});
    ws.on('message',(raw,binary)=>{
      if(binary){ws.close(1003,'JSON text frames only');return;}
      if(++ws.pending>10){ws.close(1008,'Too many pending commands');return;}
      queue=queue.then(async()=>{
        if(ws.readyState!==WebSocket.OPEN)return;
        let msg;
        try{
          limiter.take('frames:'+ws.clientIp,240);
          try{msg=JSON.parse(raw.toString());}catch{fail(422,'Invalid JSON frame.');}
          if(!ws.principal){
            const auth=z.object({type:z.literal('auth'),token:z.string().min(20).max(200)}).strict().parse(msg);
            const p=await service.authenticate(auth.token);
            if([...connections].filter(s=>s.principal?.id===p.id).length>=5)fail(429,'Too many sessions for this account.');
            if(ws.readyState!==WebSocket.OPEN)return;
            ws.principal=p;clearTimeout(timer);send(ws,{type:'ready',protocol:1});return;
          }
          // Revalidate revocation/expiry for every frame, including ping.
          ws.principal=await service.authenticateHash(ws.principal.hash);
          limiter.take('user:'+ws.principal.id,120);
          if(msg?.type==='ping'){z.object({type:z.literal('ping')}).strict().parse(msg);send(ws,{type:'pong'});return;}
          const d=command.parse(msg);let result;
          if(d.type==='chat.send')result=await measured('ws.chat.send',()=>service.sendMessage(ws.principal,d.conversationId,d.payload));
          if(d.type==='chat.vibe')result=await measured('ws.chat.vibe',()=>service.vibe(ws.principal,d.conversationId,d.payload));
          if(d.type==='meetup.ready')result=await measured('ws.meetup.ready',()=>service.ready(ws.principal,d.conversationId,d.payload));
          send(ws,{type:'ack',requestId:d.requestId,data:result});
        }catch(error){const e=errorData(error);send(ws,{type:'error',requestId:typeof msg?.requestId==='string'?msg.requestId:undefined,status:e.status,detail:e.detail});if(e.status===401||!ws.principal)expire(ws);}
      }).catch(()=>ws.close(1011,'Server error')).finally(()=>{ws.pending--;});
    });
  });
  // Transactional outbox: change streams see inserts only after MongoDB commit.
  // This also delivers changes made by another Node process or the operator CLI.
  const stream=service.db.collection('events').watch([{$match:{operationType:'insert'}}],{maxAwaitTimeMS:250});
  await stream.tryNext(); // Open cursor before accepting HTTP writes; collection starts empty on first boot.
  let delivery=Promise.resolve();
  async function publish(e){
    const targets=[...connections].filter(ws=>ws.principal&&(e.scope==='all'||e.users.includes(ws.principal.id))&&(!e.tokenHash||e.tokenHash===ws.principal.hash));
    // Independent recipient snapshots may run concurrently, but limit database
    // pressure and finish this event before the next outbox event is delivered.
    for(let start=0;start<targets.length;start+=4){
      await Promise.all(targets.slice(start,start+4).map(async ws=>{
        let data={};
        try{
          if(['chat.created','chat.message','chat.changed','meetup.changed','chat.read'].includes(e.event))data=await service.chatEvent(ws.principal,e);
          else await service.authenticateHash(ws.principal.hash);
        }catch(error){if(error.status===401){expire(ws);return;}if(![403,404].includes(error.status))ws.close(1012,'Resynchronize state');return;}
        if(e.event==='session.revoked'){expire(ws);return;}
        metric('ws.outboxToSend',Date.now()-new Date(e.createdAt).getTime());send(ws,{type:'event',event:e.event,id:e._id,...(e.conversationId?{conversationId:e.conversationId}:{}),...data});
      }));
    }
  }
  const consume=(async()=>{
    try{for await(const change of stream){delivery=delivery.then(()=>publish(change.fullDocument));await delivery;}}
    catch(error){if(!stopping){console.error('Live event stream unavailable; forcing client resync.');for(const ws of connections)ws.close(1012,'Event stream interrupted');}}
  })();
  const ping=setInterval(async()=>{
    for(const ws of connections){if(!ws.alive){ws.terminate();continue;}ws.alive=false;ws.ping();if(ws.principal){try{await service.authenticateHash(ws.principal.hash);}catch{expire(ws);}}}
  },pingIntervalMs);ping.unref();
  return {wss,connections,async close(){stopping=true;clearInterval(ping);server.off('upgrade',upgrade);for(const ws of connections)ws.terminate();await stream.close();await consume;await new Promise(resolve=>wss.close(resolve));}};
}
