import {AsyncLocalStorage} from 'node:async_hooks';
import {performance} from 'node:perf_hooks';
const context=new AsyncLocalStorage(),metrics=new Map();
export const perfEnabled=process.env.PERF_METRICS==='1';
export function metric(name,ms){
 if(!perfEnabled||!Number.isFinite(ms))return;
 const row=metrics.get(name)||{count:0,total:0,max:0,recent:[]};row.count++;row.total+=ms;row.max=Math.max(row.max,ms);row.recent.push(ms);if(row.recent.length>256)row.recent.shift();metrics.set(name,row);
}
export function performanceSnapshot(){return Object.fromEntries([...metrics].map(([name,v])=>[name,{count:v.count,meanMs:v.total/v.count,maxMs:v.max,p95Ms:v.recent.slice().sort((a,b)=>a-b)[Math.floor((v.recent.length-1)*.95)]}]));}
export async function measured(name,fn){if(!perfEnabled)return fn();const started=performance.now();try{return await fn();}finally{metric(name,performance.now()-started);}}
export function performanceMiddleware(req,res,next){
 if(!perfEnabled)return next();
 const started=performance.now(),row={dbMs:0,commands:0};
 // Header durations are server-local. Never subtract client/server wall clocks.
 const json=res.json.bind(res);res.json=body=>{res.setHeader('Server-Timing',`app;dur=${(performance.now()-started).toFixed(2)},db;dur=${row.dbMs.toFixed(2)},db_commands;desc="${row.commands}"`);return json(body);};
 res.once('finish',()=>metric('http.'+(req.route?.path||'middleware')+'.'+res.statusCode,performance.now()-started));
 context.run(row,next);
}
export function observeDatabase(client){
 if(!perfEnabled)return;
 // No commands, predicates, IDs, chat text, credentials or coordinates retained.
 client.on('commandSucceeded',event=>{if(['getMore','hello','endSessions'].includes(event.commandName))return;const row=context.getStore();if(row){row.dbMs+=event.duration;row.commands++;}metric('db.'+event.commandName,event.duration);});
 client.on('commandFailed',()=>metric('db.failure',1));
}
