import {createFcmSender,startPushWorker} from './push.js';
import {createServer} from 'node:http';
import {connectDatabase} from './database.js';
import {Service} from './service.js';
import {createHttpApp} from './http.js';
import {attachRealtime} from './realtime.js';
export async function createApplication({mongoUri,dbName='herenow',demoMode=false,origins=[],rateLimits=true,clock,cleanupIntervalMs=5000,pushSender,pushIntervalMs=1000,...socketOptions}){
  const {client,db}=await connectDatabase(mongoUri,dbName);
  const service=new Service(client,db,{demoMode,clock});
  await service.cleanup();
  const app=createHttpApp(service,{origins,rateLimits});const server=createServer(app);
  let realtime;
  try{realtime=await attachRealtime(server,service,{origins,...socketOptions});}catch(e){await client.close();throw e;}
  const sender=pushSender===undefined?await createFcmSender():pushSender;service.pushConfigured=!!sender;
  const push=startPushWorker(service,sender,{intervalMs:pushIntervalMs});
  let sweeping=false;
  const cleaner=setInterval(async()=>{if(sweeping)return;sweeping=true;try{await service.cleanup();}catch(e){console.error('Expiry cleanup failed:',e.name);}finally{sweeping=false;}},cleanupIntervalMs);cleaner.unref();
  return {app,server,service,db,realtime,push,async listen(port=8000,host='0.0.0.0'){await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,host,resolve);});return server.address();},async close(){clearInterval(cleaner);await push.close();await realtime.close();if(server.listening){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}await client.close();}};
}
