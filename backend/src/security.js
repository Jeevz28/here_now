import {digest,fail} from './core.js';
import proxyaddr from 'proxy-addr';
/** Fixed windows shared by all API instances. No email/IP/token stored in clear. */
export class SharedLimiter {
 constructor(db,enabled=true){this.collection=db.collection('rateLimits');this.enabled=enabled;}
 async take(key,limit,windowMs=60000){
  if(!this.enabled)return;
  const now=Date.now(),window=Math.floor(now/windowMs),_id=digest(key+':'+windowMs+':'+window);
  const update={$inc:{count:1},$setOnInsert:{expiresAt:new Date((window+2)*windowMs)}};
  let row;
  try{row=await this.collection.findOneAndUpdate({_id},update,{upsert:true,returnDocument:'after'});}catch(e){if(e.code!==11000)throw e;row=await this.collection.findOneAndUpdate({_id},{$inc:{count:1}},{returnDocument:'after'});}
  if(!row||row.count>limit)fail(429,'Too many requests. Try again shortly.');
 }
}
export function proxyTrust(value=process.env.TRUST_PROXY_CIDRS||''){
 const entries=value.split(',').map(x=>x.trim()).filter(Boolean);
 // No blanket hop count or boolean trust; only explicitly configured network ranges.
 if(entries.some(x=>!/[.:]/.test(x)||['0.0.0.0/0','::/0'].includes(x)))throw new Error('TRUST_PROXY_CIDRS must contain restricted proxy IPs/CIDRs.');
 return entries.length?proxyaddr.compile(entries):()=>false;
}
export const clientAddress=(req,trust)=>proxyaddr(req,trust);
