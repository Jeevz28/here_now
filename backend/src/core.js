import { randomBytes, scrypt as rawScrypt, timingSafeEqual, createHash } from 'node:crypto';
import { promisify } from 'node:util';
import { z } from 'zod';
const scrypt = promisify(rawScrypt);
export const id = () => randomBytes(16).toString('hex');
export const digest = value => createHash('sha256').update(value).digest('hex');
export class ApiError extends Error { constructor(status, message) { super(message); this.status = status; } }
export const fail = (status, message) => { throw new ApiError(status, message); };
export const text = (min, max) => z.string().trim().min(min).max(max);
export const identifier = z.string().regex(/^[a-f0-9]{32}$/);
export const schemas = {
  credentials: z.object({ email: z.string().trim().email().max(254).transform(s => s.toLowerCase()), password: z.string().min(10).max(128) }),
  profile: z.object({ name: text(1, 30), interests: text(0, 120) }),
  location: z.object({lat:z.number().finite().min(-90).max(90),lon:z.number().finite().min(-180).max(180),accuracy:z.number().finite().min(0),timestamp:z.number().finite(),mocked:z.boolean(),servicesEnabled:z.boolean(),permissionGranted:z.boolean()}),
  presence: z.object({placeId:text(1,80).nullable().default(null),category:z.enum(['Friends','Dating','Sports','Pets','Social','Gaming']),minutes:z.union([z.literal(0),z.literal(30),z.literal(60),z.literal(120)]).default(60)}),
  target: z.object({target:identifier}),
  choice: z.object({value:z.boolean()}),
  message: z.object({body:text(1,1000),clientId:z.string().regex(/^[a-zA-Z0-9_-]{8,100}$/)}),
  read: z.object({messageIds:z.array(identifier).min(1).max(100)}),
  activity: z.object({title:text(3,70),capacity:z.number().int().min(2).max(30),category:z.enum(['Friends','Sports']).default('Sports')}),
  report: z.object({target:identifier,reason:text(5,1000)}),
};
schemas.register = schemas.credentials.extend({name:text(1,30),gender:z.enum(['Male','Female','Non-binary','Prefer not to say']).default('Prefer not to say'),dob:z.string().regex(/^\d{4}-\d{2}-\d{2}$/).refine(v=>Number.isFinite(Date.parse(v)) && new Date(v).toISOString().slice(0,10)===v,'Invalid date of birth'),interests:text(0,120).default('Coffee, music, outdoors')});
export async function hashPassword(password, salt=randomBytes(16).toString('hex')) {
  const key=await scrypt(password,Buffer.from(salt,'hex'),64,{N:16384,r:8,p:1});
  return `${salt}:${key.toString('hex')}`;
}
export async function verifyPassword(password, stored) {
  const candidate=await hashPassword(password,stored.split(':')[0]);
  return timingSafeEqual(Buffer.from(candidate),Buffer.from(stored));
}
export function age(dob, now=new Date()) {
  const born=new Date(dob);let n=now.getUTCFullYear()-born.getUTCFullYear();
  if(now.getUTCMonth()<born.getUTCMonth() || (now.getUTCMonth()===born.getUTCMonth() && now.getUTCDate()<born.getUTCDate())) n--;
  return n;
}
export function own(u) {return {id:u._id,name:u.name,email:u.email,alias:u.alias,gender:u.gender||'Prefer not to say',interests:u.interests};}
export function anonymous(u) {const n=age(u.dob),start=Math.max(18,Math.floor(n/5)*5);return {id:u._id,alias:u.alias,gender:u.gender||'Prefer not to say',ageRange:`${start}–${Math.floor(n/5)*5+4}`,interests:u.interests};}
export function distance(a,b,c,d) {
  const r=Math.PI/180;const x=Math.sin((a-c)*r/2)**2+Math.cos(a*r)*Math.cos(c*r)*Math.sin((b-d)*r/2)**2;
  return 6371000*2*Math.asin(Math.sqrt(Math.min(1,x)));
}
export class Limiter {
  constructor(){this.buckets=new Map();}
  take(key,limit,windowMs=60000){const now=Date.now();let b=this.buckets.get(key);if(!b||b.end<=now){b={count:0,end:now+windowMs};this.buckets.set(key,b);}if(++b.count>limit)fail(429,'Too many requests. Try again shortly.');if(this.buckets.size>1000)for(const [k,v] of this.buckets)if(v.end<=now)this.buckets.delete(k);}
}
export function errorData(error) {
  if(error instanceof z.ZodError)return {status:422,detail:error.issues.map(x=>`${x.path.join('.')}: ${x.message}`).join('; ')};
  if(error instanceof ApiError)return {status:error.status,detail:error.message};
  if(error?.code===11000)return {status:409,detail:'This record already exists.'};
  return {status:500,detail:'The server could not complete this request.'};
}
