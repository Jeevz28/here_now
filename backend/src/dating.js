import {age,fail} from './core.js';
export const DATING_PREFERENCES=['Men','Women','Men & Women','Everyone'];
export function acceptsGender(preference,gender){
 if(!['Male','Female','Non-binary','Prefer not to say'].includes(gender))return false;
 return preference==='Everyone'||(preference==='Men'&&gender==='Male')||(preference==='Women'&&gender==='Female')||(preference==='Men & Women'&&['Male','Female'].includes(gender));
}
export function datingCompatible(a,b){return !!a&&!!b&&age(a.dob)>=18&&age(a.dob)<=110&&age(b.dob)>=18&&age(b.dob)<=110&&acceptsGender(a.datingPreference,b.gender||'Prefer not to say')&&acceptsGender(b.datingPreference,a.gender||'Prefer not to say');}
export function requireDatingPreference(u){if(!DATING_PREFERENCES.includes(u.datingPreference))fail(409,'Choose who you are interested in before entering Dating.');if(!(age(u.dob)>=18&&age(u.dob)<=110))fail(403,'Dating is available to adults aged 18 and over.');}
