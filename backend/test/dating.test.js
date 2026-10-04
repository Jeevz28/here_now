import {test} from 'node:test';import assert from 'node:assert/strict';
import {datingCompatible} from '../src/dating.js';
const genders=['Male','Female','Non-binary','Prefer not to say'];
const allowed={Men:['Male'],Women:['Female'],'Men & Women':['Male','Female'],Everyone:genders};
test('dating: all 256 gender/preference pairs require mutual acceptance',()=>{
 for(const ga of genders)for(const gb of genders)for(const pa of Object.keys(allowed))for(const pb of Object.keys(allowed)){
  const a={gender:ga,datingPreference:pa,dob:'1997-01-01'},b={gender:gb,datingPreference:pb,dob:'1997-01-01'};
  assert.equal(datingCompatible(a,b),allowed[pa].includes(gb)&&allowed[pb].includes(ga));
 }
});
test('dating: missing preference never implies orientation; adult rule fail-closed',()=>{
 const a={gender:'Male',datingPreference:'Everyone',dob:'1997-01-01'};
 for(const b of [{...a,datingPreference:undefined},{...a,datingPreference:'invalid'},{...a,dob:'2020-01-01'},{...a,dob:'invalid'}])assert.equal(datingCompatible(a,b),false);
});
