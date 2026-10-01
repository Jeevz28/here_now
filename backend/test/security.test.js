import {test} from 'node:test';
import assert from 'node:assert/strict';
import {proxyTrust,clientAddress} from '../src/security.js';
import {errorData,verifyPassword} from '../src/core.js';
import {schemas} from '../src/core.js';
test('untrusted forwarding headers cannot choose rate-limit address',()=>{
 const req={socket:{remoteAddress:'203.0.113.8'},headers:{'x-forwarded-for':'198.51.100.1'}};
 assert.equal(clientAddress(req,proxyTrust('')),'203.0.113.8');
 assert.equal(clientAddress(req,proxyTrust('10.0.0.0/24')),'203.0.113.8');
 req.socket.remoteAddress='10.0.0.2';assert.equal(clientAddress(req,proxyTrust('10.0.0.0/24')),'198.51.100.1');
 req.headers['x-forwarded-for']='192.0.2.9, 198.51.100.1';assert.equal(clientAddress(req,proxyTrust('10.0.0.0/24')),'198.51.100.1');
 for(const value of ['true','1','0.0.0.0/0','::/0'])assert.throws(()=>proxyTrust(value));
});
test('validation and unexpected failures never echo payload or stack',()=>{
 try{schemas.choice.parse({value:true,'private-test-secret':true});assert.fail();}catch(e){assert.deepEqual(errorData(e),{status:422,detail:'Invalid request data.'});}
 assert.deepEqual(errorData(new Error('mongodb://private-test-secret')),{status:500,detail:'The server could not complete this request.'});
});
test('malformed stored password hash safely fails verification',async()=>{
 for(const value of ['', 'broken', 'scrypt$999999999$8$1$bad$bad'])assert.equal(await verifyPassword('Testing12345!',value),false);
});
