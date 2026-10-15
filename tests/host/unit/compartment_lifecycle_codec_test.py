#!/usr/bin/env python3
"""Offline synthetic lifecycle60 codec assertions. Never guest evidence."""
import argparse,hashlib,json,struct,sys,time,types
from pathlib import Path
sys.dont_write_bytecode=True
MASK=(1<<64)-1;UNOBS=1<<63;SEAL=0xa69e572d43f18bc5;CACHE=0o100000000
N=1032;START=1_000_000;DRIVER=4000
ERR=(-12,-12,-12,-12,-12,-125,-12,-12,-12,-12,-125,-125,-12,-12,-12,-12,-5,-12,-125,-125,-11,-11,-12,-12)

def pack(a):return struct.pack('<'+str(len(a))+'Q',*(v&MASK for v in a))
def unpack(b):return list(struct.unpack('<'+str(len(b)//8)+'Q',b))
def fnv(b):
 h=1469598103934665603
 for c in b:h=((h^c)*1099511628211)&MASK
 return h
def seal(a):
 a=list(a);a[62]=fnv(pack(a[:62]));a[63]=(a[62]^SEAL)|1;return a
def pin(p):
 b=p.read_bytes();return {'path':str(p.resolve()),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def record(saved,index):return unpack(saved['attempts.bin'][index*512:(index+1)*512])
def put_record(saved,index,row):
 b=bytearray(saved['attempts.bin']);b[index*512:(index+1)*512]=pack(seal(row));saved['attempts.bin']=bytes(b)
 if index==N-1:
  c=bytearray(saved['control.bin']);c[512:1024]=pack(seal(row));saved['control.bin']=bytes(c)
def replace_words(saved,name,offset,array):
 b=bytearray(saved[name]);b[offset:offset+len(array)*8]=pack(array);saved[name]=bytes(b)
def gold(manifest):
 m=unpack(manifest);nonce=m[4];entries=[m[8+16*i:8+16*i+16]for i in range(4)]
 ledger=[0]*1024;head=[0]*64;head[:8]=[0x433946444c454447,1,8192,nonce,DRIVER,9,256,2];ledger[:64]=seal(head)
 for fd in range(256):ledger[64+fd]=1 if fd<9 else MASK-8;ledger[320+fd]=(CACHE|(0 if fd<3 else 2))if fd<9 else MASK-8
 ledger_hash=fnv(pack(ledger));h=[0]*64;h[:18]=[0x4339484541444552,1,512,nonce,DRIVER,1,N,8,24,1000,512,4096,8192,4096,8192,512,START,START+3_600_000_000];h[24:33]=list(range(9));h=seal(h)
 saved={'control.bin':pack(h)+bytes(3584),'child.raw':bytes(8192),'attempts.bin':bytes(N*512),'evidence.bin':bytes(N*4096),'fd-ledger.bin':pack(ledger),'driver-status':bytes(512),'wos-lifecycle60-manifest.bin':manifest}
 attempts=bytearray(saved['attempts.bin']);evidence=bytearray(saved['evidence.bin']);last=None;last_ready=None;last_b=None
 limits=[1_000_000]*32;limits[:4]=[10,10,1<<20,1<<20];limits[14:16]=[256,256]
 for i in range(N):
  kind=0 if i<8 else 1 if i<32 else 2;mode=2 if kind==1 else 1+(i&1);start=START+1000+i*100_000
  inp=[0]*64;inp[:16]=[0x4131524157494e50,1,512,nonce,i+1,mode,DRIVER,start,start+60_000_000,100,1,0,256,0,0,0];inp[16:48]=limits;inp[48:64]=[4096,2048,8192,32,0x4141,4096,1,0,77,3,CACHE|2,1024,256,16,10000,nonce]
  row=[0]*64;row[:13]=[0x4339415454454d50,1,512,nonce,i,i+1,kind,mode,3,start,start+60_000_000,ERR[i-8]&MASK if kind==1 else 10000+i,0 if kind==1 else 10000+i];row[19:24]=[UNOBS,UNOBS,0,0,fnv(pack(inp))];row[27:36]=[3,77,3,start+1,0,ledger_hash,3,100+mode,entries[mode][1]];row[39:42]=[DRIVER,1000+(i+1)*1000,start+2 if kind==1 else start+10]
  if mode==2:row[36:39]=[3,103,entries[3][1]]
  ready=[0]*256;a=[0]*32;b=[0]*32
  if kind!=1:
   ready[:32]=[0x4131524157525054,1,2048,1,nonce,i+1,mode,mode,1,0,100,100,100,100,1,0,256,255 if mode==2 else 127,0,0,0,5000,DRIVER,start,start+1,start+2,start+60_000_000,16,255,CACHE|2,77,3]
   ready[48:64]=[0,CACHE|2,0,10,0x10000,10,0,MASK,MASK,16,MASK^16,0,0,32,0o100600,100]
   ready[64:96]=limits;ready[96:99]=[0x20000,entries[mode][3],entries[mode][3]]
   if mode==2:ready[99:102]=[0x30000000,0x30000000+entries[3][3],0x54494e593431484f]
   ready[105:112]=[1,0,0x1040,56,entries[mode][4],4096,0];ready[112:128]=[8192,0,512,100,0x4141,4096,100,100,0o600,1,nonce,i+1,16,MASK^16,nonce,i+1]
   for off,value in [(128,32),(144,32),(160,10),(176,0),(192,1),(208,1)]:ready[off:off+16]=[value]*16
   ready[224:240]=[nonce,0,1,100,100,100,100,1,1,1 if mode==2 else 0,0x21000,0x22000,13 if mode==2 else 12,entries[3][3]if mode==2 else 0,0,0];ready[255]=nonce
   for hb,round_,when,checks in [(a,17,start+4,5001),(b,18,start+6,5002)]:
    hb[:24]=[0x41314c4956454842,1,nonce,i+1,mode,round_,when,32,32,10,0,round_,MASK^round_,checks,0,0,0,10,0x10000,0,1,1,100,1];hb[31]=round_
   row[13:23]=[start+3,17,18,start+5,start+7,start+8,0,10000+i,9,start+9];row[24:27]=[fnv(pack(ready)),fnv(pack(a)),fnv(pack(b))]
  attempts[i*512:(i+1)*512]=pack(seal(row));slot=pack(inp)+pack(ready)+pack(a)+pack(b)+bytes(1024);evidence[i*4096:(i+1)*4096]=slot
  last,last_ready,last_b=inp,ready,b
 saved['attempts.bin']=bytes(attempts);saved['evidence.bin']=bytes(evidence)
 current=record(saved,N-1);saved['control.bin']=pack(h)+pack(current)+bytes(3072)
 raw=bytearray(8192);raw[:512]=pack(last);raw[1024:1280]=pack(last_b);raw[4096:6144]=pack(last_ready);saved['child.raw']=bytes(raw)
 status=[0]*64;status[:18]=[0x4339535441545553,1,512,nonce,N,8,24,1000,N,START,START+3_600_000_000,DRIVER,1,0,0,current[40]+100,current[41]+20,ledger_hash];saved['driver-status']=pack(seal(status))
 return saved

def adjust_ledger(saved,fd,field,value):
 ledger=unpack(saved['fd-ledger.bin']);ledger[(64 if field=='fd'else 320)+fd]=value;saved['fd-ledger.bin']=pack(ledger);h=fnv(pack(ledger));rows=bytearray(saved['attempts.bin'])
 for i in range(N):
  row=record(saved,i);row[32]=h;rows[i*512:(i+1)*512]=pack(seal(row))
 saved['attempts.bin']=bytes(rows);control=bytearray(saved['control.bin']);control[512:1024]=rows[-512:];saved['control.bin']=bytes(control)
 st=unpack(saved['driver-status']);st[17]=h;saved['driver-status']=pack(seal(st))
def prefix(saved,kind):
 out=dict(saved);out['attempts.bin']=bytes(N*512);out['evidence.bin']=bytes(N*4096);out['driver-status']=bytes(512)
 control=bytearray(out['control.bin']);control[512:1024]=bytes(512)
 if kind!='empty':
  row=record(saved,0);slot=saved['evidence.bin'][:4096]
  row[8]=1 if kind=='request'else 2;row[20:23]=[UNOBS,0,0];row[41]=0
  if kind=='request':
   row[11:23]=[UNOBS,0,0,0,0,0,0,0,UNOBS,UNOBS,0,0];row[24:27]=[0,0,0];row[30]=0;row[40]=0;slot=slot[:512]+bytes(3584)
  out['attempts.bin']=pack(seal(row))+out['attempts.bin'][512:];out['evidence.bin']=slot+out['evidence.bin'][4096:];control[512:1024]=pack(seal(row))
 out['control.bin']=bytes(control);return out

def synthetic_manifest():
 # UNIT DATA ONLY, kept in memory; never used for packaging/runtime admission.
 # Driver builder always derives its separately written manifest from real ELFs.
 b=bytearray(1024);struct.pack_into('<8Q',b,0,0x43394d414e494645,1,1024,4,0x63594e5448455449,8,24,1000)
 for i,(size,kind,entry,phnum)in enumerate(((4096,3,4096,9),(8192,2,0x401000,4),(8192,2,0x401000,5),(4096,3,4096,7))):
  off=64+128*i;struct.pack_into('<5Q',b,off,i+1,size,kind,entry,phnum);b[off+40:off+72]=hashlib.sha256(('SYNTHETIC CODEC ONLY ROLE '+str(i)).encode()).digest()
 return bytes(b)

def main():
 p=argparse.ArgumentParser();p.add_argument('--decoder',type=Path,default=Path(__file__).resolve().parents[2]/'fixtures/validate_compartment_lifecycle_runtime.py');p.add_argument('--manifest',type=Path);p.add_argument('--result',type=Path);args=p.parse_args()
 started=time.monotonic();manifest_data=args.manifest.read_bytes()if args.manifest else synthetic_manifest();files=[args.decoder,Path(__file__)]+([args.manifest]if args.manifest else[]);before=[pin(f)for f in files]
 m=types.ModuleType('offline_user_wire_codec');m.__file__=str(args.decoder);exec(compile(args.decoder.read_bytes(),str(args.decoder),'exec'),m.__dict__)
 base=gold(manifest_data);cases=[]
 def case(name,data,accept,partial=False):
  began=time.monotonic();error=None;value=None
  try:value=(m.scan_prefix if partial else m.decode)(data);actual=True
  except ValueError as exc:error=str(exc);actual=False
  if actual:
   assert not value['VM_runtime_qualified']and not value['criterion9_qualified']and not value['executor_RETURNED_qualified']
  cases.append({'name':name,'expected_accept':accept,'actual_accept':actual,'pass':actual==accept,'seconds':time.monotonic()-began,'error':error,'qualification':value['qualification']if value else None})
 case('full1032rows_all24errno_1008positive_onlycodec',base,True)
 for kind in ('empty','request','before_wait'):case('prefix_'+kind,prefix(base,kind),True,True)
 x=dict(base);raw=bytearray(x['child.raw']);raw[1024:1280]=bytes([0x5a])*256;x['child.raw']=bytes(raw);case('final_mutable_HB_torn_is_uncredited',x,True)
 for name,fd,field,value in [('FD_unknown_getfd_bits',0,'fd',3),('source_missing_NO_CACHE',0,'fl',0)]:
  x=dict(base);adjust_ledger(x,fd,field,value);case(name,x,False)
 x=dict(base);st=unpack(x['driver-status']);st[16]=START+1;x['driver-status']=pack(seal(st));case('status_end_before_last_completed',x,False)
 for name,index,field in [('main_identity_drift',2,34),('interpreter_identity_drift',3,37)]:
  x=dict(base);r=record(x,index);r[field]+=1;put_record(x,index,r);case(name,x,False)
 for field in (40,41,42,43):
  x=prefix(base,'request');r=record(x,0);r[field]=7;put_record(x,0,r);c=bytearray(x['control.bin']);c[512:1024]=pack(seal(r));x['control.bin']=bytes(c);case('request_unused_field_'+str(field),x,False,True)
 for field,value in [(12,DRIVER),(40,0),(41,7),(42,1),(43,1)]:
  x=prefix(base,'before_wait');r=record(x,0);r[field]=value
  if field==12:r[11]=value
  put_record(x,0,r);c=bytearray(x['control.bin']);c[512:1024]=pack(seal(r));x['control.bin']=bytes(c);case('before_wait_bad_field_'+str(field),x,False,True)
 for name,index,field,value in [('wrong_PID_after_create',0,12,10001),('non_consuming_wait',0,20,0),('wrong_wait_status',0,21,0),('duplicate_positive_PID',2,11,10000),('bad_counter_order',1,40,1),('bad_crossrow_time',1,9,START),('fault_wrong_errno',8,11,MASK),('fault_fake_work',8,14,17),('row_wrong_index',1,4,0),('row_failure',0,31,1),('reserved_word',0,44,1)]:
  x=dict(base);r=record(x,index);r[field]=value
  if name=='duplicate_positive_PID':r[12]=r[20]=value
  put_record(x,index,r);case(name,x,False)
 for name,filename,offset in [('short_manifest','wos-lifecycle60-manifest.bin',0),('control_reserved','control.bin',1024),('final_raw_unused','child.raw',512),('evidence_tail','evidence.bin',3072),('wrong_READY','evidence.bin',512),('torn_row','attempts.bin',504)]:
  x=dict(base)
  if name=='short_manifest':x[filename]=x[filename][:-1]
  else:b=bytearray(x[filename]);b[offset]^=1;x[filename]=bytes(b)
  case(name,x,False)
 x=prefix(base,'request');b=bytearray(x['attempts.bin']);b[1024:1536]=base['attempts.bin'][1024:1536];x['attempts.bin']=bytes(b);case('prefix_hole',x,False,True)
 after=[pin(f)for f in files];assert before==after
 errors=[c['name']for c in cases if not c['pass']]
 result={'qualification':'PASS_CODEC_ONLY'if not errors else'FAIL_CODEC_ASSERTIONS','errors':errors,'cases':cases,'case_count':len(cases),'source_before':before,'source_after':after,'exact_captured_decoder_executed':before[0],'synthetic_total_bytes':sum(map(len,base.values())),'synthetic_fixture_sha256':{n:hashlib.sha256(v).hexdigest()for n,v in base.items()},'synthetic_only':True,'guest_execution':False,'kernel_stage_or_census_qualified':False,'criterion9_qualified':False,'full_goal_complete':False,'seconds':time.monotonic()-started}
 result['manifest_mode']='actual supplied manifest metadata for codec only'if args.manifest else'explicit synthetic codec metadata; not an artifact manifest'
 if args.result:
  with args.result.open('x')as f:json.dump(result,f,indent=2);f.write('\n')
 print(json.dumps({'qualification':result['qualification'],'case_count':len(cases),'unexpected':errors,'manifest_mode':result['manifest_mode'],'seconds':result['seconds']}))
 return 0 if not errors else 1
if __name__=='__main__':raise SystemExit(main())
