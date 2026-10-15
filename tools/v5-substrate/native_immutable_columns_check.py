#!/usr/bin/env python3
"""Cost10 genuine immutable columns and source-scan reference oracle.

This adds a fixed-size lifecycle proof to the complete shared native original,
typed source, binary/cache EOF and process oracle. No graph-sized Python map,
invented normalized value, or physical derived-GC claim is introduced.
"""
import hashlib

import native_create_update_check as common

require,integer,fields,blob,array,decoded,encoded,lines=common.require,common.integer,common.fields,common.blob,common.array,common.decoded,common.encoded,common.lines
codec,column,source,scalar,u64=common.codec,common.column,common.source,common.scalar,common.u64
FAMILY,FORMAT,LINE="immutable-columns","native-immutable-columns-workload-v1",common.LINE
TRANSPORTS,MAX_SECONDS=common.TRANSPORTS,common.MAX_SECONDS
ACCESS="actual complete caller operation through native EOF including retry/wait and callback IO; batched scan-based Points are not indexed latency"
DERIVED="native Retire terminates pin/journal; derived chunk bytes retained and counted; no physical derived/source GC"
SCANS=("old-seal/native","old-seal/fill","old-seal/cached","old-after-update","final-seal/native","final-seal/fill","final-seal/cached","reopened-0-false","reopened-0-true","reopened-1-false","reopened-1-true")


def shape(s):return common.shape(s,FAMILY)
def frames(s):return common.frames(s,FAMILY)


def metadata(record,expected_key):
    fields(record,"Key Value")
    body=blob(record["Value"])
    require(blob(record["Key"])==expected_key and 32<len(body)<=16384 and hashlib.sha256(body[:-32]).digest()==body[-32:],"actual bounded native journal key/seal")
    return decoded(body[:-32])


def points(actual,end,generation,spool):
    require(type(actual) is list and len(actual)==3,"fixed complete column probes")
    n=end["spec"]["facts"];value=5+generation
    for index,(probe,owner) in enumerate(zip(actual,(7,7+2*(n-1),5),strict=True)):
        fields(probe,"Owner Found Presence Property")
        require(probe["Owner"]==owner and type(probe["Found"]) is bool and probe["Found"]==(index<2),"actual probe identity/phantom")
        if index==2:
            empty={name:0 for name in column.ROW if name not in ("Name","Type","Axis","Scope","Before","After")}
            empty.update(Name="",Type="",Axis=None,Scope=None,Before=codec.ZERO,After=codec.ZERO)
            require(probe["Presence"]==probe["Property"]==empty,"phantom is exact empty typed row, not current fallback")
            continue
        ordinal=4+(owner-7)//2+generation*n
        for name,component in (("Presence",1),("Property",3)):
            row=decoded(spool.lookup(b"row",u64(ordinal)+u64(owner)+bytes([component])))
            require(probe[name]==row and row["After"]["Present"] and row["After"]["Provenance"]==ordinal and row["After"]["Handle"]==(owner+1 if component==1 else value),"complete exact original native predecessor/life/scope/provenance bound probe")


def held_refusal(frame,end,spool):
    fields(frame,"Type Member Input Response Nanos")
    member=integer(frame["Member"],1,4);require(member in (1,4),"actual held owner route")
    group=(member-1)//3
    response=codec.response(frame["Response"])
    require(response["code"]==2 and response["cause"]==32,"actual native errLimit and cause mask, not a fabricated refusal label")
    # A genuine source renewal precedes the captured journal and may carry
    # control traffic. Complete raw/source joins account for it independently;
    # no zero-packet inference substitutes for the unchanged native journal.
    observed=decoded(bytes.fromhex(response["aux"]))
    fields(observed,"Cookie Generation Source Binding Before After")
    m=end["ColumnManifests"][0][group];binding=end["ColumnBindings"][0][group]
    require(observed["Generation"]==m["Generation"]["ID"] and observed["Binding"]==binding and any(array(observed["Source"],16)),"held native source/generation/binding")
    cookie=decoded(spool.lookup(b"column-held-cookie",bytes([group])))
    require(observed["Cookie"]==cookie and blob(frame["Input"])==b"NIC1"+encoded({"Cookie":cookie}),"same actual acquired reader, no minted authority")
    before,after=observed["Before"],observed["After"]
    require(type(before) is type(after) is list and len(before)==len(after)==4 and before==after,"real refused Retire leaves all native journal bytes unchanged")
    base=b"generation/"+array(observed["Generation"],16).hex().encode()+b"/"
    progress,manifest,retention,ledger=[metadata(record,key) for record,key in zip(before,(base+b"progress",base+b"manifest",base+b"retention",b"ledger"),strict=True)]
    fields(progress,"Generation Owner Next Frames Rows Bytes Positions Chain Work EOF")
    fields(retention,"Version State Generation Seed Original Token TerminalAttempts Feed"+(" Dependencies" if "Dependencies" in retention else ""))
    fields(ledger,"Bytes Chunks Generations")
    require(manifest==m and progress["Generation"]==m["Generation"] and progress["EOF"] is True and all(progress[p]==m[q] for p,q in (("Next","Chunks"),("Frames","Frames"),("Rows","Rows"),("Bytes","Bytes"),("Positions","Positions"),("Chain","Chain"))),"actual complete publication journal before idle refusal")
    require(retention["Generation"]==m["Generation"] and retention["State"]==3 and retention["Version"]==1 and integer(retention["TerminalAttempts"],0,4)==0,"held Retire cannot debit/terminate native keeper authority")
    require(hashlib.sha256(blob(retention["Original"])).digest()==array(m["Generation"]["Pin"],32),"actual immutable native pin original")
    codec.retention({"Version":retention["Version"],"State":retention["State"],"Generation":retention["Generation"]["ID"],"Pin":retention["Generation"]["Pin"],"Attempts":retention["TerminalAttempts"],"Original":retention["Original"],"Token":retention["Token"]},m,spool)
    require(integer(ledger["Bytes"],1)<=binding["Policy"]["MaxRetainedBytes"] and integer(ledger["Chunks"],1)<=binding["Policy"]["MaxChunks"] and integer(ledger["Generations"],1)<=binding["Policy"]["MaxGenerations"],"actual finite derived ledger, not physical deletion")
    integer(frame["Nanos"],1,end["TotalNanos"])
    spool.insert(b"column-held-refusal",bytes([group]),encoded(frame))


def observations(directory,end,spool):
    summary=end["ImmutableColumns"]
    fields(summary,"Version Access Publications Scans ScanPoints HeldRetireRefusals SHA256 DerivedRetention")
    expected=(2,11,2,2) if end["spec"]["layout"]=="columns" else (0,0,0,0)
    require(integer(summary["Version"],1,1) and summary["Access"]==ACCESS and summary["DerivedRetention"]==DERIVED and tuple(integer(summary[k],0,11) for k in ("Publications","Scans","ScanPoints","HeldRetireRefusals"))==expected,"fresh actual layout/lifecycle contract")
    seal=hashlib.sha256();started=footer=False
    counts={"Publish":0,"Scan":0,"ScanPoints":0,"HeldRetireRefused":0};samples=[]
    for frame in lines(directory/"column-observations.jsonl",19):
        require(not footer,"trailing immutable column observation")
        if frame.get("Type")=="footer":
            fields(frame,"Type End")
            require(started and frame["End"]==summary and seal.digest()==array(summary["SHA256"],32),"complete observation prefix hash/EOF")
            footer=True;continue
        seal.update(encoded(frame)+b"\n")
        if not started:
            require(frame=={"Type":"header","Format":FORMAT,"Spec":end["spec"],"AdmissionSHA256":end["AdmissionSHA256"]},"exact fresh observation admission")
            started=True;continue
        require(end["spec"]["layout"]=="columns","KV source reference cannot invent column operations")
        kind=frame.get("Type");require(kind in counts,"unknown native column operation")
        if kind=="HeldRetireRefused":
            held_refusal(frame,end,spool);counts[kind]+=1;continue
        fields(frame,"Type Stage Access Generation Cut Rows Cached Fill Nanos"+(" Points" if "Points" in frame else ""))
        stage=frame["Stage"];generation=int(stage.startswith(("final-","reopened-1","scan-points-1")))
        require(frame["Access"]==ACCESS and frame["Cut"]==end["Final" if generation else "Old"] and frame["Generation"]==end["ColumnManifests"][generation][0]["Generation"]["ID"],"actual immutable cut/generation, not KV labels")
        n=end["spec"]["facts"]
        require(frame["Rows"]==[3+n+generation*n,n,generation*n] and type(frame["Cached"]) is type(frame["Fill"]) is bool,"actual complete typed lifecycle census including guarded presence revision")
        integer(frame["Nanos"],1,end["TotalNanos"])
        if kind=="Publish":
            require(counts[kind]<2 and stage==("old-seal","final-seal")[counts[kind]] and not frame["Cached"] and not frame["Fill"] and "Points" not in frame,"two complete real publisher operations")
            for group in range(2):spool.lookup(b"column-observation-seal",bytes([generation,group]))
        else:
            if kind=="Scan":
                require(counts[kind]<len(SCANS) and stage==SCANS[counts[kind]] and "Points" not in frame,"exact native/cache/held/reopened scan schedule")
                require(frame["Cached"]==stage.endswith(("/cached","-true")) and frame["Fill"]==stage.endswith("/fill"),"actual native/cache operation mode")
            else:
                require(counts[kind]<2 and stage==f"scan-points-{counts[kind]}" and not frame["Cached"] and not frame["Fill"],"two fixed batch scan-based Points")
                points(frame["Points"],end,generation,spool)
            for group in range(2):spool.lookup(b"column-observation-eof",stage.encode()+bytes([group]))
        spool.insert(b"column-observation",stage.encode(),encoded(frame))
        counts[kind]+=1;samples.append({"call":kind,"stage":stage,"nanos":frame["Nanos"],"rows":frame["Rows"]})
    require(started and footer and tuple(counts[k] for k in ("Publish","Scan","ScanPoints","HeldRetireRefused"))==expected,"partial/missing column operations cannot become successful format")
    if end["spec"]["layout"]=="columns":
        openings={"old-seal","final-seal","old-after-update","scan-points-0","scan-points-1","reopened-0-false","reopened-0-true","reopened-1-false","reopened-1-true","retire-0","retire-1"}
        require({key for key,_ in spool.walk(b"column-observation-open")}=={stage.encode()+bytes([g]) for stage in openings for g in range(2)},"complete exact finite acquired-owner inventory")
    nearest=[]
    for call in ("Publish","Scan","ScanPoints"):
        values=sorted(s["nanos"] for s in samples if s["call"]==call)
        if values:nearest.append({"call":call,"samples":len(values),**{f"p{p}Nanos":values[(p*len(values)+99)//100-1] for p in (50,95,99)}})
    return {"columnTiming":samples,"columnNearestRank":nearest,"derivedRetention":DERIVED,"columnAccess":ACCESS}


def verify(directory,spool_directory,policy,**keywords):
    return common._verify(directory,spool_directory,policy,family=FAMILY,**keywords)
