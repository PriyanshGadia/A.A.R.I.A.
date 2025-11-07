# quick_check.py
import asyncio, sys, os, time
from assistant_core import AssistantCore
from proactive_comm import ProactiveCommunicator, LoggingAdapter

async def run():
    print("Diagnostic: starting AssistantCore")
    ac = AssistantCore(password="test_password_123", storage_path=None, auto_recover=True)
    await ac.initialize()
    print(" -> store object:", hasattr(ac, "store"), type(ac.store))
    # profile
    await ac.save_user_profile({"name":"DiagUser","timezone":"Asia/Kolkata"})
    p = await ac.load_user_profile()
    print(" -> profile loaded:", p)
    # contact
    cid = await ac.add_contact({"name":"DiagContact","email":"diag@example.com"})
    print(" -> added contact id:", cid)
    contacts = await ac.list_contacts()
    print(" -> contacts count:", len(contacts), "sample:", contacts[:1])
    # add event
    now = time.time() + 5
    iso = __import__("datetime").datetime.utcfromtimestamp(now).isoformat()
    try:
        eid = await ac.add_event({"title":"DiagEvent", "datetime": iso})
        print(" -> added event id:", eid)
    except Exception as e:
        print(" -> add_event failed:", e)
    # health
    h = await ac.health_check()
    print(" -> health:", h)
    # Proactive test
    proactive = ProactiveCommunicator(core=ac, hologram_call_fn=None, concurrency=1)
    proactive.register_adapter("logging", LoggingAdapter("diag_sms"))
    await proactive.start()
    eid = await proactive.enqueue_message(subject_identity="owner_primary", channel="logging", payload={"text":"Diagnostic ping"}, priority=1, deliver_after=None)
    print(" -> enqueued proactive:", eid)
    await asyncio.sleep(0.5)
    await proactive.stop()
    # cleanup
    await ac.close()
    print("Diagnostic finished. If all steps printed without exceptions, storage+proactive basics are functioning.")

if __name__ == "__main__":
    asyncio.run(run())